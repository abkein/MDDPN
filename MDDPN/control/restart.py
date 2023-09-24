#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 20-09-2023 09:05:20

import re
import shutil
import logging
from typing import Dict
from pathlib import Path
from argparse import Namespace as argNamespace

from . import parsers
from ..utils import wexec
from . import regexs as rs
from .run import submit_run
from . import constants as cs
from .execution import run_polling
from .utils import RestartMode, states, LogicError


def find_last(folder: Path, basename: str) -> int:
    files = []
    for file in folder.iterdir():
        filename = file.parts[-1]
        if re.match(r"^" + basename + r"\.\d+$", filename):
            files.append(int(filename.split('.')[-1]))
    if len(files) < 1:
        return -1
    return max(files)


def max_step(state: Dict) -> int:
    fff = []
    for label in state[cs.sf.run_labels]:
        fff += [state[cs.sf.run_labels][label][cs.sf.end_step]]
    return max(fff)


def restart_cleanup(cwd: Path, state: Dict, fl: int) -> None:
    rf: Path = cwd / cs.folders.restarts
    filename = state[cs.sf.restart_files] + '.' + str(fl)
    to_save: Path = rf / filename
    temp_file: Path = cwd / filename
    shutil.copy(to_save, temp_file)
    for file in rf.iterdir():
        file.unlink()
    shutil.copy(temp_file, to_save)
    temp_file.unlink()


def restart2data(restartfile: Path, logger: logging.Logger) -> Path:
    parts = list(restartfile.parts)
    filename = parts[-1]
    file_basename = ".".join(filename.split('.')[:-1])
    datafile_parts = parts + [file_basename + ".dat"]
    datafile = Path(*datafile_parts)
    logger.debug(f"Resulting datafile: {datafile.as_posix()}")
    cmd = f"{cs.execs.lammps} -restart2data {restartfile.as_posix()} {datafile.as_posix()}"
    wexec(cmd, logger.getChild('lammps-r2d'))
    return datafile


def lsfr(restartfile: Path, logger: logging.Logger) -> int:
    datafile = restart2data(restartfile, logger.getChild('restart2data'))
    with datafile.open('r') as f:
        line = f.readline()
    if re.match(rs.datafile_header, line):
        m = re.findall(r"timestep = \d+", line)
        if len(m) == 1:
            return int(m[0].split()[-1])
        else:
            logger.error(f"Can not get last timestep from datafile header: {datafile.as_posix()}")
            raise RuntimeError(f"Can not get last timestep from datafile header: {datafile.as_posix()}")
    else:
        logger.error(f"Resulting datafile does not contain proper header: {datafile.as_posix()}")
        raise RuntimeError(f"Resulting datafile does not contain proper header: {datafile.as_posix()}")


def restart(cwd: Path, state: Dict, args: argNamespace, logger: logging.Logger) -> Dict:
    if states(state[cs.sf.state]) != states.started and states(state[cs.sf.state]) != states.restarted:
        logger.critical("Folder isn't properly initialized")
        raise LogicError("Folder isn't properly initialized")
    lockfile = cwd / cs.files.restart_lock
    if lockfile.exists():
        logger.error(f"Lockfile exists: {lockfile.as_posix()}")
        raise Exception(f"Lockfile exists: {lockfile.as_posix()}")
    if RestartMode(state[cs.sf.restart_mode]) == RestartMode.multiple:
        if args.step is None:
            last_timestep = find_last(cwd / cs.folders.restarts, state[cs.sf.restart_files])
            if last_timestep < 0:
                logger.critical(f"Cannot find any restart files in folder {(cwd / cs.folders.restarts).as_posix()}")
                raise RuntimeError(f"Cannot find any restart files in folder {(cwd / cs.folders.restarts).as_posix()}")
            logger.info("Cleaning restarts folder")
            restart_cleanup(cwd, state, last_timestep)
        else:
            last_timestep = args.step
        restart_file: Path = cwd / cs.folders.restarts / (state[cs.sf.restart_files] + f".{last_timestep}")
    elif RestartMode(state[cs.sf.restart_mode]) == RestartMode.one:
        restart_file = cwd / cs.folders.restarts / state[cs.sf.restart_files]
        last_timestep = lsfr(restart_file, logger.getChild('lsfr'))
    elif RestartMode(state[cs.sf.restart_mode]) == RestartMode.two:
        restart_file1: Path = cwd / cs.folders.restarts / (state[cs.sf.restart_files] + '.a')
        restart_file2: Path = cwd / cs.folders.restarts / (state[cs.sf.restart_files] + '.b')
        last_timestep1 = lsfr(restart_file1, logger.getChild('lsfr'))
        last_timestep2 = lsfr(restart_file2, logger.getChild('lsfr'))
        if last_timestep1 > last_timestep2:
            last_timestep = last_timestep1
            restart_file = restart_file1
            restart_file2.unlink()
        else:
            last_timestep = last_timestep2
            restart_file = restart_file2
            restart_file1.unlink()
    else:
        logger.critical("Software bug")
        raise RuntimeError("Software bug")

    logger.info(f"Last step: {last_timestep}")
    if states(state[cs.sf.state]) == states.started:
        state[cs.sf.restart] = 1
        state[cs.sf.state] = states.restarted
        state[cs.sf.restarts] = {}
    elif states(state[cs.sf.state]) == states.restarted:
        rest_cnt = int(state[cs.sf.restart])
        rest_cnt += 1
        state[cs.sf.restart] = rest_cnt
    current_label = ""
    rlabels = state[cs.sf.run_labels]
    for label in rlabels:
        if last_timestep > rlabels[label][cs.sf.begin_step]:
            if last_timestep < rlabels[label][cs.sf.end_step] - 1:
                current_label = label
                break
    logger.info(f"Current label: '{current_label}'")
    fl = False
    for label_c in reversed(state[cs.sf.labels_list]):
        if fl:
            if '0' in rlabels[label_c]:
                ml = rlabels[label_c][cs.sf.runs] - 1
                state[cs.sf.run_labels][label_c][str(ml)][cs.sf.last_step] = last_timestep
                logger.info(f"On label '{label_c}' there was {rlabels[label_c][cs.sf.runs]} runs")
                logger.info(f"Setting last '{last_timestep}' as last step for run: {ml}")
                break
        elif label_c == current_label:
            fl = True

    if last_timestep >= max_step(state) - 1:
        state[cs.sf.state] = states.comleted
        logger.info("End was reached, exiting...")
        return state

    logger.info("Generating restart file")
    num = int(state[cs.sf.run_labels][current_label][cs.sf.runs])
    out_file = parsers.gen_restart(cwd, state, logger.getChild('gen_restart'), num, current_label, restart_file.relative_to(cwd).as_posix())
    dump_file = current_label + str(num)

    if not args.test:
        logger.info("Submitting task")
        sb_jobid = submit_run(cwd, out_file, logger, state[cs.sf.run_counter] + 1)
        state[cs.sf.run_counter] += 1

        state[cs.sf.run_labels][current_label][f"{state[cs.sf.run_labels][current_label][cs.sf.runs]}"] = {
            cs.sf.jobid: sb_jobid, cs.sf.in_file: str(out_file.parts[-1]), cs.sf.dump_file: str(dump_file), "run_no": state["run_counter"]}
        state[cs.sf.run_labels][current_label][cs.sf.runs] += 1

        if not args.no_auto:
            logger.info("Staring polling")
            run_polling(cwd, args, sb_jobid, state[cs.sf.tag], logger.getChild("poll_start"))
        else:
            logger.info("Not starting polling")
    else:
        logger.info("This is a test, not submitting task")

    return state


if __name__ == "__main__":
    pass
