#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 31-08-2023 22:14:49

import logging
import re
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
from argparse import Namespace as argNamespace

from . import regexs as rs
from .. import constants as cs
from .utils import states, LogicError
from .execution import perform_run, run_polling


def run(cwd: Path, state: Dict, args: argNamespace, logger: logging.Logger) -> Dict:
    if states(state[cs.sf.state]) != states.fully_initialized:
        logger.error("Folder isn't fully initialized")
        raise LogicError("Folder isn't fully initialized")

    state[cs.sf.state] = states.started

    if not args.test:
        logger.info("Submitting task")
        sb_jobid = perform_run(cwd, state[cs.sf.run_labels]['START']['0'][cs.sf.in_file], state, logger.getChild("submitter"))
        logger.info(f"Sbatch jobid: {sb_jobid}")
        state[cs.sf.run_labels]['START']["0"][cs.sf.jobid] = sb_jobid
        state[cs.sf.run_labels]['START'][cs.sf.runs] = 1

        if not args.no_auto:
            logger.info("Staring polling")
            run_polling(cwd, args, sb_jobid, state[cs.sf.tag], logger.getChild("poll_start"))
        else:
            logger.info("Not starting polling")
    else:
        logger.info("This is a test, not submitting task")

    return state


def find_last(cwd: Path) -> int:
    rf = cwd / cs.folders.restarts
    files = []
    for file in rf.iterdir():
        try:
            files += [int(file.parts[-1].split('.')[-1])]
        except Exception:
            pass
    if len(files) < 1:
        return -1
    return max(files)


def gen_restart(cwd: Path, label: str, last_file: int, state: Dict, logger: logging.Logger) -> Tuple[Path, str]:
    variables = state[cs.sf.user_variables]
    template_file = cwd / cs.folders.in_templates / (label + ".template")
    out_in_file = cwd / cs.folders.in_file / (label + str(state[cs.sf.run_labels][label][cs.sf.runs]) + ".in")
    if out_in_file.exists():
        logger.critical(f"Output in. file {out_in_file} already exists")
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        fff = ""
        for line in fin:
            variables['lastStep'] = last_file
            if re.match(rs.variable_equal_numeric, line):
                for var in list(variables.keys()):
                    if re.match(rs.required_variable_equal_numeric(var), line):
                        line = f"variable {var} equal {variables[var]}\n"
                    if re.match(rs.required_variable_equal_numeric("preparingSteps"), line):
                        line = f"variable preparingSteps equal {state[cs.sf.run_labels][label][cs.sf.begin_step]}\n"
            elif re.match(rs.read_restart_specify(state[cs.sf.restart_files]), line):
                line = f"read_restart {cs.folders.restarts}/{state[cs.sf.restart_files]}{last_file}\n"
            elif re.match(rs.set_dump, line):
                before: List[str] = line.split()[:-1]
                fff = f"{label}{state[cs.sf.run_labels][label][cs.sf.runs]}"
                before += [f"{cs.folders.dumps}/{fff}", "\n"]
                line = " ".join(before)
            elif re.match(rs.set_restart_equ, line):
                line_list = line.split()[:-1] + [f"{cs.folders.restarts}/{state[cs.sf.restart_files]}*", "\n"]
                line = " ".join(line_list)
            fout.write(line)
    del variables['lastStep']
    return out_in_file, fff


def max_step(state: Dict) -> int:
    fff = []
    for label in state[cs.sf.run_labels]:
        fff += [state[cs.sf.run_labels][label][cs.sf.end_step]]
    return max(fff)


def restart_cleanup(cwd: Path, state: Dict, fl: int):
    rf: Path = cwd / cs.folders.restarts
    filename = state[cs.sf.restart_files] + str(fl)
    to_save: Path = rf / filename
    temp_file: Path = cwd / filename
    shutil.copy(to_save, temp_file)
    for file in rf.iterdir():
        file.unlink()
    shutil.copy(temp_file, to_save)
    temp_file.unlink()


def restart(cwd: Path, state: Dict, args: argNamespace, logger: logging.Logger) -> Dict:
    if states(state[cs.sf.state]) != states.started and states(state[cs.sf.state]) != states.restarted:
        logger.critical("Folder isn't properly initialized")
        raise LogicError("Folder isn't properly initialized")
    lockfile = cwd / cs.files.restart_lock
    if lockfile.exists():
        logger.error(f"Lockfile exists: {lockfile.as_posix()}")
        raise Exception(f"Lockfile exists: {lockfile.as_posix()}")
    if args.step is None:
        last_file = find_last(cwd)
    else:
        last_file = args.step
    if last_file < 0:
        logger.critical("Cannot find any restart files")
        raise RuntimeError("Cannot find any restart files")
    logger.info(f"Last step: {last_file}")
    logger.info("Cleaning restarts folder")
    restart_cleanup(cwd, state, last_file)
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
        if last_file > rlabels[label][cs.sf.begin_step]:
            if last_file < rlabels[label][cs.sf.end_step] - 1:
                current_label = label
                break
    logger.info(f"Current label: {current_label}")
    fl = False
    for label_c in reversed(state[cs.sf.labels_list]):
        if fl:
            if '0' in rlabels[label_c]:
                ml = rlabels[label_c][cs.sf.runs] - 1
                state[cs.sf.run_labels][label_c][str(ml)][cs.sf.last_step] = last_file
                logger.info(f"On label '{label_c}' there was {rlabels[label_c][cs.sf.runs]} runs")
                logger.info(f"Setting last '{last_file}' as last step for run: {ml}")
                break
        elif label_c == current_label:
            fl = True

    if last_file >= max_step(state) - 1:
        state[cs.sf.state] = states.comleted
        logger.info("End was reached, exiting...")
        return state

    logger.info("Generating restart file")
    out_file, dump_file = gen_restart(cwd, current_label, last_file, state, logger.getChild("gen_restart"))

    if not args.test:
        logger.info("Submitting task")
        sb_jobid = perform_run(cwd, out_file, state, logger.getChild("submitter"))

        state[cs.sf.run_labels][current_label][f"{state[cs.sf.run_labels][current_label][cs.sf.runs]}"] = {
            cs.sf.jobid: sb_jobid, cs.sf.last_step: last_file, cs.sf.in_file: str(out_file.parts[-1]), cs.sf.ddf: str(dump_file), "run_no": state["run_counter"]}
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
