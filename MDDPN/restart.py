#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 23:10:22

import re
import shutil
from pathlib import Path
from typing import Dict, Tuple, Any

from MPMU import wexec

from . import parsers
from . import regexs as rs
from .run import submit_run, run_polling
from . import constants as cs
from .utils import RestartMode, states, logs, RC


def find_last(folder: Path, basename: str) -> int:
    files = []
    for file in folder.iterdir():
        filename = file.parts[-1]
        if re.match(r"^" + basename + r"\.\d+$", filename):
            files.append(int(filename.split('.')[-1]))
    if len(files) < 1:
        return -1
    return max(files)


def restart_cleanup(fl: int) -> None:
    rf: Path = cs.sp.cwd / cs.folders.restarts
    filename = cs.sp.state[cs.sf.restart_files] + '.' + str(fl)
    to_save: Path = rf / filename
    temp_file: Path = cs.sp.cwd / filename
    shutil.copy(to_save, temp_file)
    for file in rf.iterdir():
        file.unlink()
    shutil.copy(temp_file, to_save)
    temp_file.unlink()


@logs
def restart2data(restartfile: Path) -> Path:
    parts = list(restartfile.parts)
    filename = parts[-1]
    file_basename = ".".join(filename.split('.')[:-1])
    datafile_parts = parts + [file_basename + ".dat"]
    datafile = Path(*datafile_parts)
    cs.sp.logger.debug(f"Resulting datafile: {datafile.as_posix()}")
    cmd = f"{cs.execs.lammps} -restart2data {restartfile.as_posix()} {datafile.as_posix()}"
    wexec(cmd, cs.sp.logger.getChild('lammps-r2d'))
    return datafile


@logs
def retrieve_last_step_from_restart(restartfile: Path) -> int:
    datafile = restart2data(restartfile)
    with datafile.open('r') as f:
        line = f.readline()
    if re.match(rs.datafile_header, line):
        m = re.findall(r"timestep = \d+", line)
        if len(m) == 1:
            return int(m[0].split()[-1])
        else:
            cs.sp.logger.error(f"Can not get last timestep from datafile header: {datafile.as_posix()}")
            raise RuntimeError(f"Can not get last timestep from datafile header: {datafile.as_posix()}")
    else:
        cs.sp.logger.error(f"Resulting datafile does not contain proper header: {datafile.as_posix()}")
        raise RuntimeError(f"Resulting datafile does not contain proper header: {datafile.as_posix()}")


@logs
def retrieve_last_timestep() -> Tuple[int, Path]:
    if RestartMode(cs.sp.state[cs.sf.restart_mode]) == RestartMode.multiple:
        if cs.sp.args.step is None:
            last_timestep: int = find_last(cs.sp.cwd / cs.folders.restarts, cs.sp.state[cs.sf.restart_files])
            if last_timestep < 0:
                cs.sp.logger.critical(f"Cannot find any restart files in folder {(cs.sp.cwd / cs.folders.restarts).as_posix()}")
                raise RuntimeError(f"Cannot find any restart files in folder {(cs.sp.cwd / cs.folders.restarts).as_posix()}")
            cs.sp.logger.info("Cleaning restarts folder")
            restart_cleanup(last_timestep)
        else:
            last_timestep = cs.sp.args.step
        restart_file: Path = cs.sp.cwd / cs.folders.restarts / (cs.sp.state[cs.sf.restart_files] + f".{last_timestep}")
        if not restart_file.exists():
            cs.sp.logger.critical("Specified step restart file not found")
        raise RuntimeError("Specified step restart file not found")
    elif RestartMode(cs.sp.state[cs.sf.restart_mode]) == RestartMode.one:
        restart_file = cs.sp.cwd / cs.folders.restarts / cs.sp.state[cs.sf.restart_files]
        last_timestep = retrieve_last_step_from_restart(restart_file)
    elif RestartMode(cs.sp.state[cs.sf.restart_mode]) == RestartMode.two:
        restart_file1: Path = cs.sp.cwd / cs.folders.restarts / (cs.sp.state[cs.sf.restart_files] + '.a')
        restart_file2: Path = cs.sp.cwd / cs.folders.restarts / (cs.sp.state[cs.sf.restart_files] + '.b')
        last_timestep1 = retrieve_last_step_from_restart(restart_file1)
        last_timestep2 = retrieve_last_step_from_restart(restart_file2)
        if last_timestep1 > last_timestep2:
            last_timestep = last_timestep1
            restart_file = restart_file1
            restart_file2.unlink()
        else:
            last_timestep = last_timestep2
            restart_file = restart_file2
            restart_file1.unlink()
    else:
        cs.sp.logger.critical("Software bug")
        raise RuntimeError("Software bug")

    return (last_timestep, restart_file)


@logs
def retrieve_current_label(last_timestep: int) -> str:
    rlabels: Dict[str, Any] = cs.sp.state[cs.sf.run_labels]
    for label in rlabels.keys():
        if last_timestep < rlabels[label][cs.sf.begin_step]:
            continue

        if rlabels[label][cs.sf.end_step] is not None:
            if last_timestep < rlabels[label][cs.sf.end_step] - 1: return label
            else: continue
        else:
            cs.sp.logger.debug(f"End step of label '{label}' is undefined")
            nts = (cs.sp.cwd / cs.folders.signals / f"{label}.signal")
            if not nts.exists():
                cs.sp.logger.debug("Signal file for label does not exists, assuming label was not continued")
                return label
            else:
                cs.sp.logger.debug("Found signal file for label, assuming label was continued")
                with nts.open('r') as fp: rrt = fp.readline().split("#")[0].strip()

                if re.match(r"\d+", rrt):
                    lls = int(rrt)
                    if lls < int(cs.sp.state[cs.sf.restart_every]):
                        cs.sp.logger.debug(f"Signal step is less than 1000 and is {lls} — so small, continue with current label")
                        nts.unlink()
                        return label
                    cs.sp.state[cs.sf.run_labels][label][cs.sf.end_step] = lls
                    flg = False
                    for ml_label in rlabels.keys():
                        if not flg:
                            flg = ml_label == label
                        else:
                            cs.sp.state[cs.sf.run_labels][ml_label][cs.sf.begin_step] += lls
                            if cs.sp.state[cs.sf.run_labels][ml_label][cs.sf.end_step] is not None:
                                cs.sp.state[cs.sf.run_labels][ml_label][cs.sf.end_step] += lls
                else:
                    cs.sp.logger.error("Signal file does not contain readable timestep")
                    return label

    raise RuntimeError("Inconsistent state or software bug.")


@logs
def set_last_timestep(last_timestep: int, current_label: str):
    fl = False
    for label_c in reversed(cs.sp.state[cs.sf.labels_list]):
        if fl:
            if '0' in cs.sp.state[cs.sf.run_labels][label_c] and cs.sp.state[cs.sf.run_labels][label_c][cs.sf.runs] > 0:
                ml = cs.sp.state[cs.sf.run_labels][label_c][cs.sf.runs] - 1
                cs.sp.state[cs.sf.run_labels][label_c][str(ml)][cs.sf.last_step] = last_timestep
                cs.sp.logger.debug(f"On label '{label_c}' there was {cs.sp.state[cs.sf.run_labels][label_c][cs.sf.runs]} runs")
                cs.sp.logger.debug(f"Setting last '{last_timestep}' as last step for run: {ml}")
                break
        elif label_c == current_label:
            fl = True


@logs
def restart() -> RC:
    cstate = states(cs.sp.state[cs.sf.state])
    if cstate != states.started and cstate != states.restarted and cstate != states.fully_initialized:
        cs.sp.logger.error("Folder isn't in appropriate state")
        raise RuntimeError("Folder isn't in appropriate state")
    lockfile = cs.sp.cwd / f"{cs.sp.state[cs.sf.tag]}.lock"
    if lockfile.exists():
        cs.sp.logger.error(f"Lockfile exists: {lockfile.as_posix()}")
        raise Exception(f"Lockfile exists: {lockfile.as_posix()}")

    if cstate == states.fully_initialized:
        current_label = "START"
        restart_file = None
        cs.sp.state[cs.sf.state] = states.started
    else:
        last_timestep, restart_file = retrieve_last_timestep()
        cs.sp.logger.info(f"Last step: {last_timestep}")
        if cstate == states.started:
            cs.sp.state[cs.sf.restart] = 1
            cs.sp.state[cs.sf.state] = states.restarted
            cs.sp.state[cs.sf.restarts] = {}
        elif cstate == states.restarted:
            rest_cnt = int(cs.sp.state[cs.sf.restart])
            cs.sp.state[cs.sf.restart] = rest_cnt + 1

        current_label = retrieve_current_label(last_timestep)
        cs.sp.logger.info(f"Current label: '{current_label}'")

        set_last_timestep(last_timestep, current_label)

        sldk = [cs.sp.state[cs.sf.run_labels][lbl][cs.sf.end_step] for lbl in cs.sp.state[cs.sf.run_labels]]
        if None not in sldk:
            if last_timestep >= max(sldk) - int(cs.sp.state[cs.sf.restart_every]):
                cs.sp.state[cs.sf.state] = states.comleted
                cs.sp.logger.info("End was reached, exiting...")
                return RC.END_REACHED

    cs.sp.logger.info("Generating restart file")
    num = int(cs.sp.state[cs.sf.run_labels][current_label][cs.sf.runs])
    in_file = parsers.generator(num, current_label, restart_file)

    if not cs.sp.args.test:
        cs.sp.logger.info("Submitting task")
        cs.sp.state[cs.sf.run_counter] += 1
        sb_jobid = submit_run(in_file, cs.sp.state[cs.sf.run_counter])

        cs.sp.state[cs.sf.run_labels][current_label][f"{num}"] = {
            cs.sf.jobid: sb_jobid,
            cs.sf.in_file: str(in_file.parts[-1]),
            cs.sf.dump_file: current_label + str(num),
            cs.sf.run_no: cs.sp.state[cs.sf.run_counter]
        }
        cs.sp.state[cs.sf.run_labels][current_label][cs.sf.runs] += 1

        if not cs.sp.args.no_auto:
            cs.sp.logger.info("Staring polling")
            run_polling(sb_jobid, cs.sp.state[cs.sf.tag])
        else:
            cs.sp.logger.info("Not starting polling")
    else:
        cs.sp.logger.info("This is a test, not submitting task")

    return RC.OK


if __name__ == "__main__":
    pass
