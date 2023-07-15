#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 15-07-2023 13:43:21

import re
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
from argparse import Namespace as argNamespace

from . import regexs as rs
from . import constants as cs
from .utils import states, LogicError
from .execution import perform_run, run_polling


def run(cwd: Path, state: Dict, args: argNamespace) -> Dict:
    if states(state[cs.state_field]) != states.fully_initialized:
        raise LogicError("Folder isn't properly initialized")

    state[cs.state_field] = states.started

    if not args.test:
        sb_jobid = perform_run(cwd, state[cs.Frun_labels]['START']['0'][cs.Fin_file], state)
        state[cs.Frun_labels]['START']["0"][cs.Fjobid] = sb_jobid
        state[cs.Frun_labels]['START'][cs.Fruns] = 1

        if not args.no_auto:
            run_polling(cwd, args, sb_jobid)

    return state


def find_last(cwd: Path) -> int:
    rf = cwd / cs.restarts_folder
    files = []
    for file in rf.iterdir():
        try:
            files += [int(file.parts[-1].split('.')[-1])]
        except Exception:
            pass
    return max(files)


def gen_restart(cwd: Path, label: str, last_file: int, state: Dict) -> Tuple[Path, str]:
    variables = state[cs.Fuser_variables]
    template_file = cs.in_templates_dir / (label + ".template")
    out_in_file = cwd / cs.in_file_dir / (label + str(state[cs.Frun_labels][label][cs.Fruns]) + ".in")
    if out_in_file.exists():
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
                        line = f"variable preparingSteps equal {state[cs.Frun_labels][label]['begin_step']}\n"
            elif re.match(rs.read_restart_specify(state[cs.Frestart_files]), line):
                line = f"read_restart {cs.restarts_folder}/{state[cs.Frestart_files]}{last_file}\n"
            elif re.match(rs.set_dump, line):
                before: List[str] = line.split()[:-1]
                fff = f"{label}{state[cs.Frun_labels][label][cs.Fruns]}"
                before += [f"{cs.dumps_folder}/{fff}", "\n"]
                line = " ".join(before)
            elif re.match(rs.set_restart, line):
                line_list = line.split()[:-1] + [f"{cs.restarts_folder}/{state[cs.Frestart_files]}*", "\n"]
                line = " ".join(line_list)
            fout.write(line)
    del variables['lastStep']
    return out_in_file, fff


def max_step(state: Dict) -> int:
    fff = []
    for label in state[cs.Frun_labels]:
        fff += [state[cs.Frun_labels][label]["end_step"]]
    return max(fff)


def restart_cleanup(cwd: Path, state: Dict, fl: int):
    rf: Path = cwd / cs.restarts_folder
    filename = state[cs.Frestart_files] + str(fl)
    to_save: Path = rf / filename
    temp_file: Path = cwd / filename
    shutil.copy(to_save, temp_file)
    for file in rf.iterdir():
        file.unlink()
    shutil.copy(temp_file, to_save)
    temp_file.unlink()


def restart(cwd: Path, state: Dict, args: argNamespace) -> Dict:
    if states(state[cs.state_field]) != states.started and states(state[cs.state_field]) != states.restarted:
        raise LogicError("Folder isn't properly initialized")
    if args.step is None:
        last_file = find_last(cwd)
    else:
        last_file = args.step
    restart_cleanup(cwd, state, last_file)
    if states(state[cs.state_field]) == states.started:
        state[cs.restart_field] = 1
        state[cs.state_field] = states.restarted
        state["restarts"] = {}
    elif states(state[cs.state_field]) == states.restarted:
        rest_cnt = int(state[cs.restart_field])
        rest_cnt += 1
        state[cs.restart_field] = rest_cnt
    current_label = ""
    rlabels = state[cs.Frun_labels]
    for label in rlabels:
        if last_file > rlabels[label]["begin_step"]:
            if last_file < rlabels[label]["end_step"] - 1:
                current_label = label
                break
    fl = False
    for label_c in reversed(state[cs.Flabels_list]):
        if fl:
            if '0' in rlabels[label_c]:
                state[cs.Frun_labels][label_c][f"{rlabels[label_c][cs.Fruns]}"]["last_step"] = last_file
                break
        elif label_c == current_label:
            fl = True

    if last_file >= max_step(state) - 1:
        state["state"] = states.comleted
        return state

    out_file, dump_file = gen_restart(cwd, current_label, last_file, state)

    if not args.test:
        sb_jobid = perform_run(cwd, out_file, state)

        state[cs.Frun_labels][current_label][f"{state[cs.Frun_labels][current_label][cs.Fruns]}"] = {
            cs.Fjobid: sb_jobid, "last_step": last_file, cs.Fin_file: str(out_file.parts[-1]), "dump.f": str(dump_file), "run_no": state["run_counter"]}
        state[cs.Frun_labels][current_label][cs.Fruns] += 1

        if not args.no_auto:
            run_polling(cwd, args, sb_jobid)

    return state


if __name__ == "__main__":
    pass
