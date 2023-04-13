#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 23:26:39

import re
from typing import List
from pathlib import Path
from typing import Dict
from argparse import Namespace as argNamespace

from .utils import states, LogicError
from .execution import perform_run, run_polling
from . import constants as cs


def run(cwd: Path, state: Dict, args: argNamespace):
    if states(state[cs.state_field]) != states.fully_initialized:
        raise LogicError("Folder isn't properly initialized")

    sb_jobid = perform_run(cwd, state[cs.Frun_labels]['START']['0'][cs.Fin_file], state)
    state[cs.state_field] = states.started
    state[cs.Frun_labels]['START']["0"][cs.Fjobid] = sb_jobid

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


def gen_restart(cwd: Path, label: str, last_file: int, state: Dict) -> tuple[Path, str]:
    variables = state[cs.Fuser_variables]
    template_file = cs.in_templates_dir / (label + ".template")
    out_in_file = cwd / cs.in_file_dir / (label + str(state[cs.Frun_labels][label][cs.Fruns]) + ".in")
    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        fff = ""
        for line in fin:
            variables['lastStep'] = last_file
            for var in list(variables.keys()):
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {variables[var]}\n"
                if re.match(r"^variable[ \t]+preparingSteps[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable preparingSteps equal {state[cs.Frun_labels][label]['begin_step']}\n"
            if re.match(r"read_restart[ \t]+" + state[cs.Frestart_files] + r"\.\d+", line):
                line = f"read_restart {cs.restarts_folder}/{state[cs.Frestart_files]}.{last_file}\n"
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                before: List[str] = line.split()[:-1]
                # lkl = ""
                # for el in before:
                #     lkl += el + " "
                # before = lkl
                # dump_file = state['dump_file'].split('.')

                # dump_file = dump_file[:-1] + [f"{label}{state['run_labels'][label]['runs']}"] + [dump_file[-1]]
                # for el in dump_file:
                #     fff += el + "."
                # fff = fff[:-1]
                # line = before + fff
                # line += "\n"
                fff = f"{label}{state[cs.Frun_labels][label][cs.Fruns]}"
                line = before + [f"{cs.dumps_folder}/{fff}", "\n"]
                line = " ".join(line)
            fout.write(line)
    del variables['lastStep']
    return out_in_file, fff


def max_step(state: Dict) -> int:
    fff = []
    for label in state[cs.Frun_labels]:
        fff += [state[cs.Frun_labels][label]["end_step"]]
    return max(fff)


def restart(cwd: Path, state: Dict, args: argNamespace):
    if states(state[cs.state_field]) != states.started and states(state[cs.state_field]) != states.restarted:
        raise LogicError("Folder isn't properly initialized")
    if args.step is None:
        last_file = find_last(cwd)
    else:
        last_file = args.step
    if last_file >= max_step(state) - 1:
        state["state"] = states.comleted
        return state
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
    out_file, dump_file = gen_restart(cwd, current_label, last_file, state)
    sb_jobid = perform_run(cwd, out_file, state)
    fl = False
    for label_c in reversed(state[cs.Flabels_list]):
        if fl:
            if '0' in rlabels[label_c]:
                state[cs.Frun_labels][label_c][rlabels[label_c][cs.Fruns]]["last_step"] = last_file
                break
        elif label_c == current_label:
            fl = True
    state[cs.Frun_labels][current_label][f"{state[cs.Frun_labels][current_label][cs.Fruns]}"] = {
        cs.Fjobid: sb_jobid, "last_step": last_file, cs.Fin_file: str(out_file.parts[-1]), "dump.f": str(dump_file), "run_no": state["run_counter"]}
    state[cs.Frun_labels][current_label][cs.Fruns] += 1

    if not args.no_auto:
        run_polling(cwd, args, sb_jobid)

    return state
