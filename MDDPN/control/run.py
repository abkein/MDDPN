#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 20:35:48

import re
import json

from .utils import states, LogicError
from .execution import perform_run, run_polling
from .constants import state_file, state_field, restarts_folder, in_templates_dir, restart_field


def run(cwd, args):
    if not (cwd / state_file).exists():
        raise FileNotFoundError("State file 'state.json' not found")
    with (cwd / state_file).open('r') as f:
        state = json.load(f)
    if states(state[state_field]) != states.fully_initialized:
        raise LogicError("Folder isn't properly initialized")
    sb_jobid = perform_run(
        cwd, state["run_labels"]['START']['0']['in.file'], state)
    state[state_field] = states.started
    state["run_labels"]['START']["0"]["sb_jobid"] = sb_jobid
    (cwd / state_file).unlink()
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)
    if not args.no_auto:
        run_polling(cwd, args, sb_jobid)


def find_last(cwd):
    rf = cwd / restarts_folder
    files = []
    for file in rf.iterdir():
        try:
            files += [int(file.parts[-1].split('.')[-1])]
        except Exception:
            pass
    return max(files)


def gen_restart(cwd, label, last_file, state):
    variables = state['user_variables']
    template_file = in_templates_dir / \
        ("in." + label + ".template")
    out_in_file = cwd / \
        ("in." + label + str(state['run_labels'][label]['runs']) + ".lm")
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
                    line = f"variable preparingSteps equal {state['run_labels'][label]['begin_step']}\n"
            if re.match(r"read_restart[ \t]+" + restarts_folder + r"\/" + state['restart_files'] + r"\d+", line):
                line = f"read_restart {restarts_folder}/{state['restart_files']}{last_file}\n"
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                before = line.split()[:-1]
                lkl = ""
                for el in before:
                    lkl += el + " "
                before = lkl
                dump_file = state['dump_file'].split('.')
                if len(dump_file) == 2:
                    # print("Only 2")
                    dump_file = dump_file[:-1] + \
                        [f"{label}{state['run_labels'][label]['runs']}"] + \
                        [dump_file[-1]]
                else:
                    # print("More than 2")
                    dump_file = dump_file[:-1] + \
                        [f"{label}{state['run_labels'][label]['runs']}"] + \
                        [dump_file[-1]]
                for el in dump_file:
                    fff += el + "."
                fff = fff[:-1]
                line = before + fff
                line += "\n"
            fout.write(line)
    del variables['lastStep']
    return out_in_file, fff


# def gen_restarts(cwd, nums):
#     from random import randrange
#     files = []
#     for i, label in enumerate(nums):
#         files += ["nucl.restart." + str(randrange(
#             0 if i == 0 else nums[list(nums.keys())[i - 1]], nums[label])) for _ in range(3)]
#     print(files)
#     for file in files:
#         (cwd / restarts_folder / file).touch()

def max_step(state):
    fff = []
    for label in state["run_labels"]:
        fff += [state["run_labels"][label]["end_step"]]
    return max(fff)


# def g_labels(state):


def restart(cwd, args):
    if not (cwd / state_file).exists():
        raise FileNotFoundError("State file 'state.json' not found")
    with (cwd / state_file).open('r') as f:
        state = json.load(f)
    if states(state[state_field]) != states.started and states(state[state_field]) != states.restarted:
        raise LogicError("Folder isn't properly initialized")
    if args.step is None:
        last_file = find_last(cwd)
    else:
        last_file = args.step
    if last_file >= max_step(state) - 1:
        state["state"] = states.comleted
        return 0
    if states(state[state_field]) == states.started:
        state[restart_field] = 1
        state[state_field] = states.restarted
        state["restarts"] = {}
    elif states(state[state_field]) == states.restarted:
        rest_cnt = int(state[restart_field])
        rest_cnt += 1
        state[restart_field] = rest_cnt
    # gen_restarts(cwd, state['ps']['run_labels'])
    current_label = ""
    rlabels = state['run_labels']
    for label in rlabels:
        if last_file > rlabels[label]["begin_step"]:
            if last_file < rlabels[label]["end_step"] - 1:
                current_label = label
                break
    out_file, dump_file = gen_restart(cwd, current_label, last_file, state)
    sb_jobid = perform_run(cwd, out_file, state)
    fl = False
    for label_c in reversed(state["labels"]):
        if fl:
            # print(current_label, label_c, str(
            #     int(state["run_labels"][label_c]["runs"])))
            if '0' in state["run_labels"][label_c]:
                state["run_labels"][label_c][str(int(state["run_labels"][label_c]["runs"]))]["last_step"] = last_file
                break
        elif label_c == current_label:
            fl = True
    state["run_labels"][current_label][f"{state['run_labels'][label]['runs']}"] = {
        "sb_jobid": sb_jobid, "last_step": last_file, "in.file": str(out_file.parts[-1]), "dump.f": str(dump_file), "run_no": state["run_counter"]}
    state["run_labels"][label]['runs'] += 1
    # state["current_step"] = last_file

    (cwd / state_file).unlink()
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)

    if not args.no_auto:
        run_polling(cwd, args, sb_jobid)
