#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 23:18:59

import argparse
import json
import re
from pathlib import Path
from typing import Dict

from .utils import states
from . import constants as cs

# TODO:
# gen_in not properly processes folders


def process_file(file: Path, state: Dict) -> Dict:
    labels = {"START": [0]}
    runs = {}
    variables = {}
    runc = 0
    label = "START"
    with file.open('r') as fin:
        for line in fin:
            if re.match(r"^variable[ \t]+[a-zA-Z]+[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = eval(VAR_VAL)
                variables[VAR_NAME] = VAR_VAL
                variables["v_" + VAR_NAME] = VAR_VAL
            if re.match(r"^variable[ \t]+[a-zA-Z]+[ \t]+equal[ \t]+\$\(.+\)", line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = VAR_VAL[2:-1]
                VAR_VAL = eval(VAR_VAL, globals(), variables)
                variables[VAR_NAME] = VAR_VAL
                variables["v_" + VAR_NAME] = VAR_VAL
            if re.match(r"^timestep[ \t]+[\d]+[\.\/]?\d+", line):
                w_timestep, TIME_STEP = line.split()
                TIME_STEP = eval(TIME_STEP)
                variables['dt'] = TIME_STEP
                state['time_step'] = TIME_STEP
            if re.match(r"^run[ \t]+\d+[.\/]?\d+", line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval(RUN_STEPS)
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]
                runc += 1
            if re.match(r"^run[ \t]+\${[a-zA-Z]+}", line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval("variables['" + RUN_STEPS[2:-1] + "']")
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]
                runc += 1
            if re.match(r"#[ \t]+label:[ \t][a-zA-Z]+", line):
                label = line.split()[-1]
                labels[label] = []
            if re.match(r"restart[ \t]+(\$\{[a-zA-Z]+\}|[\d]+)[ \t]+[a-zA-Z]+\.\*", line):
                # "rest.nucl.*" into "rest.nucl."
                state[cs.Frestart_files] = line.split()[-1][:-1]
            # if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                # state["dump_file"] = line.split()[-1]
    vt = 0
    labels_list = list(labels.keys())
    for label in labels:
        labels[label] = {"begin_step": vt, "end_step": sum(labels[label]) + vt, cs.Fruns: 0}  # type: ignore
        vt = labels[label]["end_step"]  # type: ignore
    labels["START"]["0"] = {cs.Fdump_file: "START0"}  # type: ignore
    # state['runc'] = runc
    state[cs.Frun_labels] = labels
    state[cs.Flabels_list] = labels_list
    state[cs.Fvariables] = variables
    state[cs.Fruns] = runs
    return state


def gen_in(cwd: Path, state: Dict, variables: Dict[str, float]) -> Path:
    out_in_file = cwd / cs.in_file_dir / "START0.in"
    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    else:
        out_in_file.touch()
    with cs.start_template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            for var, value in variables.items():
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {value}\n"
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                before = line.split()[:-1]
                # lkl = ""
                # for el in before:
                #     lkl += el + " "
                # before = lkl
                # dump_file = state['dump_file'].split('.')
                # dump_file = dump_file[:-1] + ["START0"] + [dump_file[-1]]
                # fff = ""
                # for el in dump_file:
                #     fff += el + "."
                # fff = fff[:-1]
                # line = before + fff
                line = before + [f"{cs.dumps_folder}/START0", "\n"]
                line = " ".join(line)
            fout.write(line)
    return out_in_file


def check_required_fs(cwd: Path):
    if (cwd / cs.state_file).exists():
        raise FileExistsError(f"File {cs.state_file} already exists")
    (cwd / cs.state_file).touch()
    if (cwd / cs.restarts_folder).exists():
        raise FileExistsError(f"Directory {cs.restarts_folder} already exists")
    (cwd / cs.restarts_folder).mkdir()
    if (cwd / cs.in_file_dir).exists():
        raise FileExistsError(f"Directory {cs.in_file_dir} already exists")
    (cwd / cs.in_file_dir).mkdir()
    if (cwd / cs.dumps_folder).exists():
        raise FileExistsError(f"Directory {cs.dumps_folder} already exists")
    (cwd / cs.dumps_folder).mkdir()
    if (cwd / cs.sl_dir).exists():
        raise FileExistsError(f"Directory {cs.sl_dir} already exists")
    (cwd / cs.sl_dir).mkdir()
    return True


def init(cwd: Path, args: argparse.Namespace):
    check_required_fs(cwd)
    sldir = cwd / cs.sl_dir

    state = {cs.state_field: states.fully_initialized}
    if args.file:
        pfile = (cwd / cs.params_file) if args.fname is None else args.fname
        with pfile.open('r') as f:
            variables = json.load(f)
    else:
        variables = json.loads(args.params)
    state[cs.Fuser_variables] = variables

    tstate = process_file(cs.start_template_file, {})
    in_file = gen_in(cwd, tstate, variables)
    state = process_file(in_file, state)
    state[cs.Frun_labels]["START"]["0"][cs.Fin_file] = str(in_file.parts[-1])
    state[cs.Frun_labels]["START"]["0"][cs.Frun_no] = 1
    state[cs.Frun_counter] = 0
    state[cs.Fslurm_directory_field] = str(sldir)
    with (cwd / cs.state_file).open('w') as f:
        json.dump(state, f)
    return 0
