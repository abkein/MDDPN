#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 20:28:25

import json
import re
from pathlib import Path
from typing import Dict

from .constants import state_file, restarts_folder, params_file_def, state_field, in_templates_dir, sl_dir_def
from .utils import states


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
            if re.match(r"restart[ \t]+(\$\{[a-zA-Z]+\}|[\d]+)[ \t]+" + restarts_folder + r"\/[a-zA-Z\.]+\*", line):
                # restarts/rest.nucl.* into rest.nucl.
                state['restart_files'] = line.split()[-1].split('/')[-1][:-1]
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                state["dump_file"] = line.split()[-1]
    vt = 0
    labels_list = list(labels.keys())
    for label in labels:
        labels[label] = {"begin_step": vt, "end_step": sum(labels[label]) + vt, "runs": 0}  # type: ignore
        vt = labels[label]["end_step"]  # type: ignore
    labels["START"]["0"] = {"dump_f": "dump.START0.bp"}  # type: ignore
    # state['runc'] = runc
    state["run_labels"] = labels
    state["labels"] = labels_list
    state['variables'] = variables
    state['runs'] = runs
    return state


def gen_in(cwd: Path, variables: Dict[str, float], state: Dict) -> Path:
    template_file = in_templates_dir / "in.START.template"
    out_in_file = cwd / "in.START0.lm"

    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            for var, value in variables.items():
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {value}\n"
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                before = line.split()[:-1]
                lkl = ""
                for el in before:
                    lkl += el + " "
                before = lkl
                dump_file = state['dump_file'].split('.')
                dump_file = dump_file[:-1] + ["START0"] + [dump_file[-1]]
                fff = ""
                for el in dump_file:
                    fff += el + "."
                fff = fff[:-1]
                line = before + fff
                line += "\n"
            fout.write(line)
    return out_in_file


def init(cwd, args):
    if (cwd / state_file).exists():
        raise FileExistsError("File 'state.json' already exists")
    if (cwd / restarts_folder).exists():
        raise FileExistsError(f"Directory {restarts_folder} already exists")
    (cwd / restarts_folder).mkdir()
    if args.min:
        state = {state_field: states.initialized}
    else:
        state = {state_field: states.fully_initialized}
        params_file = params_file_def
        if args.file:
            pfile = (cwd / params_file) if args.fname is None else args.fname
            with pfile.open('r') as f:
                variables = json.load(f)
        else:
            variables = json.loads(args.params)
        state['user_variables'] = variables
        tstate = process_file(in_templates_dir / "in.START.template", {})
        in_file = gen_in(cwd, variables, tstate)
        state = process_file(in_file, state)
        # in_file.unlink()
        # in_file = gen_in(cwd, variables)
        state["run_labels"]["START"]["0"]['in.file'] = str(in_file.parts[-1])
        state["run_labels"]["START"]["0"]["run_no"] = 1
        state["run_counter"] = 0
        sldir = cwd / sl_dir_def
        state["slurm_directory"] = str(sldir)
        if not sldir.exists():
            sldir.mkdir(parents=True, exist_ok=True)
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)