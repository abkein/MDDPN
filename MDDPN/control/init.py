#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 15-04-2023 14:30:54

import re
import json
import time
import argparse
from typing import Dict
from pathlib import Path

from .utils import states
from . import regexs as rs
from . import constants as cs
from .. import uw_constants as ucs

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
            if re.match(rs.required_variable_equal_numeric("SEED_I"), line):
                VAR_VAL = round(time.time())
                state[cs.Fuser_variables]["SEED_I"] = VAR_VAL
                variables["SEED_I"] = VAR_VAL
                variables["v_SEED_I"] = VAR_VAL
            elif re.match(rs.required_variable_equal_numeric("SEED_II"), line):
                VAR_VAL = round(time.time())
                state[cs.Fuser_variables]["SEED_II"] = VAR_VAL
                variables["SEED_II"] = VAR_VAL
                variables["v_SEED_II"] = VAR_VAL
            elif re.match(rs.required_variable_equal_numeric("SEED_III"), line):
                VAR_VAL = round(time.time())
                state[cs.Fuser_variables]["SEED_III"] = VAR_VAL
                variables["SEED_III"] = VAR_VAL
                variables["v_SEED_III"] = VAR_VAL
            elif re.match(rs.variable_equal_numeric, line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = eval(VAR_VAL)
                variables[VAR_NAME] = VAR_VAL
                variables["v_" + VAR_NAME] = VAR_VAL
            elif re.match(rs.variable_equal_formula, line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = VAR_VAL[2:-1]
                VAR_VAL = eval(VAR_VAL, globals(), variables)
                variables[VAR_NAME] = VAR_VAL
                variables["v_" + VAR_NAME] = VAR_VAL
            elif re.match(rs.set_timestep, line):
                w_timestep, TIME_STEP = line.split()
                TIME_STEP = eval(TIME_STEP)
                variables['dt'] = TIME_STEP
                state['time_step'] = TIME_STEP
            elif re.match(rs.run_numeric, line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval(RUN_STEPS)
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]
                runc += 1
            elif re.match(rs.run_formula, line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval("variables['" + RUN_STEPS[2:-1] + "']")
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]
                runc += 1
            elif re.match(rs.label_declaration, line):
                label = line.split()[-1]
                labels[label] = []
            elif re.match(rs.set_restart, line):
                state[cs.Frestart_files] = line.split()[-1][:-1]
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


def gen_in(cwd: Path, state: Dict) -> Path:
    variables = state[cs.Fuser_variables]
    out_in_file = cwd / cs.in_file_dir / "START0.in"
    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    else:
        out_in_file.touch()
    with cs.start_template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            if re.match(rs.variable_equal_numeric, line):
                for var, value in variables.items():
                    if re.match(rs.required_variable_equal_numeric(var), line):
                        line = f"variable {var} equal {value}\n"
            elif re.match(rs.set_dump, line):
                before = line.split()[:-1] + [f"{cs.dumps_folder}/START0", "\n"]
                line = " ".join(before)
            elif re.match(rs.set_restart, line):
                line_list = line.split()[:-1] + [f"{cs.restarts_folder}/{state[cs.Frestart_files]}*", "\n"]
                line = " ".join(line_list)
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
    if (cwd / ucs.data_processing_folder).exists():
        raise FileExistsError(f"Directory {ucs.data_processing_folder} already exists")
    (cwd / ucs.data_processing_folder).mkdir()
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

    state = process_file(cs.start_template_file, state)
    in_file = gen_in(cwd, state)
    state = process_file(in_file, state)
    state[cs.Frun_labels]["START"]["0"][cs.Fin_file] = str(in_file.parts[-1])
    state[cs.Frun_labels]["START"]["0"][cs.Frun_no] = 1
    state[cs.Frun_counter] = 0
    state[cs.Fslurm_directory_field] = str(sldir)
    with (cwd / cs.state_file).open('w') as f:
        json.dump(state, f)
    return 0


if __name__ == "__main__":
    pass
