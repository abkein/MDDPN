#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 27-07-2023 12:49:18

import re
import json
import time
import argparse
from pathlib import Path
from typing import List, Dict, Union

from .utils import states
from . import regexs as rs
from .. import constants as cs

# TODO:
# gen_in not properly processes folders


def process_file(file: Path, state: Dict) -> Dict:
    labels: Dict[str, Union[int, List[int]]] = {"START": [0]}
    runs = {}
    variables = {}
    runc = 0
    label = "START"
    with file.open('r') as fin:
        for line in fin:
            if re.match(rs.required_variable_equal_numeric("SEED_I"), line):
                VAR_VAL = round(time.time())
                state[cs.sf.user_variables]["SEED_I"] = VAR_VAL
                variables["SEED_I"] = VAR_VAL
                variables["v_SEED_I"] = VAR_VAL
            elif re.match(rs.required_variable_equal_numeric("SEED_II"), line):
                VAR_VAL = round(time.time())
                state[cs.sf.user_variables]["SEED_II"] = VAR_VAL
                variables["SEED_II"] = VAR_VAL
                variables["v_SEED_II"] = VAR_VAL
            elif re.match(rs.required_variable_equal_numeric("SEED_III"), line):
                VAR_VAL = round(time.time())
                state[cs.sf.user_variables]["SEED_III"] = VAR_VAL
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
                state[cs.sf.time_step] = TIME_STEP
            elif re.match(rs.run_numeric, line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval(RUN_STEPS)
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]  # type: ignore
                runc += 1
            elif re.match(rs.run_formula, line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval("variables['" + RUN_STEPS[2:-1] + "']")
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]  # type: ignore
                runc += 1
            elif re.match(rs.label_declaration, line):
                label = line.split()[-1]
                labels[label] = []
            elif re.match(rs.set_restart, line):
                w_restart, RESTART_FREQUENCY, RESTART_FILES = line.split()
                state[cs.sf.restart_files] = RESTART_FILES[:-1]
                RESTART_FREQUENCY = eval("variables['" + RESTART_FREQUENCY[2:-1] + "']")
                state[cs.sf.restart_every] = RESTART_FREQUENCY
    vt = 0
    labels_list = list(labels.keys())
    for label in labels:
        labels[label] = {cs.sf.begin_step: vt, cs.sf.end_step: sum(labels[label]) + vt, cs.sf.runs: 0}  # type: ignore
        vt = labels[label][cs.sf.end_step]  # type: ignore
    labels["START"]["0"] = {cs.sf.dump_file: "START0"}  # type: ignore
    state[cs.sf.run_labels] = labels
    state[cs.sf.labels_list] = labels_list
    state[cs.sf.variables] = variables
    state[cs.sf.runs] = runs
    return state


def gen_in(cwd: Path, state: Dict) -> Path:
    variables = state[cs.sf.user_variables]
    out_in_file = cwd / cs.folders.in_file / "START0.in"
    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file.as_posix()} already exists")
    else:
        out_in_file.touch()
    stf: Path = (cwd / cs.folders.in_templates / cs.files.start_template)
    with stf.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            if re.match(rs.variable_equal_numeric, line):
                for var, value in variables.items():
                    if re.match(rs.required_variable_equal_numeric(var), line):
                        line = f"variable {var} equal {value}\n"
            elif re.match(rs.set_dump, line):
                before = line.split()[:-1] + [f"{cs.folders.dumps}/START0", "\n"]
                line = " ".join(before)
            elif re.match(rs.set_restart, line):
                line_list = line.split()[:-1] + [f"{cs.folders.restarts}/{state[cs.sf.restart_files]}*", "\n"]
                line = " ".join(line_list)
            fout.write(line)
    return out_in_file


def check_required_fs(cwd: Path):
    if (n := (cwd / cs.files.state)).exists():
        raise FileExistsError(f"File {n.as_posix()} already exists")
    n.touch()
    if (n := (cwd / cs.folders.restarts)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.in_file)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.dumps)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.sl)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.data_processing)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.special_restarts)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    return True


def init(cwd: Path, args: argparse.Namespace):
    check_required_fs(cwd)
    sldir = cwd / cs.folders.sl

    state = {cs.sf.state: states.fully_initialized}
    if args.file:
        pfile = (cwd / cs.files.params) if args.fname is None else args.fname
        with pfile.open('r') as f:
            variables = json.load(f)
    else:
        variables = json.loads(args.params)
    state[cs.sf.user_variables] = variables

    stf: Path = (cwd / cs.folders.in_templates / cs.files.start_template)
    state = process_file(stf, state)
    in_file = gen_in(cwd, state)
    state = process_file(in_file, state)
    state[cs.sf.run_labels]["START"]["0"][cs.sf.in_file] = str(in_file.parts[-1])
    state[cs.sf.run_labels]["START"]["0"][cs.sf.run_no] = 1
    state[cs.sf.run_counter] = 0
    state[cs.sf.slurm_directory] = str(sldir)
    with (cwd / cs.files.state).open('w') as f:
        json.dump(state, f)
    return 0


if __name__ == "__main__":
    pass
