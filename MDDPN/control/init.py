#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 27-08-2023 02:15:46

import logging
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


def process_file(file: Path, state: Dict, logger: logging.Logger) -> Dict:
    labels: Dict[str, Union[int, List[int]]] = {"START": [0]}
    runs = {}
    variables = {}
    runc = 0
    label = "START"
    logger.info("Start line by line parsing")
    try:
        with file.open('r') as fin:
            for i, line in enumerate(fin):
                if re.match(rs.required_variable_equal_numeric("SEED_I"), line):
                    logger.debug(f"Line {i}, found required variable 'SEED_I'")
                    VAR_VAL = round(time.time())
                    logger.debug(f"    Setting 'SEED_I'={VAR_VAL}")
                    state[cs.sf.user_variables]["SEED_I"] = VAR_VAL
                    variables["SEED_I"] = VAR_VAL
                    variables["v_SEED_I"] = VAR_VAL
                elif re.match(rs.required_variable_equal_numeric("SEED_II"), line):
                    logger.debug(f"Line {i}, found required variable 'SEED_II'")
                    VAR_VAL = round(time.time())
                    logger.debug(f"    Setting 'SEED_II'={VAR_VAL}")
                    state[cs.sf.user_variables]["SEED_II"] = VAR_VAL
                    variables["SEED_II"] = VAR_VAL
                    variables["v_SEED_II"] = VAR_VAL
                elif re.match(rs.required_variable_equal_numeric("SEED_III"), line):
                    logger.debug(f"Line {i}, found required variable 'SEED_III'")
                    VAR_VAL = round(time.time())
                    logger.debug(f"    Setting 'SEED_III'={VAR_VAL}")
                    state[cs.sf.user_variables]["SEED_III"] = VAR_VAL
                    variables["SEED_III"] = VAR_VAL
                    variables["v_SEED_III"] = VAR_VAL
                elif re.match(rs.variable_equal_numeric, line):
                    logger.debug(f"Line {i}, found variable equal numeric")
                    w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                    VAR_VAL = eval(VAR_VAL)
                    logger.debug(f"    Setting '{VAR_NAME}'={VAR_VAL}")
                    variables[VAR_NAME] = VAR_VAL
                    variables["v_" + VAR_NAME] = VAR_VAL
                elif re.match(rs.variable_equal_formula, line):
                    logger.debug(f"Line {i}, found variable equal formula")
                    w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                    VAR_VAL = VAR_VAL[2:-1]
                    logger.debug(f"    Formula: '{VAR_VAL}'")
                    try:
                        VAR_VAL = eval(VAR_VAL, globals(), variables)
                    except NameError as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{VAR_VAL}', some variables lost")
                        raise
                    except Exception as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{VAR_VAL}', unknown error")
                        raise
                    logger.debug(f"    Evaluated value: {VAR_VAL}")
                    logger.debug(f"    Setting '{VAR_NAME}'={VAR_VAL}")
                    variables[VAR_NAME] = VAR_VAL
                    variables["v_" + VAR_NAME] = VAR_VAL
                elif re.match(rs.set_timestep_num, line):
                    logger.debug(f"Line {i}, found numeric timestep")
                    w_timestep, TIME_STEP = line.split()
                    TIME_STEP = eval(TIME_STEP)
                    logger.debug(f"    Setting 'dt'={TIME_STEP}")
                    variables['dt'] = TIME_STEP
                    logger.debug(f"    Setting '{cs.sf.time_step}'={TIME_STEP}")
                    state[cs.sf.time_step] = TIME_STEP
                elif re.match(rs.set_timestep_equ, line):
                    logger.debug(f"Line {i}, found timestep equal formula")
                    w_timestep, TIME_STEP = line.split()
                    TIME_STEP = TIME_STEP[2:-1]
                    logger.debug(f"    Formula: '{TIME_STEP}'")
                    try:
                        TIME_STEP = eval(TIME_STEP, globals(), variables)
                    except NameError as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{TIME_STEP}', some variables lost")
                        raise
                    except Exception as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{TIME_STEP}', unknown error")
                        raise
                    logger.debug(f"    Evaluated value: {TIME_STEP}")
                    logger.debug(f"    Setting 'dt'={TIME_STEP}")
                    variables['dt'] = TIME_STEP
                    logger.debug(f"    Setting '{cs.sf.time_step}'={TIME_STEP}")
                    state[cs.sf.time_step] = TIME_STEP
                elif re.match(rs.run_numeric, line):
                    logger.debug(f"Line {i}, found run numeric")
                    w_run, RUN_STEPS = line.split()
                    RUN_STEPS = eval(RUN_STEPS)
                    logger.debug(f"    Setting runs[run{str(runc)}]={RUN_STEPS}")
                    runs["run" + str(runc)] = RUN_STEPS
                    logger.debug(f"    Setting labels[{label}]+=[{RUN_STEPS}]")
                    labels[label] += [RUN_STEPS]  # type: ignore
                    runc += 1
                    logger.debug(f"    Adding run, total runs now: {runc}")
                elif re.match(rs.run_formula, line):
                    logger.debug(f"Line {i}, found run formula")
                    w_run, RUN_STEPS = line.split()
                    logger.debug(f"    Formula: '{RUN_STEPS}'")
                    try:
                        RUN_STEPS = eval(RUN_STEPS[2:-1], globals(), variables)
                    except NameError as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{RUN_STEPS}', some variables lost")
                        raise
                    except Exception as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{RUN_STEPS}', unknown error")
                        raise
                    logger.debug(f"    Evaluated value: {RUN_STEPS}")
                    logger.debug(f"    Setting runs[run{str(runc)}]={RUN_STEPS}")
                    runs["run" + str(runc)] = RUN_STEPS
                    logger.debug(f"    Setting labels[{label}]+=[{RUN_STEPS}]")
                    labels[label] += [RUN_STEPS]  # type: ignore
                    runc += 1
                    logger.debug(f"    Adding run, total runs now: {runc}")
                elif re.match(rs.label_declaration, line):
                    logger.debug(f"Line {i}, found new label")
                    label = line.split()[-1]
                    logger.debug(f"    Label: '{label}'")
                    labels[label] = []
                elif re.match(rs.set_restart_num, line):
                    logger.debug(f"Line {i}, found restart numeric")
                    w_restart, RESTART_FREQUENCY, RESTART_FILES = line.split()
                    logger.debug("    Setting restart files to")
                    logger.debug(f"    {RESTART_FILES[:-1]}")
                    state[cs.sf.restart_files] = RESTART_FILES[:-1]
                    RESTART_FREQUENCY = eval(RESTART_FREQUENCY[2:-1])
                    state[cs.sf.restart_every] = RESTART_FREQUENCY
                    logger.debug(f"    Restart frequency: {RESTART_FREQUENCY}")
                elif re.match(rs.set_restart_equ, line):
                    logger.debug(f"Line {i}, found restart formula")
                    w_restart, RESTART_FREQUENCY, RESTART_FILES = line.split()
                    logger.debug("    Setting restart files to")
                    logger.debug(f"    {RESTART_FILES[:-1]}")
                    state[cs.sf.restart_files] = RESTART_FILES[:-1]
                    logger.debug(f"    Formula: '{RESTART_FREQUENCY[2:-1]}'")
                    try:
                        RESTART_FREQUENCY = eval(RESTART_FREQUENCY[2:-1],  globals(), variables)
                    except NameError as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{RESTART_FREQUENCY}', some variables lost")
                        raise
                    except Exception as e:
                        logger.critical(str(e))
                        logger.critical(f"Unable to evaluate '{RESTART_FREQUENCY}', unknown error")
                        raise
                    logger.debug(f"    Evaluated value: {RESTART_FREQUENCY}")
                    state[cs.sf.restart_every] = RESTART_FREQUENCY
                    logger.debug(f"    Restart frequency: {RESTART_FREQUENCY}")
    except Exception as e:
        logger.critical(str(e))
        logger.info("Dumping variables dict:")
        logger.info(json.dumps(variables, indent=4))
        raise
    logger.info("Done parsing")
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


def gen_in(cwd: Path, state: Dict, logger: logging.Logger) -> Path:
    variables = state[cs.sf.user_variables]
    logger.debug("The following variables were set:")
    logger.debug(json.dumps(variables, indent=4))
    out_in_file = cwd / cs.folders.in_file / "START0.in"
    if out_in_file.exists():
        logger.error(f"Output in. file already exists: {out_in_file.as_posix()}")
        raise FileExistsError(f"Output in. file already exists: {out_in_file.as_posix()}")
    stf: Path = (cwd / cs.folders.in_templates / cs.files.start_template)
    logger.info("Starting line by line rewriting")
    with stf.open('r') as fin, out_in_file.open('w') as fout:
        for i, line in enumerate(fin):
            if re.match(rs.variable_equal_numeric, line):
                logger.debug(f"Line {i}, found numeric variable")
                fl = True
                for var, value in variables.items():
                    if re.match(rs.required_variable_equal_numeric(var), line):
                        fl = False
                        logger.debug(f"    Variable: '{var}', setting to {value}")
                        line = f"variable {var} equal {value}\n"
                if fl:
                    logger.debug("    Known user variables were not found in this line")
            elif re.match(rs.set_dump, line):
                logger.debug(f"Line {i}, found set dump")
                before = line.split()[:-1] + [f"{cs.folders.dumps}/START0", "\n"]
                line = " ".join(before)
            elif re.match(rs.set_restart_num, line):
                logger.debug(f"Line {i}, found set restart numeric")
                line_list = line.split()[:-1] + [f"{cs.folders.restarts}/{state[cs.sf.restart_files]}*", "\n"]
                line = " ".join(line_list)
            elif re.match(rs.set_restart_equ, line):
                logger.debug(f"Line {i}, found set restart formula")
                line_list = line.split()[:-1] + [f"{cs.folders.restarts}/{state[cs.sf.restart_files]}*", "\n"]
                line = " ".join(line_list)
            fout.write(line)
    logger.info("Done line by line rewriting")
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


def init(cwd: Path, args: argparse.Namespace, logger: logging.Logger):
    try:
        check_required_fs(cwd)
    except FileExistsError as e:
        logger.error(str(e))
        logger.error("Some folders or files were existed")
        raise
    sldir = cwd / cs.folders.sl

    state = {cs.sf.state: states.fully_initialized, cs.sf.tag: round(time.time())}
    if args.file:
        pfile = (cwd / cs.files.params) if args.fname is None else args.fname
        logger.info(f"Trying to get params from file: {pfile.as_posix()}")
        with pfile.open('r') as f:
            variables = json.load(f)
    else:
        logger.info("Parsing params from CLI arguments")
        variables = json.loads(args.params)
    logger.debug(f"The following arguments were parsed: {[json.dumps(variables)]}")
    state[cs.sf.user_variables] = variables

    stf: Path = (cwd / cs.folders.in_templates / cs.files.start_template)
    if not stf.exists():
        logger.error(f"Start template file {stf.as_posix()} was not found, unable to proceed.")
        raise FileNotFoundError(f"File {stf.as_posix()} not found")

    logger.info("Processing 1-st stage: processing template file")
    state = process_file(stf, state, logger.getChild("process1"))
    logger.info("Generating input file")
    in_file = gen_in(cwd, state, logger.getChild("generator"))
    logger.info("Processing 2-nd stage: processing generated input file")
    state = process_file(in_file, state, logger.getChild("process2"))
    state[cs.sf.run_labels]["START"]["0"][cs.sf.in_file] = str(in_file.parts[-1])
    state[cs.sf.run_labels]["START"]["0"][cs.sf.run_no] = 1
    state[cs.sf.run_counter] = 0
    state[cs.sf.slurm_directory] = str(sldir)
    with (cwd / cs.files.state).open('w') as f:
        json.dump(state, f)
    logger.info("Initialization complete")
    return 0


if __name__ == "__main__":
    pass
