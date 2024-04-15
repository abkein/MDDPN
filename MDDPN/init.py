#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-09-2023 16:30:31

import re
import json
import time
import logging
import argparse
from typing import Dict
from pathlib import Path

from . import parsers
from . import regexs as rs
from . import constants as cs
from .utils import states, RestartMode, gsr

# TODO:
# gen_in not properly processes folders


def process_file(file: Path, state: Dict, logger: logging.Logger) -> Dict:
    state[cs.sf.run_labels] = {"START": [0]}
    # state["runcsa"] = 0
    state["clabel"] = "START"
    state["c_lmp_label"] = None
    # state[cs.sf.runs] = {}
    state[cs.sf.user_variables]['step'] = 0
    state[cs.sf.user_variables]['temp'] = 0
    state[cs.sf.user_variables]['test'] = 1
    state[cs.sf.user_variables]['v_test'] = 1
    state[cs.sf.variables] = {}
    for key, val in state[cs.sf.user_variables].items():
        _val = eval(val) if isinstance(val, str) else val
        state[cs.sf.variables][key] = _val
        state[cs.sf.variables]['v_' + key] = _val
    logger.info("Start line by line parsing")
    prev_line = ""
    try:
        with file.open('r') as fin:
            for i, line in enumerate(fin):
                if re.match(rs.required_variable_equal_numeric("SEED_I"), line):
                    logger.debug(f"Line {i}, found required variable 'SEED_I'")
                    VAR_VAL = round(time.time())
                    logger.debug(f"    Setting 'SEED_I'={VAR_VAL}")
                    state[cs.sf.user_variables]["SEED_I"] = VAR_VAL
                    state[cs.sf.variables]["SEED_I"] = VAR_VAL
                    state[cs.sf.variables]["v_SEED_I"] = VAR_VAL
                elif re.match(rs.required_variable_equal_numeric("SEED_II"), line):
                    logger.debug(f"Line {i}, found required variable 'SEED_II'")
                    VAR_VAL = round(time.time())
                    logger.debug(f"    Setting 'SEED_II'={VAR_VAL}")
                    state[cs.sf.user_variables]["SEED_II"] = VAR_VAL
                    state[cs.sf.variables]["SEED_II"] = VAR_VAL
                    state[cs.sf.variables]["v_SEED_II"] = VAR_VAL
                elif re.match(rs.required_variable_equal_numeric("SEED_III"), line):
                    logger.debug(f"Line {i}, found required variable 'SEED_III'")
                    VAR_VAL = round(time.time())
                    logger.debug(f"    Setting 'SEED_III'={VAR_VAL}")
                    state[cs.sf.user_variables]["SEED_III"] = VAR_VAL
                    state[cs.sf.variables]["SEED_III"] = VAR_VAL
                    state[cs.sf.variables]["v_SEED_III"] = VAR_VAL
                elif re.match(rs.variable_equal, line):
                    logger.debug(f"Line {i}, found variable equal")
                    state = parsers.variable(state, line, logger.getChild('variable'))
                elif re.match(rs.variable_loop, line):
                    logger.debug(f"Line {i}, found variable loop")
                    state = parsers.variable(state, line, logger.getChild('variable'))
                elif re.match(rs.set_timestep, line):
                    logger.debug(f"Line {i}, found timestep")
                    state = parsers.timestep(state, line, logger.getChild('timestep'))
                elif re.match(rs.run, line):
                    logger.debug(f"Line {i}, found run formula")
                    state = parsers.run(state, line, logger.getChild('run'))
                elif re.match(rs.label_declaration, line):
                    logger.debug(f"Line {i}, found new label")
                    if state['c_lmp_label'] is not None:
                        logger.error("    lmp label was not closed in previous label")
                        raise RuntimeError("    lmp label was not closed in previous label")
                    state["clabel"] = line.strip().split()[-1]
                    logger.debug(f"    Label: '{state['clabel']}'")
                    state[cs.sf.run_labels][state["clabel"]] = []
                elif re.match(rs.lmp_label, line):
                    logger.debug(f"Line {i}, found LAMMPS label")
                    if state['c_lmp_label'] is not None:
                        logger.error("    Attemting to defile new lmp label, while previous was not closed — nested lmp labels are not supported")
                        raise RuntimeError("    Attemting to defile new lmp label, while previous was not closed — nested lmp labels are not supported")
                    state['c_lmp_label'] = line.split('#')[0].strip().split()[-1]
                    logger.debug(f"    Setting current lmp_label to '{state['c_lmp_label']}'")
                elif re.match(rs.jump, line):
                    logger.debug(f"Line {i}, found jump")
                    if re.match(rs.next, prev_line):
                        jmp_label = line.split('#')[0].strip().split()[-1]
                        if state['c_lmp_label'] == jmp_label:
                            logger.debug(f"    Jump and currenl lmp_label are equal '{jmp_label}'")
                            var = prev_line.split("#")[0].strip().split()[-1]
                            cnt = state[cs.sf.variables][var]
                            logger.debug(f"    Loop with label {state['c_lmp_label']} will run {cnt} times")
                            state[cs.sf.run_labels][state["clabel"]] = [gsr(state['c_lmp_label'], el, cnt) for el in state[cs.sf.run_labels][state["clabel"]]]  # type: ignore
                            logger.debug("    Setting current lmp label to None")
                            state['c_lmp_label'] = None
                        else:
                            logger.error(f"Current lmp_label is '{state['c_lmp_label']}', but jump is '{jmp_label}'")
                            raise RuntimeError(f"Current lmp_label is '{state['c_lmp_label']}', but jump is '{jmp_label}'")
                    else:
                        logger.error("Line before jump does not contain next command")
                        raise RuntimeError("Line before jump does not contain next command")
                elif re.match(rs.ift, line):
                    logger.debug(f"Line {i}, found conditional with one outcome")
                    state = parsers.ift(state, line, logger.getChild('ift'))
                elif re.match(rs.set_restart, line):
                    logger.debug(f"Line {i}, found restart")
                    state = parsers.restart(state, line, logger.getChild('restart'))
                else:
                    logger.debug(f"Line {i}, nothing was found")
                prev_line = line
    except Exception:
        logger.exception("An exception ocurred while parsing")
        logger.critical("Dumping variables dict:")
        logger.critical(json.dumps(state[cs.sf.variables], indent=4))
        raise
    logger.info("Done parsing")
    vt: int = 0
    for label in state[cs.sf.run_labels]:
        if None in state[cs.sf.run_labels][label]:
            state[cs.sf.run_labels][label] = {cs.sf.begin_step: round(vt), cs.sf.end_step: None, cs.sf.runs: 0}
            vt = 0
        else:
            tg: int = round(sum(state[cs.sf.run_labels][label]) + vt)
            state[cs.sf.run_labels][label] = {cs.sf.begin_step: vt, cs.sf.end_step: tg, cs.sf.runs: 0}
            vt = tg

    state[cs.sf.run_labels]["START"]["0"] = {cs.sf.dump_file: "START0"}
    labels_list = list(state[cs.sf.run_labels].keys())
    state[cs.sf.labels_list] = labels_list
    # del state["runcsa"]
    del state['clabel']
    del state['c_lmp_label']
    return state


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
    if (n := (cwd / cs.folders.slurm)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.special_restarts)).exists():
        raise FileExistsError(f"Directory {n.as_posix()} already exists")
    n.mkdir()
    if (n := (cwd / cs.folders.signals)).exists():
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
    # sldir = cwd / cs.folders.sl

    state = {cs.sf.state: states.fully_initialized, cs.sf.tag: round(time.time())}
    if args.fname is not None:
        pfile: Path = cwd / args.fname
        logger.info(f"Trying to get params from file: {pfile.as_posix()}")
        with pfile.open('r') as f:
            variables = json.load(f)
    elif args.conf:
        logger.info("Getting params from configuration file")
        variables = cs.sp.params
    else:
        logger.info("Getting params from CLI arguments")
        variables = json.loads(args.params)
    logger.debug(f"The following arguments were parsed: {[json.dumps(variables, indent=4)]}")
    state[cs.sf.user_variables] = variables

    state[cs.sf.restart_mode] = RestartMode(str(args.restart_mode))

    stf: Path = (cwd / cs.folders.in_templates / cs.files.template)
    if not stf.exists():
        logger.error(f"Start template file {stf.as_posix()} was not found, unable to proceed.")
        raise FileNotFoundError(f"File {stf.as_posix()} not found")

    logger.info("Processing 1-st stage: processing template file")
    state = process_file(stf, state, logger.getChild("process1"))
    logger.info("Generating input file")
    in_file = parsers.generator(cwd, state, logger.getChild('generator'), 0, "START")
    logger.info("Processing 2-nd stage: processing generated input file")
    state = process_file(in_file, state, logger.getChild("process2"))
    state[cs.sf.run_labels]["START"]["0"][cs.sf.in_file] = str(in_file.parts[-1])
    state[cs.sf.run_labels]["START"]["0"][cs.sf.run_no] = 1
    state[cs.sf.run_counter] = 0
    with (cwd / cs.files.state).open('w') as f:
        json.dump(state, f)
    logger.info("Initialization complete")
    return 0


if __name__ == "__main__":
    pass
