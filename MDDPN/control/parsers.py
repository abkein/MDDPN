#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 20-09-2023 20:07:21

import re
import json
import shutil
import logging
from pathlib import Path
from typing import Dict, Any

from . import regexs as rs
from . import constants as cs
from .utils import RestartMode, try_eval, Part


def eva(variables: Dict, evaluand: str, logger: logging.Logger):
    if re.match(r"\d+", evaluand):
        evaluated = eval(evaluand)
        logger.debug(f"    Evaluand is numeric: {evaluand}")
    else:
        logger.debug("    Evaluand frequency is formula")
        evaluand = evaluand.replace('$', '').replace("{", '').replace('}', '')
        evaluand = evaluand.replace('^', '**')
        evaluated = try_eval(evaluand, variables, logger.getChild('try_eval'))

    return evaluated


def restart(state: Dict, line: str, logger: logging.Logger) -> Dict[str, Any]:
    rmode = RestartMode.none
    if re.match(rs.restart_one, line):
        logger.debug("    found one-file restart mode")
        rmode = RestartMode.one
        w_restart, RESTART_FREQUENCY, RESTART_FILE = line.split('#')[0].strip().split()
        pass
    elif re.match(rs.restart_two, line):
        logger.debug("    found two-file restart mode")
        rmode = RestartMode.two
        w_restart, RESTART_FREQUENCY, RESTART_FILE1, RESTART_FILE2 = line.split('#')[0].strip().split()
        pass
    elif re.match(rs.restart_multiple, line):
        logger.debug("    found multiple-file restart mode")
        rmode = RestartMode.multiple
        w_restart, RESTART_FREQUENCY, RESTART_FILES = line.split('#')[0].strip().split()
        # RESTART_FILES = Path(RESTART_FILES).parts[-1].split('.')[-2]
        # state[cs.sf.restart_files] = RESTART_FILES
        pass
    else:
        logger.error("    Unknown type of restart")
        # logger.debug(line)
        raise RuntimeError("    Unknown type of restart")
    if RestartMode(state[cs.sf.restart_mode]) == RestartMode.none:
        logger.debug(f"    Setting non-specified restart mode to {rmode}")
        state[cs.sf.restart_mode] = rmode
    elif RestartMode(state[cs.sf.restart_mode]) == rmode:
        logger.debug("    Restart modes are equal, pass")
        pass
    else:
        logger.warning(f"    Restart modes are not equal, specified: {state[cs.sf.restart_mode]}, infile: {rmode}")
        logger.warning("    Using specified restart mode")
    RESTART_FREQUENCY = eva(state[cs.sf.variables], RESTART_FREQUENCY, logger.getChild('eval'))
    state[cs.sf.restart_every] = RESTART_FREQUENCY
    state[cs.sf.restart_files] = 'restart'

    return state


def variable(state: Dict, line: str, logger: logging.Logger) -> Dict[str, Any]:
    w_variable, VAR_NAME, w_equal, VAR_VAL = line.split('#')[0].strip().split()
    if VAR_NAME not in state[cs.sf.variables]:
        logger.debug(f"    Variable '{VAR_NAME}' was not found in dict")
        VAR_VAL = eva(state[cs.sf.variables], VAR_VAL, logger.getChild('eval'))
        logger.debug(f"    Setting '{VAR_NAME}'={VAR_VAL}")
        state[cs.sf.variables][VAR_NAME] = VAR_VAL
        logger.debug(f"    Setting 'v_{VAR_NAME}'={VAR_VAL}")
        state[cs.sf.variables]["v_" + VAR_NAME] = VAR_VAL
    else:
        logger.debug(f"    Variable '{VAR_NAME}' was found in dict, not changing")

    return state


def timestep(state: Dict, line: str, logger: logging.Logger) -> Dict[str, Any]:
    w_timestep, TIME_STEP = line.split('#')[0].strip().split()
    TIME_STEP = eva(state[cs.sf.variables], TIME_STEP, logger.getChild('eval'))
    logger.debug(f"    Setting 'dt'={TIME_STEP}")
    state[cs.sf.variables]['dt'] = TIME_STEP
    logger.debug(f"    Setting '{cs.sf.time_step}'={TIME_STEP}")
    state[cs.sf.time_step] = TIME_STEP

    return state


def run(state: Dict, line: str, logger: logging.Logger) -> Dict[str, Any]:
    w_run, RUN_STEPS = line.split()
    RUN_STEPS = eva(state[cs.sf.variables], RUN_STEPS, logger.getChild('eval'))
    if state['c_lmp_label'] is not None:
        logger.debug(f"    Current LAMMPS label is set: '{state['c_lmp_label']}'")
        logger.debug(f"    Setting state[{cs.sf.run_labels}][{state['clabel']}]+=[" + "{" + f"'{state['c_lmp_label']}':{RUN_STEPS}" + "}]")
        state[cs.sf.run_labels][state["clabel"]] += [{state['c_lmp_label']: RUN_STEPS}]  # type: ignore
    else:
        logger.debug(f"    Setting state[{cs.sf.run_labels}][{state['clabel']}]+=[{RUN_STEPS}]")
        state[cs.sf.run_labels][state["clabel"]] += [RUN_STEPS]  # type: ignore

    return state


def generator(cwd: Path, state: Dict, logger: logging.Logger, num: int, label: str):
    logger.debug(f"Generating file with label {label} and run no: {num}")
    variables = state[cs.sf.user_variables]
    logger.debug("The following variables were set:")
    logger.debug(json.dumps(variables, indent=4))
    out_in_file = cwd / cs.folders.in_file / f"{label}{num}.in"
    if out_in_file.exists():
        logger.error(f"Output in. file already exists: {out_in_file.as_posix()}")
        raise FileExistsError(f"Output in. file already exists: {out_in_file.as_posix()}")
    stf: Path = (cwd / cs.folders.in_templates / cs.files.template)
    logger.info("Starting line by line rewriting")
    with stf.open('r') as fin, out_in_file.open('w') as fout:
        for i, line in enumerate(fin):
            if re.match(rs.variable_equal_const, line):
                logger.debug(f"Line {i}, found const variable")
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split('#')[0].strip().split()
                logger.debug(f"    Variable: '{VAR_NAME}'={state[cs.sf.variables][VAR_NAME]}")
                line = f"variable {VAR_NAME} equal {state[cs.sf.variables][VAR_NAME]}\n"
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
                w_dump, DUMP_NAME, GROUP, DUMP_STYLE, DUMP_FREQUENCY, DUMP_FILE, *other_args = line.split("#")[0].strip().split()
                dfn = f"{cs.folders.dumps}/{label}{num}"
                line = f"dump {DUMP_NAME} {GROUP} {DUMP_STYLE} {DUMP_FREQUENCY} {dfn} "
                line += " ".join(other_args) + "\n"
                logger.debug(f"Dump file will be {dfn}")
            elif re.match(rs.write_restart, line):
                logger.debug(f"Line {i}, found write_restart")
                logger.debug(f"    Redirecting to '{cs.folders.special_restarts}/{label}.{num}'")
                line = f"write_restart {cs.folders.special_restarts}/{label}.{num}\n"
            elif re.match(rs.set_restart, line):
                logger.debug(f"Line {i}, found set restart")
                rfn = f"{cs.folders.restarts}/{state[cs.sf.restart_files]}"
                if RestartMode(state[cs.sf.restart_mode]) == RestartMode.multiple:
                    logger.debug("Restart mode is multiple-filed")
                    line_list = line.split()[:-1] + [f"{rfn}.*", "\n"]
                elif RestartMode(state[cs.sf.restart_mode]) == RestartMode.one:
                    logger.debug("Restart mode is one-filed")
                    line_list = line.split()[:-1] + [f"{rfn}", "\n"]
                elif RestartMode(state[cs.sf.restart_mode]) == RestartMode.two:
                    logger.debug("Restart mode is two-filed")
                    line_list = line.split()[:-1] + [f"{rfn}.a {rfn}.b", "\n"]
                else:
                    logger.critical("Software bug")
                    raise RuntimeError("Software bug")
                line = " ".join(line_list)
            fout.write(line)
    logger.info("Done line by line rewriting")
    return out_in_file


def gen_restart(cwd: Path, state: Dict, logger: logging.Logger, num: int, current_label: str,  restart_file_name: str, test: bool = False) -> Path:
    logger.info("Generating input file from template")
    out_file = generator(cwd, state, logger.getChild('generator'), num, current_label)
    out_file_tmp_name = out_file.parts[-1] + ".bak"
    out_file_tmp_parts = list(out_file.parts[:-1]) + [out_file_tmp_name]
    out_file_tmp = Path(*out_file_tmp_parts)

    logger.info("Generating restart file")

    shutil.copy(out_file, out_file_tmp)
    out_file.unlink()

    part = Part.none
    fl = False
    was_fl = True
    label = None
    logger.debug("Start line by line rewriting")
    with out_file_tmp.open('r') as fin, out_file.open('w') as fout:
        fout.write(f"read_restart {restart_file_name}\n")
        fout.write("run 0\n")
        for i, line in enumerate(fin):
            if re.match(rs.part_spec, line):
                logger.debug(f"Line {i}, part declaration")
                w_hashtag, w_part, part_name = line.split()
                part = Part(part_name)
                if part == Part.run:
                    # cl_run = int(state[cs.sf.run_labels][current_label][cs.sf.runs]) + 1
                    # line += f"write_restart {cs.folders.special_restarts}/restart.tmp.{current_label}.{cl_run}\n"
                    line += "run 0\n"
                logger.debug(f"    Part: {str(part)}")
            if part == Part.start:
                logger.debug(f"Line {i}, start part, skipping")
                continue
            elif part == Part.save:
                pass
            elif part == Part.run:
                if re.match(rs.label_declaration, line):
                    logger.debug(f"Line {i}, label declaration")
                    if was_fl:
                        label = line.strip().split()[-1]
                        logger.debug(f"    Label: {label}")
                        if label != current_label:
                            logger.debug(f"    This label will be skipped, because '{label}' is before current label '{current_label}'")
                            fl = True
                        else:
                            logger.debug(f"    Label '{label}' is current label '{current_label}', last of file will be written")
                            fl = False
                            was_fl = False
                    else:
                        logger.debug(f"    Label '{label}' seems to be after current label '{current_label}'")
                if fl:
                    logger.debug(f"Line {i}, skipping label")
                    continue
            logger.debug(f"Line {i}, writing")
            fout.write(line)
    logger.debug("Done line by line rewriting")

    # shutil.copy(out_file_tmp, out_file)
    out_file_tmp.unlink()

    return out_file


if __name__ == "__main__":
    pass
