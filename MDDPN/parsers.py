#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 01-05-2024 22:33:28

import re
import json
import shutil
from pathlib import Path
from typing import Dict, Any, Union

from . import regexs as rs
from . import constants as cs
from .utils import RestartMode, Part, logs


@logs
def try_eval(equ: str, vars: Dict):
    cs.sp.logger.debug(f"    Formula: '{equ}'")
    try:
        eval_val = eval(equ,  globals(), vars)
    except NameError as e:
        cs.sp.logger.critical(str(e))
        cs.sp.logger.critical(f"Unable to evaluate '{equ}', some variables lost")
        raise
    except Exception as e:
        cs.sp.logger.critical(str(e))
        cs.sp.logger.critical(f"Unable to evaluate '{equ}', unknown error")
        raise
    cs.sp.logger.debug(f"    Evaluated value: {eval_val}")
    return eval_val



@logs
def eva(variables: Dict, evaluand: str):
    if re.match(r"\d+", evaluand):
        evaluated = eval(evaluand)
        cs.sp.logger.debug(f"    Evaluand is numeric: {evaluand}")
    else:
        cs.sp.logger.debug("    Evaluand frequency is formula")
        evaluand = evaluand.replace('$', '').replace("{", '').replace('}', '')
        evaluand = evaluand.replace('^', '**')
        evaluated = try_eval(evaluand, variables)

    return evaluated


@logs
def ift(state: Dict, line: str) -> Dict[str, Any]:
    line = line.split('#')[0].strip()
    line = line.replace('"', "").replace("'", "").replace('$', '').replace('{', '').replace('}', '')
    line = line[2:].strip()
    condition, outcome = line.split('then')
    condition = condition.strip()
    outcome = outcome.strip()
    mc = re.findall(r"(\W|^)temp(\W|$)", condition)
    if len(mc) == 0:
        pass
    if len(re.findall(rs.jump_inc, line)) > 0:
        jmp_label = line.split('#')[0].strip().split()[-1]
        cs.sp.logger.debug("   Jump found in outcome")
        if state['c_lmp_label'] == jmp_label:
            cs.sp.logger.debug(f"    Jump and currenl lmp_label are equal '{jmp_label}'")
            state[cs.sf.run_labels][state["clabel"]] = [(None if isinstance(el, dict) else el) for el in state[cs.sf.run_labels][state["clabel"]]]
            # state[cs.sf.run_labels][state["clabel"]] = None
            state['c_lmp_label'] = None
        else:
            cs.sp.logger.error(f"Current lmp_label is '{state['c_lmp_label']}', but jump is '{jmp_label}'")
            raise RuntimeError(f"Current lmp_label is '{state['c_lmp_label']}', but jump is '{jmp_label}'")
    else:
        cs.sp.logger.debug("    Nothing was found in outcome")
    return state


@logs
def restart(state: Dict, line: str) -> Dict[str, Any]:
    rmode = RestartMode.none
    if re.match(rs.restart_one, line):
        cs.sp.logger.debug("    found one-file restart mode")
        rmode = RestartMode.one
        w_restart, RESTART_FREQUENCY, RESTART_FILE = line.split('#')[0].strip().split()
        pass
    elif re.match(rs.restart_two, line):
        cs.sp.logger.debug("    found two-file restart mode")
        rmode = RestartMode.two
        w_restart, RESTART_FREQUENCY, RESTART_FILE1, RESTART_FILE2 = line.split('#')[0].strip().split()
        pass
    elif re.match(rs.restart_multiple, line):
        cs.sp.logger.debug("    found multiple-file restart mode")
        rmode = RestartMode.multiple
        w_restart, RESTART_FREQUENCY, RESTART_FILES = line.split('#')[0].strip().split()
        # RESTART_FILES = Path(RESTART_FILES).parts[-1].split('.')[-2]
        # state[cs.sf.restart_files] = RESTART_FILES
        pass
    else:
        cs.sp.logger.error("    Unknown type of restart")
        # logger.debug(line)
        raise RuntimeError("    Unknown type of restart")
    if RestartMode(state[cs.sf.restart_mode]) == RestartMode.none:
        cs.sp.logger.debug(f"    Setting non-specified restart mode to {rmode}")
        state[cs.sf.restart_mode] = rmode
    elif RestartMode(state[cs.sf.restart_mode]) == rmode:
        cs.sp.logger.debug("    Restart modes are equal, pass")
        pass
    else:
        cs.sp.logger.warning(f"    Restart modes are not equal, specified: {state[cs.sf.restart_mode]}, infile: {rmode}")
        cs.sp.logger.warning("    Using specified restart mode")
    RESTART_FREQUENCY = eva(state[cs.sf.variables], RESTART_FREQUENCY)
    state[cs.sf.restart_every] = RESTART_FREQUENCY
    state[cs.sf.restart_files] = 'restart'

    return state


@logs
def variable(state: Dict, line: str) -> Dict[str, Any]:
    w_variable, VAR_NAME, w_equal, VAR_VAL = line.split('#')[0].strip().split()
    if VAR_NAME not in state[cs.sf.variables]:
        cs.sp.logger.debug(f"    Variable '{VAR_NAME}' was not found in dict")
        VAR_VAL = eva(state[cs.sf.variables], VAR_VAL)
        cs.sp.logger.debug(f"    Setting '{VAR_NAME}'={VAR_VAL}")
        state[cs.sf.variables][VAR_NAME] = VAR_VAL
        cs.sp.logger.debug(f"    Setting 'v_{VAR_NAME}'={VAR_VAL}")
        state[cs.sf.variables]["v_" + VAR_NAME] = VAR_VAL
    else:
        cs.sp.logger.debug(f"    Variable '{VAR_NAME}' was found in dict, not changing")

    return state


@logs
def timestep(state: Dict, line: str) -> Dict[str, Any]:
    w_timestep, TIME_STEP = line.split('#')[0].strip().split()
    TIME_STEP = eva(state[cs.sf.variables], TIME_STEP)
    cs.sp.logger.debug(f"    Setting 'dt'={TIME_STEP}")
    state[cs.sf.variables]['dt'] = TIME_STEP
    cs.sp.logger.debug(f"    Setting '{cs.sf.time_step}'={TIME_STEP}")
    state[cs.sf.time_step] = TIME_STEP

    return state


@logs
def run(state: Dict, line: str) -> Dict[str, Any]:
    w_run, RUN_STEPS = line.split()
    RUN_STEPS = eva(state[cs.sf.variables], RUN_STEPS)
    if state['c_lmp_label'] is not None:
        cs.sp.logger.debug(f"    Current LAMMPS label is set: '{state['c_lmp_label']}'")
        cs.sp.logger.debug(f"    Setting state[{cs.sf.run_labels}][{state['clabel']}]+=[" + "{" + f"'{state['c_lmp_label']}':{RUN_STEPS}" + "}]")
        state[cs.sf.run_labels][state["clabel"]] += [{state['c_lmp_label']: RUN_STEPS}]  # type: ignore
    else:
        cs.sp.logger.debug(f"    Setting state[{cs.sf.run_labels}][{state['clabel']}]+=[{RUN_STEPS}]")
        state[cs.sf.run_labels][state["clabel"]] += [RUN_STEPS]  # type: ignore

    return state


@logs
def __generator(num: int, label: str) -> Path:
    cs.sp.logger.debug(f"Generating file with label {label} and run no: {num}")
    variables = cs.sp.state[cs.sf.user_variables]
    cs.sp.logger.debug("The following variables were set:")
    cs.sp.logger.debug(json.dumps(variables, indent=4))
    out_in_file = cs.sp.cwd / cs.folders.in_file / f"{label}{num}.in"
    if out_in_file.exists():
        out_in_file_trash = out_in_file.parent.absolute() / (out_in_file.parts[-1] + ".trash")
        cs.sp.logger.error(f"Output in. file already exists: {out_in_file.as_posix()}. Moving it to {out_in_file_trash.as_posix()}")

    stf: Path = (cs.sp.cwd / cs.folders.in_templates / cs.files.template)

    cs.sp.logger.info("Starting line by line rewriting")
    with stf.open('r') as fin, out_in_file.open('w') as fout:
        for i, line in enumerate(fin):
            if re.match(rs.variable_equal_const, line):
                cs.sp.logger.debug(f"Line {i}, found const variable")
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split('#')[0].strip().split()
                line = f"variable {VAR_NAME} equal {cs.sp.state[cs.sf.variables][VAR_NAME]}\n"
                cs.sp.logger.debug(f"    Variable: '{VAR_NAME}'={cs.sp.state[cs.sf.variables][VAR_NAME]}")
            if re.match(rs.variable_equal_numeric, line):
                cs.sp.logger.debug(f"Line {i}, found numeric variable")
                fl = True
                for var, value in variables.items():
                    if re.match(rs.required_variable_equal_numeric(var), line):
                        fl = False
                        cs.sp.logger.debug(f"    Variable: '{var}', setting to {value}")
                        line = f"variable {var} equal {value}\n"
                if fl:
                    cs.sp.logger.debug("    Known user variables were not found in this line")
            elif re.match(rs.set_dump, line):
                cs.sp.logger.debug(f"Line {i}, found set dump")
                w_dump, DUMP_NAME, GROUP, DUMP_STYLE, DUMP_FREQUENCY, DUMP_FILE, *other_args = line.split("#")[0].strip().split()
                dfn = f"{cs.folders.dumps}/{label}{num}"
                line = f"dump {DUMP_NAME} {GROUP} {DUMP_STYLE} {DUMP_FREQUENCY} {dfn} "
                line += " ".join(other_args) + "\n"
                cs.sp.logger.debug(f"Dump file will be {dfn}")
            elif re.match(rs.write_restart, line):
                cs.sp.logger.debug(f"Line {i}, found write_restart")
                cs.sp.logger.debug(f"    Redirecting to '{cs.folders.special_restarts}/{label}.{num}'")
                line = f"write_restart {cs.folders.special_restarts}/{label}.{num}\n"
            elif re.match(rs.set_restart, line):
                cs.sp.logger.debug(f"Line {i}, found set restart")
                rfn = f"{cs.folders.restarts}/{cs.sp.state[cs.sf.restart_files]}"
                if RestartMode(cs.sp.state[cs.sf.restart_mode]) == RestartMode.multiple:
                    cs.sp.logger.debug("Restart mode is multiple-filed")
                    line_list = line.split()[:-1] + [f"{rfn}.*", "\n"]
                elif RestartMode(cs.sp.state[cs.sf.restart_mode]) == RestartMode.one:
                    cs.sp.logger.debug("Restart mode is one-filed")
                    line_list = line.split()[:-1] + [f"{rfn}", "\n"]
                elif RestartMode(cs.sp.state[cs.sf.restart_mode]) == RestartMode.two:
                    cs.sp.logger.debug("Restart mode is two-filed")
                    line_list = line.split()[:-1] + [f"{rfn}.a {rfn}.b", "\n"]
                else: raise RuntimeError("Software bug")
                line = " ".join(line_list)
            else:
                pass  # leave unrecognized line
            fout.write(line)
    cs.sp.logger.info("Done line by line rewriting")
    return out_in_file


@logs
def generator(num: int, current_label: str,  restart_file: Union[Path, None] = None) -> Path:
    cs.sp.logger.info("Generating input file from template")
    out_file = __generator(num, current_label)
    if restart_file is None:
        return out_file
    out_file_tmp_name = out_file.parts[-1] + ".bak"
    out_file_tmp_parts = list(out_file.parts[:-1]) + [out_file_tmp_name]
    out_file_tmp = Path(*out_file_tmp_parts)

    cs.sp.logger.info("Generating restart file")

    shutil.copy(out_file, out_file_tmp)
    out_file.unlink()

    part = Part.none
    fl = False
    was_fl = True
    label = None
    cs.sp.logger.debug("Start line by line rewriting")
    with out_file_tmp.open('r') as fin, out_file.open('w') as fout:
        fout.write(f"read_restart {restart_file.as_posix()}\n")
        fout.write("run 0\n")
        for i, line in enumerate(fin):
            if re.match(rs.part_spec, line):
                cs.sp.logger.debug(f"Line {i}, part declaration")
                w_hashtag, w_part, part_name = line.split()
                part = Part(part_name)
                if part == Part.run:
                    # cl_run = int(state[cs.sf.run_labels][current_label][cs.sf.runs]) + 1
                    # line += f"write_restart {cs.folders.special_restarts}/restart.tmp.{current_label}.{cl_run}\n"
                    line += "run 0\n"
                cs.sp.logger.debug(f"    Part: {str(part)}")
            if part == Part.start:
                cs.sp.logger.debug(f"Line {i}, start part, skipping")
                continue
            elif part == Part.save:
                pass
            elif part == Part.run:
                if re.match(rs.label_declaration, line):
                    cs.sp.logger.debug(f"Line {i}, label declaration")
                    if was_fl:
                        label = line.strip().split()[-1]
                        cs.sp.logger.debug(f"    Label: {label}")
                        if label != current_label:
                            cs.sp.logger.debug(f"    This label will be skipped, because '{label}' is before current label '{current_label}'")
                            fl = True
                        else:
                            cs.sp.logger.debug(f"    Label '{label}' is current label '{current_label}', last of file will be written")
                            fl = False
                            was_fl = False
                    else:
                        cs.sp.logger.debug(f"    Label '{label}' seems to be after current label '{current_label}'")
                if fl:
                    cs.sp.logger.debug(f"Line {i}, skipping label")
                    continue
            cs.sp.logger.debug(f"Line {i}, writing")
            fout.write(line)
    cs.sp.logger.debug("Done line by line rewriting")

    # shutil.copy(out_file_tmp, out_file)
    out_file_tmp.unlink()

    return out_file


if __name__ == "__main__":
    pass
