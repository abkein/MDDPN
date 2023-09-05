#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 05-09-2023 20:47:37

import os
import json
import errno
import shlex
import logging
import argparse
import subprocess as sb
from enum import Enum
from pathlib import Path
from typing import Generator, Dict, Tuple
from contextlib import contextmanager

from .. import constants as cs


class states(str, Enum):
    initialized = "initialized"
    fully_initialized = "fully_initialized"
    started = "started"
    restarted = "restarted"
    comleted = "comleted"
    cluster_analysis_comleted = "cluster_analysis_comleted"
    data_obtained = "data_obtained"


class STRNodes(str, Enum):
    HOST = 'host'
    ANGR = 'angr'
    ALL = 'all'


class RestartMode(str, Enum):
    none = "None"
    one = 'one'
    two = 'two'
    multiple = 'multiple'


class Part(str, Enum):
    none = "None"
    start = "start"
    save = "save"
    run = "run"


class LogicError(Exception):
    pass


def is_tool(name):
    try:
        devnull = open(os.devnull)
        sb.Popen([name], stdout=devnull, stderr=devnull).communicate()
    except OSError as e:
        if e.errno == errno.ENOENT:
            return False
    return True


def find_exec(prog):
    if is_tool(prog):
        cmd = "which"
        return sb.call([cmd, prog])


def com_set(cwd: Path, args: argparse.Namespace) -> Dict:
    file = cwd / args.file
    if not file.exists():
        raise FileNotFoundError(f"There is no file {file.as_posix()}")
    with file.open('r') as f:
        fp = json.load(f)
    fp[args.variable] = args.value
    with file.open('w') as f:
        json.dump(fp, f)
    return fp


@contextmanager
def load_state(cwd) -> Generator:
    stf = cwd / cs.files.state
    if not stf.exists():
        raise FileNotFoundError(f"State file '{stf.as_posix()}' not found")
    with stf.open('r') as f:
        state = json.load(f)
    try:
        yield state
    finally:
        with stf.open('w') as f:
            json.dump(state, f, indent=4)


def wexec(cmd: str, logger: logging.Logger) -> Tuple[str, str]:
    cmds = shlex.split(cmd)
    proc = sb.run(cmds, capture_output=True)
    bout = proc.stdout.decode()
    berr = proc.stderr.decode()
    if proc.returncode != 0:
        logger.error("Process returned non-zero exitcode")
        logger.error("### OUTPUT ###")
        logger.error("bout")
        logger.error("### ERROR ###")
        logger.error(berr)
        logger.error("")
        raise RuntimeError("Process returned non-zero exitcode")
    return bout, berr


def setup_logger(cwd: Path, name: str, level: int = logging.INFO) -> logging.Logger:
    folder = cwd / cs.folders.log
    folder.mkdir(exist_ok=True, parents=True)
    logfile = folder / cs.files.logfile
    folder = folder / cs.folders.pass_log
    folder.mkdir(exist_ok=True, parents=True)

    dir_list = list(folder.iterdir())
    if len(dir_list) == 0:
        last = 0
    else:
        last = max([int(file.relative_to(folder).as_posix()[len(cs.files.pass_log_prefix):-len(cs.files.pass_log_suffix)]) for file in dir_list])
    logfile_pass = folder / (cs.files.pass_log_prefix + str(last + 1) + cs.files.pass_log_suffix)

    handler = logging.FileHandler(logfile)
    handler.setFormatter(cs.sp.formatter)
    handler_pass = logging.FileHandler(logfile_pass)
    handler_pass.setFormatter(cs.sp.formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(handler_pass)

    return logger


def try_eval(equ: str, vars: Dict, logger: logging.Logger):
    logger.debug(f"    Formula: '{equ}'")
    try:
        eval_val = eval(equ,  globals(), vars)
    except NameError as e:
        logger.critical(str(e))
        logger.critical(f"Unable to evaluate '{equ}', some variables lost")
        raise
    except Exception as e:
        logger.critical(str(e))
        logger.critical(f"Unable to evaluate '{equ}', unknown error")
        raise
    logger.debug(f"    Evaluated value: {eval_val}")
    return eval_val


if __name__ == "__main__":
    pass
