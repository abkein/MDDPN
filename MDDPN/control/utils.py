#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 09-09-2023 23:22:22

import json
import logging
import argparse
from enum import Enum
from pathlib import Path
from typing import Generator, Dict
from contextlib import contextmanager

from . import constants as cs


class states(str, Enum):
    initialized = "initialized"
    fully_initialized = "fully_initialized"
    started = "started"
    restarted = "restarted"
    comleted = "comleted"
    post_processor_called = "post_processor_called"
    post_process_done = "post_process_done"


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
