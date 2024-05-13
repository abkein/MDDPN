#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 03-05-2024 20:31:51

import sys
import json
import logging
import functools
from enum import Enum
from contextlib import contextmanager
from typing import Generator, Dict, Any, Callable, Union

from . import constants as cs


def logs(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        old_logger = cs.sp.logger
        cs.sp.logger = old_logger.getChild(func.__name__)
        result = func(*args, **kwargs)
        cs.sp.logger = old_logger
        return result
    return wrapper


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


class RC(int, Enum):
    OK = 0
    FILE_EXISTS = 1
    FILE_NOT_FOUND = 2
    END_REACHED = 3


class AP:
    def __init__(self, executable: str, arguments: Union[str, None] = None, ppexec: Union[str, None] = None, ppargs: Union[str, None] = None) -> None:
        self.executable = executable
        self.arguments = arguments
        self.ppexec = ppexec
        self.ppargs = ppargs


@contextmanager
def load_state() -> Generator[Dict[str, Any], Dict[str, Any], None]:
    stf = cs.sp.cwd / cs.files.state
    if not stf.exists(): raise FileNotFoundError(f"State file '{stf.as_posix()}' not found")
    with stf.open('r') as f:
        state: Dict[str, Any] = json.load(f)
        cs.sp.state = state
    try: yield state
    finally:
        with stf.open('w') as f:
            json.dump(cs.sp.state, f, indent=4)


def setup_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    folder = cs.sp.cwd / cs.folders.log
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

    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(level)

    handler = logging.FileHandler(logfile)
    handler.setFormatter(cs.sp.formatter)
    logger.addHandler(handler)

    handler_pass = logging.FileHandler(logfile_pass)
    handler_pass.setFormatter(cs.sp.formatter)
    logger.addHandler(handler_pass)

    if not cs.sp.args.no_screen:
        soutHandler = logging.StreamHandler(stream=sys.stdout)
        soutHandler.setLevel(logging.DEBUG)
        soutHandler.setFormatter(cs.sp.screen_formatter)
        logger.addHandler(soutHandler)
        serrHandler = logging.StreamHandler(stream=sys.stderr)
        serrHandler.setFormatter(cs.sp.screen_formatter)
        serrHandler.setLevel(logging.WARNING)
        logger.addHandler(serrHandler)

    return logger


def gsr(label: str, obj, cnt: int):
    if isinstance(obj, dict):
        if label == list(obj.keys())[0]:
            return obj[label]*cnt

    return obj


if __name__ == "__main__":
    pass
