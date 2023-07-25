#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 12:51:54

import json
import argparse
from enum import Enum
from pathlib import Path
from typing import Generator
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


class LogicError(Exception):
    pass


def com_set(cwd: Path, args: argparse.Namespace):
    file = cwd / args.file
    if not file.exists():
        raise FileNotFoundError(f"There is no file {file.as_posix()}")
    with file.open('r') as f:
        fp = json.load(f)
    fp[args.variable] = args.value
    with file.open('w') as f:
        json.dump(fp, f)
    return 0


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
            json.dump(state, f)
