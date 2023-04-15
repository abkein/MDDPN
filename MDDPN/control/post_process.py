#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 15-04-2023 14:31:20

import argparse
import warnings
from typing import Dict
from pathlib import Path

from .utils import states
from . import constants as cs
from .execution import perform_processing_run

from .. import uw_constants as ucs


def state_runs_check(state: dict) -> bool:
    fl = True
    rlabels = state[cs.Frun_labels]
    for label in rlabels:
        rc = 0
        while str(rc) in rlabels[label]:
            rc += 1
        prc = rlabels[label][cs.Fruns]
        if prc != rc:
            fl = False
            warnings.warn(f"Label {label} runs: present={prc}, real={rc}, changing")
    return fl


def state_validate(cwd: Path, state: dict) -> bool:
    fl = True
    rlabels = state[cs.Frun_labels]
    for label in rlabels:
        for i in range(int(rlabels[label][cs.Fruns])):
            dump_file: Path = cwd / ucs.data_processing_folder / rlabels[label][str(i)]["dump_f"]
            if not dump_file.exists():
                fl = False
                warnings.warn(f"Dump file {dump_file.as_posix()} not exists")
    return fl


def end(cwd: Path, state: Dict, args: argparse.Namespace):

    origin = cwd / "temperature.log"
    origin.rename("temperature.log.bak")
    target = cwd / "temperature.log"
    target.touch()
    with origin.open('r') as fin, target.open('w') as fout:
        for line in fin:
            if line[0] == '#':
                continue
            fout.write(line)

    if not (state_runs_check(state) and state_validate(cwd, state)):
        print("Stopped")
        return state

    df = []
    rlabels = state[cs.Frun_labels]

    for label in rlabels:
        for i in range(int(rlabels[label][cs.Fruns])):
            df.append(rlabels[label][str(i)]["dump_f"])

    job_id = perform_processing_run(cwd, state, df, args.params)

    state["post_process"] = job_id
    print(f"Sbatch job id: {job_id}")

    state[cs.state_field] = states.data_obtained
    return state


if __name__ == "__main__":
    pass
