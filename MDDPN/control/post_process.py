#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 06-05-2023 22:40:11

import json
import argparse
import warnings
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import numpy as np

from .utils import states
from . import constants as cs
from .. import uw_constants as ucs
from .execution import perform_processing_run


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
            dump_file: Path = cwd / cs.dumps_folder / rlabels[label][str(i)]["dump_f"]
            if not dump_file.exists():
                fl = False
                warnings.warn(f"Dump file {dump_file.as_posix()} not exists")
    return fl


def calc_xi(xilog: Path, temps: Path) -> Tuple[float, int, int]:
    xist = pd.read_csv(xilog, header=None).to_numpy(dtype=np.int32).flatten()
    tf = pd.read_csv(temps, header=None)

    temp_time = tf[0].to_numpy(dtype=np.int32)
    temp_temp = tf[1].to_numpy(dtype=np.float32)

    temp1 = temp_temp[np.abs(temp_time - xist[0]) < 2][0]
    temp2 = temp_temp[np.abs(temp_time - xist[1]) < 2][0]
    # print(f"Temp1:{temp1},temp2:{temp2}")
    # print(f"Time1:{xist[0]},time2:{xist[1]}")

    return (float(np.abs((temp1 - temp2) / (xist[0] - xist[1]))), int(xist[0]), int(xist[1]))


def end(cwd: Path, state: Dict, args: argparse.Namespace) -> Dict:
    origin = cwd / "temperature.log"
    origin.rename("temperature.log.bak")
    origin = cwd / "temperature.log.bak"
    target = cwd / "temperature.log"
    target.touch()
    with origin.open('r') as fin, target.open('w') as fout:
        for line in fin:
            if line[0] == '#':
                continue
            fout.write(line)

    if not (state_runs_check(state) and state_validate(cwd, state)):
        print("Stopped, not valid state")
        return state

    xi, step_before, step_after = calc_xi(cwd / "xi.log", target)
    print(f"XI: {xi}")

    df = []
    rlabels = state[cs.Frun_labels]

    for label in rlabels:
        for i in range(int(rlabels[label][cs.Fruns])):
            df.append(rlabels[label][str(i)]["dump_f"])

    if (stf := (cwd / ucs.data_file)).exists():
        with open(stf, 'r') as fp:
            son = json.load(fp)
        son["xi"] = xi
        son["step_before"] = step_before
        son["step_after"] = step_after
        son["storages"] = df
        son["time_step"] = state["time_step"]
        son["every"] = state[cs.Frestart_every]
        son["dump_folder"] = cs.dumps_folder
        son["data_processing_folder"] = cs.data_processing_folder

    else:
        stf.touch()
        son = {
            "step_before": step_before,
            "step_after": step_after,
            "xi": xi,
            "storages": df,
            "time_step": state["time_step"],
            "every": state[cs.Frestart_every],
            "dump_folder": cs.dumps_folder,
            "data_processing_folder": cs.data_processing_folder}

    with open(stf, 'w') as fp:
        json.dump(son, fp)

    job_id = perform_processing_run(cwd, state, args.params)

    state["post_process"] = job_id

    state[cs.state_field] = states.data_obtained
    return state


if __name__ == "__main__":
    pass
