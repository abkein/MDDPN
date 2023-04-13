#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 20:38:36

from pathlib import Path
import argparse
import json
import warnings

from .execution import perform_processing_run
from .constants import state_file


def state_runs_repair(state: dict) -> dict:
    for label in state["run_labels"]:
        rc = 0
        while str(rc) in state["run_labels"][label]:
            rc += 1
        prc = state["run_labels"][label]["runs"]
        if prc != rc:
            warnings.warn(f"Label {label} runs: present={prc}, real={rc}, changing")
        state["run_labels"][label]["runs"] = rc
    return state


def state_validate(cwd: Path, state: dict) -> bool:
    fl = True
    for label in state["run_labels"]:
        for i in range(int(state["run_labels"][label]["runs"])):
            dump_file: Path = cwd / state["run_labels"][label][str(i)]["dump_f"]
            if not dump_file.exists():
                fl = False
                warnings.warn(f"Dump file {dump_file.as_posix()} not exists")
    return fl


def end(cwd: Path, args: argparse.Namespace):

    origin = cwd / "temperature.log"
    origin.rename("temperature.log.bak")
    target = cwd / "temperature.log"
    target.touch()
    with origin.open('r') as fin, target.open('w') as fout:
        for line in fin:
            if line[0] == '#':
                continue
            fout.write(line)
    # origin.unlink()

    if args.files is None:
        if not (cwd / state_file).exists():
            raise FileNotFoundError("State file 'state.json' not found")
        with (cwd / state_file).open('r') as f:
            state = json.load(f)
        state = state_runs_repair(state)
        if not state_validate(cwd, state):
            return 1

        df = []

        for label in state["run_labels"]:
            for i in range(int(state["run_labels"][label]["runs"])):
                df.append(state["run_labels"][label][str(i)]["dump_f"])
        print(df)
        with (cwd / state_file).open('w') as f:
            json.dump(state, f)
    else:
        df = json.loads(args.files)

    # print("#####################OK#####################")
    # return

    job_id = perform_processing_run(cwd, {}, df, args.params)

    # state["post_process"] = job_id
    print(f"SBJID: {job_id}")

    # with (cwd / state_file).oplisten('r') as f:
    #     state = json.load(f)
    # time_step = state['variables']['dt']

    # temperatures = pd.read_csv(cwd / "temperature.log", header=None)
    # temptime = temperatures[0].to_numpy(dtype=np.uint64)
    # temperatures = temperatures[1].to_numpy(dtype=np.float64)

    # xilog = pd.read_csv(
    #     cwd / "xi.log", header=None).to_numpy(dtype=np.uint32).flatten()
    # xilog -= 1
    # dtemp_xi = temperatures[temptime == xilog[0]] - \
    #     temperatures[temptime == xilog[1]]
    # xi = dtemp_xi / (xilog[1] - xilog[0]) / time_step

    # state['xi'] = xi[0]

    # (cwd / state_file).unlink()
    # (cwd / state_file).touch()
    # state["state"] = states.data_obtained
