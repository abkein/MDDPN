#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 15-04-2023 23:06:56

# TODO:
# Change parser description
# Add description af parameter dT

import csv
import json
import argparse
from typing import List
from pathlib import Path

import numpy as np
from numpy import typing as npt

import adios2
from .. import uw_constants as ucs


def rms(arr: npt.NDArray[np.uint32]) -> npt.NDArray[np.float32]:
    N = len(arr)
    average = np.sum(arr, axis=0) / N
    barr = np.sum((average - arr[:])**2, axis=0)
    return np.sqrt(barr / N)


def process(file: Path, xi: float, time_step: float, wevery: int, dT: float):
    hms = round(dT / xi / time_step / wevery)
    print(f"Hms is {hms} steps")

    with adios2.open(file.as_posix(), 'w') as adout:  # type: ignore
        for step in adout:
            dist: npt.NDArray[np.uint32] = step.read("dist")
            break

    buffer: npt.NDArray[np.uint32] = np.zeros((hms, len(dist)), dtype=np.uint32)  # type: ignore

    with adios2.open(file.as_posix(), 'w') as adout:  # type: ignore
        total_steps = adout.steps()
        write_file = Path(file.as_posix() + ".csv")
        write_file.touch()
        with open(write_file, "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            counter = 0
            for step in adout:
                dist: npt.NDArray[np.uint32] = step.read("dist")

                buffer[counter, :] = dist

                if counter == hms:
                    writer.writerow(rms(buffer))
                    counter = 0
                    csv_file.flush()

                if step.current_step() == total_steps - 1:
                    break


if __name__ == "___main__":
    parser = argparse.ArgumentParser(prog="fluct.py", description='CHANGEME.')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')
    parser.add_argument('--dT', type=float, default=0.08, help='???')
    parser.add_argument('storages', type=str, help='Folder in which search for ADIOS2 distribution matrices')

    args = parser.parse_args()
    if args.debug:
        print("Envolved args:")
        print(args)
        exit()
    cwd = Path.cwd()
    with open(cwd / ucs.data_file, 'r') as f:
        fp = json.load(f)

    storages: List[str] = json.loads(args.storages)

    for file in storages:
        if not (cwd / ucs.data_processing_folder / file).exists():
            raise FileNotFoundError(f"File {(cwd / ucs.data_processing_folder / file).as_posix()} cannot be found")

    try:
        _temp = (fp['xi'], fp['dt'], fp['every'])
    except KeyError:
        raise
    for file in storages:
        process(cwd / ucs.data_processing_folder / file, fp['xi'], fp['dt'], fp['every'], args.dT)
