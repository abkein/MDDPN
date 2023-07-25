#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 15:56:36

# TODO:
# Change parser description
# Add description af parameter dT

import re
import csv
import json
import argparse
from typing import List
from pathlib import Path

import adios2
import numpy as np
from numpy import typing as npt

from .. import constants as cs


def rms(arr: npt.NDArray[np.uint32]) -> npt.NDArray[np.float32]:
    N = len(arr)
    average = np.sum(arr, axis=0) / N
    barr = np.sum((average - arr[:])**2, axis=0)
    # print(barr)
    return np.sqrt(barr / N)


def process(file: Path, hms: int, lc: int, cut: int = 100):
    # with adios2.open(file.as_posix(), 'r') as adout:  # type: ignore
    #     for step in adout:
    #         dist: npt.NDArray[np.uint32] = step.read("dist")
    #         break

    buffer: npt.NDArray[np.uint32] = np.zeros((hms, cut), dtype=np.uint32)  # type: ignore

    with adios2.open(file.as_posix(), 'r') as adout:  # type: ignore
        total_steps = adout.steps()
        write_file = Path(file.as_posix() + ".csv")
        write_file.touch()
        with open(write_file, "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            counter = lc
            for step in adout:
                dist: npt.NDArray[np.uint32] = step.read(cs.cf.mat_dist)
                dist = dist[:cut]

                buffer[counter, :] = dist

                counter += 1

                if counter == hms:
                    writer.writerow(rms(buffer))
                    counter = 0
                    csv_file.flush()
                    print("written")

                if step.current_step() == total_steps - 1:
                    break
    return counter


def main():
    parser = argparse.ArgumentParser(prog="fluct.py", description='CHANGEME.')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')
    parser.add_argument('--dT', type=float, default=0.08, help='???')
    parser.add_argument('--re', type=str, help='File regex')

    args = parser.parse_args()
    if args.debug:
        print("Envolved args:")
        print(args)
        exit()
    cwd = Path.cwd()
    with open(cwd / cs.files.data, 'r') as f:
        fp = json.load(f)

    folder: Path = cwd / fp[cs.cf.data_processing_folder]

    storages: List[str] = []
    stf: Path
    for stf in folder.iterdir():
        if re.match(args.re, stf.relative_to(folder).as_posix()):
            storages.append(stf.relative_to(folder).as_posix())

    for file in storages:
        stf = cwd / fp[cs.cf.data_processing_folder] / file
        if not stf.exists():
            raise FileNotFoundError(f"File {stf.as_posix()} cannot be found")

    try:
        _temp = (fp[cs.cf.xi], fp[cs.cf.time_step], fp[cs.cf.every])
    except KeyError:
        raise

    hms = round(args.dT / fp[cs.cf.xi] / fp[cs.cf.time_step] / fp[cs.cf.every])
    print(f"Hms is {hms} steps")

    lc = 0
    for file in storages:
        stf = cwd / fp[cs.cf.data_processing_folder] / file
        lc = process(stf, hms, lc)


if __name__ == "___main__":
    main()
