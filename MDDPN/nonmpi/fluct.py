#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 11:59:35

# TODO:
# Change parser description
# Add description af parameter dT

import json
import argparse
from pathlib import Path

import numpy as np
from numpy import typing as npt

import adios2


def process(file: Path, xi: float, time_step: float, wevery: int, dT: float):
    hms = round(dT / xi / time_step / wevery)
    print(f"Hms is {hms} steps")

    with adios2.open(str(file), 'w') as adout:  # type: ignore
        total_steps = adout.steps()
        for step in adout:
            sti: int = step.read("step")
            dist: npt.NDArray[np.uint16] = step.read("dist")



            if step.current_step() == total_steps - 1:
                break


if __name__ == "___main__":
    parser = argparse.ArgumentParser(
        description='CHANGEME.')
    parser.add_argument('--debug', action='store_true',
                        help='Debug, prints only parsed arguments')
    parser.add_argument('fileregex', type=str, required=True,
                        help='ADIOS2 file of cluster distribution matrix')
    parser.add_argument('--dT', type=float, default=0.08,
                        help='???')

    args = parser.parse_args()
    if args.debug:
        print("Envolved args:")
        print(args)
        exit()
    cwd = Path.cwd()
    with open(cwd / "data.json", 'r') as f:
        fp = json.load(f)

    files = []
    for file in cwd.iterdir()


    process(cwd / "ntb.bp", fp['xi'], fp['dt'], fp['every'], args.dT)
