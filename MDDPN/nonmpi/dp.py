#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 16:44:43

import csv
import json
import argparse
from pathlib import Path
from typing import Dict, Tuple, Union

import numpy as np
import pandas as pd
from numpy import typing as npt

from ..core import calc
from .. import constants as cs


BUF_SIZE = 65536  # 64kb


def find_file(cwd: Path, file: Union[str, None], subf: str, defname: str) -> Tuple[bool, Path]:
    if (cwd / subf).exists():
        if file is None:
            if (f := (cwd / subf / defname)).exists():
                return True, f
            raise FileNotFoundError(f"File {subf}/{defname} cannot be found")
        elif (f := (cwd / subf / file)).exists():
            return True, f
        raise FileNotFoundError(f"File {subf}/{file} cannot be found")
    elif file is None:
        if (f := (cwd / defname)).exists():
            return False, f
        raise FileNotFoundError(f"File {defname} cannot be found")
    elif (f := (cwd / file)).exists():
        return False, f
    raise FileNotFoundError(f"File {file} cannot be found")


def run(infile: Path, outfile: Path, conf: Dict, temp_mat: Tuple[npt.NDArray[np.uint64], npt.NDArray[np.float32]], cut: int, km: int):
    temptime, temperatures = temp_mat
    dis = conf[cs.cf.every]
    N_atoms = conf[cs.cf.N_atoms]
    time_step = conf[cs.cf.time_step]
    volume = conf[cs.cf.volume]
    sizes: npt.NDArray[np.uint32] = np.arange(1, cut + 1, 1, dtype=np.uint32)
    with pd.read_csv(infile, header=None, chunksize=BUF_SIZE) as reader, open(outfile, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        chunk: pd.DataFrame
        for chunk in reader:
            for index, row in chunk.iterrows():
                step = int(row[0])
                dist = row[1:].to_numpy(dtype=np.uint32)
                # print(dist)
                temp: float = temperatures[np.abs(temptime - int(step * dis)) <= 1][0]  # type: ignore
                tow = calc.get_row(step, sizes, dist, temp, N_atoms, volume, time_step, dis, km)
                writer.writerow(tow)


def ncut(infile: Path) -> int:
    with infile.open("r") as f:
        reader = csv.reader(f, delimiter=",")
        for line in reader:
            return len(line) - 1
    return 0


def km(infile: Path, conf: Dict, cut: int, eps: float) -> int:
    fst = round(conf[cs.cf.step_before] / conf[cs.cf.every])
    N_atoms = conf[cs.cf.N_atoms]
    sizes = np.arange(1, cut + 1, 1)
    # index = np.array([], dtype=np.uint32)
    with pd.read_csv(infile, header=None, chunksize=BUF_SIZE) as reader:
        chunk: pd.DataFrame
        for chunk in reader:
            index = chunk[0].to_numpy(dtype=np.uint32)
            if len(index[index == fst]) == 1:
                ind = np.where(index == fst)
                dist = chunk.iloc[ind].to_numpy(dtype=np.uint32)[0][1:]  # type: ignore
                # print(dist)

                ld = np.array([np.sum(sizes[:i]*dist[:i]) / N_atoms for i in range(1, len(dist))], dtype=np.float32)
                km = np.argmin(np.abs(ld - eps))
                return int(km)
    raise KeyError(f"Step before {conf[cs.cf.step_before]}/{conf[cs.cf.every]}={fst} not found in matrix")


def main():
    parser = argparse.ArgumentParser(description='Process some floats.')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')
    parser.add_argument('--file', action='store', type=str, default=None, required=False, help='File to proceed')
    # parser.add_argument('--mode', action='store', type=int, default=3, help='Mode to run')

    args = parser.parse_args()

    if args.debug:
        print("Envolved args:")
        print(args)

    cwd = Path.cwd()
    conf_file = cwd / cs.files.data  # "data.json"

    with conf_file.open('r') as f:
        son = json.load(f)

    subf = son[cs.cf.data_processing_folder]

    mode, data_file = find_file(cwd, args.file, subf, cs.files.cluster_distribution_matrix)

    outfile = cwd / cs.files.comp_data

    temperatures_mat = pd.read_csv(cwd / cs.files.temperature, header=None)
    temptime = temperatures_mat[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures_mat[1].to_numpy(dtype=np.float32)

    cut = ncut(data_file)

    kmin = km(data_file, son, cut, 0.9)

    print(f"kmin is {kmin}")

    run(data_file, outfile, son, (temptime, temperatures), cut, kmin)


if __name__ == "__main__":
    import sys
    sys.exit(main())
