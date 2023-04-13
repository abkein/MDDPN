#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 11-04-2023 21:09:21

from pathlib import Path
from typing import Dict
import csv

import pandas as pd
import numpy as np
from numpy import typing as npt
import freud

from . import adios2
from .mpiworks import MPIComm, MPI_TAGS
from ..core.distribution import get_dist
from ..core import calc


def thread(mpi_comm: MPIComm, mpi_rank: int, mpi_size: int):
    mpi_comm.Barrier()

    dasdictt: Dict[str, int | Dict[str, int]] = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA_1)
    ino: int = dasdictt["no"]  # type: ignore
    storages: Dict[str, int] = dasdictt["storages"]  # type: ignore

    N_atoms: int
    bdims: npt.NDArray[np.float32]

    N_atoms, bdims = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA_2)
    box = freud.box.Box.from_box(bdims)
    volume = box.volume
    sizes: npt.NDArray[np.uint32] = np.arange(1, N_atoms + 1, dtype=np.uint64)

    cwd: Path
    kmax: int
    g: int
    dt: float
    dis: int
    cwd, kmax, g, dt, dis = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA_3)

    temperatures_mat = pd.read_csv(cwd / "temperature.log", header=None)
    temptime = temperatures_mat[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures_mat[1].to_numpy(dtype=np.float64)

    worker_counter = 0
    print(f"MPI rank {mpi_rank}, reader, storages: {storages}")
    output_csv_fp = (cwd / f"rdata.{mpi_rank}.csv").as_posix()
    ntb_fp = (cwd / f"ntb.{mpi_rank}.bp").as_posix()
    with adios2.open(ntb_fp, 'w') as adout, open(output_csv_fp, "w") as csv_file:  # type: ignore
        writer = csv.writer(csv_file, delimiter=',')
        storage: str
        for storage in storages:
            storage_fp = (cwd / storage).as_posix()
            with adios2.open(storage_fp, 'r') as reader:  # type: ignore
                total_steps = reader.steps()
                i = 0
                for step in reader:
                    if i < storages[storage]["begin"]:  # type: ignore
                        i += 1
                        continue
                    arr = step.read('atoms')
                    arr = arr[:, 2:5].astype(dtype=np.float32)

                    stepnd = worker_counter + ino

                    # print(f"MPI rank {mpi_rank}, reader, {worker_counter}")

                    dist = get_dist(arr, N_atoms, box)

                    # write to ntb
                    adout.write("step", np.array(stepnd))  # type: ignore
                    adout.write("dist", dist, dist.shape, np.full(len(dist.shape), 0), dist.shape, end_step=True)  # type: ignore

                    temp = temperatures[np.abs(temptime - int(stepnd * dis)) <= 1][0]
                    tow = calc.get_row(stepnd, sizes, dist, temp, N_atoms, kmax, g, volume, dt, dis)

                    # write tp csv
                    writer.writerow(tow)
                    csv_file.flush()

                    worker_counter += 1
                    mpi_comm.send(obj=worker_counter, dest=0, tag=MPI_TAGS.STATE)

                    if i == storages[storage]["end"] + storages[storage]["begin"] - 1:  # type: ignore
                        print(f"MPI rank {mpi_rank}, reader, reached end of distribution, {storage, i, worker_counter}")
                        break

                    i += 1

                    if step.current_step() == total_steps - 1:
                        print(f"MPI rank {mpi_rank}, reader, reached end of storage, {storage, i, worker_counter}")
                        break


if __name__ == "__main__":
    pass
