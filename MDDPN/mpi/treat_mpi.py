#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-04-2023 21:44:42


import os
import time
from pathlib import Path
from typing import Literal


os.environ['OPENBLAS_NUM_THREADS'] = '1'


import freud
import numpy as np
from numpy import typing as npt
import pandas as pd

from .mpiworks import MPI_TAGS, MPIComm
from ..core import calc


def treat_mpi(mpi_comm: MPIComm, mpi_rank: int, mpi_size: int) -> Literal[0]:
    mpi_comm.Barrier()
    proc_rank = mpi_rank - 1
    cwd: Path
    N_atoms: int
    bdims: npt.NDArray[np.float32]
    kmax: int
    g: int
    dt: float
    dis: int
    cwd, N_atoms, bdims, kmax, g, dt, dis = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA)
    box = freud.box.Box.from_box(bdims)
    volume = box.volume
    sizes: npt.NDArray[np.uint32] = np.arange(1, N_atoms + 1, dtype=np.uint64)

    temperatures = pd.read_csv(cwd / "temperature.log", header=None)
    temptime = temperatures[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures[1].to_numpy(dtype=np.float64)

    while True:
        step: int
        dist: npt.NDArray[np.uint32]
        step, dist = mpi_comm.recv(source=proc_rank, tag=MPI_TAGS.DATA)

        try:
            temp = temperatures[np.abs(temptime - int(step * dis)) <= 1][0]
            tow = calc.get_row(step, sizes, dist, temp, N_atoms, kmax, g, volume, dt, dis)
        except Exception as e:
            etime = time.time()
            eid = round(etime) * mpi_rank
            print(f"{time.strftime('%d:%m:%Y %H:%M:%S', time.gmtime(etime))} MPI RANK: {mpi_rank}, treater. Handled exception {eid}:")
            print(e)
            print(f"END OF EXCEPTION {eid}")
            tow = np.zeros(10, dtype=np.float32)

        mpi_comm.send(obj=tow, dest=1, tag=MPI_TAGS.WRITE)
        mpi_comm.send(obj=step, dest=0, tag=MPI_TAGS.STATE)

        if mpi_comm.iprobe(source=proc_rank, tag=MPI_TAGS.SERVICE) and not mpi_comm.iprobe(source=proc_rank, tag=MPI_TAGS.DATA):
            if mpi_comm.recv(source=proc_rank, tag=MPI_TAGS.SERVICE) == 1:
                break

    print(f"MPI rank {mpi_rank}, treater finished")
    return 0


if __name__ == "__main__":
    pass
