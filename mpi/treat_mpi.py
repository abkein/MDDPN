#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# First created by Egor Perevoshchikov at 2022-10-29 15:41.
# Last-update: 2023-02-19 19:23:27

import numpy as np
from pathlib import Path
import pandas as pd
import freud
from mpiworks import MPI_TAGS, MPIComm
from typing import Tuple, Union
import time


def mean_size(sizes: np.ndarray, dist: np.ndarray) -> float:
    return np.sum(sizes * dist) / np.sum(dist)


def xas(dist: np.ndarray, N: int) -> float:
    return 1 - dist[0] / N


def maxsize(sizes: np.ndarray, dist: np.ndarray) -> int:
    return sizes[np.nonzero(dist)][-1]


def nvv(sizes: np.ndarray, dist: np.ndarray, volume: float, kmin: int) -> float:
    return np.sum(dist[sizes <= kmin] * sizes[sizes <= kmin]) / volume


def nd(sizes: np.ndarray, dist: np.ndarray, volume: float, kmin: int) -> float:
    return np.sum(dist[sizes >= kmin]) / volume


def nl(T: float) -> float:
    if T <= 0.6:
        return 0.896
    else:
        return -0.9566 * np.exp(0.601 * T) + 2.25316


def sigma(T: float) -> float:
    if T <= 0.6:
        return -2.525 * T + 2.84017
    else:
        return -5.086 * T + 4.21614


def nvs(sizes: np.ndarray, dist: np.ndarray, volume: float, kmin: int, T: float) -> Union[float, None]:
    ms = sizes[-1]
    n1 = dist[0] / volume
    dzd = dist[kmin - 1:ms]
    kks = np.arange(kmin, ms + 1, dtype=np.uint32)**(1 / 3)
    num = n1 * np.sum(kks**2 * dzd) / volume
    rl = (3 / (4 * np.pi * nl(T)))**(1 / 3)
    cplx = 2 * sigma(T) / (nl(T) * T * rl)
    denum = np.sum(kks**2 * dzd * np.exp(cplx / kks)) / volume

    if num == 0:
        return None
    return num / denum


def treat_mpi(mpi_comm: MPIComm, mpi_rank: int, mpi_size: int):
    mpi_comm.Barrier()
    proc_rank = mpi_rank - 1
    cwd, N_atoms, bdims, kmax, g, dt, dis = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA)  # type: Tuple[Path, int, np.ndarray, int, int, float, int]
    box = freud.box.Box.from_box(bdims)
    volume = box.volume
    sizes = np.arange(1, N_atoms + 1)

    temperatures = pd.read_csv(cwd / "temperature.log", header=None)
    temptime = temperatures[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures[1].to_numpy(dtype=np.float64)

    # receiving_time = 0
    # mtr = 0
    # logic_time = 0
    # sending_time = 0

    while True:

        # start = time.time()

        step, dist = mpi_comm.recv(source=proc_rank, tag=MPI_TAGS.DATA)  # type: Tuple[int, np.ndarray]

        # middle1 = time.time()

        temp = temperatures[temptime == int(step * dis)]
        tow = np.zeros(9, dtype=np.float64)
        tow[0] = step * dt * dis
        tow[1] = mean_size(sizes, dist)
        tow[2] = maxsize(sizes, dist)
        tow[3] = xas(dist, N_atoms)
        tow[4] = nvv(sizes, dist, volume, kmax)
        tow[5] = nd(sizes, dist, volume, g)
        tow[6] = nvs(sizes, dist, volume, kmax, temp)
        tow[7] = temp
        tow[8] = step

        # middle2 = time.time()

        mpi_comm.send(obj=tow, dest=1, tag=MPI_TAGS.WRITE)

        mpi_comm.send(obj=step, dest=0, tag=MPI_TAGS.SERVICE)

        # end = time.time()

        if mpi_comm.iprobe(source=proc_rank, tag=MPI_TAGS.SERVICE) and not mpi_comm.iprobe(source=proc_rank, tag=MPI_TAGS.DATA):
            if mpi_comm.recv(source=proc_rank, tag=MPI_TAGS.SERVICE) == 1:
                break

        # receiving_time += middle1 - start
        # logic_time += middle2 - middle1
        # sending_time += end - middle2
        # mtr += 1
        # print(f"Treater: receiving: {receiving_time/mtr}, logic: {logic_time/mtr}, sending: {sending_time/mtr}")

    print(f"MPI rank {mpi_rank}, treater finished")
    return 0
