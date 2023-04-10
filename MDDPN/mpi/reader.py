#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 10-04-2023 22:59:40


import time
from pathlib import Path
from typing import Dict, Literal

from . import adios2
import numpy as np

from .mpiworks import MPIComm, MPI_TAGS


def reader(cwd: Path, mpi_comm: MPIComm, mpi_rank: int, mpi_size: int) -> Literal[0]:
    mpi_comm.Barrier()
    dasdictt: Dict[str, int | Dict[str, int]] = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA)
    ino: int = dasdictt["no"]  # type: ignore
    storages: Dict[str, int] = dasdictt["storages"]  # type: ignore
    proceeder_rank = mpi_rank + 1
    worker_counter = 0
    sync_value = 0
    print(f"MPI rank {mpi_rank}, reader, storages: {storages}")
    storage: str
    for storage in storages:
        with adios2.open(str(cwd / storage), 'r') as reader:  # type: ignore
            total_steps = reader.steps()
            i = 0
            for step in reader:
                if i < storages[storage]["begin"]:  # type: ignore
                    i += 1
                    continue
                arr = step.read('atoms')
                arr = arr[:, 2:5].astype(dtype=np.float32)
                tpl = (worker_counter + ino, mpi_rank, arr)
                print(f"MPI rank {mpi_rank}, reader, {worker_counter}")

                mpi_comm.send(obj=tpl, dest=proceeder_rank, tag=MPI_TAGS.DATA)
                worker_counter += 1
                mpi_comm.send(obj=worker_counter, dest=0, tag=MPI_TAGS.STATE)

                if i == storages[storage]["end"] + storages[storage]["begin"] - 1:  # type: ignore
                    print(f"MPI rank {mpi_rank}, reader, reached end of distribution, {storage, i, worker_counter}")
                    break
                i += 1
                while mpi_comm.iprobe(source=proceeder_rank, tag=MPI_TAGS.SERVICE):
                    sync_value: int = mpi_comm.recv(source=proceeder_rank, tag=MPI_TAGS.SERVICE)
                while worker_counter - sync_value > 50:
                    time.sleep(0.5)

                if step.current_step() == total_steps - 1:
                    print(f"MPI rank {mpi_rank}, reader, reached end of storage, {storage, i, worker_counter}")
                    break

    mpi_comm.send(obj=1, dest=proceeder_rank, tag=MPI_TAGS.SERVICE)
    print(f"MPI rank {mpi_rank}, reader finished")
    return 0
