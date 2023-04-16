#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 16-04-2023 09:13:03

from pathlib import Path
from typing import Union, List, Tuple

from mpi4py import MPI

MPIComm = Union[MPI.Intracomm, MPI.Intercomm]
GatherResponseType = List[Tuple[str, int]]


class setts():
    def __init__(self, cwd: Path, mpi_comm: MPIComm, mpi_rank: int, mpi_size: int) -> None:
        self.cwd: Path = cwd
        self.mpi_comm: MPIComm = mpi_comm
        self.mpi_rank: int = mpi_rank
        self.mpi_size: int = mpi_size
        pass
