# -*- coding: utf-8 -*-

# First created by Egor Perevoshchikov at 2022-10-29 15:41.
# Last-update: 2023-02-19 19:22:35

import time
import secrets
from typing import List, Tuple, Union, Dict
from mpi4py import MPI
from enum import Enum
import adios2
from pathlib import Path
import numpy as np
import csv

import warnings
import functools
import sys, os

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

MPIComm = Union[MPI.Intracomm, MPI.Intercomm]
GatherResponseType = List[Tuple[str, int]]


class MPISanityError(RuntimeError):
    pass

# class SpecialObject():
#     pass


class MPI_TAGS(int, Enum):
    SANITY = 0
    DISTRIBUTION = 1
    NEIGHBORS = 2
    SERV_DATA = 3
    TO_ACCEPT = 4
    WRITE = 5
    DATA = 6
    SERVICE = 7
    ONE_WRITE = 8
    ONLINE = 9


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func


def base_sanity(mpi_size, mpi_rank, min):
    if mpi_size == 1:
        print('You are running an MPI program with only one slot/task!')
        print('Are you using `mpirun` (or `srun` when in SLURM)?')
        print('If you are, then please use an `-n` of at least 2!')
        print('(Or, when in SLURM, use an `--ntasks` of at least 2.)')
        print('If you did all that, then your MPI setup may be bad.')
        raise MPISanityError("Only one execution thread was started")

    if mpi_size < min:
        print(
            f"This program requires at least {min} mpi tasks, but world size is only {mpi_size}")
        raise MPISanityError(f"Number of started threads is not enought to properly run this app. You must run at least {min} threads")

    if mpi_size >= 1000 and mpi_rank == 0:
        print('WARNING:  Your world size {} is over 999!'.format(mpi_size))
        print("The output formatting will be a little weird, but that's it.")

    return 0


def root_sanity(mpi_comm: MPIComm, no_print: bool = False):
    # if no_print:
    #     blockPrint()

    random_number = secrets.randbelow(round(time.time()))
    mpi_comm.bcast(random_number)
    print('Controller @ MPI Rank   0:  Input {}'.format(random_number))

    response_array = mpi_comm.gather(None)  # type: GatherResponseType

    mpi_size = mpi_comm.Get_size()
    if len(response_array) != mpi_size:
        print(
            f"ERROR!  The MPI world has {mpi_size} members, but we only gathered {len(response_array)}!")
        return 1

    for i in range(1, mpi_size):
        if len(response_array[i]) != 2:
            print(
                f"WARNING!  MPI rank {i} sent a mis-sized ({len(response_array[i])}) tuple!")
            continue
        if type(response_array[i][0]) is not str:
            print(
                f"WARNING!  MPI rank {i} sent a tuple with a {str(type(response_array[i][0]))} instead of a str!")
            continue
        if type(response_array[i][1]) is not int:
            print(
                f"WARNING!  MPI rank {i} sent a tuple with a {str(type(response_array[i][1]))} instead of an int!")
            continue

        if random_number + i == response_array[i][1]:
            result = 'OK'
        else:
            result = 'BAD'

        print(
            f"Worker at MPI Rank {i}: Output {response_array[i][1]} is {result} (from {response_array[i][0]})")

        mpi_comm.send(obj=0, dest=i, tag=MPI_TAGS.SANITY)

    # if no_print:
    #     enablePrint()
    return 0


def nonroot_sanity(mpi_comm: MPIComm):
    mpi_rank = mpi_comm.Get_rank()

    random_number = mpi_comm.bcast(None)  # type: int

    # Sanity check: Did we actually get an int?
    if type(random_number) is not int:
        print(
            f"ERROR in MPI rank {mpi_rank}: Received a non-integer '{random_number}' from the broadcast!")
        return 1

    # Our response is the random number + our rank
    response_number = random_number + mpi_rank
    response = (
        MPI.Get_processor_name(),
        response_number,
    )
    mpi_comm.gather(response)

    def get_message(mpi_comm: MPIComm) -> Union[int, None]:
        message = mpi_comm.recv(source=0, tag=MPI_TAGS.SANITY)  # type: int
        if type(message) is not int:
            print(
                f"ERROR in MPI rank {mpi_rank}: Received a non-integer message!")
            return None
        else:
            return message

    message = get_message(mpi_comm)
    while (message is not None) and (message != 0):
        mpi_comm.send(obj=int(message / 2), dest=0, tag=MPI_TAGS.SANITY)
        message = get_message(mpi_comm)

    # Did we get an error?
    if message is None:
        return 1
    return 0


def ad_mpi_writer(file: Path, mpi_comm: MPIComm, mpi_rank: int, mpi_size: int):
    mpi_comm.Barrier()
    threads = mpi_comm.recv(source=0, tag=MPI_TAGS.TO_ACCEPT)  # type: List[int]
    with adios2.open(str(file), 'w') as adout:  # type: ignore
        while True:
            for thread in threads:
                if mpi_comm.iprobe(source=thread, tag=MPI_TAGS.WRITE):
                    step, arr = mpi_comm.recv(source=thread, tag=MPI_TAGS.WRITE)  # type: Tuple[int, np.ndarray]
                    adout.write("step", np.array(step))
                    adout.write("dist", arr, arr.shape, np.full(len(arr.shape), 0), arr.shape, end_step=True)
                    mpi_comm.send(obj=step, dest=0, tag=MPI_TAGS.SERVICE)


def csvWriter(file: Path, mpi_comm: MPIComm, mpi_rank, mpi_size):
    mpi_comm.Barrier()
    threads = mpi_comm.recv(source=0, tag=MPI_TAGS.TO_ACCEPT)  # type: List[int]

    ctr = 0

    with open(file, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        while True:
            for thread in threads:
                if mpi_comm.iprobe(source=thread, tag=MPI_TAGS.WRITE):
                    data = mpi_comm.recv(source=thread, tag=MPI_TAGS.WRITE)  # type: np.ndarray
                    writer.writerow(data)
                    ctr += 1
                    mpi_comm.send(obj=ctr, dest=0, tag=MPI_TAGS.SERVICE)
