#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-05-2023 23:04:50


import os
import csv
import time
import json
import argparse
from enum import Enum
from math import floor
from pathlib import Path
from datetime import datetime
from typing import Dict, Union, Tuple, List, Any


os.environ['OPENBLAS_NUM_THREADS'] = '1'


import numpy as np
from mpi4py import MPI
from numpy import typing as npt
from . import adios2

from . import reader
from . import one_threaded
from . import mpiworks as MW
from . import fastd_mpi as fd
from . import treat_mpi as treat
from .mpiworks import MPI_TAGS, MPISanityError
from .utils import setts
from . import matrice

from .. import uw_constants as ucs


class Role(Enum):
    reader = 0
    proceeder = 1
    treater = 2
    kill = 3
    one_thread = 4
    csvWriter = 5
    ad_mpi_writer = 6
    matr = 7


def bearbeit(folder: Path, storages: Dict[str, int]) -> Tuple[int, npt.NDArray[np.float32]]:
    storage = list(storages.keys())[0]
    adin = adios2.open((folder / storage).as_posix(), 'r')  # type: ignore

    N = int(adin.read('natoms'))
    Lx = float(adin.read('boxxhi'))
    Ly = float(adin.read('boxyhi'))
    Lz = float(adin.read('boxzhi'))

    adin.close()

    bdims = np.array([Lx, Ly, Lz])
    return (N, bdims)


def storage_rsolve(dump_folder: Path, _storages: List[str]) -> Dict[str, int]:
    storages = {}
    for storage in _storages:
        file = dump_folder / storage
        if not file.exists():
            raise FileNotFoundError(f"Storage {file} cannot be found")
        with adios2.open(file.as_posix(), 'r') as reader_c:  # type: ignore
            storages[storage] = reader_c.steps()
    return storages


def distribute(storages: Dict[str, int], mm: int) -> Dict[str, Dict[str, Union[int, Dict[str, int]]]]:
    ll = sum(list(storages.values()))
    dp = np.linspace(0, ll - 1, mm + 1, dtype=int)
    bp = dp
    dp = dp[1:] - dp[:-1]
    dp = np.vstack([bp[:-1].astype(dtype=int), np.cumsum(dp).astype(dtype=int)])
    wd = {}
    st = {}
    for storage, value in storages.items():
        st[storage] = value
    ls = 0
    for i, (begin_, end_) in enumerate(dp.T):
        begin = int(begin_)
        end = int(end_)
        beg = 0 + ls
        en = end - begin
        wd[str(i)] = {"no": begin, "storages": {}}
        for storage in list(st):
            value = st[storage]
            if en >= value:
                wd[str(i)]["storages"][storage] = {"begin": beg, "end": value}
                en -= value
                ls = 0
                beg = 0
                del st[storage]
            elif en < value:
                wd[str(i)]["storages"][storage] = {"begin": beg, "end": en}
                st[storage] -= en
                ls += en
                break
    return wd


def after_ditribution(sts: setts, m: int):
    cwd, mpi_comm, mpi_rank, mpi_size = sts.cwd, sts.mpi_comm, sts.mpi_rank, sts.mpi_size
    mpi_comm.Barrier()
    print(f"MPI rank {mpi_rank}, barrier off")
    states = {}

    response_array = []
    while True:
        for i in range(1, mpi_size):
            if mpi_comm.iprobe(source=i, tag=MPI_TAGS.ONLINE):
                resp = mpi_comm.recv(source=i, tag=MPI_TAGS.ONLINE)
                print(f"Recieved from {i}: {resp}")
                states[str(i)] = {}
                states[str(i)]['name'] = resp
                response_array.append((i, resp))
        if len(response_array) == mpi_size - 1:
            break
    mpi_comm.Barrier()
    print(f"MPI rank {mpi_rank}, second barrier off")
    completed_threads = []
    fl = True
    start = time.time()
    while fl:
        for i in range(m, mpi_size):
            if mpi_comm.iprobe(source=i, tag=MPI_TAGS.STATE):
                tstate = mpi_comm.recv(source=i, tag=MPI_TAGS.STATE)
                if tstate == -1:
                    completed_threads.append(i)
                    print(f"MPI ROOT, rank {i} has completed")
                    if len(completed_threads) == mpi_size - m:
                        with open(cwd / "st.json", "w") as fp:
                            json.dump(states, fp)
                        fl = False
                        break
                else:
                    states[str(i)]['state'] = tstate
        if time.time() - start > 20:
            with open(cwd / "st.json", "w") as fp:
                json.dump(states, fp)
            start = time.time()
    for i in range(1, m):
        mpi_comm.send(obj=-1, dest=i, tag=MPI_TAGS.COMMAND)
    print("MPI ROOT: exiting...")

    return 0


def perform_group_run(sts: setts, params: Dict, nv: int):
    mpi_comm, mpi_size = sts.mpi_comm, sts.mpi_size

    mpi_comm.send(obj=Role.csvWriter, dest=1, tag=MPI_TAGS.DISTRIBUTION)
    mpi_comm.send(obj=Role.ad_mpi_writer, dest=2, tag=MPI_TAGS.DISTRIBUTION)

    thread_len = 3
    thread_num = floor((mpi_size - nv) / thread_len)
    print(f"Thread num: {thread_num}")
    wd: Dict[str, Dict[str, int | Dict[str, int]]] = distribute(params["storages"], thread_num)
    print("Distribution")
    print(json.dumps(wd, indent=4))

    for i in range(thread_num):
        for j in range(thread_len):
            mpi_comm.send(obj=Role(j), dest=thread_len*i + j + nv, tag=MPI_TAGS.DISTRIBUTION)

    for i in range(thread_len * thread_num + nv, mpi_size):
        mpi_comm.send(obj=Role.kill, dest=i, tag=MPI_TAGS.DISTRIBUTION)
    mpi_comm.send(obj=[nv + 1 + thread_len*i for i in range(thread_num)], dest=2, tag=MPI_TAGS.TO_ACCEPT)
    mpi_comm.send(obj=[nv + 2 + thread_len*i for i in range(thread_num)], dest=1, tag=MPI_TAGS.TO_ACCEPT)

    mpi_comm.send(obj=params["data_processing_folder"], dest=2, tag=MPI_TAGS.SERV_DATA)
    mpi_comm.send(obj=params["data_processing_folder"], dest=1, tag=MPI_TAGS.SERV_DATA)

    for i in range(thread_num):
        mpi_comm.send(obj=wd[str(i)], dest=nv + thread_len * i, tag=MPI_TAGS.SERV_DATA)
        mpi_comm.send(obj=params["dump_folder"], dest=nv + thread_len * i, tag=MPI_TAGS.SERV_DATA_1)
        mpi_comm.send(obj=(params["N_atoms"], params["bdims"]), dest=nv + thread_len * i + 1, tag=MPI_TAGS.SERV_DATA)
        mpi_comm.send(obj=params, dest=nv + thread_len * i + 2, tag=MPI_TAGS.SERV_DATA)

    return after_ditribution(sts, 3)


def perform_one_threaded(sts: setts, params: Dict, nv: int):
    mpi_comm, mpi_size = sts.mpi_comm, sts.mpi_size

    thread_num = mpi_size - nv
    print(f"Thread num: {thread_num}")
    wd: Dict[str, Dict[str, int | Dict[str, int]]] = distribute(params['storages'], thread_num)
    print("Distribution")
    print(json.dumps(wd, indent=4))

    for i in range(thread_num):
        mpi_comm.send(obj=Role.one_thread, dest=i + nv, tag=MPI_TAGS.DISTRIBUTION)

    for i in range(thread_num):
        mkl = (wd[str(i)]["no"], wd[str(i)]["storages"])
        mpi_comm.send(obj=mkl, dest=nv + i, tag=MPI_TAGS.SERV_DATA_1)
        mpi_comm.send(obj=params, dest=nv + i, tag=MPI_TAGS.SERV_DATA_2)

    return after_ditribution(sts, 1)


def gen_matrix(cwd: Path, params: Dict, storages: List[Path], cut: int):
    output_csv_fp = cwd / params["data_processing_folder"] / "matrice.csv"
    with open(output_csv_fp, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for storage in storages:
            with adios2.open(storage.as_posix(), 'r') as reader:  # type: ignore
                for step in reader:
                    stee: int = step.read('step')
                    dist = step.read('dist')
                    writer.writerow(np.hstack([stee, dist[:cut]]).astype(dtype=np.uint32).flatten())


def after_new(sts: setts, m: int):
    cwd, mpi_comm, mpi_rank, mpi_size = sts.cwd, sts.mpi_comm, sts.mpi_rank, sts.mpi_size
    mpi_comm.Barrier()
    print(f"MPI rank {mpi_rank}, barrier off")
    states = {}

    response_array = []
    while True:
        for i in range(1, mpi_size):
            if mpi_comm.iprobe(source=i, tag=MPI_TAGS.ONLINE):
                resp = mpi_comm.recv(source=i, tag=MPI_TAGS.ONLINE)
                print(f"Recieved from {i}: {resp}")
                states[str(i)] = {}
                states[str(i)]['name'] = resp
                response_array.append((i, resp))
        if len(response_array) == mpi_size - 1:
            break
    mpi_comm.Barrier()
    print(f"MPI rank {mpi_rank}, second barrier off")
    completed_threads = []
    fl = True
    start = time.time()
    while fl:
        for i in range(m, mpi_size):
            if mpi_comm.iprobe(source=i, tag=MPI_TAGS.STATE):
                tstate: int = mpi_comm.recv(source=i, tag=MPI_TAGS.STATE)
                if tstate == -1:
                    completed_threads.append(i)
                    print(f"MPI ROOT, rank {i} has been completed")
                    if len(completed_threads) == mpi_size - m:
                        with open(cwd / "st.json", "w") as fp:
                            json.dump(states, fp)
                        fl = False
                        break
                else:
                    states[str(i)]['state'] = tstate
        if time.time() - start > 20:
            with open(cwd / "st.json", "w") as fp:
                json.dump(states, fp)
            start = time.time()
    for i in range(1, m):
        mpi_comm.send(obj=-1, dest=i, tag=MPI_TAGS.COMMAND)

    storages = []
    max_sizes = []
    for i in range(m, mpi_size):
        storage: Path
        max_cluster_size: int
        storage, max_cluster_size = mpi_comm.recv(source=i, tag=MPI_TAGS.SERV_DATA_3)
        storages.append((i, storage))
        max_sizes.append(max_cluster_size)

    # storages = [(i, storage) for i, storage in enumerate(storages)]
    storages.sort(key=lambda x: x[1])
    storages = [storage[1] for storage in storages]

    stf = (cwd / ucs.data_file)
    with open(stf, 'r') as fp:
        son: Dict[str, Any] = json.load(fp)

    son["mat_storages"] = [storage.as_posix() for storage in storages]

    with open(stf, 'w') as fp:
        json.dump(son, fp)

    gen_matrix(cwd, son, storages, max(max_sizes))

    print("MPI ROOT: exiting...")
    return 0


def perform_new(sts: setts, params: Dict, nv: int):
    mpi_comm, mpi_size = sts.mpi_comm, sts.mpi_size

    thread_num = mpi_size - nv
    print(f"Thread num: {thread_num}")
    wd: Dict[str, Dict[str, int | Dict[str, int]]] = distribute(params['storages'], thread_num)
    print("Distribution")
    print(json.dumps(wd, indent=4))

    for i in range(thread_num):
        mpi_comm.send(obj=Role.matr, dest=i + nv, tag=MPI_TAGS.DISTRIBUTION)

    for i in range(thread_num):
        mkl = (wd[str(i)]["no"], wd[str(i)]["storages"])
        mpi_comm.send(obj=mkl, dest=nv + i, tag=MPI_TAGS.SERV_DATA_1)
        mpi_comm.send(obj=params, dest=nv + i, tag=MPI_TAGS.SERV_DATA_2)

    return after_new(sts, 1)


def main(sts: setts):
    cwd = sts.cwd
    print("Started at ", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))

    parser = argparse.ArgumentParser(description='Generate cluster distribution matrix from ADIOS2 LAMMPS data.')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')
    parser.add_argument('--mode', action='store', type=int, default=3, help='Mode to run')  # type: ignore
    args = parser.parse_args()

    if args.debug:
        print("Envolved args:")
        print(args)
    else:
        stf = (cwd / ucs.data_file)
        with open(stf, 'r') as fp:
            son: Dict[str, Any] = json.load(fp)
        _storages: List[str] = son["storages"]
        storages = storage_rsolve(cwd / son["dump_folder"], _storages)
        N_atoms, bdims = bearbeit(cwd / son["dump_folder"], storages)
        son["N_atoms"] = N_atoms
        son["Volume"] = np.prod(bdims)
        son["dimensions"] = list(bdims)
        son["storages"] = storages
        with open(stf, 'w') as fp:
            json.dump(son, fp)

        if args.mode == 1:
            return perform_group_run(sts, son, 3)
        elif args.mode == 2:
            return perform_one_threaded(sts, son, 1)
        elif args.mode == 3:
            return perform_new(sts, son, 1)


def mpi_goto(sts: setts):
    mpi_comm = sts.mpi_comm
    mrole: Role = mpi_comm.recv(source=0, tag=MPI_TAGS.DISTRIBUTION)
    if mrole == Role.reader:
        mpi_comm.send(obj="reader", dest=0, tag=MPI_TAGS.ONLINE)
        return reader.reader(sts)
    elif mrole == Role.proceeder:
        mpi_comm.send(obj="proceeder", dest=0, tag=MPI_TAGS.ONLINE)
        return fd.proceed(sts)
    elif mrole == Role.treater:
        mpi_comm.send(obj="treater", dest=0, tag=MPI_TAGS.ONLINE)
        return treat.treat_mpi(sts)
    elif mrole == Role.kill:
        mpi_comm.send(obj="killed", dest=0, tag=MPI_TAGS.ONLINE)
        mpi_comm.Barrier()
        return 0
    elif mrole == Role.one_thread:
        mpi_comm.send(obj="one_thread", dest=0, tag=MPI_TAGS.ONLINE)
        return one_threaded.thread(sts)
    elif mrole == Role.csvWriter:
        mpi_comm.send(obj="csvWriter", dest=0, tag=MPI_TAGS.ONLINE)
        return MW.csvWriter(sts)
    elif mrole == Role.ad_mpi_writer:
        mpi_comm.send(obj="ad_mpi_writer", dest=0, tag=MPI_TAGS.ONLINE)
        return MW.ad_mpi_writer(sts)
    elif mrole == Role.matr:
        mpi_comm.send(obj="matr", dest=0, tag=MPI_TAGS.ONLINE)
        return matrice.thread(sts)
    else:
        raise RuntimeError(f"Cannot find role {mrole}. Fatal error")


def mpi_root(sts: setts):
    mpi_comm = sts.mpi_comm
    ret = MW.root_sanity(mpi_comm)
    if ret != 0:
        raise MPISanityError("MPI root sanity doesn't passed")
    else:
        print("Sanity: \U0001F7E2")
        return main(sts)
    return ret


def mpi_nonroot(sts: setts):
    mpi_comm, mpi_rank = sts.mpi_comm, sts.mpi_rank
    ret = MW.nonroot_sanity(mpi_comm)
    mpi_comm.Barrier()
    if ret != 0:
        raise MPISanityError(f"MPI nonroot sanity doesn't passed, rank {mpi_rank}")
        return ret
    else:
        return mpi_goto(sts)
    # elif mpi_rank == 1:
    #     mpi_comm.send(obj="csvWriter", dest=0, tag=MPI_TAGS.ONLINE)
    #     return MW.csvWriter(sts)
    # elif mpi_rank == 2:
    #     mpi_comm.send(obj="ad_mpi_writer", dest=0, tag=MPI_TAGS.ONLINE)
    #     return MW.ad_mpi_writer(sts)


def mpi_wrap():
    mpi_comm = MPI.COMM_WORLD
    mpi_rank = mpi_comm.Get_rank()
    mpi_size = mpi_comm.Get_size()

    ret = MW.base_sanity(mpi_size, mpi_rank, 6)
    if ret != 0:
        raise MPISanityError(f"MPI base sanity doesn't passed, mpi rank {mpi_rank}")
        return ret

    cwd = Path.cwd()
    sts = setts(cwd, mpi_comm, mpi_rank, mpi_size)

    if mpi_rank == 0:
        return mpi_root(sts)
    else:
        return mpi_nonroot(sts)


if __name__ == "__main__":
    import sys
    sys.exit(mpi_wrap())
