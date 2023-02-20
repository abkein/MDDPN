# -*- coding: utf-8 -*-

# First created by Egor Perevoshchikov at 2022-10-29 15:41.
# Last-update: 2023-02-19 19:22:35

import warnings
import numpy as np
import freud
from mpiworks import MPIComm, MPI_TAGS
from typing import Tuple

warnings.simplefilter("error")


def scale2box(data: np.ndarray, box: freud.box) -> np.ndarray:
    data[:, 0] = data[:, 0] * box.Lx
    data[:, 1] = data[:, 1] * box.Ly
    data[:, 2] = data[:, 2] * box.Lz
    return data


def asdf(points: np.ndarray, box: freud.box) -> np.ndarray:
    points = box.wrap(points)
    system = freud.AABBQuery(box, points)
    cl = freud.cluster.Cluster()
    cl.compute(system, neighbors={'mode': 'ball',
               "r_min": 0, 'exclude_ii': True, "r_max": 1.5})
    cl_props = freud.cluster.ClusterProperties()
    cl_props.compute((box, points), cl.cluster_idx)
    return cl_props.sizes


def matrix_verify(mat: np.ndarray, frame_count: int, N: int) -> Tuple[bool, int]:
    for i in range(1, frame_count + 1):
        number = np.sum(mat[:, 0] * mat[:, i])
        if number != N:
            return (False, number)
    return (True, N)


def clmatrix(data: np.ndarray, box: freud.box, N: int) -> np.ndarray:
    ar = asdf(data, box)
    unique, counts = np.unique(ar, return_counts=True)
    mat = np.zeros(N + 1, dtype=np.uint32)
    for size, ctn in zip(unique, counts):
        mat[size] = ctn
    return np.array(mat[1:])


def proceed(mpi_comm: MPIComm, mpi_rank: int, mpi_size: int):
    mpi_comm.Barrier()
    N, bdims = mpi_comm.recv(source=0, tag=MPI_TAGS.SERV_DATA)  # type: Tuple[int, np.ndarray]
    box = freud.box.Box.from_box(bdims)
    reader_rank = mpi_rank - 1
    trt_rank = mpi_rank + 1
    while True:
        step, sender, data = mpi_comm.recv(source=reader_rank, tag=MPI_TAGS.DATA)  # type: Tuple[int, int, np.ndarray]
        if sender != reader_rank:
            pass

        data = scale2box(data, box)
        dist = clmatrix(data, box, N)

        # if __debug__:
        #     ret, temp_num = matrix_verify(mat, frame_count, N)
        #     if not ret:
        #         rs = "[main] Lost particles: {} from {}".format(
        #             int(temp_num), int(N))
        #         raise RuntimeError(rs)

        tpl = (step, dist)
        mpi_comm.send(obj=tpl, dest=2, tag=MPI_TAGS.WRITE)
        mpi_comm.send(obj=tpl, dest=trt_rank, tag=MPI_TAGS.DATA)

        mpi_comm.send(obj=step, dest=reader_rank, tag=MPI_TAGS.SERVICE)

        mpi_comm.send(obj=step, dest=0, tag=MPI_TAGS.SERVICE)

        if mpi_comm.iprobe(source=reader_rank, tag=MPI_TAGS.SERVICE) and not mpi_comm.iprobe(source=reader_rank, tag=MPI_TAGS.DATA):
            if mpi_comm.recv(source=reader_rank, tag=MPI_TAGS.SERVICE) == 1:
                break
    mpi_comm.send(obj=1, dest=trt_rank, tag=MPI_TAGS.SERVICE)

    print("Process end.")
    return 0


def proc(data, N, box):
    data = scale2box(data, box)
    dist = clmatrix(data, box, N)
    sizes = np.arange(1, N + 1)
    return (sizes, dist)


if __name__ == "__main__":
    print("Here is nothing to run")
