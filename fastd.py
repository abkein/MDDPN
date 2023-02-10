# -*- coding: utf-8 -*-

# First created by Egor Perevoshchikov at 2022-10-29 15:41.
# Last-update: 2023-01-07 16:21:38
#


import adios2
import freud
import numpy as np
# import pandas as pd
# import time
import warnings
warnings.simplefilter("error")


def scale2box(data, box):
    data[:, 0] = data[:, 0] * box.Lx
    data[:, 1] = data[:, 1] * box.Ly
    data[:, 2] = data[:, 2] * box.Lz
    return data


def asdf(points, box):
    points = box.wrap(points)
    system = freud.AABBQuery(box, points)
    cl = freud.cluster.Cluster()
    cl.compute(system, neighbors={'mode': 'ball',
               "r_min": 0, 'exclude_ii': True, "r_max": 1.5})
    cl_props = freud.cluster.ClusterProperties()
    cl_props.compute((box, points), cl.cluster_idx)
    return cl_props.sizes


def matrix_verify(mat, frame_count, N):
    for i in range(1, frame_count + 1):
        number = np.sum(mat[:, 0] * mat[:, i])
        if number != N:
            return (False, number)
    return (True, N)


def clmatrix(data, box, N):
    ar = asdf(data, box)
    unique, counts = np.unique(ar, return_counts=True)
    mat = np.zeros(N + 1, dtype=np.uint32)
    for size, ctn in zip(unique, counts):
        mat[size] = ctn
    return np.array(mat[1:])


class SpecialObject():
    def __init__(self) -> None:
        pass


def proceed(input, N, box, savepath, sync, output: None):
    print("Launch proceed")
    adin = adios2.open(str(savepath), "w")  # type: ignore
    adin.write("Lx", np.array([box.Lx]))
    adin.write("Ly", np.array([box.Ly]))
    adin.write("Lz", np.array([box.Lz]))
    adin.write("natoms", np.array([N]))

    sizes = np.arange(1, N + 1)
    adin.write("sizes", np.array(sizes), shape=sizes.shape,
               start=[0], count=sizes.shape)
    # ctr = 0
    if output is None:
        while True:
            data = input.get()
            if isinstance(data, SpecialObject):
                break
            data, ctr = data
            data = scale2box(data, box)
            dist = clmatrix(data, box, N)

            # if __debug__:
            #     ret, temp_num = matrix_verify(mat, frame_count, N)
            #     if not ret:
            #         rs = "[main] Lost particles: {} from {}".format(
            #             int(temp_num), int(N))
            #         raise RuntimeError(rs)

            adin.write("dist", np.array(dist), shape=dist.shape,
                       start=[0], count=dist.shape, end_step=True)
            print("Processed ", ctr, " step")
            # ctr += 1
            sync.value = ctr
    else:
        while True:
            data = input.get()
            if isinstance(data, SpecialObject):
                break
            data, ctr = data
            data = scale2box(data, box)
            dist = clmatrix(data, box, N)

            # if __debug__:
            #     ret, temp_num = matrix_verify(mat, frame_count, N)
            #     if not ret:
            #         rs = "[main] Lost particles: {} from {}".format(
            #             int(temp_num), int(N))
            #         raise RuntimeError(rs)

            adin.write("dist", np.array(dist), shape=dist.shape,
                       start=[0], count=dist.shape, end_step=True)
            output.put((dist, ctr))
            print("Processed ", ctr, " step")
            # ctr += 1
            sync.value = ctr

    adin.close()
    print("Process end.")


def proc(data, N, box):
    data = scale2box(data, box)
    dist = clmatrix(data, box, N)
    sizes = np.arange(1, N + 1)
    return (sizes, dist)


if __name__ == "__main__":
    print("Here is nothing to run")
