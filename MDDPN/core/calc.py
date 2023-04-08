#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-04-2023 15:00:33


from typing import Union

import numpy as np
from numpy import typing as npt

from .props import sigma, nl


def mean_size(sizes: npt.NDArray[np.uint32], dist: npt.NDArray[np.uint32]) -> float:
    return np.sum(sizes * dist) / np.sum(dist)  # type: ignore


def condensation_degree(dist: npt.NDArray[np.uint32], N: int) -> float:
    return 1 - dist[0] / N


def maxsize(sizes: npt.NDArray[np.uint32], dist: npt.NDArray[np.uint32]) -> int:
    return sizes[np.nonzero(dist)][-1]  # type: ignore


def nvv(sizes: npt.NDArray[np.uint32], dist: npt.NDArray[np.uint32], volume: float, kmin: int) -> float:
    return np.sum(dist[sizes <= kmin] * sizes[sizes <= kmin]) / volume  # type: ignore


def nd(sizes: npt.NDArray[np.uint32], dist: npt.NDArray[np.uint32], volume: float, kmin: int) -> float:
    return np.sum(dist[sizes >= kmin]) / volume  # type: ignore


def nvs(sizes: npt.NDArray[np.uint32], dist: npt.NDArray[np.uint32], volume: float, kmin: int, T: float) -> Union[float, None]:
    ms = sizes[-1]
    n1 = dist[0] / volume
    dzd = dist[kmin - 1:ms]
    kks = np.arange(kmin, ms + 1, dtype=np.uint32)**(1 / 3)
    num = n1 * np.sum(kks**2 * dzd) / volume
    if num == 0:
        return None
    rl = (3 / (4 * np.pi * nl(T)))**(1 / 3)
    cplx = 2 * sigma(T) / (nl(T) * T * rl)
    denum = np.sum(kks**2 * dzd * np.exp(cplx / kks)) / volume

    return num / denum


def get_row(step: int, sizes: npt.NDArray[np.uint32], dist: npt.NDArray[np.uint32], temp: float, N_atoms: int, kmax: int, g: int, volume: float, dt: float, dis: int) -> npt.NDArray[np.float32]:
    tow = np.zeros(10, dtype=np.float32)
    tow[0] = round(step * dt * dis)
    tow[1] = mean_size(sizes, dist)
    tow[2] = maxsize(sizes, dist)
    tow[3] = condensation_degree(dist, N_atoms)
    tow[4] = nvv(sizes, dist, volume, kmax)
    tow[5] = nd(sizes, dist, volume, g)
    tow[6] = len(dist[dist > 1])
    tow[7] = temp
    tow[8] = step
    tow[9] = np.sum(dist[g-1:])
    return tow


if __name__ == "__main__":
    pass

# something
