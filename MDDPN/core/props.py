#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-05-2023 18:01:14


import numpy as np

from ..utils import float


def nl(T: float) -> float:
    return 0.9367410354674542 * np.exp(-0.46391125909214476 * T**(2.791206046910478))


def sigma(T: float) -> float:
    return -1.8111682291065432 * T + 1.8524737887189553


if __name__ == "__main__":
    pass
