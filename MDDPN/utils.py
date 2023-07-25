#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 24-07-2023 11:18:47


import numpy as np
from typing import Union, Iterable


float = Union[float, np.floating]
# int = int | np.intp


def is_iter(arr: Union[Iterable[float], float]) -> bool:
    try:
        iter(arr)  # type: ignore
        return True
    except Exception:
        return False


if __name__ == "__main__":
    pass
