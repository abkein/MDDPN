#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 26-07-2023 13:14:24


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


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1,
                     length=100, fill='█', printEnd="\r") -> None:
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                     (iteration / total))
    filledLength = int(length * iteration // total)
    barx = fill * filledLength + '-' * (length - filledLength)
    # if time_per_100_iteration is not None:
    #     suffix += ' ETA: ' + \
    #         str(datetime.timedelta(seconds=round(
    #             time_per_100_iteration*(total-iteration)/100)))
    suffix += f"{iteration}/{total}"
    print(f'\r{prefix} |{barx}| {percent}% {suffix}', end=printEnd)
    if iteration == total:
        print()


if __name__ == "__main__":
    pass
