#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 16-04-2023 16:39:46

import re
import json
import argparse
from typing import List
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(prog="unite.py", description='CHANGEME.')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')
    # parser.add_argument('--dT', type=float, default=0.08, help='???')
    parser.add_argument('--re', type=str, help='File regex')

    args = parser.parse_args()
    if args.debug:
        print("Envolved args:")
        print(args)
        exit()

    cwd = Path.cwd()

    rrr = -1
    pieces = args.re.split('.')
    for i, piece in enumerate(pieces):
        if "*" in piece:
            rrr = i
            break

    if rrr == -1:
        raise RuntimeError("rrr is -1")

    lhs = ".".join(pieces[:rrr])
    rhs = ".".join(pieces[rrr + 1:])

    files: List[int] = []
    for file in cwd.iterdir():
        if re.match(args.re, file.relative_to(cwd).as_posix()):
            files.append(int(file.relative_to(cwd).as_posix().split('.')[rrr]))

    files.sort()

    nfiles = [lhs + "." + str(file) + "." + rhs for file in files]

    print(json.dumps(nfiles))

    target = cwd / (lhs + "." + rhs)
    target.touch()
    with target.open('w') as fout:
        for origin in nfiles:
            with (cwd / origin).open('r') as fin:
                for line in fin:
                    fout.write(line)


if __name__ == "__main__":
    main()
