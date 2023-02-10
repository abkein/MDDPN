#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import numpy as np
import adios2
import argparse
import json
import freud
from datetime import datetime


def main(tdir: Path):
    print("Started")
    adin = adios2.open(str(tdir / "dump.bp"), 'r')  # type: ignore
    N = int(adin.read('natoms'))
    Lx = adin.read('boxxhi')
    Ly = adin.read('boxyhi')
    Lz = adin.read('boxzhi')
    total_count = adin.steps()
    print("Total step count: ", total_count)

    box = freud.box.Box.from_box(np.array([Lx, Ly, Lz]))

    print("Box volume is: ", box.volume)
    print("N atoms: ", N)
    son = {'N': N, "Volume": box.volume, "ts": total_count}
    with open(tdir / 'data.json', 'w') as fp:
        json.dump(son, fp)


if __name__ == "__main__":
    print("Started at ", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
    parser = argparse.ArgumentParser(description='Process some floats.')
    parser.add_argument('folder', metavar='folder', type=str, nargs=1,
                        help='Folder in which search for .bp files')
    args = parser.parse_args()
    print(args)
    tdir = Path(args.folder[0]).resolve()
    if not tdir.exists():
        raise FileNotFoundError("No such directory")
    print("Resolved path: ", tdir)
    main(tdir)
else:
    raise ImportError("Cannot be imported")
