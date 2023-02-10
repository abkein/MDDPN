#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# First created by Egor Perevoshchikov at 2022-10-29 15:41.
# Last-update: 2023-01-07 16:21:14
#

from curses.ascii import ctrl
from pathlib import Path
# import sys
# import os
# import psutil

import numpy as np
from multiprocessing import Process, Manager
import adios2
import freud
import fastd as fd
import time
import argparse
import json
from datetime import datetime
import treat


def reader(cwd: Path, storages, output, sync):
    print("Launch reader")
    ctr_a = 0
    for storage, end_step in storages.items():
        with adios2.open(str(cwd / storage), 'r') as reader:
            total_steps = reader.steps()
            if end_step >= total_steps:
                raise RuntimeError(
                    f"End step {end_step} in {storage} is bigger that total step count ({total_steps})")

            for step in reader:
                ctr = step.current_step() + ctr_a
                arr = step.read('atoms')
                arr = arr[:, 2:5]
                output.put((arr, ctr))
                print(f"Readed step {ctr} of {end_step} of {storage}")
                while ctr - sync.value > 50:
                    time.sleep(0.5)
                if ctr == end_step - 1:
                    ctr_a += ctr
                    break
    print("Reading complete")
    output.put(fd.SpecialObject())


prdata_file = "ntb.bp"
data_file = 'data.json'


def bearbeit(cwd: Path, args):
    print("Started")

    storages = json.load(args.storages)

    for storage, end_step in storages.items():
        if end_step == "full":
            with adios2.open(str(cwd / storage), 'r') as reader:
                storages[storage] = reader.steps()

    adin = adios2.open(str(cwd / list(storages.items())[0][0]), 'r')

    N = int(adin.read('natoms'))
    Lx = adin.read('boxxhi')
    Ly = adin.read('boxyhi')
    Lz = adin.read('boxzhi')

    # total_count = adin.steps()
    # print("Total step count: ", total_count)
    adin.close()

    box = freud.box.Box.from_box(np.array([Lx, Ly, Lz]))
    print("Box volume is: ", box.volume)
    print("N atoms: ", N)
    son = {'N': N, "Volume": box.volume}
    with open(cwd / data_file, 'w') as fp:
        json.dump(son, fp)

    mpman = Manager()
    dataQueue = mpman.Queue()
    treatQue = mpman.Queue() if args.add_treat else None
    sync = mpman.Value(int, 0)

    proccedProc = Process(target=fd.proceed,
                          args=(dataQueue, N, box, cwd /
                                prdata_file, sync, treatQue),
                          name="WproceedW")
    proccedProc.start()

    if args.add_treat:
        treatProc = Process(target=treat.treat_async,
                            args=(treatQue, cwd, args.outfile,
                                  args.kmax, args.critical_size, args.timestep, args.dis),
                            name="WtreatW")
        treatProc.start()

    reader(cwd, storages, dataQueue, sync)

    proccedProc.join()
    if args.add_treat:
        treatProc.join()

    print("End. Exit...")


def main(cwd, args):
    if args.debug:
        print("Envolved args:")
        print(args)
    else:
        bearbeit(cwd, args)


if __name__ == "__main__":
    print("Started at ", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
    parser = argparse.ArgumentParser(description='Process some floats.')
    parser.add_argument('--debug', action='store_true',
                        help='Debug, prints only parsed arguments')
    parser.add_argument('-o', '--outfile', type=str, default="rdata.csv",
                        help='File in which save obtained data')
    parser.add_argument('-a', '--add-treat', metavar='treat', action='store_const',
                        const=True, default=False, help='Involve treat')
    parser.add_argument('-k', '--kmax', metavar='kmax', type=int, nargs='?', default=10,
                        action='store', help='kmax')
    parser.add_argument('-g', '--critical_size', metavar='g', type=int, nargs='?', default=19,
                        action='store', help='Critical size')
    parser.add_argument('-t', '--timestep', metavar='ts', type=float, nargs='?', default=0.005,
                        action='store', help='Timestep')
    parser.add_argument('-s', '--dis', metavar='dis', type=float, nargs='?', default=1000,
                        action='store', help='Time between dump records')
    parser.add_argument('storages', type=str, nargs=1,
                        help='Folder in which search for .bp files')
    args = parser.parse_args()
    # print(args)
    cwd = Path.cwd()
    main(cwd, args)
else:
    raise ImportError("Cannot be imported")
