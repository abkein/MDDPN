#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-04-2023 15:03:22


from pathlib import Path

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


def stateWriter(cwd, stateQue):
    savefile = "pstates.json"
    to_write = {}
    while True:
        data = stateQue.get()
        if isinstance(data, fd.SpecialObject):
            break
        to_write[data[0]] = data[1]
        with open(cwd / savefile, 'w') as fp:
            json.dump(to_write, fp)


def reader(cwd: Path, storages, output, sync, stateQue):
    print("Launch reader")
    counter = 0
    for storage, end_step in storages.items():
        with adios2.open(str(cwd / storage), 'r') as reader:
            total_steps = reader.steps()

            for step in reader:
                ctr = step.current_step()
                arr = step.read('atoms')
                arr = arr[:, 2:5]
                output.put((arr, counter))
                print(f"Readed step {ctr} of {end_step} of {storage}")
                while ctr - sync.value > 50:
                    time.sleep(0.5)
                counter += 1
                if ctr == end_step - 1 or ctr == total_steps - 1:
                    break

    print("Reading complete")
    output.put(fd.SpecialObject())


prdata_file = "ntb.bp"
data_file = 'data.json'


def storage_check(storages):
    for storage, end_step in storages.items():
        with adios2.open(str(cwd / storage), 'r') as reader:
            total_steps = reader.steps()
            if end_step > total_steps:
                raise RuntimeError(
                    f"End step {end_step} in {storage} is bigger that total step count ({total_steps})")


def storage_rsolve(storages):
    for storage, end_step in storages.items():
        if end_step == "full":
            with adios2.open(str(cwd / storage), 'r') as reader_c:
                storages[storage] = reader_c.steps()
    return storages


def bearbeit(cwd: Path, args):
    print("Started")

    storages = json.loads(args.storages[0])

    storages = storage_rsolve(storages)

    storage_check(storages)

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
    stateQueue = mpman.Queue()
    treatQue = mpman.Queue() if args.add_treat else None
    sync = mpman.Value(int, 0)

    proccedProc = Process(target=fd.proceed,
                          args=(dataQueue, N, box, cwd /
                                prdata_file, sync, stateQueue, treatQue),
                          name="WproceedW")
    proccedProc.start()

    if args.add_treat:
        treatProc = Process(target=treat.treat_async,
                            args=(treatQue, cwd, args.outfile, stateQueue,
                                  args.kmax, args.critical_size, args.timestep, args.dis),
                            name="WtreatW")
        treatProc.start()

    statesProc = Process(target=stateWriter, args=(cwd, stateQueue),
                         name="WwriterW")
    statesProc.start()

    reader(cwd, storages, dataQueue, sync, stateQueue)

    proccedProc.join()
    if args.add_treat:
        treatProc.join()

    stateQueue.put(fd.SpecialObject())
    statesProc.join()

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
