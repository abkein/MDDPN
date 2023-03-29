#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# First created by Egor Perevoshchikov at 2022-10-29 15:41.
# Last-update: 2023-01-07 16:21:26
#


import numpy as np
# import sys
from pathlib import Path
import pandas as pd
import adios2
import freud
# import time
from datetime import datetime
import argparse
import json
import fastd as fd


def mean_size(sizes, dist):
    return np.sum(sizes * dist) / np.sum(dist)


def xas(dist, N):
    return 1 - dist[0] / N


def maxsize(sizes, dist):
    return sizes[np.nonzero(dist)][-1]


def nvv(sizes, dist, volume, kmin):
    return np.sum(dist[sizes <= kmin] * sizes[sizes <= kmin]) / volume


def nd(sizes, dist, volume, kmin):
    return np.sum(dist[sizes >= kmin]) / volume


def nl(T):
    # if T <= 0.6:
    #     return 0.896
    # else:
    #     return -0.9566 * np.exp(0.601 * T) + 2.25316
    return 0.9357815789568936*np.exp(-0.4942307357685498*T**(3.217571917919023))


def sigma(T):
    # if T <= 0.6:
    #     return -2.525 * T + 2.84017
    # else:
    #     return -5.086 * T + 4.21614
    return -4.11425 * T + 3.56286


def nvs(sizes, dist, volume, kmin, T):
    ms = sizes[-1]
    n1 = dist[0] / volume
    dzd = dist[kmin - 1:ms]
    kks = np.arange(kmin, ms + 1, dtype=np.uint32)**(1 / 3)
    num = n1 * np.sum(kks**2 * dzd) / volume
    rl = (3 / (4 * np.pi * nl(T)))**(1 / 3)
    cplx = 2 * sigma(T) / (nl(T) * T * rl)
    denum = np.sum(kks**2 * dzd * np.exp(cplx / kks)) / volume

    if num == 0:
        return None
    return num / denum


def treat(tdir, savefile, kmax=10, g=19, dt=0.005, dis=1000):
    print("Started treat")
    print("Try to read ", str(tdir / "ntb.bp"))

    adin = adios2.open(str(tdir / "ntb.bp"), "r")  # type: ignore
    # while 'natoms' not in adin.available_variables():
    #     # print("Treat sleep for 1 sec")
    #     print("Avail: ", adin.available_variables())
    #     adin.close()
    #     adin = adios2.open(str(tdir / "ntb.bp"), "r")  # type: ignore
    #     time.sleep(1)
    N_atoms = int(adin.read('natoms').reshape(1)[0])
    Lx = adin.read('Lx').reshape(1)[0]
    Ly = adin.read('Ly').reshape(1)[0]
    Lz = adin.read('Lz').reshape(1)[0]
    sizes = adin.read('sizes')
    box = freud.box.Box.from_box(np.array([Lx, Ly, Lz]))

    temperatures = pd.read_csv(
        tdir / "temperature.log", header=None, skiprows=1)
    temptime = temperatures[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures[1].to_numpy(dtype=np.float64)
    # dis = temptime[1] - temptime[0]

    print("Start reading")
    total_count = adin.steps()
    while Path(tdir / savefile).exists():
        savefile += 'a'
    for step in adin:
        if step.current_step() == 0:
            continue
        if step.current_step() == total_count - 1:
            break
        dist = step.read('dist')

        temp = temperatures[temptime == int(step.current_step() * dis)]
        tow = np.zeros(8, dtype=np.float64)
        tow[0] = step.current_step() * dt * dis
        tow[1] = mean_size(sizes, dist)
        tow[2] = maxsize(sizes, dist)
        tow[3] = xas(dist, N_atoms)
        tow[4] = nvv(sizes, dist, box.volume, kmax)
        tow[5] = nd(sizes, dist, box.volume, g)
        tow[6] = nvs(sizes, dist, box.volume, kmax, temp)
        tow[7] = temp

        pd.DataFrame(tow).T.to_csv(tdir / savefile,
                                   mode='a', header=False, index=False, na_rep='nan')
        print("Treated ", step.current_step(), "step of ", total_count)

    adin.close()

    print("Treat end.")


def is_iter(arr) -> bool:
    try:
        iter(arr)
        return True
    except Exception:
        return False


def treat_async(input, cwd, savefile, stateQue, kmax=10, g=19, dt=0.005, dis=1000):
    with open(cwd / 'data.json', 'r') as fp:
        son = json.load(fp)
    N_atoms = son['N']

    sizes = np.arange(1, N_atoms + 1)
    volume = son['Volume']
    # total_count = son['ts']

    temperatures = pd.read_csv(
        cwd / "temperature.log", header=None)
    temptime = temperatures[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures[1].to_numpy(dtype=np.float64)

    while Path(cwd / savefile).exists():
        savefile += 'a'
    savefile = cwd / savefile
    while True:
        data = input.get()
        if isinstance(data, fd.SpecialObject):
            break

        dist, step = data

        temp = temperatures[temptime == int(step * dis)]
        if is_iter(temp):
            temp = float(temp[0])
        tow = np.zeros(8, dtype=np.float64)
        tow[0] = step * dt * dis
        tow[1] = mean_size(sizes, dist)
        tow[2] = maxsize(sizes, dist)
        tow[3] = xas(dist, N_atoms)
        tow[4] = nvv(sizes, dist, volume, kmax)
        tow[5] = nd(sizes, dist, volume, g)
        tow[6] = nvs(sizes, dist, volume, kmax, temp)
        tow[7] = temp

        pd.DataFrame(tow).T.to_csv(savefile, mode='a',
                                   header=False, index=False, na_rep='nan')
        # print("Treated ", step, "step of ", total_count)

    print("Treat end.")


if __name__ == "__main__":
    print("Started at ", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
    parser = argparse.ArgumentParser(description='Process some floats.')
    parser.add_argument('folder', metavar='folder', type=str, nargs=1,
                        help='Folder in which search for .bp files')
    parser.add_argument('outfile', metavar='outfile', type=str, nargs='?', default="rdata.csv",
                        help='File in which save obtained data')
    parser.add_argument('-k', '--kmax', metavar='kmax', type=int, nargs='?', default=10,
                        action='store', help='kmax')
    parser.add_argument('-g', '--critical_size', metavar='g', type=int, nargs='?', default=19,
                        action='store', help='Critical size')
    parser.add_argument('-t', '--timestep', metavar='ts', type=float, nargs='?', default=0.005,
                        action='store', help='Timestep')
    parser.add_argument('-s', '--dis', metavar='dis', type=float, nargs='?', default=1000,
                        action='store', help='Time between dump records')
    args = parser.parse_args()
    print(args)
    tdir = Path(args.folder[0]).resolve()
    if not tdir.exists():
        raise FileNotFoundError("No such directory")
    print("Resolved path: ", tdir)
    treat(tdir, args.outfile,
          kmax=args.kmax, g=args.critical_size, dt=args.timestep, dis=args.dis)
