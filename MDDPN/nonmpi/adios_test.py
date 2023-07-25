#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-04-2023 15:03:06


import json
import adios2
import argparse
import numpy as np
from pathlib import Path
# from pprint import pprint


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
                                                     (iteration / float(total)))
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


def info(cwd, args):
    storage = Path(args.storage).resolve()
    adin = adios2.open(str(storage), 'r')   # type: ignore
    parsed = adin.available_variables()
    print(json.dumps(parsed, indent=4))
    print(f"Available steps: {adin.steps()} (0-{adin.steps()-1})")
    adin.close()


def srmdir(dir, d=False):
    for file in dir.iterdir():
        if file.is_dir():
            srmdir(file)
            file.rmdir()
        else:
            file.unlink()
    if d:
        dir.rmdir()


def merge(cwd, args):
    if args.storage1 == args.output or args.storage2 == args.output:
        raise Exception("Original and output storages are the same storage")
    print("Opening 1-st storage")
    adin1 = adios2.open(str(cwd / args.storage1), 'r')   # type: ignore
    print("Opening output storage")
    adout = adios2.open(str(cwd / args.output), 'w')  # type: ignore

    first_steps = adin1.steps()
    print("Starting copying from 1-st storage")
    for step in adin1:
        ctr = step.current_step()
        printProgressBar(ctr, first_steps)
        for var in adin1.available_variables():
            arr = step.read(var)
            adout.write(var, arr, arr.shape, np.full(
                len(arr.shape), 0), arr.shape)
        adout.end_step()
        if ctr == first_steps - 1:
            break
    adin1.close()

    print("Opening 2-nd storage")
    adin2 = adios2.open(str(cwd / args.storage2), 'r')   # type: ignore
    second_steps = adin2.steps()
    print("Starting copying from 2-st storage")
    for step in adin2:
        ctr = step.current_step()
        printProgressBar(ctr, second_steps)
        for var in adin2.available_variables():
            arr = np.array(step.read(var))
            adout.write(var, arr, arr.shape, np.full(
                len(arr.shape), 0), arr.shape)
        adout.end_step()
        if ctr == second_steps - 1:
            break
    adin2.close()
    adout.close()

    if args.delete:
        srmdir(cwd / args.storage1, True)
        if args.storage1 != args.storage2:
            srmdir(cwd / args.storage2, True)


def gen_samples(cwd, args):
    adout1 = adios2.open(str(cwd / "M1"), 'w')   # type: ignore
    adout2 = adios2.open(str(cwd / "M2"), 'w')   # type: ignore
    for i in range(100):
        write1 = np.random.uniform(10, 100, 100)
        write2 = np.random.uniform(10, 100, 100)
        adout1.write("variable", write1, shape=write1.shape,
                     start=[0], count=write1.shape, end_step=True)
        adout2.write("variable", write2, shape=write2.shape,
                     start=[0], count=write2.shape, end_step=True)
    adout1.close()
    adout2.close()


def main(cwd, args):
    # print("PWD: ", Path(".").resolve())
    if args.debug:
        print("Envolved args:")
        print(args)
    else:
        if args.command == 'info':
            info(cwd, args)
        elif args.command == 'merge':
            merge(cwd, args)
        elif args.command == 'gen':
            gen_samples(cwd, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="To be continued...")
    parser.add_argument('--debug', action='store_true',
                        help='Debug, prints only parsed arguments')

    sub_parsers = parser.add_subparsers(
        help='sub-command help', dest="command")

    parser_init = sub_parsers.add_parser(
        'info', help='Get info about ADIOS2 binary storage')
    parser_init.add_argument(
        'storage', action="store", type=str, help='Path (relative to cwd) to storage')

    parser_init = sub_parsers.add_parser(
        'merge', help='Megre two storages into one')
    parser_init.add_argument(
        'storage1', action="store", type=str, help='Path (relative to cwd) to first storage')
    parser_init.add_argument(
        'storage2', action="store", type=str, help='Path (relative to cwd) to second storage')
    parser_init.add_argument(
        'output', action="store", type=str, help='Name of the output storage')
    parser_init.add_argument(
        '-d', '--delete', action="store_true", help='Wether delete or not the original storages')

    parser_init = sub_parsers.add_parser(
        'gen', help='Generates sample two ADIOS2-storages with 100 steps of random arrays of size 100 of floats in range [10-100)')
    # parser_init.add_argument("-p", '--params', action="store", type=str,
    #                          help='Obtain simulation parameters from command-line')
    # parser_init.add_argument("-f", '--file', action="store_true",
    #                          help='Obtain simulation parameters from file')
    # parser_init.add_argument("-fn", '--fname', action="store",
    #                          help='Specify file to get parameters from')
    # # init_sub_parsers = parser_init.add_subparsers(
    # #     help='sub-command help', dest="init_min")
    # # sub_parser_init = init_sub_parsers.add_parser(
    # #     'min', help='Initialize directory')

    # parser_run = sub_parsers.add_parser(
    #     'run', help='Run LAMMPS simulation')
    # parser_run.add_argument(
    #     '--no_auto', action='store_true', help='Don\'t run polling sbatch and don\'t auto restart')

    # parser_restart = sub_parsers.add_parser(
    #     'restart', help='Generate restart file and run it')
    # parser_restart.add_argument(
    #     '--gen', action='store_false', help='Don\'t run restart')

    args = parser.parse_args()
    cwd = Path.cwd()
    main(cwd, args)
