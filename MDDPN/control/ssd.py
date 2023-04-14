#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 14-04-2023 21:11:08


# TODO:
# preparing steps for every label
# add description in argument parser


import argparse
from pathlib import Path

from .init import init
from .run import run, restart
from .post_process import end
from .utils import com_set, load_state


def main_main(cwd: Path, args: argparse.Namespace):
    if args.debug:
        print("Envolved args:")
        print(args)
        return 0
    elif args.command == 'init':
        return init(cwd, args)
    else:
        with load_state(cwd) as state:
            if args.command == 'run':
                state = run(cwd, state, args)
            elif args.command == 'restart':
                state = restart(cwd, state, args)
            elif args.command == 'end':
                # raise NotImplementedError("Don't use it until it fixed")
                return end(cwd, args)
            elif args.command == 'set':
                return com_set(cwd, args)
            else:
                raise RuntimeError(f"There is no such command as {args.command}")


def main():
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')

    sub_parsers = parser.add_subparsers(help='sub-command help', dest="command")

    parser_init = sub_parsers.add_parser('init', help='Initialize directory')
    parser_init.add_argument('--min', action="store_true", help='Don\'t create in. files')
    parser_init.add_argument("-p", '--params', action="store", type=str, help='Obtain simulation parameters from command-line')
    parser_init.add_argument("-f", '--file', action="store_true", help='Obtain simulation parameters from file')
    parser_init.add_argument("-fn", '--fname', action="store", help='Specify file to get parameters from')

    parser_set = sub_parsers.add_parser('set', help='Set variable in config json file')
    parser_set.add_argument('file', action="store", type=str, help='File in which set the variable')
    parser_set.add_argument('variable', action="store", type=str, help='The variable to set')
    parser_set.add_argument('value', action="store", type=float, help='Value of variable')

    parser_run = sub_parsers.add_parser('run', help='Run LAMMPS simulation')
    parser_run.add_argument('--test', action='store_true', help='Whether actually run LAMMPS or not. Test purposes only')
    parser_run.add_argument('--no_auto', action='store_true', help='Don\'t run polling sbatch and don\'t auto restart')

    parser_restart = sub_parsers.add_parser('restart', help='Generate restart file and run it')
    parser_restart.add_argument('--test', action='store_true', help='Whether actually run LAMMPS or not. Test purposes only')
    parser_restart.add_argument('-s', '--step', action='store', type=int, help='From which step do the restart')
    parser_restart.add_argument('--no_auto', action='store_true', help='Don\'t run polling sbatch and don\'t auto restart')

    parser_end = sub_parsers.add_parser('end', help='Post-processing')
    parser_end.add_argument('--params', action='store', type=str, help='Post-processing parameters')
    parser_end.add_argument('--files', action='store', type=str, default=None, help='Post-processing parameters')

    args = parser.parse_args()
    cwd = Path.cwd()
    return main_main(cwd, args)


if __name__ == '__main__':
    main()
