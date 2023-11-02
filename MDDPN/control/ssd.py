#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-11-2023 15:32:17

import sys
import logging
import argparse
from pathlib import Path

from . import config
from .run import run
from .init import init
from .ender import ender
from .restart import restart
from . import constants as cs
from .utils import com_set, load_state, setup_logger


def main_main(cwd: Path, args: argparse.Namespace):
    logger = setup_logger(cwd, "ssd", logging.DEBUG if args.debug else logging.INFO)
    logger.info(f"Root folder: {cwd.as_posix()}")
    logger.info(f"Envolved args: {args}")
    try:
        if args.command == 'genconf':
            logger.info("'genconf' command received")
            return config.genconf(cwd / cs.files.config, logger.getChild('genconf'))
        elif args.command == 'checkconf':
            logger.info("'checkconf' command received")
            return config.configure(cwd / cs.files.config, logger.getChild('configure'))
        else:
            config.configure(cwd / cs.files.config, logger.getChild('configure'))
            if args.command == 'init':
                logger.info("'init' command received")
                return init(cwd, args, logger.getChild("init"))
            else:
                with load_state(cwd) as state:
                    if args.command == 'run':
                        logger.info(msg="'run' command received")
                        state = run(cwd, state, args, logger.getChild("run"))
                    elif args.command == 'restart':
                        logger.info(msg="'restart' command received")
                        state = restart(cwd, state, args, logger.getChild("restart"))
                    elif args.command == 'end':
                        logger.info("'end' command received")
                        if not cs.sp.allow_post_process:
                            logger.error("Post processing disallowed")
                            return 1
                        else:
                            state = ender(cwd, state, args, logger.getChild('ender'))
                    elif args.command == 'set':
                        logger.info("'set' command received")
                        state = com_set(cwd, args)
                    else:
                        logger.critical(f"Unknown '{args.command}' command received")
                        raise RuntimeError(f"There is no such command as {args.command}")
    except Exception:
        logger.exception("Uncaught exception")
        raise
    return 0


def main():
    parser = argparse.ArgumentParser(prog='MDDPN.py')
    parser.add_argument('--debug', action='store_true', help='Sets logging level to debug')

    sub_parsers = parser.add_subparsers(help='sub-command help', dest="command")

    parser_init = sub_parsers.add_parser('init', help='Initialize directory')
    parser_init.add_argument("-p", '--params', action="store", type=str, help='Obtain simulation parameters from command-line')
    parser_init.add_argument("-rm", '--restart_mode', choices=['one', 'two', 'multiple'], help='Specify two-filed restarts instead of restart.*')
    # parser_init.add_argument("-f", '--file', action="store_true", help='Obtain simulation parameters from file')
    parser_init.add_argument("-fn", '--fname', action="store", type=str, help='Specify file to get parameters from')
    parser_init.add_argument("-c", dest='conf', action="store_true", help='Get params from configuration file')

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
    parser_end.add_argument('--ongoing', action='store_true', help='Do post processing while simulation is in progress')
    parser_end.add_argument('--params', action='store', type=str, default=None, help='Post-processing parameters')
    # parser_end.add_argument('--version', action='store', type=int, default=1, help='Post-processing parameters')
    # parser_end.add_argument('--part', action='store', type=str, default=None, help='Set partition (defaulting to small)')
    # parser_end.add_argument('--nodes', action='store', type=STRNodes, default=STRNodes.ALL, help='Set nodes (default all possible)')
    # parser_end.add_argument('--files', action='store', type=str, default=None, help='Post-processing parameters')

    parser_gen_conf = sub_parsers.add_parser('genconf', help='Generate config file (all possible options with default values)')
    parser_check_conf = sub_parsers.add_parser('checkconf', help='Check config file')

    args = parser.parse_args()
    cwd = Path.cwd()
    return main_main(cwd, args)


if __name__ == '__main__':
    sys.exit(main())
