#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 23:31:43

import sys
import logging
import argparse
from pathlib import Path

from .init import init
from .ender import ender
from .restart import restart
from . import config, constants as cs
from .utils import load_state, setup_logger, logs, RC


@logs
def endd():
    if not cs.sp.allow_post_process:
        cs.sp.logger.error("Post processing disallowed")
        return 1
    else:
        return ender()


def choose() -> int:
    cs.sp.logger = setup_logger("MDDPN", logging.DEBUG)  # if args.debug else logging.INFO)
    cs.sp.logger.info(f"Root folder: {cs.sp.cwd.as_posix()}")
    cs.sp.logger.info(f"Envolved args: {cs.sp.args}")
    try:
        if cs.sp.args.command == "genconf":
            cs.sp.logger.info("'genconf' command received")
            return config.genconf(Path(cs.sp.args.conf).resolve())
        if not config.configure(config.loadconf()):
            return 1
        if cs.sp.args.command == "checkconf":
            cs.sp.logger.info("'checkconf' command received")
            return 0
        elif cs.sp.args.command == "init":
            cs.sp.logger.info("'init' command received")
            return init()
        else:
            with load_state() as _:
                if cs.sp.args.command == "run" or cs.sp.args.command == "restart":
                    cs.sp.logger.info(msg="'restart' command received (or 'run)")
                    lrc: RC = restart()
                    if lrc == RC.END_REACHED:
                        cs.sp.logger.info("End was reached, trying to start post processing")
                        return endd()
                    else: return int(lrc)
                elif cs.sp.args.command == "end":
                    cs.sp.logger.info("'end' command received")
                    return endd()
                else:
                    cs.sp.logger.error(f"Unknown '{cs.sp.args.command}' command received")
                    return 1
    except Exception as e:
        cs.sp.logger.error("Uncaught exception")
        cs.sp.logger.exception(e)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="MDDPN.py")
    parser.add_argument("--debug", action="store_true", help="Sets logging level to debug")
    parser.add_argument("-c", "--conf", action="store", type=str, help=f"Specify conffile. Defaults to './{cs.files.config_json}'")
    parser.add_argument("--toml", action="store_true", help="Change conffile format to toml")

    sub_parsers = parser.add_subparsers(help="sub-command help", dest="command")

    parser_init = sub_parsers.add_parser("init", help="Initialize directory")
    parser_init.add_argument("-p", "--params", action="store", type=str, help="Obtain simulation parameters from command-line")
    parser_init.add_argument("-rm", "--restart_mode", choices=["one", "two", "multiple"], help="Specify two-filed restarts instead of restart.*",)
    parser_init.add_argument("-fn", "--fname", action="store", type=str, help="Specify file to get parameters from")
    parser_init.add_argument("--pfc", "--params_from_conf", action="store_true", help="Get params from configuration file")

    parser_run = sub_parsers.add_parser("run", help="Run LAMMPS simulation")
    parser_run.add_argument("--test", action="store_true", help="Whether actually run LAMMPS or not. Test purposes only")
    parser_run.add_argument("--no_auto", action="store_true", help="Don't run polling sbatch and don't auto restart")

    parser_restart = sub_parsers.add_parser("restart", help="Generate restart file and run it")
    parser_restart.add_argument("--test", action="store_true", help="Whether actually run LAMMPS or not. Test purposes only")
    parser_restart.add_argument("-s", "--step", action="store", type=int, help="From which step do the restart")
    parser_restart.add_argument("--no_auto", action="store_true", help="Don't run polling sbatch and don't auto restart")

    parser_end = sub_parsers.add_parser("end", help="Post-processing")
    parser_end.add_argument("--ongoing", action="store_true", help="Do post processing while simulation is in progress")
    parser_end.add_argument("--anyway", action="store_true", help="Proceed anyway despite of errors in state file")
    parser_end.add_argument("--params", action="store", type=str, default=None, help="Post-processing parameters")

    parser_gen_conf = sub_parsers.add_parser("genconf", help="Generate config file (all possible options with default values)")
    parser_check_conf = sub_parsers.add_parser("checkconf", help="Check config file")

    args = parser.parse_args()
    cs.sp.args = args
    cwd = Path.cwd()
    cs.sp.cwd = cwd
    return choose()


if __name__ == "__main__":
    sys.exit(main())
