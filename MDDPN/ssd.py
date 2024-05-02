#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 20:15:15

import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any

from . import config
# from .run import run
from .init import init
from .ender import ender
from .restart import restart
from . import constants as cs
from .utils import load_state, setup_logger, logs


def minilog(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    soutHandler = logging.StreamHandler(stream=sys.stdout)
    soutHandler.setLevel(logging.DEBUG)
    soutHandler.setFormatter(formatter)
    logger.addHandler(soutHandler)
    serrHandler = logging.StreamHandler(stream=sys.stderr)
    serrHandler.setFormatter(formatter)
    serrHandler.setLevel(logging.WARNING)
    logger.addHandler(serrHandler)
    return logger


def com_set() -> Dict[str, Any]:
    file = cs.sp.cwd / cs.sp.args.file
    if not file.exists():
        raise FileNotFoundError(f"There is no file {file.as_posix()}")
    with file.open('r') as f:
        fp = json.load(f)
    fp[cs.sp.args.variable] = cs.sp.args.value
    with file.open('w') as f:
        json.dump(fp, f)
    return fp


@logs
def stateful() -> int:
    with load_state() as _:
        if cs.sp.args.command == "run" or cs.sp.args.command == "restart":
            cs.sp.logger.info(msg="'restart' command received (or 'run)")
            return restart()
        elif cs.sp.args.command == "end":
            cs.sp.logger.info("'end' command received")
            if not cs.sp.allow_post_process:
                cs.sp.logger.error("Post processing disallowed")
                return 1
            else:
                return ender()
        elif cs.sp.args.command == "set":
            cs.sp.logger.info("'set' command received")
            com_set()
        else:
            cs.sp.logger.error(f"Unknown '{cs.sp.args.command}' command received")
            return 1

    return 0


def choose() -> int:
    # cs.sp.logger = setup_logger(cwd, "ssd", logging.DEBUG)  # if args.debug else logging.INFO)
    cs.sp.logger = minilog("MDDPN")
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
            return stateful()
    except Exception as e:
        cs.sp.logger.error("Uncaught exception")
        cs.sp.logger.exception(e)
        return 1


def main():
    parser = argparse.ArgumentParser(prog="MDDPN.py")
    parser.add_argument("--debug", action="store_true", help="Sets logging level to debug")
    parser.add_argument("-c", "--conf", action="store", type=str, help=f"Specify conffile. Defaults to './{cs.files.config_json}'")
    parser.add_argument("--toml", action="store_true", help="Change conffile format to toml")

    sub_parsers = parser.add_subparsers(help="sub-command help", dest="command")

    parser_init = sub_parsers.add_parser("init", help="Initialize directory")
    parser_init.add_argument("-p", "--params", action="store", type=str, help="Obtain simulation parameters from command-line")
    parser_init.add_argument("-rm", "--restart_mode", choices=["one", "two", "multiple"], help="Specify two-filed restarts instead of restart.*",)
    # parser_init.add_argument("-f", '--file', action="store_true", help='Obtain simulation parameters from file')
    parser_init.add_argument("-fn", "--fname", action="store", type=str, help="Specify file to get parameters from")
    parser_init.add_argument("--pfc", "--params_from_conf", action="store_true", help="Get params from configuration file")

    parser_set = sub_parsers.add_parser("set", help="Set variable in config json file")
    parser_set.add_argument("file", action="store", type=str, help="File in which set the variable")
    parser_set.add_argument("variable", action="store", type=str, help="The variable to set")
    parser_set.add_argument("value", action="store", type=float, help="Value of variable")

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
    # parser_end.add_argument('--version', action='store', type=int, default=1, help='Post-processing parameters')
    # parser_end.add_argument('--part', action='store', type=str, default=None, help='Set partition (defaulting to small)')
    # parser_end.add_argument('--nodes', action='store', type=STRNodes, default=STRNodes.ALL, help='Set nodes (default all possible)')
    # parser_end.add_argument('--files', action='store', type=str, default=None, help='Post-processing parameters')

    parser_gen_conf = sub_parsers.add_parser("genconf", help="Generate config file (all possible options with default values)")
    parser_check_conf = sub_parsers.add_parser("checkconf", help="Check config file")

    args = parser.parse_args()
    cs.sp.args = args
    cwd = Path.cwd()
    cs.sp.cwd = cwd
    return choose()


if __name__ == "__main__":
    sys.exit(main())
