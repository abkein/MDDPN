#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-09-2023 17:39:58

import shlex
import logging
import argparse
import subprocess as sb
from pathlib import Path


def run_polling(cwd: Path, args: argparse.Namespace, sb_jobid: int, tag: int, logger: logging.Logger) -> None:
    every = 5
    if args.debug:
        cmd = f"polling.py --debug --tag {tag} --jobid {sb_jobid} --every {every} '{cwd.as_posix()}'"
    else:
        cmd = f"polling.py --tag {tag} --jobid {sb_jobid} --every {every} '{cwd.as_posix()}'"
    logger.info("Starting poll process")
    logger.debug(f"    {cmd}")
    cmds = shlex.split(cmd)
    sb.Popen(cmds, start_new_session=True)
