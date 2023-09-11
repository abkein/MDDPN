#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 11-09-2023 23:21:38

import shlex
import logging
import argparse
import subprocess as sb
from pathlib import Path

from .. import constants as cs


def run_polling(cwd: Path, args: argparse.Namespace, sb_jobid: int, tag: int, logger: logging.Logger) -> None:
    every = 5
    if args.debug:
        cmd = f"{cs.execs.MDpoll} --debug --tag {tag} --jobid {sb_jobid} --every {every} '{cwd.as_posix()}'"
    else:
        cmd = f"{cs.execs.MDpoll} --tag {tag} --jobid {sb_jobid} --every {every} '{cwd.as_posix()}'"
    logger.info("Starting poll process")
    logger.debug(f"    {cmd}")
    cmds = shlex.split(cmd)
    sb.Popen(cmds, start_new_session=True)


if __name__ == "__main__":
    pass
