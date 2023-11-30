#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 30-11-2023 07:04:41

import logging
from typing import Dict
from pathlib import Path
from argparse import Namespace as argNamespace

from .. import sbatch
from . import constants as cs
from .execution import run_polling
from .utils import states, LogicError
from .testrun import test_run
from ..utils import config


def submit_run(cwd: Path, infile: Path, logger: logging.Logger, opt: int) -> int:
    if cs.sp.run_tests:
        if not test_run(cwd, infile, logger.getChild('test_run')):
            logger.error("Test run was unsuccessfull")
            raise RuntimeError("Test run was unsuccessfull")
    cs.sp.sconf_main[sbatch.cs.fields.executable] = cs.execs.lammps
    cs.sp.sconf_main[sbatch.cs.fields.args] = "-v test 1 -nonbuf -echo both -log '{jd}/log.lammps' -in " + infile.as_posix()
    # cs.sp.sconf_main[sbatch.cs.folders.run] + cs.sp.sconf_main[sbatch.cs.fields.jname] +
    return sbatch.sbatch.run(cwd, logger.getChild("submitter"), config(cs.sp.sconf_main), opt)


def run(cwd: Path, state: Dict, args: argNamespace, logger: logging.Logger) -> Dict:
    if states(state[cs.sf.state]) != states.fully_initialized:
        logger.error("Folder isn't fully initialized")
        raise LogicError("Folder isn't fully initialized")

    state[cs.sf.state] = states.started

    if not args.test:
        logger.info("Submitting task")
        infile_path: Path = cwd / cs.folders.in_file / state[cs.sf.run_labels]['START']['0'][cs.sf.in_file]
        sb_jobid = submit_run(cwd, infile_path, logger, 0)
        logger.info(f"Sbatch jobid: {sb_jobid}")
        state[cs.sf.run_labels]['START']["0"][cs.sf.jobid] = sb_jobid
        state[cs.sf.run_labels]['START'][cs.sf.runs] = 1

        if not args.no_auto:
            logger.info("Staring polling")
            run_polling(cwd, args, sb_jobid, state[cs.sf.tag], logger.getChild("poll_start"))
        else:
            logger.info("Not starting polling")
    else:
        logger.info("This is a test, not submitting task")

    return state


if __name__ == "__main__":
    pass
