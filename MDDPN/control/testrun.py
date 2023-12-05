#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 05-12-2023 16:34:02

import os
import time
import shutil
import logging
from typing import List, Callable
from pathlib import Path

from . import polling
from .. import sbatch
from . import constants as cs
from ..utils import config


ignored_folders = [cs.folders.dumps, cs.folders.special_restarts, cs.folders.post_process]


def gen_ignore(cwd: Path) -> Callable[[str, List[str]], List[str]]:
    def ign(pwd: str, list_files: List[str]) -> List[str]:
        if Path(pwd) == cwd:
            return ignored_folders
        return ignored_folders

    return ign


def test_run(cwd: Path, in_file: Path, logger: logging.Logger) -> bool:
    new_cwd = cwd / ".." / (cs.folders.tmp_dir_basename + f"{round(time.time())}")
    new_cwd = new_cwd.resolve()
    new_in_file = new_cwd / in_file.relative_to(cwd)
    logger.debug(f"Copying folder to {new_cwd.as_posix()}")
    shutil.copytree(cwd, new_cwd, ignore=gen_ignore(cwd))
    for el in ignored_folders:
        (new_cwd / el).mkdir(exist_ok=True)

    cs.sp.sconf_test[sbatch.cs.fields.executable] = cs.execs.lammps
    cs.sp.sconf_test[sbatch.cs.fields.args] = (
        "-v test 0 -echo both -log '{jd}/log.lammps' -in " + new_in_file.as_posix()
    )
    os.chdir(new_cwd)
    logger.info("Submitting test run and waiting it to complete")
    jobid = sbatch.sbatch.run(new_cwd, logger.getChild("submitter"), config(cs.sp.sconf_test))
    logger.info(f"Submitted test jod id: {jobid}")
    try:
        res_state = polling.loop(new_cwd, jobid, 20, logger.getChild("poll"), False)
    except Exception as e:
        logger.error("Exception during polling test run")
        logger.exception(e)
        raise
    os.chdir(cwd)
    logger.debug(f"Polling complete, result state: '{str(res_state)}'")
    if res_state == polling.SStates.COMPLETED:
        logger.info("State is OK, cleaning temporary dir")
        # shutil.rmtree(new_cwd)
        return True
    else:
        print("Error on test run")
        return False
