#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-09-2023 23:40:55

import time
import shutil
import logging
from pathlib import Path

from . import polling
from .. import sbatch
from . import constants as cs


def test_run(cwd: Path, in_file: Path, logger: logging.Logger) -> bool:
    new_cwd = Path(cs.folders.def_lin_tmp) / (cs.folders.tmp_dir_basename + f"{round(time.time())}")
    new_in_file = new_cwd / in_file.relative_to(cwd)
    logger.debug(f"Copying folder to {new_cwd.as_posix()}")
    shutil.copytree(cwd, new_cwd)

    cs.sp.sconf_test[sbatch.cs.fields.executable] = cs.execs.lammps
    cs.sp.sconf_test[sbatch.cs.fields.args] = f"-skiprun -in {new_in_file.as_posix()}"
    logger.info("Submitting test run and waiting it to complete")
    jobid = sbatch.sbatch.run(new_cwd, logger.getChild("submitter"), cs.sp.sconf_test)
    logger.info(f"Submitted test jod id: {jobid}")
    try:
        res_state = polling.loop(new_cwd, jobid, 20, logger.getChild('poll'), False)
    except Exception as e:
        logger.error("Exception during polling test run")
        logger.exception(e)
        raise
    logger.debug(f"Polling complete, result state: '{str(res_state)}'")
    if res_state == polling.SStates.COMPLETED:
        logger.info("State is OK, cleaning temporary dir")
        shutil.rmtree(new_cwd)
        return True
    else:
        print("Error on test run")
        return False
