#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 03-05-2024 18:55:16

import os
import time
import shutil
from typing import Dict, Union, Callable, List
from pathlib import Path

from MPMU import confdict
import pysbatch_ng

from . import constants as cs
from .utils import logs


ignored_folders = [cs.folders.dumps, cs.folders.special_restarts, cs.folders.post_process]


def gen_ignore(cwd: Path) -> Callable[[str, List[str]], List[str]]:
    def ign(pwd: str, list_files: List[str]) -> List[str]:
        if Path(pwd) == cwd:
            return ignored_folders
        return ignored_folders

    return ign


@logs
def test_run(in_file: Path) -> bool:
    new_cwd = cs.sp.cwd / ".." / (cs.folders.tmp_dir_basename + f"{round(time.time())}")
    new_cwd = new_cwd.resolve()
    new_in_file = new_cwd / in_file.relative_to(cs.sp.cwd)
    cs.sp.logger.debug(f"Copying folder to {new_cwd.as_posix()}")
    shutil.copytree(cs.sp.cwd, new_cwd, ignore=gen_ignore(cs.sp.cwd))
    for el in ignored_folders:
        (new_cwd / el).mkdir(exist_ok=True)

    cs.sp.sconf_test[pysbatch_ng.cs.fields.executable] = cs.execs.lammps
    cs.sp.sconf_test[pysbatch_ng.cs.fields.args] = ("-v test 0 -echo both -log '{jd}/log.lammps' -in " + new_in_file.as_posix())
    os.chdir(new_cwd)
    cs.sp.logger.info("Submitting test run and waiting it to complete")
    jobid = pysbatch_ng.sbatch.run(new_cwd, cs.sp.logger.getChild("submitter"), confdict(cs.sp.sconf_test))
    cs.sp.logger.info(f"Submitted test jod id: {jobid}")
    try:
        success = pysbatch_ng.polling.loop(jobid, 20, cs.sp.logger.getChild("poll"), 60*60)
    except Exception as e:
        cs.sp.logger.error("Exception during polling test run")
        cs.sp.logger.exception(e)
        raise
    os.chdir(cs.sp.cwd)
    cs.sp.logger.debug(f"Polling complete.")
    if success:
        cs.sp.logger.info("Success, cleaning temporary dir")
        shutil.rmtree(new_cwd)
        return True
    else:
        cs.sp.logger.error("Error on test run")
        return False

@logs
def run_polling(jobid: int, tag: int, cmd: Union[str, None] = None) -> None:
    pysb_conf: Dict[str, Union[str, int, bool, Path]] = {}
    # if cs.sp.args.debug:
    pysb_conf[pysbatch_ng.cs.fields.debug] = True
    pysb_conf[pysbatch_ng.cs.fields.cwd] = cs.sp.cwd.as_posix()
    pysb_conf[pysbatch_ng.cs.fields.jobid] = jobid
    pysb_conf[pysbatch_ng.cs.fields.ptag] = tag
    pysb_conf[pysbatch_ng.cs.fields.logfolder] = cs.folders.slurm
    pysb_conf[pysbatch_ng.cs.fields.logto] = 'file'
    if not cmd: pysb_conf[pysbatch_ng.cs.fields.cmd] = f"{cs.execs.MDDPN} --debug restart"
    else: pysb_conf[pysbatch_ng.cs.fields.cmd] = cmd
    pysb_conf[pysbatch_ng.cs.fields.every] = 5
    pysb_conf[pysbatch_ng.cs.fields.times_criteria] = 288

    conff = {
        pysbatch_ng.cs.fields.spoll: pysb_conf,
        pysbatch_ng.cs.fields.sbatch: cs.sp.sconf_test
        }

    pysbatch_ng.spoll.run_conf(conff, cs.sp.cwd / cs.folders.slurm, cs.sp.logger.getChild("spoll"))


@logs
def submit_run(infile: Path, number: int) -> int:
    """_summary_

    Args:
        cwd (Path): _description_
        infile (Path): _description_
        logger (logging.Logger): _description_
        number (int): Number of task. At this stage there is no jobid yet, so it used instead. Defaults to None.

    Raises:
        RuntimeError: Thrown if test run was unsuccesful

    Returns:
        jobid (int): slurm's jobid
    """
    if cs.sp.run_tests:
        if not test_run(infile): raise RuntimeError("Test run was unsuccessfull")
    cs.sp.sconf_main[pysbatch_ng.cs.fields.executable] = cs.execs.lammps
    cs.sp.sconf_main[pysbatch_ng.cs.fields.args] = "-v test 1 -nonbuf -echo both -log '{jd}/log.lammps' -in " + infile.as_posix()
    return pysbatch_ng.sbatch.run(cs.sp.cwd, cs.sp.logger.getChild("submitter"), confdict(cs.sp.sconf_main), number)


# def run(cwd: Path, state: Dict, args: argparse.Namespace, logger: logging.Logger) -> Dict:
#     if states(state[cs.sf.state]) != states.fully_initialized:
#         logger.error("Folder isn't fully initialized")
#         raise RuntimeError("Folder isn't fully initialized")

#     state[cs.sf.state] = states.started

#     if not args.test:
#         logger.info("Submitting task")
#         infile_path: Path = cwd / cs.folders.in_file / state[cs.sf.run_labels]['START']['0'][cs.sf.in_file]
#         sb_jobid = submit_run(cwd, infile_path, logger, 0)
#         logger.info(f"Sbatch jobid: {sb_jobid}")
#         state[cs.sf.run_labels]['START']["0"][cs.sf.jobid] = sb_jobid
#         state[cs.sf.run_labels]['START'][cs.sf.runs] = 1

#         if not args.no_auto:
#             logger.info("Staring polling")
#             run_polling(cwd, args, sb_jobid, state[cs.sf.tag], logger.getChild("poll_start"))
#         else:
#             logger.info("Not starting polling")
#     else:
#         logger.info("This is a test, not submitting task")

#     return state


if __name__ == "__main__":
    pass
