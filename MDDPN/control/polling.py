#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-09-2023 20:17:01

import re
import sys
import time
import logging
import argparse
from enum import Enum
from pathlib import Path

from ..utils import wexec
from . import constants as cs


time_criteria = cs.params.time_criteria


class SStates(str, Enum):
    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"
    # SPECIAL_A = "SPECIAL_A"
    UNKNOWN_STATE = "UNKNOWN_STATE"


# sacct -j 87180 -n -p -o jobid,state
# 87180|COMPLETED|
# 87180.batch|COMPLETED|
# 87180.0|COMPLETED|


# [SStates.BOOT_FAIL, SStates.CANCELLED, SStates.COMPLETED, SStates.CONFIGURING, SStates.COMPLETING, SStates.DEADLINE, SStates.FAILED, SStates.NODE_FAIL, SStates.OUT_OF_MEMORY, SStates.PENDING, SStates.PREEMPTED, SStates.RUNNING,
    # SStates.RESV_DEL_HOLD, SStates.REQUEUE_FED, SStates.REQUEUE_HOLD, SStates.REQUEUED, SStates.RESIZING, SStates.REVOKED, SStates.SIGNALING, SStates.SPECIAL_EXIT, SStates.STAGE_OUT, SStates.STOPPED, SStates.SUSPENDED, SStates.TIMEOUT]
# "BOOT_FAIL", "CANCELLED", "COMPLETED", "CONFIGURING", "COMPLETING", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PENDING", "PREEMPTED", "RUNNING", "RESV_DEL_HOLD", "REQUEUE_FED", "REQUEUE_HOLD", "REQUEUED", "RESIZING", "REVOKED", "SIGNALING", "SPECIAL_EXIT", "STAGE_OUT", "STOPPED", "SUSPENDED", "TIMEOUT"

all_states = [SStates.BOOT_FAIL, SStates.CANCELLED, SStates.COMPLETED, SStates.CONFIGURING, SStates.COMPLETING, SStates.DEADLINE, SStates.FAILED, SStates.NODE_FAIL, SStates.OUT_OF_MEMORY, SStates.PENDING, SStates.PREEMPTED, SStates.RUNNING,
              SStates.RESV_DEL_HOLD, SStates.REQUEUE_FED, SStates.REQUEUE_HOLD, SStates.REQUEUED, SStates.RESIZING, SStates.REVOKED, SStates.SIGNALING, SStates.SPECIAL_EXIT, SStates.STAGE_OUT, SStates.STOPPED, SStates.SUSPENDED, SStates.TIMEOUT]

states_str = ["BOOT_FAIL", "CANCELLED", "COMPLETED", "CONFIGURING", "COMPLETING", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PENDING", "PREEMPTED", "RUNNING",
              "RESV_DEL_HOLD", "REQUEUE_FED", "REQUEUE_HOLD", "REQUEUED", "RESIZING", "REVOKED", "SIGNALING", "SPECIAL_EXIT", "STAGE_OUT", "STOPPED", "SUSPENDED", "TIMEOUT"]

failure_states = [SStates.BOOT_FAIL, SStates.DEADLINE,
                  SStates.NODE_FAIL, SStates.OUT_OF_MEMORY, SStates.STOPPED, SStates.CANCELLED]
states_to_restart = [SStates.COMPLETED, SStates.FAILED, SStates.TIMEOUT]

# unknown_states = [SStates.CONFIGURING, SStates.COMPLETING, SStates.PENDING, SStates.PREEMPTED,  SStates.RESV_DEL_HOLD, SStates.REQUEUE_FED, SStates.REQUEUE_HOLD, SStates.REQUEUED, SStates.RESIZING, SStates.REVOKED, SStates.SIGNALING, SStates.SPECIAL_EXIT, SStates.STAGE_OUT, SStates.STOPPED, SStates.SUSPENDED, SStates.TIMEOUT]


class LogicError(Exception):
    pass


def perform_restart(cwd: Path, logger: logging.Logger) -> str:
    cmd = f"{cs.execs.MDDPN} --debug restart"
    bout = wexec(cmd, logger.getChild('MDDPN'))
    return bout


def perform_check(jobid: int, logger: logging.Logger) -> SStates:
    cmd = f"{cs.execs.sacct} -j {jobid} -n -p -o jobid,state"
    bout = wexec(cmd, logger.getChild('sacct'))
    for line in bout.splitlines():
        if re.match(r"^\d+\|[a-zA-Z]+\|", line):
            return SStates(line.split('|')[1])
    return SStates.UNKNOWN_STATE


# def tm() -> str:
#     return str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S"))


def loop(cwd: Path, args: argparse.Namespace):
    jobid = args.jobid
    last_state = SStates.RUNNING
    last_state_time = time.time()
    logfile_name = str(jobid) + "_" + str(round(time.time())) + "_poll.log"
    logfile = cwd / cs.folders.slurm / logfile_name

    handler = logging.FileHandler(logfile)
    handler.setFormatter(cs.sp.formatter)

    logger = logging.getLogger('polling')
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    logger.addHandler(handler)

    lockfile = cwd / cs.files.restart_lock
    if lockfile.exists():
        logger.error(f"Lockfile exists: {lockfile.as_posix()}")
        raise Exception(f"Lockfile exists: {lockfile.as_posix()}")
    lockfile.touch()

    logger.info("Started main loop")

    fl = True

    try:
        while fl:
            time.sleep(args.every * 60)
            logger.info("Checking job")
            try:
                state = perform_check(jobid, logger.getChild("task_check"))
            except Exception as e:
                logger.critical("Check failed. Exiting...")
                logger.critical(str(e))
                raise
            logger.info(f"Job state: {str(state)}")
            if state in states_to_restart:
                logger.info(f"Succesfully reached restart state: {str(state)}. Restarting task")
                lockfile.unlink()
                lout = perform_restart(cwd, logger.getChild("restart"))
                logger.info("Succesfully restarted task. Exiting...")
                logger.debug("#####  Normal output:  #####")
                logger.debug(lout)
                fl = False
            elif state in failure_states:
                logger.error(f"Something went wrong with slurm job. State: {str(state)} Exiting...")
                fl = False
            elif state == SStates.UNKNOWN_STATE:
                logger.error(f"Unknown slurm job state: {str(state)} Exiting...")
                fl = False
            elif state == SStates.PENDING:
                logger.info("Pending...")
            elif state != SStates.RUNNING:
                if state == last_state:
                    if time.time() - last_state_time > time_criteria:
                        logger.error(f"State {state} was too long (>{time_criteria} secs). Exiting...")
                        fl = False
                    else:
                        logger.info(f"State {state} still for {time_criteria} secs\n")
                else:
                    last_state = state
                    last_state_time = time.time()
                    logger.warning(f"Strange state {state} encountered\n")
            elif state == SStates.RUNNING:
                last_state = state
                last_state_time = time.time()
                logger.info("RUNNING")
            else:
                logger.critical("HOW?")
                raise LogicError("HOW?")
    except Exception as e:
        logger.critical("Uncaught exception")
        logger.critical(str(e))
        # lockfile.unlink()
        raise

    lockfile.unlink()
    return 0


def main():
    parser = argparse.ArgumentParser(prog='polling.py')
    parser.add_argument('--debug', action='store_true', help='Debug, prints only parsed arguments')
    parser.add_argument('--jobid', action='store', type=int, help='Slurm job ID')
    parser.add_argument('--tag', action='store', type=int, help='Project tag')
    parser.add_argument('--every', action='store', type=int, help='Perform poll every N-minutes')
    parser.add_argument('cwd', action='store', type=str, help='Current working directory')
    args = parser.parse_args()
    cwd = Path.cwd()
    return loop(cwd, args)


if __name__ == "__main__":
    sys.exit(main())
