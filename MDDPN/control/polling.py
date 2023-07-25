#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 12:16:17

import re
import sys
import time
import shlex
import argparse
from enum import Enum
import subprocess as sb
from typing import Tuple
from pathlib import Path
from datetime import datetime

from .. import constants as cs


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


def perform_restart(cwd: Path) -> Tuple[str, str]:
    cmd = "MDDPN.py restart"
    cmds = shlex.split(cmd)
    # p = sb.Popen(cmds, start_new_session=True)
    p = sb.run(cmds, capture_output=True)
    bout = p.stdout.decode()
    berr = p.stderr.decode()
    return bout, berr


def perform_check(jobid: int) -> SStates:
    cmd = f"sacct -j {jobid} -n -p -o jobid,state"
    cmds = shlex.split(cmd)
    p = sb.run(cmds, capture_output=True)
    bout = p.stdout.decode()
    # berr = p.stderr.decode()
    # f.writelines("sacct output\n")
    # f.writelines(bout)
    # f.writelines("sacct errors\n")
    # f.writelines(berr)
    # f.writelines("GGG\n")
    for line in bout.splitlines():
        if re.match(r"^\d+\|[a-zA-Z]+\|", line):
            return SStates(line.split('|')[1])
    return SStates.UNKNOWN_STATE


def tm() -> str:
    return str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S"))


def loop(cwd: Path, args: argparse.Namespace):
    jobid = args.jobid
    last_state = SStates.RUNNING
    last_state_time = time.time()
    logfile = str(jobid) + "_" + str(round(time.time())) + "_poll.log"
    logfile = cwd / cs.folders.sl / logfile
    with logfile.open("w") as f:
        f.writelines(f"{tm()}: Started main loop\n")
    while True:
        time.sleep(args.every * 60)
        try:
            with logfile.open("a") as f:
                state = perform_check(jobid)
        except Exception as e:
            with logfile.open("a") as f:
                f.writelines(f"{tm()}: Check perform function raised an exception. Exiting...\n")
                f.writelines(str(e))
            raise
        with logfile.open("a") as f:
            if state in states_to_restart:
                f.writelines(f"{tm()}: Succesfully reached restart state: {str(state)}. Restarting task\n")
                lout, lerr = perform_restart(cwd)
                f.writelines(f"{tm()}: Succesfully restarted task. Exiting...\n")
                f.writelines("#####  Normal output:  #####")
                f.writelines(lout)
                f.writelines("\n")
                f.writelines("#####  Errors:  #####")
                f.writelines(lerr)
                f.writelines("\n")
                return 0
            elif state in failure_states:
                f.writelines(f"{tm()}: Something went wrong with slurm job. State: {str(state)} Exiting...\n")
                return 0
            elif state == SStates.UNKNOWN_STATE:
                f.writelines(f"{tm()}: Unknown slurm job state: {str(state)} Exiting...\n")
                return 0
            elif state == SStates.PENDING:
                f.writelines(f"{tm()}: Pending...\n")
            elif state != SStates.RUNNING:
                if state == last_state:
                    if time.time() - last_state_time > time_criteria:
                        f.writelines(f"{tm()}: State {state} was too long (>{time_criteria} secs). Exiting...\n")
                        return 0
                    else:
                        f.writelines(f"{tm()}: State {state} still for {time_criteria} secs\n")
                else:
                    last_state = state
                    last_state_time = time.time()
                    f.writelines(f"{tm()}: Strange state {state} encountered\n")
            elif state == SStates.RUNNING:
                last_state = state
                last_state_time = time.time()
                f.writelines(f"{tm()}: RUNNING\n")
            else:
                f.writelines(f"{tm()}: HOW?\n")
                raise LogicError("HOW?")


def main():
    parser = argparse.ArgumentParser(prog='polling.py')
    parser.add_argument('--jobid', action='store', type=int, help='Slurm job ID')
    parser.add_argument('--every', action='store', type=int, help='Perform poll every N-minutes')
    parser.add_argument('cwd', action='store', type=str, help='Current working directory')
    args = parser.parse_args()
    cwd = Path.cwd()
    return loop(cwd, args)


if __name__ == "__main__":
    sys.exit(main())
