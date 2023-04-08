#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-04-2023 15:03:29


import subprocess as sb
import re
import shlex
from enum import Enum
import argparse
from pathlib import Path
import time
from datetime import datetime
import os


sl_dir_def = "slinfo"
time_criteria = 15 * 60


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

failure_states = [SStates.BOOT_FAIL, SStates.CANCELLED, SStates.DEADLINE,
                  SStates.NODE_FAIL, SStates.OUT_OF_MEMORY, SStates.STOPPED]
states_to_restart = [SStates.COMPLETED, SStates.FAILED, SStates.TIMEOUT]

# unknown_states = [SStates.CONFIGURING, SStates.COMPLETING, SStates.PENDING, SStates.PREEMPTED,  SStates.RESV_DEL_HOLD, SStates.REQUEUE_FED, SStates.REQUEUE_HOLD, SStates.REQUEUED, SStates.RESIZING, SStates.REVOKED, SStates.SIGNALING, SStates.SPECIAL_EXIT, SStates.STAGE_OUT, SStates.STOPPED, SStates.SUSPENDED, SStates.TIMEOUT]


class LogicError(Exception):
    pass


def perform_restart(cwd):
    # raise NotImplementedError
    cmd = "ssd.py restart"
    cmds = shlex.split(cmd)
    p = sb.Popen(cmds, start_new_session=True)
    # a = sb.run(cmds, capture_output=True)


def perform_check(jobid):
    cmd = f"sacct -j {jobid} -n -p -o jobid,state"
    cmds = shlex.split(cmd)
    # p = sb.Popen(cmds, start_new_session=True)
    a = sb.run(cmds, capture_output=True)
    bout = a.stdout.decode('ascii')
    for line in bout.splitlines():
        if re.match(r"^\d+\|[a-zA-Z]+\|", line):
            # print(line)
            return SStates(line.split('|')[1])
        # for state in states_str:
        #     if state in all_states:
        #         if re.match(r"^\d+\|" + state + "\|", line):
        #             return SStates(state)
        #         else:
        #             return SStates.UNKNOWN_STATE
        #     else:
        #         raise LogicError("Unknown state in 'states_str'")
        # break


# datetime.now().strftime("%d:%m:%Y, %H:%M:%S")

def loop(cwd, args):
    jobid = args.jobid
    last_state = SStates.UNKNOWN_STATE
    last_state_time = time.time()
    logfile = str(jobid) + "_" + str(round(time.time())) + "_" + "poll.log"
    logdir = Path(sl_dir_def)
    logfile = Path(logdir / logfile)
    with logfile.open("w") as f:
        f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                     " " + "Started main loop\n")
    while True:
        try:
            state = perform_check(jobid)
        except Exception as e:
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime(
                    "%d:%m:%Y-%H:%M:%S")) + " " + "Check perform function raised an exception. Exiting...\n")
                f.writelines(str(e))
            raise
        if state in states_to_restart:
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                             " " + f"Succesfully reached restart state: {str(state)}. Restarting task\n")
            perform_restart(cwd)
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                             " " + "Succesfully restarted task. Exiting...\n")
            return 0
        elif state in failure_states:
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                             " " + f"Something went wrong with slurm job. State: {str(state)} Exiting...\n")
            return 0
        elif state == SStates.UNKNOWN_STATE:
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                             " " + f"Unknown slurm job state: {str(state)} Exiting...\n")
            return 0
        elif state == SStates.PENDING:
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) + " " +
                             f"Pending...\n")
        elif state != SStates.RUNNING:
            if state == last_state:
                if time.time() - last_state_time > time_criteria:
                    with logfile.open("a") as f:
                        f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) + " " +
                                     f"State {state} was too long (>{time_criteria} secs). Exiting...\n")
                    return 0
                else:
                    with logfile.open("a") as f:
                        f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) + " " +
                                     f"State {state} still for {time_criteria} secs\n")
            else:
                last_state = state
                last_state_time = time.time()
                with logfile.open("a") as f:
                    f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                                 " " + f"Strange state {state} encountered\n")
        elif state == SStates.RUNNING:
            last_state = state
            last_state_time = time.time()
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                             " " + "RUNNING\n")
        else:
            with logfile.open("a") as f:
                f.writelines(str(datetime.now().strftime("%d:%m:%Y-%H:%M:%S")) +
                             " " + "HOW?\n")
            raise LogicError("HOW?")
        time.sleep(args.every * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='llOll')
    parser.add_argument('--jobid', action='store', type=int,
                        help='Slurm job ID')
    parser.add_argument('--every', action='store', type=int,
                        help='Perform poll every N-minutes')
    parser.add_argument('cwd', action='store', type=str,
                        help='Current working directory')
    args = parser.parse_args()
    cwd = Path.cwd()
    loop(cwd, args)
