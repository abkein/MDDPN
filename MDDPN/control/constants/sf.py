#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 03-09-2023 12:02:32

from .union import time_step, restart_every


state: str = 'state'
restart_mode: str = "restart_mode"
tag: str = 'tag'
restart: str = 'restart_count'
in_file: str = "in.file"
jobs_list: str = "jobs"
run_counter: str = "run_counter"
run_labels: str = "run_labels"
user_variables: str = "user_variables"
run_no: str = "run_no"
labels_list: str = "labels"
variables: str = "variables"
runs: str = "runs"
dump_file: str = "dump_f"
jobid: str = "sb_jobid"
restart_files: str = 'restart_files'
restarts: str = "restarts"
begin_step: str = "begin_step"
end_step: str = "end_step"
last_step: str = "last_step"

post_process_id: str = "post_process"

if __name__ == "__main__":
    pass
