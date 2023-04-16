#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 16-04-2023 14:58:37

from pathlib import Path


state_field = 'state'
restart_field = 'restart_count'
in_file_field = "in.file"
jobs_list_field = "jobs"
data_processing_folder = "data_processing"
restarts_folder = "restarts"

state_file = 'state.json'
sl_dir = "slinfo"
default_job_name = "lammps"
params_file = "params.json"
in_templates_dir = Path("../in.templates/")
lammps_exec = "/scratch/perevoshchikyy/repos/lammps_al/build/lmp_mpi"

sbatch_nodes = 4
sbatch_tasks_pn = 32
sbatch_part = "medium"

MDDPN_exec = "/scratch/perevoshchikyy/MD/MDDPN/launcher.py"
sbatch_processing_node_count = 1
sbatch_processing_part = "test"

Fslurm_directory_field = "slurm_directory"
Frun_counter = "run_counter"
Frun_labels = "run_labels"
Fuser_variables = "user_variables"
Fin_file = "in.file"
Frun_no = "run_no"
Flabels_list = "labels"
Fvariables = "variables"
Fruns = "runs"
Fdump_file = "dump_f"
in_file_dir = "in_files"
dumps_folder = "dumps"
Fjobid = "sb_jobid"
Frestart_files = 'restart_files'
Frestart_every = "every"
start_template_file = in_templates_dir / "START.template"

time_criteria = 15 * 60


if __name__ == "__main__":
    pass
