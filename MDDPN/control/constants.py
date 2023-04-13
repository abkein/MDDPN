#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 20:28:41

from pathlib import Path


state_field = 'state'
restart_field = 'restart_count'
in_file_field = "in.file"
jobs_list_field = "jobs"

restarts_folder = "restarts"
dump_folder = "dumps"
state_file = 'state.json'
sl_dir_def = "slinfo"
default_job_name = "lammps"
params_file_def = "params.json"
in_templates_dir = Path("../in.templates/")
lammps_exec = "/scratch/perevoshchikyy/repos/lammps_al/build/lmp_mpi"

sbatch_nodes = 4
sbatch_tasks_pn = 32
sbatch_part = "medium"

MDDPN_exec = "/scratch/perevoshchikyy/MD/MDDPN/launcher.py"
sbatch_processing_node_count = 1
sbatch_processing_part = "small"
