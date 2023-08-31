#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 27-08-2023 02:43:32

default_job_name: str = "lammps"
sbatch_nodes: int = 4
sbatch_tasks_pn: int = 32
sbatch_part: str = "medium"
sbatch_processing_node_count: int = 1
sbatch_processing_part: str = "small"
time_criteria = 24 * 60 * 60

if __name__ == "__main__":
    pass
