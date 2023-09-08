#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 16:38:57

from .union import time_step
from .union import restart_every as every

xi: str = "xi"
step_before: str = "step_before"
step_after: str = "step_after"
storages: str = "storages"
# time_step: str = "time_step"
# every: str = "every"
dump_folder: str = "dump_folder"
data_processing_folder: str = "data_processing_folder"

number: str = "no"
begin: str = "begin"
end: str = "end"

natoms: str = "natoms"
boxxhi: str = 'boxxhi'
boxyhi: str = 'boxyhi'
boxzhi: str = 'boxzhi'
lammps_dist: str = 'atoms'

dimensions: str = "dimensions"
volume: str = "Volume"
N_atoms: str = "N_atoms"
# bdims: str = "bdims"
matrix_storages: str = "mat_storages"

pp_state_name: str = "name"
pp_state: str = "state"
mat_step: str = "step"
mat_dist: str = "dist"

if __name__ == "__main__":
    pass
