#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 15:31:01

state: str = 'state.json'

params: str = "params.json"
lammps_exec: str = "/scratch/perevoshchikyy/repos/lammps_al/build/lmp_mpi"

MDDPN_exec: str = "/scratch/perevoshchikyy/MD/MDDPN/launcher.py"

post_process_state: str = "st.json"
start_template: str = "START.template"

cluster_distribution_matrix: str = "matrice.csv"
data = 'data.json'

temperature: str = "temperature.log"
temperature_backup: str = "temperature.log.bak"
xi_log: str = "xi.log"

mat_storage: str = "ntb.bp"
comp_data: str = "rdata.csv"  # computed data

if __name__ == "__main__":
    pass
