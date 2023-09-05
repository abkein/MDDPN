#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 05-09-2023 19:12:35

state: str = 'state.json'

logfile = "main.log"
pass_log_prefix: str = ""
pass_log_suffix: str = ".log"

restart_lock: str = "restart.lock"

params: str = "params.json"

post_process_state: str = "st.json"
# start_template: str = "START.template"
template: str = "in.template"

cluster_distribution_matrix: str = "matrice.csv"
data = 'data.json'

temperature: str = "temperature.log"
temperature_backup: str = "temperature.log.bak"
xi_log: str = "xi.log"

mat_storage: str = "ntb.bp"
comp_data: str = "rdata.csv"  # computed data

if __name__ == "__main__":
    pass
