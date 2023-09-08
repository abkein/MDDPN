#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-09-2023 20:30:54

state: str = 'state.json'

logfile = "main.log"
pass_log_prefix: str = ""
pass_log_suffix: str = ".log"
config: str = "conf.json"
restart_lock: str = "restart.lock"

template: str = "in.template"  # this can be overriden at runtime
params: str = "params.json"  # this can be overriden at runtime

# conf: str = "../MDDPN.conf.py"

# post_process_state: str = "st.json"
# cluster_distribution_matrix: str = "matrice.csv"
# data = 'data.json'
# temperature: str = "temperature.log"
# temperature_backup: str = "temperature.log.bak"
# xi_log: str = "xi.log"
# mat_storage: str = "ntb.bp"
# comp_data: str = "rdata.csv"  # computed data


if __name__ == "__main__":
    pass
