#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 31-08-2023 10:34:29

variable_equal_numeric = r"^variable\s+[a-zA-Z_]+\s+equal\s+[\d]+[\.\/]?\d+"
variable_equal_formula = r"^variable\s+[a-zA-Z_]+\s+equal\s+\$\(.+\)"

run_numeric = r"^run\s+\d+[.\/]?\d+"
run_formula = r"^run\s+\$\{[a-zA-Z_]+\}"

set_timestep_num = r"^timestep\s+[\d]+[\.\/]?\d+"
set_timestep_equ = r"^timestep\s+\$\{[a-zA-Z_]+\}"
set_restart_num = r"^restart\s+\d+\s+[a-zA-Z]+\.\*"
set_restart_equ = r"^restart\s+\$\{[a-zA-Z_]+\}\s+[a-zA-Z]+\.\*"
# set_restart = r"^restart\s+(\$\{[a-zA-Z]+\}|[\d]+)\s+[a-zA-Z]+\.\*"
set_dump = r"^dump\s+[a-zA-Z]+\s+[a-zA-Z]+\s+atom\/adios\s+(\$\{[a-zA-Z]+\}|\d+)\s+[a-zA-Z\.\/]+"

label_declaration = r"^#\s+label:\s[a-zA-Z]+"


sbatch_jobid = r"^Submitted\s+batch\s+job\s+\d+"


def required_variable_equal_numeric(var) -> str:
    return r"^variable\s+" + str(var) + r"\s+equal\s+[\d]+[\.\/]?\d+"


def read_restart_specify(restart_file):
    return r"read_restart\s+" + restart_file + r"\d+"


if __name__ == "__main__":
    pass
