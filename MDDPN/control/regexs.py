#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 14-04-2023 21:29:27

variable_equal_numeric = r"^variable[ \t]+[a-zA-Z]+[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+"
variable_equal_formula = r"^variable[ \t]+[a-zA-Z]+[ \t]+equal[ \t]+\$\(.+\)"

run_numeric = r"^run[ \t]+\d+[.\/]?\d+"
run_formula = r"^run[ \t]+\${[a-zA-Z]+}"

set_timestep = r"^timestep[ \t]+[\d]+[\.\/]?\d+"
set_restart = r"restart[ \t]+(\$\{[a-zA-Z]+\}|[\d]+)[ \t]+[a-zA-Z]+\.\*"
set_dump = r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+"

label_declaration = r"#[ \t]+label:[ \t][a-zA-Z]+"


def required_variable_equal_numeric(var) -> str:
    return r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+"


def read_restart_specify(restart_file):
    return r"read_restart[ \t]+" + restart_file + r"\d+"


if __name__ == "__main__":
    pass
