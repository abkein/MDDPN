#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 04-09-2023 22:55:30

variable_equal_numeric = r"^\s*variable\s+[a-zA-Z_\d]+\s+equal\s+\d+[\.\/]?\d+\s*"
variable_equal_const = r"^\s*variable\s+[a-zA-Z_\d]+\s+equal\s+(\d+|\$\{.*?\})\s*#!const\s*$"

variable_equal = r"^\s*variable\s+[a-zA-Z_\d]+\s+equal\s+.*$"
variable_loop = r"^\s*variable\s+[a-zA-Z_\d]+\s+loop\s+.*$"

run = r"^\s*run\s+.*$"

set_timestep = r"^\s*timestep\s+.*$"

set_restart = r"^\s*restart\s+.*$"
restart_one = r"^\s*restart\s+(\d+|\$\{.*?\})\s+[a-zA-Z_/\d]+\.[a-zA-Z_\d]+\s*$"
restart_two = r"^\s*restart\s+(\d+|\$\{.*?\})\s+[a-zA-Z_/\d]+\.[a-zA-Z_\d]+\s+[a-zA-Z_/\d]+\.[a-zA-Z_\d]\s*$"
restart_multiple = r"^\s*restart\s+(\d+|\$\{.*?\})\s+[a-zA-Z_/]+\.\*\s*$"

set_dump = r"^\s*dump\s+[a-zA-Z_]+\s+[a-zA-Z_]+\s+[a-zA-Z_]+\/?[a-zA-Z_]+\s+(\d+|\$\{[a-zA-Z_\d]+\})\s+[a-zA-Z_\d]+\.?[a-zA-Z_]*(\s+[a-zA-Z_]+)*\s*$"

lmp_label = r"^\s*label\s+[a-zA-Z_]+\s*$"
jump = r"^\s*jump\s+SELF\s+[a-zA-Z_]+\s*$"
next = r"^\s*next\s+[a-zA-Z_]+\s*$"
label_declaration = r"^\s*#\s+label:\s[a-zA-Z]+\s*"

datafile_header = r"^LAMMPS data file via write_data, version \d{1,2} [a-zA-Z]{3} \d{4}, timestep = \d+, units = [a-zA-Z_]+$"

part_spec = r"^\s*# part: [a-zA-Z_]+\s*$"


def required_variable_equal_numeric(var) -> str:
    return r"^\s*variable\s+" + str(var) + r"\s+equal\s+[\d]+[\.\/]?\d+\s*"


def read_restart_specify(restart_file):
    return r"\s*read_restart\s+" + restart_file + r"\d+\s*"


if __name__ == "__main__":
    pass
