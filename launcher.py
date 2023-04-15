#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 12-04-2023 10:20:15


import sys

from MDDPN.mpi import multmpi


sys.exit(multmpi.mpi_wrap())
