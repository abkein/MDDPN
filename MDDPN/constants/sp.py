#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-09-2023 22:45:49

import logging
import argparse
from pathlib import Path
from typing import Dict, Any


logger: logging.Logger = logging.Logger("null")
formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')

params: Dict[str, Any] = {}
post_processor: str = ''

sconf_main: Dict[str, Any] = {}
sconf_post: Dict[str, Any] = {}
sconf_test: Dict[str, Any] = {}

run_tests: bool = True
allow_post_process: bool = True

state: Dict[str, Any] = {}
cwd: Path = Path()
args: argparse.Namespace = argparse.Namespace()

if __name__ == "__main__":
    pass
