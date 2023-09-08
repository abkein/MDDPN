#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-09-2023 23:07:39

import logging
from typing import Dict, Any


formatter: logging.Formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')

params: Dict[str, Any] = {}
post_processor: str = ''

sconf_main: Dict = {}
sconf_post: Dict = {}
