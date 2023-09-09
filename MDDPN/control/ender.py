#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 09-09-2023 20:41:18

import logging
import argparse
from pathlib import Path
from typing import Dict, Any

from .. import sbatch
from .utils import states
from . import constants as cs


def ender(cwd: Path, state: Dict, args: argparse.Namespace, logger: logging.Logger) -> Dict[str, Any]:
    logger.info(f"Trying to import {cs.sp.post_processor}")
    import importlib.util
    import sys
    spec = importlib.util.spec_from_file_location("post_processor", cs.sp.post_processor)
    if spec is None or spec.loader is None:
        logger.critical(f"Cannot import module by path {cs.sp.post_processor}")
        raise ImportError()

    processor = importlib.util.module_from_spec(spec)
    sys.modules["post_processor"] = processor
    spec.loader.exec_module(processor)
    logger.info("Import successful, calling")
    state[cs.sf.state] = states.post_processor_called
    try:
        executable, args = processor.end(cwd, state.copy(), args, logger.getChild("post_processing.end"), cs)
    except Exception as e:
        logger.error("Post processor raised an exception")
        logger.exception(e)
        return state
    logger.info("Post processor returned, running sbatch")
    cs.sp.sconf_post[sbatch.cs.fields.executable] = executable
    cs.sp.sconf_post[sbatch.cs.fields.args] = args
    job_id = sbatch.sbatch.run(cwd, logger.getChild("submitter"), cs.sp.sconf_post)
    state[cs.sf.state] = states.post_process_done
    state[cs.sf.post_process_id] = job_id
    return state
