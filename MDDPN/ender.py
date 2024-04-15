#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 15-04-2024 23:31:23

import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Union
from MPMU import config
import pysbatch_ng as sbatch

from .utils import states
from . import constants as cs


def state_runs_check(state: dict, logger: logging.Logger) -> bool:
    fl = True
    rlabels: dict = state[cs.sf.run_labels]
    for label in rlabels:
        rc = 0
        while str(rc) in rlabels[label]:
            rc += 1
        prc: int = rlabels[label][cs.sf.runs]
        if prc != rc:
            fl = False
            logger.warning(f"Label {label} runs: present={prc}, real={rc}")
    return fl


def state_validate(cwd: Path, state: dict, logger: logging.Logger) -> bool:
    fl = True
    rlabels: dict = state[cs.sf.run_labels]
    for label in rlabels:
        for i in range(int(rlabels[label][cs.sf.runs])):
            logger.debug(f"Checking {label}:{i}:{cs.sf.dump_file}")
            try:
                dump_file: Path = cwd / cs.folders.dumps / rlabels[label][str(i)][cs.sf.dump_file]
            except KeyError:
                logging.exception(json.dumps(rlabels, indent=4))
                raise
            if not dump_file.exists():
                fl = False
                logger.warning(f"Dump file {dump_file.as_posix()} not exists")
    return fl


def ender(cwd: Path, state: Dict, args: argparse.Namespace, logger: logging.Logger) -> Dict[str, Any]:
    if not args.anyway:
        if not (state_runs_check(state, logger.getChild('runs_check')) and state_validate(cwd, state, logger.getChild('validate'))):
            logger.error("Stopped, state is inconsistent")
            raise RuntimeError("Stopped, state is inconsistent")

    logger.info(f"Trying to import {cs.sp.post_processor}")
    import importlib.util
    import sys

    processor_path = Path(cs.sp.post_processor).resolve()
    processor_path_init = processor_path / "__init__.py"
    if not processor_path_init.exists():
        with processor_path_init.open("w") as fp:
            fp.write(
                """
                     # This file was automatically created by MDDPN

                     from . import pp

                     """
            )

    spec = importlib.util.spec_from_file_location(
        "post_processor", processor_path_init.as_posix(), submodule_search_locations=[processor_path.as_posix()]
    )

    if spec is None:
        logger.critical(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")
        raise ImportError(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")
    elif spec.loader is None:
        logger.critical(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")
        raise ImportError(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")

    processor = importlib.util.module_from_spec(spec)
    sys.modules["post_processor"] = processor
    spec.loader.exec_module(processor)
    logger.info("Import successful, calling")
    if not args.ongoing:
        state[cs.sf.state] = states.post_processor_called
    executable: Union[str, None]
    argsuments: Union[str, None]
    try:
        nworkers: int = cs.sp.sconf_post[sbatch.cs.fields.nnodes]*cs.sp.sconf_post[sbatch.cs.fields.ntpn]
        executable, argsuments = processor.pp.end(
            cwd, state.copy(), args, logger.getChild("post_processing.end"), nworkers
        )
    except Exception as e:
        logger.error("Post processor raised an exception")
        logger.exception(e)
        return state
    if executable is None:
        logger.error("Post processor not returned executable")
        return state
    logger.info("Post processor returned, running sbatch")
    cs.sp.sconf_post[sbatch.cs.fields.executable] = executable
    cs.sp.sconf_post[sbatch.cs.fields.args] = argsuments
    job_id = sbatch.sbatch.run(cwd, logger.getChild("submitter"), config(cs.sp.sconf_post))
    if not args.ongoing:
        state[cs.sf.state] = states.post_process_done
    state[cs.sf.post_process_id] = job_id
    return state
