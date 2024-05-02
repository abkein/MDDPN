#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 20:29:38

import json
from pathlib import Path
from typing import Dict, Union
from MPMU import confdict
import pysbatch_ng as sbatch

from .run import run_polling
from . import constants as cs
from .utils import states, logs, AP


@logs
def state_runs_check() -> bool:
    fl = True
    rlabels: Dict = cs.sp.state[cs.sf.run_labels]
    for label in rlabels:
        rc = 0
        while str(rc) in rlabels[label]:
            rc += 1
        prc: int = rlabels[label][cs.sf.runs]
        if prc != rc:
            fl = False
            cs.sp.logger.warning(f"Label {label} runs: present={prc}, real={rc}")
    return fl


@logs
def state_validate() -> bool:
    fl = True
    rlabels: Dict = cs.sp.state[cs.sf.run_labels]
    for label in rlabels:
        for i in range(int(rlabels[label][cs.sf.runs])):
            cs.sp.logger.debug(f"Checking {label}:{i}:{cs.sf.dump_file}")
            try:
                dump_file: Path = cs.sp.cwd / cs.folders.dumps / rlabels[label][str(i)][cs.sf.dump_file]
            except KeyError:
                cs.sp.logger.exception(json.dumps(rlabels, indent=4))
                raise
            if not dump_file.exists():
                fl = False
                cs.sp.logger.warning(f"Dump file {dump_file.as_posix()} not exists")
    return fl


@logs
def ender() -> int:
    if not cs.sp.args.anyway:
        if not (state_runs_check() and state_validate()):
            cs.sp.logger.error("Stopped, state is inconsistent")
            raise RuntimeError("Stopped, state is inconsistent")

    cs.sp.logger.info(f"Trying to import {cs.sp.post_processor}")
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
        cs.sp.logger.critical(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")
        raise ImportError(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")
    elif spec.loader is None:
        cs.sp.logger.critical(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")
        raise ImportError(f"Cannot import module by path {processor_path.as_posix()}\nSomething went wrong")

    processor = importlib.util.module_from_spec(spec)
    sys.modules["post_processor"] = processor
    spec.loader.exec_module(processor)
    cs.sp.logger.info("Import successful, calling")
    if not cs.sp.args.ongoing:
        cs.sp.state[cs.sf.state] = states.post_processor_called
    # executable: Union[str, None]
    # argsuments: Union[str, None]
    try:
        nworkers: int = cs.sp.sconf_post[sbatch.cs.fields.nnodes]*cs.sp.sconf_post[sbatch.cs.fields.ntpn]
        ap: AP = processor.pp.end(cs.sp.cwd, cs.sp.state.copy(), cs.sp.args, cs.sp.logger.getChild("post_processing.end"), nworkers)
    except Exception as e:
        cs.sp.logger.error("Post processor raised an exception")
        cs.sp.logger.exception(e)
        return 1
    # if executable is None:
    #     cs.sp.logger.error("Post processor not returned executable")
    #     return 1
    cs.sp.logger.info("Post processor returned, running sbatch")
    cs.sp.sconf_post[sbatch.cs.fields.executable] = ap.executable
    cs.sp.sconf_post[sbatch.cs.fields.args] = ap.arguments
    job_id = sbatch.sbatch.run(cs.sp.cwd, cs.sp.logger.getChild("submitter"), confdict(cs.sp.sconf_post))
    if not cs.sp.args.ongoing:
        cs.sp.state[cs.sf.state] = states.post_process_done
    cs.sp.state[cs.sf.post_process_id] = job_id

    cs.sp.logger.info("Staring polling")
    ppcmd = ap.ppexec
    if ppcmd and ap.ppexec and ap.ppargs:
        ppcmd += " " + ap.ppargs
    run_polling(job_id, cs.sp.state[cs.sf.tag], ppcmd)
    return 0
