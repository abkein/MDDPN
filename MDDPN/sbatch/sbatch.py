#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 14-09-2023 20:42:18

import re
from pathlib import Path
from logging import Logger
from typing import Dict, Union, Any

from ..utils import wexec
from .config import configure
from . import constants as cs


def run(cwd: Path, logger: Logger, config: Dict[str, Any], add_conf: Union[Dict, None] = None) -> int:
    logger.debug("Preparing...")
    if add_conf is not None:
        for k, v in add_conf.items():
            config[k] = v

    logger.debug('Configuring...')
    configure(config, logger.getChild('configure'))

    tdir = cwd / cs.folders.run / cs.ps.jname
    tdir.mkdir(parents=True, exist_ok=True)

    job_file = tdir / f"{cs.ps.jname}.job"

    with job_file.open('w') as fh:
        fh.writelines("#!/usr/bin/env bash\n")
        fh.writelines(f"#SBATCH --job-name={cs.ps.jname}\n")
        fh.writelines(f"#SBATCH --output={tdir}/{cs.ps.jname}.out\n")
        fh.writelines(f"#SBATCH --error={tdir}/{cs.ps.jname}.err\n")
        fh.writelines("#SBATCH --begin=now\n")
        if cs.ps.nnodes is not None:
            fh.writelines(f"#SBATCH --nodes={cs.ps.nnodes}\n")
        if cs.ps.ntpn is not None:
            fh.writelines(f"#SBATCH --ntasks-per-node={cs.ps.ntpn}\n")
        if cs.ps.partition is not None:
            fh.writelines(f"#SBATCH --partition={cs.ps.partition}\n")
        if cs.ps.exclude_str is not None:
            fh.writelines(f"#SBATCH --exclude={cs.ps.exclude_str}\n")
        if config[cs.fields.args] is not None:
            fh.writelines(f"{cs.ps.pre} srun -u {config[cs.fields.executable]} {config[cs.fields.args]}")
        else:
            fh.writelines(f"srun -u {config[cs.fields.executable]}")

    logger.info("Submitting task...")
    cmd = f"{cs.execs.sbatch} {job_file}"
    bout = wexec(cmd, logger.getChild('sbatch'))

    if re.match(cs.re.sbatch_jobid, bout):
        *beg, num = bout.split()
        print("Sbatch jobid: ", num)
        logger.info(f"Sbatch jobid: {num}")
    else:
        logger.error("Cannot parse sbatch jobid")
        logger.error("### OUTPUT ###")
        logger.error("bout")
        raise RuntimeError("sbatch command not returned task jobid")
    return int(num)


if __name__ == "__main__":
    raise NotImplementedError
