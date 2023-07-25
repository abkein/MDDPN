#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-07-2023 12:24:09

import json
import re
import shlex
import argparse
import warnings
import subprocess as sb
from pathlib import Path
from typing import Dict

from .. import constants as cs
from .utils import STRNodes


def run_polling(cwd: Path, args: argparse.Namespace, sb_jobid: int) -> None:
    every = 5
    cmd = f"polling.py --jobid {sb_jobid} --every {every} '{cwd.as_posix()}'"
    cmds = shlex.split(cmd)
    sb.Popen(cmds, start_new_session=True)


def perform_run(cwd: Path, in_file_name: Path, state: Dict) -> int:
    sldir = Path(state[cs.sf.slurm_directory])
    tdir = sldir / str(state[cs.sf.run_counter])
    tdir.mkdir(parents=True, exist_ok=True)
    state[cs.sf.run_counter] += 1

    jname = cs.params.default_job_name

    job_file = tdir / f"{jname}.job"
    job_file.touch()

    with job_file.open('w') as fh:
        fh.writelines("#!/usr/bin/env bash\n")
        fh.writelines(f"#SBATCH --job-name={jname}\n")
        fh.writelines(f"#SBATCH --output={tdir}/{jname}.out\n")
        fh.writelines(f"#SBATCH --error={tdir}/{jname}.err\n")
        # fh.writelines("#SBATCH --time=2-00:00\n")
        # fh.writelines("#SBATCH --mem=12000\n")
        # fh.writelines("#SBATCH --qos=normal\n")
        fh.writelines("#SBATCH --mail-type=ALL\n")
        fh.writelines("#SBATCH --mail-user=perevoshchikyy@mpei.ru\n")
        fh.writelines("#SBATCH --begin=now\n")

        fh.writelines(f"#SBATCH --nodes={cs.params.sbatch_nodes}\n")
        fh.writelines(f"#SBATCH --ntasks-per-node={cs.params.sbatch_tasks_pn}\n")
        fh.writelines(f"#SBATCH --partition={cs.params.sbatch_part}\n")
        fh.writelines(f"srun -u {cs.files.lammps_exec} -in {cs.folders.in_file}/{in_file_name}")

    sbatch = sb.run(["sbatch", f"{job_file}"], capture_output=True)
    bout = sbatch.stdout.decode('ascii')
    if re.match(r"^Submitted[ \t]+batch[ \t]+job[ \t]+\d+", bout):
        *beg, num = bout.split()
        print("Sbatch jobid: ", num)
    else:
        print("SBATCH OUTPUT:")
        print(bout)
        print()
        raise RuntimeError("sbatch command not returned task jobid")
    return int(num)


def perform_processing_run(cwd: Path, state: Dict, params: str = None, part: str = None, nodes: STRNodes = STRNodes.ALL) -> int:  # type: ignore
    sldir = Path(state[cs.sf.slurm_directory])
    tdir = sldir / cs.folders.data_processing
    tdir.mkdir(parents=True, exist_ok=True)

    jname = "MDDPN"

    job_file = tdir / f"{jname}.job"
    job_file.touch()

    params_line = ''
    if params is not None:
        param_dict = json.loads(params)
        for key, value in param_dict.items():
            params_line += f"--{key}={value} "

    with job_file.open('w') as fh:
        fh.writelines("#!/usr/bin/env bash\n")
        fh.writelines(f"#SBATCH --job-name={jname}\n")
        fh.writelines(f"#SBATCH --output={tdir}/{jname}.out\n")
        fh.writelines(f"#SBATCH --error={tdir}/{jname}.err\n")
        # fh.writelines("#SBATCH --time=2-00:00\n")
        # fh.writelines("#SBATCH --mem=12000\n")
        # fh.writelines("#SBATCH --qos=normal\n")
        fh.writelines("#SBATCH --mail-type=ALL\n")
        fh.writelines("#SBATCH --mail-user=perevoshchikyy@mpei.ru\n")
        fh.writelines("#SBATCH --begin=now\n")

        fh.writelines(f"#SBATCH --nodes={cs.params.sbatch_processing_node_count}\n")
        fh.writelines(f"#SBATCH --ntasks-per-node={cs.params.sbatch_tasks_pn}\n")
        if nodes != STRNodes.ALL:
            if nodes == STRNodes.HOST:
                fh.writelines("#SBATCH --exclude=angr[1-20]\n")
                # fh.writelines("#SBATCH --nodelist=host[1-18]\n")
            elif nodes == STRNodes.ANGR:
                # fh.writelines("#SBATCH --nodelist=angr[1-20]\n")
                fh.writelines("#SBATCH --exclude=host[1-18]\n")
            else:
                warnings.warn(f"There is no such nodeset as {str(nodes)}")
        fh.writelines(f"#SBATCH --partition={cs.params.sbatch_processing_part if part is None else part}\n")
        # fh.writelines(
        #     f"LD_PRELOAD=/usr/lib64/libhdf5.so.10 srun -u {cs.MDDPN_exec} {params_line}")
        fh.writelines(
            f"srun -u {cs.files.MDDPN_exec} {params_line}")

    sbatch = sb.run(["sbatch", f"{job_file}"], capture_output=True)
    bout = sbatch.stdout.decode('ascii')
    berr = sbatch.stderr.decode('ascii')
    if re.match(r"^Submitted[ \t]+batch[ \t]+job[ \t]+\d+", bout):
        *beg, num = bout.split()
        print("Sbatch jobid: ", num)
    else:
        print("SBATCH OUTPUT:")
        print(bout)
        print()
        print("SBATCH ERROR:")
        print(berr)
        print()
        raise RuntimeError("sbatch command not returned task jobid")
    return int(num)


if __name__ == "__main__":
    pass
