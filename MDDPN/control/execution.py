#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 14-04-2023 21:07:59

import re
import shlex
import argparse
import subprocess as sb
from pathlib import Path
from typing import List, Dict

from . import constants as cs


def run_polling(cwd: Path, args: argparse.Namespace, sb_jobid: int) -> None:
    every = 5
    cmd = f"polling.py --jobid {sb_jobid} --every {every} '{cwd.as_posix()}'"
    cmds = shlex.split(cmd)
    sb.Popen(cmds, start_new_session=True)


def perform_run(cwd: Path, in_file_name: Path, state: Dict) -> int:
    sldir = Path(state[cs.Fslurm_directory_field])
    tdir = sldir / str(state[cs.Frun_counter])
    tdir.mkdir(parents=True, exist_ok=True)
    state[cs.Frun_counter] += 1

    jname = cs.default_job_name

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

        fh.writelines(f"#SBATCH --nodes={cs.sbatch_nodes}\n")
        fh.writelines(f"#SBATCH --ntasks-per-node={cs.sbatch_tasks_pn}\n")
        fh.writelines(f"#SBATCH --partition={cs.sbatch_part}\n")
        fh.writelines(f"srun -u {cs.lammps_exec} -in {cs.in_file_dir}/{in_file_name}")

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


def perform_processing_run(cwd: Path, state: dict, df: List[str], params: dict) -> int:
    sldir = Path(state[cs.Fslurm_directory_field])
    tdir = sldir / cs.data_processing_folder
    tdir.mkdir(parents=True, exist_ok=True)

    jname = "MDDPN"

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

        fh.writelines(f"#SBATCH --nodes={cs.sbatch_processing_node_count}\n")
        fh.writelines(f"#SBATCH --ntasks-per-node={cs.sbatch_tasks_pn}\n")
        fh.writelines(f"#SBATCH --partition={cs.sbatch_processing_part}\n")
        fh.writelines(
            f"srun -u {cs.MDDPN_exec} -k {params['k']} -g {params['g']} '{df}'")

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
