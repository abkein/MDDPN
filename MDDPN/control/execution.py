#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 13-04-2023 20:34:59

from pathlib import Path
import re
import shlex
import subprocess as sb
from typing import List

from .constants import default_job_name, sbatch_nodes, sbatch_part, sbatch_tasks_pn, lammps_exec, MDDPN_exec, sbatch_processing_node_count, sbatch_processing_part


def run_polling(cwd, args, sb_jobid):
    every = 5
    cmd = f"polling.py --jobid {sb_jobid} --every {every} '{str(cwd)}'"
    cmds = shlex.split(cmd)
    p = sb.Popen(cmds, start_new_session=True)


def perform_run(cwd, in_file_name, state):
    sldir = Path(state["slurm_directory"])
    tdir = sldir / str(state["run_counter"])
    tdir.mkdir(parents=True, exist_ok=True)
    state["run_counter"] += 1

    jname = default_job_name

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

        fh.writelines(f"#SBATCH --nodes={sbatch_nodes}\n")
        fh.writelines(f"#SBATCH --ntasks-per-node={sbatch_tasks_pn}\n")
        fh.writelines(f"#SBATCH --partition={sbatch_part}\n")
        fh.writelines(
            f"srun -u {lammps_exec} -in {in_file_name}")

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


def perform_processing_run(cwd: Path, state: dict, df: List[str], params: dict):
    sldir = Path(state["slurm_directory"])
    tdir = sldir / "data_processing"
    tdir.mkdir(parents=True, exist_ok=True)
    # state["run_counter"] += 1

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

        fh.writelines(f"#SBATCH --nodes={sbatch_processing_node_count}\n")
        fh.writelines(f"#SBATCH --ntasks-per-node={sbatch_tasks_pn}\n")
        fh.writelines(f"#SBATCH --partition={sbatch_processing_part}\n")
        fh.writelines(
            f"srun -u {MDDPN_exec} -k {params['k']} -g {params['g']} '{df}'")

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