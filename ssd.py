#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# TODO:
# preparing steps for every label


import json
from pathlib import Path
import re
import argparse
from enum import Enum
import subprocess as sb
import shlex
import pandas as pd
import numpy as np


state_field = 'state'
restart_field = 'restart_count'
in_file_field = "in.file"
jobs_list_field = "jobs"

restarts_folder = "restarts"
state_file = 'state.json'
sl_dir_def = "slinfo"
default_job_name = "lammps"
in_out_filename = "in.lm"
params_file_def = "params.json"
in_templates_dir = Path.cwd() / "../in.templates/"
lammps_exec = "/scratch/perevoshchikyy/repos/lammps_al/build/lmp_mpi"

sbatch_nodes = 4
sbatch_tasks_pn = 32
sbatch_part = "medium"


class states(str, Enum):
    initialized = "initialized"
    fully_initialized = "fully_initialized"
    started = "started"
    restarted = "restarted"
    comleted = "comleted"
    cluster_analysis_comleted = "cluster_analysis_comleted"
    data_obtained = "data_obtained"


class LogicError(Exception):
    pass


def process_file(file):
    ps = {'run_labels': {}}
    runc = 0
    label = None
    with file.open('r') as fin:
        for line in fin:
            if re.match(r"^variable[ \t]+[a-zA-Z]+[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = eval(VAR_VAL)
                ps[VAR_NAME] = VAR_VAL
                ps["v_" + VAR_NAME] = VAR_VAL
            if re.match(r"^variable[ \t]+[a-zA-Z]+[ \t]+equal[ \t]+\$\(.+\)", line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = VAR_VAL[2:-1]
                VAR_VAL = eval(VAR_VAL, globals(), ps)
                ps[VAR_NAME] = VAR_VAL
                ps["v_" + VAR_NAME] = VAR_VAL
            if re.match(r"^timestep[ \t]+[\d]+[\.\/]?\d+", line):
                w_timestep, TIME_STEP = line.split()
                ps['dt'] = eval(TIME_STEP)
            if re.match(r"^run[ \t]+\d+[.\/]?\d+", line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval(RUN_STEPS)
                ps["run" + str(runc)] = RUN_STEPS
                ps["run_labels"][label] += [RUN_STEPS]
                runc += 1
            if re.match(r"^run[ \t]+\${[a-zA-Z]+}", line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval("ps['" + RUN_STEPS[2:-1] + "']")
                ps["run" + str(runc)] = RUN_STEPS
                ps["run_labels"][label] += [RUN_STEPS]
                runc += 1
            if re.match(r"#[ \t]+label:[ \t][a-zA-Z]+", line):
                label = line.split()[-1]
                ps['run_labels'][label] = []
            if re.match(r"restart[ \t]+(\$\{[a-zA-Z]+\}|[\d]+)[ \t]+" + restarts_folder + r"\/[a-zA-Z\.]+\*", line):
                ps['restart_files'] = line.split()[-1].split('/')[-1][:-1]
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                ps["dump_file"] = line.split()[-1]
    vt = 0
    for i in ps['run_labels']:
        ps["run_labels"][i] = sum(ps["run_labels"][i]) + vt
        vt = ps["run_labels"][i]
    ps['runc'] = runc
    return ps


def gen_in(cwd, variables):
    template_file = in_templates_dir / "in.template"
    out_in_file = cwd / in_out_filename

    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            for var in list(variables.keys()):
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {variables[var]}\n"
            fout.write(line)
    return out_in_file


def init(cwd, args):
    if (cwd / state_file).exists():
        raise FileExistsError("File 'state.json' already exists")
    if (cwd / restarts_folder).exists():
        raise FileExistsError("Directory 'restarts' already exists")
    (cwd / restarts_folder).mkdir()
    if args.min:
        state = {state_field: states.initialized}
    else:
        state = {state_field: states.fully_initialized,
                 in_file_field: in_out_filename}
        params_file = params_file_def
        if args.file:
            pfile = (cwd / params_file) if args.fname is None else args.fname
            with pfile.open('r') as f:
                variables = json.load(f)
        else:
            variables = json.loads(args.params)
        in_file = gen_in(cwd, variables)
        state['vars'] = variables
        state['ps'] = process_file(in_file)
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)


def perform_run(cwd, args, in_file_name):
    sldir = cwd / sl_dir_def
    if not sldir.exists():
        sldir.mkdir(parents=True, exist_ok=True)

    jname = default_job_name

    job_file = sldir / f"{jname}.job"
    job_file.touch()

    with job_file.open('w') as fh:
        fh.writelines("#!/bin/bash\n")
        fh.writelines(f"#SBATCH --job-name={jname}\n")
        fh.writelines(f"#SBATCH --output=slinfo/{jname}.out\n")
        fh.writelines(f"#SBATCH --error=slinfo/{jname}.err\n")
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


def run_polling(cwd, args, sb_jobid):
    every = 5
    cmd = f"polling.py --jobid {sb_jobid} --every {every} '{str(cwd)}'"
    cmds = shlex.split(cmd)
    p = sb.Popen(cmds, start_new_session=True)


def run(cwd, args):
    if not (cwd / state_file).exists():
        raise FileNotFoundError("State file 'state.json' not found")
    with (cwd / state_file).open('r') as f:
        state = json.load(f)
    if states(state[state_field]) != states.fully_initialized:
        raise LogicError("Folder isn't properly initialized")
    sb_jobid = perform_run(cwd, args, state[in_file_field])
    state[state_field] = states.started
    state[jobs_list_field] = [sb_jobid]
    (cwd / state_file).unlink()
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)
    if not args.no_auto:
        run_polling(cwd, args, sb_jobid)


# def make_epmty(cwd):
#     for file in cwd.iterdir():
#         if file.is_dir():
#             make_epmty(file)
#             file.rmdir()
#         else:
#             file.unlink()


def find_last(cwd):
    rf = cwd / restarts_folder
    files = []
    for file in rf.iterdir():
        try:
            files += [int(file.parts[-1].split('.')[-1])]
        except Exception:
            pass
    return max(files)


def gen_restart(cwd, label, last_file, state):
    variables = state['vars']
    template_file = in_templates_dir / ("in." + label + ".template")
    out_in_file = cwd / ("in." + label + ".lm")
    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            variables['lastStep'] = last_file
            for var in list(variables.keys()):
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {variables[var]}\n"
                if re.match(r"^variable[ \t]+preparingSteps[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    last_label = ""
                    for c_label in state['ps']['run_labels']:
                        if c_label == label:
                            if last_label == "":
                                line = "variable preparingSteps equal 0\n"
                            else:
                                line = f"variable preparingSteps equal {state['ps']['run_labels'][last_label]}\n"
                        else:
                            last_label = c_label
                    # state['ps']['run_labels'][label]
            if re.match(r"read_restart[ \t]+" + restarts_folder + r"\/" + state['ps']['restart_files'] + r"\d+", line):
                line = f"read_restart {restarts_folder}/{state['ps']['restart_files']}{last_file}\n"
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                before = line.split()[:-1]
                lkl = ""
                for el in before:
                    lkl += el + " "
                before = lkl
                dump_file = state['ps']['dump_file'].split('.')
                if len(dump_file) == 2:
                    # print("Only 2")
                    dump_file = dump_file[:-1] + \
                        [f"{label}"] + [dump_file[-1]]
                else:
                    # print("More than 2")
                    dump_file = dump_file[:-1] + \
                        [f"{label}"] + [dump_file[-1]]
                fff = ""
                for el in dump_file:
                    fff += el + "."
                line = before + fff
                line = line[:-1]
                line += "\n"
            fout.write(line)
    del variables['lastStep']
    return out_in_file


# def gen_restarts(cwd, nums):
#     from random import randrange
#     files = []
#     for i, label in enumerate(nums):
#         files += ["nucl.restart." + str(randrange(
#             0 if i == 0 else nums[list(nums.keys())[i - 1]], nums[label])) for _ in range(3)]
#     print(files)
#     for file in files:
#         (cwd / restarts_folder / file).touch()


def restart(cwd, args):
    if not (cwd / state_file).exists():
        raise FileNotFoundError("State file 'state.json' not found")
    with (cwd / state_file).open('r') as f:
        state = json.load(f)
    if states(state[state_field]) != states.started and states(state[state_field]) != states.restarted:
        raise LogicError("Folder isn't properly initialized")
    last_file = find_last(cwd)
    if last_file > max(list(state['ps']["run_labels"].values())) - 1:
        return 0
    if states(state[state_field]) == states.started:
        state[restart_field] = 1
        state[state_field] = states.restarted
        state["restarts"] = {}
    elif states(state[state_field]) == states.restarted:
        rest_cnt = int(state[restart_field])
        rest_cnt += 1
        state[restart_field] = rest_cnt
    # gen_restarts(cwd, state['ps']['run_labels'])
    current_label = ""
    for i, label in enumerate(state['ps']['run_labels']):
        if last_file > state['ps']['run_labels'][label]:
            if last_file < state['ps']['run_labels'][list(state['ps']['run_labels'].keys())[i + 1]]:
                current_label = list(state['ps']['run_labels'].keys())[i + 1]
                break
    out_file = gen_restart(cwd, current_label, last_file, state)
    sb_jobid = perform_run(cwd, args, out_file)

    state["restarts"][current_label] = {
        "sb_jobid": sb_jobid, "last_step": last_file}
    state["current_step"] = last_file

    (cwd / state_file).unlink()
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)

    if not args.no_auto:
        run_polling(cwd, args, sb_jobid)


def end(cwd, args):
    origin = cwd / "temperature.log"
    target = cwd / "temperature.log.temp"
    with origin.open('r') as fin, target.open('w') as fout:
        for line in fin:
            if line[0] == '#':
                continue
            fout.write(line)
    origin.unlink()
    target.rename("temperature.log")

    with (cwd / state_file).open('r') as f:
        state = json.load(f)
    time_step = state['ps']['dt']

    temperatures = pd.read_csv(cwd / "temperature.log", header=None)
    temptime = temperatures[0].to_numpy(dtype=np.uint64)
    temperatures = temperatures[1].to_numpy(dtype=np.float64)

    xilog = pd.read_csv(
        cwd / "xi.log", header=None).to_numpy(dtype=np.uint32).flatten()
    xilog -= 1
    dtemp_xi = temperatures[temptime == xilog[0]] - \
        temperatures[temptime == xilog[1]]
    xi = dtemp_xi / (xilog[1] - xilog[0]) / time_step

    state['xi'] = xi[0]

    (cwd / state_file).unlink()
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)


def main(cwd, args):
    # print("PWD: ", Path(".").resolve())
    if args.debug:
        print("Envolved args:")
        print(args)
    else:
        if args.command == 'init':
            init(cwd, args)
        elif args.command == 'run':
            run(cwd, args)
        elif args.command == 'restart':
            restart(cwd, args)
        elif args.command == 'end':
            end(cwd, args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--debug', action='store_true',
                        help='Debug, prints only parsed arguments')

    sub_parsers = parser.add_subparsers(
        help='sub-command help', dest="command")

    parser_init = sub_parsers.add_parser(
        'init', help='Initialize directory')
    parser_init.add_argument(
        '--min', action="store_true", help='Don\'t create in. files')
    parser_init.add_argument("-p", '--params', action="store", type=str,
                             help='Obtain simulation parameters from command-line')
    parser_init.add_argument("-f", '--file', action="store_true",
                             help='Obtain simulation parameters from file')
    parser_init.add_argument("-fn", '--fname', action="store",
                             help='Specify file to get parameters from')
    # init_sub_parsers = parser_init.add_subparsers(
    #     help='sub-command help', dest="init_min")
    # sub_parser_init = init_sub_parsers.add_parser(
    #     'min', help='Initialize directory')

    parser_run = sub_parsers.add_parser(
        'run', help='Run LAMMPS simulation')
    parser_run.add_argument(
        '--no_auto', action='store_true', help='Don\'t run polling sbatch and don\'t auto restart')

    parser_restart = sub_parsers.add_parser(
        'restart', help='Generate restart file and run it')
    parser_restart.add_argument(
        '--gen', action='store_false', help='Don\'t run restart')
    parser_restart.add_argument(
        '--no_auto', action='store_true', help='Don\'t run polling sbatch and don\'t auto restart')

    parser_end = sub_parsers.add_parser(
        'end', help='Post-processing')

    args = parser.parse_args()
    cwd = Path.cwd()
    main(cwd, args)
else:
    raise ImportError("Cannot be imported")
