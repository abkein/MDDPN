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
dump_folder = "dumps"
state_file = 'state.json'
sl_dir_def = "slinfo"
default_job_name = "lammps"
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


def process_file(file, state):
    labels = {"START": [0]}
    runs = {}
    variables = {}
    runc = 0
    label = None
    with file.open('r') as fin:
        for line in fin:
            if re.match(r"^variable[ \t]+[a-zA-Z]+[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = eval(VAR_VAL)
                variables[VAR_NAME] = VAR_VAL
                variables["v_" + VAR_NAME] = VAR_VAL
            if re.match(r"^variable[ \t]+[a-zA-Z]+[ \t]+equal[ \t]+\$\(.+\)", line):
                w_variable, VAR_NAME, w_equal, VAR_VAL = line.split()
                VAR_VAL = VAR_VAL[2:-1]
                VAR_VAL = eval(VAR_VAL, globals(), variables)
                variables[VAR_NAME] = VAR_VAL
                variables["v_" + VAR_NAME] = VAR_VAL
            if re.match(r"^timestep[ \t]+[\d]+[\.\/]?\d+", line):
                w_timestep, TIME_STEP = line.split()
                TIME_STEP = eval(TIME_STEP)
                variables['dt'] = TIME_STEP
                state['time_step'] = TIME_STEP
            if re.match(r"^run[ \t]+\d+[.\/]?\d+", line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval(RUN_STEPS)
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]
                runc += 1
            if re.match(r"^run[ \t]+\${[a-zA-Z]+}", line):
                w_run, RUN_STEPS = line.split()
                RUN_STEPS = eval("variables['" + RUN_STEPS[2:-1] + "']")
                runs["run" + str(runc)] = RUN_STEPS
                labels[label] += [RUN_STEPS]
                runc += 1
            if re.match(r"#[ \t]+label:[ \t][a-zA-Z]+", line):
                label = line.split()[-1]
                labels[label] = []
            if re.match(r"restart[ \t]+(\$\{[a-zA-Z]+\}|[\d]+)[ \t]+" + restarts_folder + r"\/[a-zA-Z\.]+\*", line):
                # restarts/rest.nucl.* into rest.nucl.
                state['restart_files'] = line.split()[-1].split('/')[-1][:-1]
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                state["dump_file"] = line.split()[-1]
    vt = 0
    labels_list = list(labels.keys())
    for label in labels:
        labels[label] = {"begin_step": vt, "end_step": sum(
            labels[label]) + vt, "runs": 0}
        vt = labels[label]["end_step"]
    labels["START"]["0"] = {"dump_f": "dump.START0.bp"}
    # state['runc'] = runc
    state["run_labels"] = labels
    state["labels"] = labels_list
    state['variables'] = variables
    state['runs'] = runs
    return state


def gen_in(cwd, variables):
    template_file = in_templates_dir / "in.START.template"
    out_in_file = cwd / "in.START0.lm"

    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            for var, value in variables.items():
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {value}\n"
            fout.write(line)
    return out_in_file


def init(cwd, args):
    if (cwd / state_file).exists():
        raise FileExistsError("File 'state.json' already exists")
    if (cwd / restarts_folder).exists():
        raise FileExistsError(f"Directory {restarts_folder} already exists")
    (cwd / restarts_folder).mkdir()
    if args.min:
        state = {state_field: states.initialized}
    else:
        state = {state_field: states.fully_initialized}
        params_file = params_file_def
        if args.file:
            pfile = (cwd / params_file) if args.fname is None else args.fname
            with pfile.open('r') as f:
                variables = json.load(f)
        else:
            variables = json.loads(args.params)
        state['user_variables'] = variables
        in_file = gen_in(cwd, variables)
        state = process_file(in_file, state)
        state["run_labels"]["START"]["0"]['in.file'] = str(in_file.parts[-1])
        state["run_labels"]["START"]["0"]["run_no"] = 1
        state["run_counter"] = 0
        sldir = cwd / sl_dir_def
        state["slurm_directory"] = str(sldir)
        if not sldir.exists():
            sldir.mkdir(parents=True, exist_ok=True)
    (cwd / state_file).touch()
    with (cwd / state_file).open('w') as f:
        json.dump(state, f)


def perform_run(cwd, args, in_file_name, state):
    sldir = Path(state["slurm_directory"])
    tdir = sldir / str(state["run_counter"])
    tdir.mkdir(parents=True, exist_ok=True)
    state["run_counter"] += 1

    jname = default_job_name

    job_file = tdir / f"{jname}.job"
    job_file.touch()

    with job_file.open('w') as fh:
        fh.writelines("#!/bin/bash\n")
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
    sb_jobid = perform_run(
        cwd, args, state["run_labels"]['START']['0']['in.file'], state)
    state[state_field] = states.started
    state["run_labels"]['START']["0"]["sb_jobid"] = sb_jobid
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
    variables = state['user_variables']
    template_file = in_templates_dir / \
        ("in." + label + ".template")
    out_in_file = cwd / \
        ("in." + label + str(state['run_labels'][label]['runs']) + ".lm")
    if out_in_file.exists():
        raise FileExistsError(f"Output in. file {out_in_file} already exists")
    with template_file.open('r') as fin, out_in_file.open('w') as fout:
        for line in fin:
            variables['lastStep'] = last_file
            for var in list(variables.keys()):
                if re.match(r"^variable[ \t]+" + str(var) + r"[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable {var} equal {variables[var]}\n"
                if re.match(r"^variable[ \t]+preparingSteps[ ,\t]+equal[ ,\t]+[\d]+[\.\/]?\d+", line):
                    line = f"variable preparingSteps equal {state['run_labels'][label]['begin_step']}\n"
            if re.match(r"read_restart[ \t]+" + restarts_folder + r"\/" + state['restart_files'] + r"\d+", line):
                line = f"read_restart {restarts_folder}/{state['restart_files']}{last_file}\n"
            if re.match(r"dump[ \t]+[a-zA-Z]+[ \t]+[a-zA-Z]+[ \t]+atom\/adios[ \t]+(\$\{[a-zA-Z]+\}|\d+)[ \t]+[a-zA-Z\.\/]+", line):
                before = line.split()[:-1]
                lkl = ""
                for el in before:
                    lkl += el + " "
                before = lkl
                dump_file = state['dump_file'].split('.')
                if len(dump_file) == 2:
                    # print("Only 2")
                    dump_file = dump_file[:-1] + \
                        [f"{label}{state['run_labels'][label]['runs']}"] + \
                        [dump_file[-1]]
                else:
                    # print("More than 2")
                    dump_file = dump_file[:-1] + \
                        [f"{label}{state['run_labels'][label]['runs']}"] + \
                        [dump_file[-1]]
                fff = ""
                for el in dump_file:
                    fff += el + "."
                fff = fff[:-1]
                line = before + fff
                line += "\n"
            fout.write(line)
    del variables['lastStep']
    return out_in_file, fff


# def gen_restarts(cwd, nums):
#     from random import randrange
#     files = []
#     for i, label in enumerate(nums):
#         files += ["nucl.restart." + str(randrange(
#             0 if i == 0 else nums[list(nums.keys())[i - 1]], nums[label])) for _ in range(3)]
#     print(files)
#     for file in files:
#         (cwd / restarts_folder / file).touch()

def max_step(state):
    fff = []
    for label in state["run_labels"]:
        fff += [state["run_labels"][label]["end_step"]]
    return max(fff)


# def g_labels(state):


def restart(cwd, args):
    if not (cwd / state_file).exists():
        raise FileNotFoundError("State file 'state.json' not found")
    with (cwd / state_file).open('r') as f:
        state = json.load(f)
    if states(state[state_field]) != states.started and states(state[state_field]) != states.restarted:
        raise LogicError("Folder isn't properly initialized")
    last_file = find_last(cwd)
    if last_file >= max_step(state) - 1:
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
    for i, label in enumerate(state['run_labels']):
        if last_file > state['run_labels'][label]["begin_step"]:
            if last_file < state['run_labels'][label]["end_step"] - 1:
                current_label = label
                break
    out_file, dump_file = gen_restart(cwd, current_label, last_file, state)
    sb_jobid = perform_run(cwd, args, out_file, state)
    fl = False
    for label_c in reversed(state["labels"]):
        if fl:
            # print(current_label, label_c, str(
            #     int(state["run_labels"][label_c]["runs"])))
            if '0' in state["run_labels"][label_c]:
                state["run_labels"][label_c][str(
                    int(state["run_labels"][label_c]["runs"]))]["last_step"] = last_file
                break
        elif label_c == current_label:
            fl = True
    state["run_labels"][current_label][f"{state['run_labels'][label]['runs']}"] = {
        "sb_jobid": sb_jobid, "last_step": last_file, "in.file": str(out_file.parts[-1]), "dump.f": str(dump_file), "run_no": state["run_counter"]}
    state["run_labels"][label]['runs'] += 1
    # state["current_step"] = last_file

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
