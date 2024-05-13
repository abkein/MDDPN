#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 03-05-2024 03:30:36

import json
from pathlib import Path
from typing import Dict, Any, Union

import toml
from MPMU import is_exe
import pysbatch_ng as sbatch

from . import constants as cs
from .utils import logs


@logs
def execs_check() -> bool:
    fl = True
    if not is_exe(cs.execs.MDDPN, cs.sp.logger.getChild('is_exe')):
        cs.sp.logger.error("MDDPN executable not found")
        fl = False
    if not is_exe(cs.execs.lammps, cs.sp.logger.getChild('is_exe')):
        cs.sp.logger.error("lammps executable not found")
        fl = False
    if not is_exe(cs.execs.lammps_nonmpi, cs.sp.logger.getChild('is_exe')):
        cs.sp.logger.error("lammps_nonmpi executable not found")
        fl = False
    if not is_exe(cs.execs.spoll, cs.sp.logger.getChild('is_exe')):
        cs.sp.logger.error("spoll executable not found")
        fl = False
    return fl


@logs
def basic(conf: Dict[str, Any]) -> bool:
    fl = True
    cs.sp.logger.debug("Getting executables paths")
    if cs.cf.sect_execs in conf:
        execs = conf[cs.cf.sect_execs]
        if cs.cf.lammps in execs:
            cs.execs.lammps = execs[cs.cf.lammps]
        if cs.cf.lammps_nonmpi in execs:
            cs.execs.lammps_nonmpi = execs[cs.cf.lammps_nonmpi]
        if cs.cf.MDDPN in execs:
            cs.execs.MDDPN = execs[cs.cf.MDDPN]
        if cs.cf.spoll in execs:
            cs.execs.spoll = execs[cs.cf.spoll]

    cs.sp.logger.debug("Getting filenames")
    if cs.cf.sect_files in conf:
        files = conf[cs.cf.sect_files]
        if cs.cf.template in files:
            cs.files.template = files[cs.cf.template]

    cs.sp.logger.debug("Getting folders")
    if cs.cf.sect_folders in conf:
        folders = conf[cs.cf.sect_folders]
        if cs.cf.in_templates in folders:
            cs.folders.in_templates = folders[cs.cf.in_templates]

    if cs.cf.sect_params in conf:
        cs.sp.params = conf[cs.cf.sect_params]

    if cs.cf.sect_post in conf:
        post_conf = conf[cs.cf.sect_post]

        if cs.cf.do_post in post_conf:
            cs.sp.allow_post_process = bool(post_conf[cs.cf.do_post])

        if cs.cf.post_processor in post_conf and cs.sp.allow_post_process:
            if not Path(post_conf[cs.cf.post_processor]).resolve().exists():
                cs.sp.logger.error(f"Cannot find post processor package by specified path: {Path(post_conf[cs.cf.post_processor]).as_posix()}:\nNo such directory")
                fl = False
            if not Path(post_conf[cs.cf.post_processor]).resolve().is_dir():
                cs.sp.logger.error("Specified post processor path must be a package, i.e. specified path must be a directory")
                fl = False
            cs.sp.post_processor = post_conf[cs.cf.post_processor]

    if cs.cf.do_test_run in conf:
        cs.sp.run_tests = bool(conf[cs.cf.do_test_run])

    return fl


def gensconf(_conf: Dict[str, Any], section: str) -> Dict[str, Any]:
    conf = {}

    if sbatch.cs.fields.execs in _conf: conf[sbatch.cs.fields.execs] = _conf[sbatch.cs.fields.execs]

    for k, v in _conf[section].items():
        conf[k] = v

    conf[sbatch.cs.fields.folder] = cs.folders.slurm
    return conf


@logs
def loadconf(conffile: Union[Path, None] = None, conf_format: Union[str, None] = None) -> Dict[str, Any]:
    if not conffile:
        if cs.sp.args.conf:
            conffile = Path(cs.sp.args.conf).resolve()
            cs.sp.logger.debug(f"Searching for specified conffile: '{conffile.as_posix()}'")
        else:
            conffile = cs.sp.cwd / (cs.files.config_toml if cs.sp.args.toml else cs.files.config_json)
            cs.sp.logger.debug(f"Searching for conffile: '{conffile.as_posix()}'")

    if conffile.exists():
        cs.sp.conffile_path = conffile
        cs.sp.conffile_format = conf_format if conf_format else ('toml' if cs.sp.args.toml else 'json')

        cs.sp.logger.debug("Found configuration file")
        with conffile.open('r') as fp:
            cs.sp.logger.debug("Reading configuration file")

            if cs.sp.conffile_format == 'toml':
                cs.sp.logger.debug('Using toml')
                conf = toml.load(fp)[cs.cf.sect_MDDPN]
            else:
                cs.sp.logger.debug('Using json')
                conf = json.load(fp)[cs.cf.sect_MDDPN]
                cs.sp.conffile_format = 'json'
    else: raise FileNotFoundError(f"Config file {conffile.as_posix()} was not found")


    return conf

@logs
def configure(conf: Dict[str, Any]) -> bool:
    fl = True
    cs.sp.logger.debug("Getting basic constants")
    fl = fl and basic(conf)
    cs.sp.logger.debug("Checking executables")
    fl = fl and execs_check()

    if cs.cf.sect_sbatch not in conf:
        cs.sp.logger.error(f"Cannot find '{cs.cf.sect_sbatch}' entry in configuration file")
        fl = False
    if cs.cf.sect_sbatch_main not in conf[cs.cf.sect_sbatch]:
        cs.sp.logger.error(f"Cannot find '{cs.cf.sect_sbatch}.{cs.cf.sect_sbatch_main}' entry in configuration file")
        fl = False
    cs.sp.logger.debug("Generating slurm configuration for main runs")
    cs.sp.sconf_main = gensconf(conf[cs.cf.sect_sbatch], cs.cf.sect_sbatch_main)
    cs.sp.logger.debug("Checking slurm configuration for main runs")
    fl = fl and sbatch.config.configure(cs.sp.sconf_main, cs.sp.logger.getChild('sbatch.checksconf'), is_check=True)

    if cs.cf.sect_sbatch_post in conf[cs.cf.sect_sbatch]:
        cs.sp.logger.debug("Generating slurm configuration for post processing")
        cs.sp.sconf_post = gensconf(conf[cs.cf.sect_sbatch], cs.cf.sect_sbatch_post)
        cs.sp.logger.debug("Checking slurm configuration for main runs")
        fl = fl and sbatch.config.configure(cs.sp.sconf_post, cs.sp.logger.getChild('checksconf'), is_check=True)
    else:
        cs.sp.logger.warning(f"Post processing is disabled due to non-existent '{cs.cf.sect_sbatch}.{cs.cf.sect_sbatch_post}' entry in the configuration file")
        cs.sp.allow_post_process = False

    if cs.cf.sect_sbatch_test in conf[cs.cf.sect_sbatch]:
        cs.sp.logger.debug("Generating slurm configuration for testing runs")
        cs.sp.sconf_test = gensconf(conf[cs.cf.sect_sbatch], cs.cf.sect_sbatch_test)
        cs.sp.logger.debug("Checking slurm configuration for testing runs")
        fl = fl and sbatch.config.configure(cs.sp.sconf_test, cs.sp.logger.getChild('checksconf'), is_check=True)
    else:
        cs.sp.logger.warning(f"Test runs are disabled due to non-existent '{cs.cf.sect_sbatch}.{cs.cf.sect_sbatch_test}' entry in the configuration file")
        cs.sp.run_tests = False

    if fl: cs.sp.logger.info("Configuration OK")
    else: cs.sp.logger.error("Configuration invalid")
    return fl


@logs
def genconf(conffile: Path):
    if conffile.exists(): raise RuntimeError("Default config file exists in present directory")
    else: cs.sp.logger.debug(f"{conffile.as_posix()} not exists")

    conf: Dict[str, Any] = {}

    execs: Dict[str, str] = {}
    execs['lammps'] = cs.execs.lammps
    execs['MDDPN'] = cs.execs.MDDPN
    execs['spoll'] = cs.execs.spoll
    conf['execs'] = execs

    conf['post_processor'] = "/path/to/my/python/file"
    conf['test_run'] = cs.sp.run_tests
    conf['post_processing'] = cs.sp.allow_post_process

    files = {}
    files['template'] = cs.files.template
    conf['files'] = files

    folders = {}
    folders['in_templates'] = cs.folders.in_templates
    conf['folders'] = folders

    conf['slurm'] = {}
    conf['slurm']['main'] = sbatch.config.genconf()
    del conf['slurm']['main'][sbatch.cs.fields.execs]
    del conf['slurm']['main'][sbatch.cs.fields.folder]
    conf['slurm']['post'] = conf['slurm']['main']
    conf['slurm']['test'] = conf['slurm']['main']

    with conffile.open('w') as fp:
        json.dump(conf, fp, indent=4)

    return 0


if __name__ == "__main__":
    pass
