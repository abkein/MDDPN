#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 11-09-2023 23:22:54

import json
import logging
from pathlib import Path
from typing import Dict, Any

from .. import sbatch
from ..utils import is_exe
from . import constants as cs


def check(logger: logging.Logger):
    if not is_exe(cs.execs.MDDPN):
        logger.error("MDDPN executable not found")
        raise FileNotFoundError("MDDPN executable not found")
    if not is_exe(cs.execs.sacct):
        logger.error("sacct executable not found")
        raise FileNotFoundError("sacct executable not found")
    if not is_exe(cs.execs.lammps):
        logger.error("lammps executable not found")
        raise FileNotFoundError("lammps executable not found")
    if not is_exe(cs.execs.sbatch):
        logger.error("sbatch executable not found")
        raise FileNotFoundError("sbatch executable not found")
    if not is_exe(cs.execs.sinfo):
        logger.error("sinfo executable not found")
        raise FileNotFoundError("sinfo executable not found")


def basic(conf: Dict[str, Any], logger: logging.Logger):
    logger.debug("Getting executables paths")
    # print(list(conf.keys()))
    if 'execs' in conf:
        execs = conf['execs']
        if 'lammps' in execs:
            cs.execs.lammps = execs['lammps']
        if 'MDDPN' in execs:
            cs.execs.MDDPN = execs['MDDPN']
        if 'sbatch' in execs:
            cs.execs.sbatch = execs['sbatch']
        if 'sacct' in execs:
            cs.execs.sacct = execs['sacct']
        if 'sinfo' in execs:
            cs.execs.sinfo = execs['sinfo']
        if 'MDpoll' in execs:
            cs.execs.MDpoll = execs['MDpoll']

    logger.debug("Getting filenames")
    if 'files' in conf:
        files = conf['files']
        if 'template' in files:
            cs.files.template = files['template']

    logger.debug("Getting folders")
    if 'folders' in conf:
        folders = conf['folders']
        if 'in_templates' in folders:
            cs.folders.in_templates = folders['in_templates']

    if 'params' in conf:
        cs.sp.params = conf['params']

    if 'post_processor' in conf:
        cs.sp.post_processor = conf['post_processor']


def gensconf(conf: Dict[str, Any], logger: logging.Logger):
    conf[sbatch.cs.fields.execs] = {
        'sinfo': cs.execs.sinfo,
        'sacct': cs.execs.sacct,
        'sbatch': cs.execs.sbatch
    }
    return conf


def configure(conffile: Path, logger: logging.Logger):
    if conffile.exists():
        logger.info("Found configuration file")
        with conffile.open('r') as fp:
            logger.debug("Reading configuration file")
            conf = json.load(fp)

        logger.debug("Getting basic constants")
        basic(conf, logger.getChild('basic'))
        logger.debug("Checking executables")
        check(logger.getChild("execs_check"))

        logger.debug("Generating slurm configuration for main runs")
        cs.sp.sconf_main = gensconf(conf['slurm']['main'], logger.getChild('gensconf'))
        logger.debug("Checking slurm configuration for main runs")
        sbatch.config.configure(cs.sp.sconf_main, logger.getChild('checksconf'))

        logger.debug("Generating slurm configuration for post processing")
        cs.sp.sconf_post = gensconf(conf['slurm']['post'], logger.getChild('gensconf'))
        logger.debug("Checking slurm configuration for main runs")
        sbatch.config.configure(cs.sp.sconf_post, logger.getChild('checksconf'))
    else:
        logger.info(f"Config file {conffile.as_posix()} was not found")

    return 0


def genconf(conffile: Path, logger: logging.Logger):
    if conffile.exists():
        logger.error("Default config file exists in present directory")
        raise RuntimeError("Default config file exists in present directory")
    else:
        logger.debug(f"{conffile.as_posix()} not exists")

    conf = {}

    execs = {}
    execs['lammps'] = cs.execs.lammps
    execs['MDDPN'] = cs.execs.MDDPN
    execs['sbatch'] = cs.execs.sbatch
    execs['sacct'] = cs.execs.sacct
    execs['sinfo'] = cs.execs.sinfo
    execs['MDpoll'] = cs.execs.MDpoll
    conf['execs'] = execs

    conf['post_processor'] = "/path/to/my/python/file"

    files = {}
    files['template'] = cs.files.template
    conf['files'] = files

    folders = {}
    folders['in_templates'] = cs.folders.in_templates
    conf['folders'] = folders

    conf['slurm'] = {}
    conf['slurm']['main'] = sbatch.config.genconf()
    del conf['slurm']['main'][sbatch.cs.fields.execs]
    conf['slurm']['post'] = sbatch.config.genconf()
    del conf['slurm']['post'][sbatch.cs.fields.execs]

    with conffile.open('w') as fp:
        json.dump(conf, fp, indent=4)

    return 0


if __name__ == "__main__":
    pass
