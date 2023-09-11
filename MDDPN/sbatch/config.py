#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 11-09-2023 23:46:13

import json
from logging import Logger
from typing import Dict, Set, Any

from hamcrest import is_

from ..utils import wexec, is_exe
from .utils import ranges
from . import constants as cs


def get_info(logger: Logger):
    logger.debug("Getting nodelist")
    cmd = f"{cs.execs.sinfo} -h --hide -o %N"
    nodelist_out = wexec(cmd, logger.getChild('sinfo'))
    nodelist = {}
    for nsl in nodelist_out.split(','):
        nn, nr_s = nsl.strip().replace("]", "").split('[')
        nra, nrb = nr_s.split('-')
        nodelist[nn] = set(range(int(nra), int(nrb)+1))
    cs.obj.nodelist = nodelist
    logger.info(f"Following nodes were found: {cs.obj.nodelist}")

    logger.debug("Getting partitions list")
    cmd = f"{cs.execs.sinfo} -h --hide -o %P"
    partitions_out = wexec(cmd, logger.getChild('sinfo'))
    partitions = []
    for el in partitions_out.split():
        # if re.match(r"^[a-zA-Z_]+$", el):
        partitions.append(el.replace("*", ""))
    cs.obj.partitions = set(partitions)
    logger.info(f"Following partitions were found: {cs.obj.partitions}")


def nodelist_parse(conf, logger: Logger) -> Dict[str, Set[Any]]:
    exclude = {}
    for node_name, node_num in conf.items():
        if isinstance(node_num, (list, set)):
            nodelist = set(node_num)
        else:
            try:
                nodelist = set([int(node_num)])
            except ValueError:
                try:
                    nodelist = set(json.loads(node_num))
                except json.decoder.JSONDecodeError:
                    try:
                        nra, nrb = node_num[1:-1].split('-')
                        nodelist = set(range(int(nra), int(nrb)+1))
                    except Exception:
                        logger.critical("Cannot transform given nodelist to unique set")
                        raise
                except Exception:
                    logger.critical("Cannot transform given nodelist to unique set")
                    raise
            except Exception:
                logger.critical("Cannot transform given nodelist to unique set")
                raise
        exclude[node_name] = nodelist

    return exclude


def checkin(main: Dict[str, Set[Any]], sub: Dict[str, Set[Any]], logger: Logger):
    kys = set(sub.keys()) - set(main.keys())
    if len(kys) != 0:
        logger.error(f"There are no such nodes like {kys}")
        raise RuntimeError(f"There are no such nodes like {kys}")
    nds = {}
    for key in sub.keys():
        itr = sub[key] - main[key]
        if len(itr) != 0:
            nds[key] = itr
    raise RuntimeError(f"There are no such nodes like {nds}")


def __checkin(main: Dict[str, Set[Any]], sub: Dict[str, Set[Any]]):
    if set(sub.keys()) <= set(main.keys()):
        return all([sub[key] <= main[key] for key in sub.keys()])
    else:
        return False


def chuie(use: Dict[str, Set[Any]], exclude: Dict[str, Set[Any]], logger: Logger):  # check if use nodes in exclude nodes
    inter = set(use.keys()) & set(exclude.keys())
    nds = {}
    for key in inter:
        its = use[key] & exclude[key]
        if len(its) != 0:
            nds[key] = its
    logger.error(f"There is intersection of excluded and used nodes: {nds}")
    raise RuntimeError(f"There is intersection of excluded and used nodes: {nds}")


def __chuie(use: Dict[str, Set[Any]], exclude: Dict[str, Set[Any]]):  # check if use nodes in exclude nodes
    inter = set(use.keys()) & set(exclude.keys())
    if len(inter) != 0:
        return not all([len(use[key] & exclude[key]) == 0 for key in inter])
    else:
        return False


def gnnis(nodelist: Dict[str, Set[Any]], exclude: Dict[str, Set[Any]]):  # get nodes not in subset
    nds = {key: (nodelist[key] if key not in exclude.keys() else nodelist[key] - exclude[key]) for key in nodelist.keys()}
    for key in nds.keys():
        if len(nds[key]) == 0:
            del nds[key]
    return nds


def excludes(conf: Dict[str, Any], logger: Logger):
    if cs.fields.nodes_exclude in conf:
        main_nodes_exclude = nodelist_parse(conf[cs.fields.nodes_exclude], logger)
        if __checkin(cs.obj.nodelist, main_nodes_exclude):
            cs.obj.nodes_exclude = main_nodes_exclude
            logger.debug(f"Following nodes will be excluded for main runs: {cs.obj.nodes_exclude}")
        else:
            checkin(cs.obj.nodelist, main_nodes_exclude, logger)

    if cs.fields.nodes_use in conf:
        main_nodes_use = nodelist_parse(conf[cs.fields.nodes_use], logger)
        if __checkin(cs.obj.nodelist, main_nodes_use):
            if cs.obj.nodes_exclude is not None:
                if __chuie(main_nodes_use, cs.obj.nodes_exclude):
                    chuie(main_nodes_use, cs.obj.nodes_exclude, logger)
                cs.obj.nodes_exclude = gnnis(cs.obj.nodelist, main_nodes_use)
                logger.debug(f"Following nodes will be excluded for main runs: {cs.obj.nodes_exclude}")
            else:
                cs.obj.nodes_exclude = gnnis(cs.obj.nodelist, main_nodes_use)
                logger.debug(f"Following nodes will be excluded for main runs: {cs.obj.nodes_exclude}")
        else:
            checkin(cs.obj.nodelist, main_nodes_use, logger)


def gensline(nodelist: Dict[str, Set[Any]]):
    s = ""
    for k, v in nodelist.items():
        for a, b in ranges(v):
            if a == b:
                s += f"{k}{a},"
            else:
                s += f"{k}[{a}-{b}],"
    return s[:-1]


def basic(conf: Dict[str, Any], logger: Logger, is_check: bool = False):
    if cs.fields.execs in conf:
        execs = conf[cs.fields.execs]
        if 'sinfo' in execs:
            cs.execs.sinfo = execs['sinfo']
        # if 'sacct' in execs:
        #     cs.execs.sacct = execs['sacct']
        if 'sbatch' in execs:
            cs.execs.sbatch = execs['sbatch']
    if cs.fields.folder in conf:
        cs.folders.run = conf[cs.fields.folder]
    if cs.fields.jname in conf:
        cs.ps.jname = conf[cs.fields.jname]
    if cs.fields.nnodes in conf:
        cs.ps.nnodes = conf[cs.fields.nnodes]
    if cs.fields.ntpn in conf:
        cs.ps.ntpn = conf[cs.fields.ntpn]
    if cs.fields.partition in conf:
        cs.ps.partition = conf[cs.fields.partition]
    if not is_check:
        if cs.fields.executable in conf:
            if not is_exe(conf[cs.fields.executable], logger.getChild('is_exe')):
                logger.error(f"Specified executable {conf[cs.fields.executable]} is not an executable")
                raise RuntimeError(f"Specified executable {conf[cs.fields.executable]} is not an executable")
        else:
            logger.error("Executable is not specified")
            raise RuntimeError("Executable is not specified")


def configure(conf: Dict[str, Any], logger: Logger, is_check: bool = False):
    # logger.debug("Following configuration is set")
    # logger.debug(json.dumps(conf))
    logger.debug("Setting basic configuration")
    basic(conf, logger.getChild('basic'), is_check)
    logger.debug("Getting info about nodes, partitions, policies")
    get_info(logger)
    logger.debug("Getting partitions to exclude/use")
    excludes(conf, logger)
    if cs.obj.nodes_exclude is not None:
        cs.ps.exclude_str = gensline(cs.obj.nodes_exclude)


def genconf() -> Dict[str, Any]:
    conf = {}

    execs = {}
    execs['sinfo'] = 'sinfo'
    execs['sbatch'] = 'sbatch'
    conf[cs.fields.execs] = execs

    conf[cs.fields.jname] = 'SoMeNaMe'
    conf[cs.fields.nnodes] = 1
    conf[cs.fields.ntpn] = 4
    conf[cs.fields.partition] = 'test'
    conf[cs.fields.folder] = 'slurm'
    conf[cs.fields.nodes_exclude] = {
        'host': 1,
        'angr': [3, 4, 5],
        'ghost': "[6-18]"
    }
    conf[cs.fields.nodes_use] = {
        'host': 2,
        'angr': [1, 2, 6, 7, 8],
        'ghost': "[1-5]"
    }

    return conf


if __name__ == "__main__":
    pass
