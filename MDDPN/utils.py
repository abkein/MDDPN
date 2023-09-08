#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 08-09-2023 17:50:07

import shlex
import logging
import subprocess as sb


def wexec(cmd: str, logger: logging.Logger) -> str:
    logger.debug(f"Calling '{cmd}'")
    cmds = shlex.split(cmd)
    proc = sb.run(cmds, capture_output=True)
    bout = proc.stdout.decode()
    berr = proc.stderr.decode()
    if proc.returncode != 0:
        logger.error("Process returned non-zero exitcode")
        logger.error("### OUTPUT ###")
        logger.error("bout")
        logger.error("### ERROR ###")
        logger.error(berr)
        logger.error("")
        raise RuntimeError("Process returned non-zero exitcode")
    return bout


if __name__ == "__main__":
    pass
