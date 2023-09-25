#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 25-09-2023 20:42:19

import os
import shlex
import logging
import subprocess as sb
from typing import Dict, List, TypeVar
import string


KT = TypeVar('KT')
VT = TypeVar('VT')


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


def is_exe(fpath: str, logger: logging.Logger, exit: bool = False):
    logger.debug(f"Checking: '{fpath}'")
    if not (os.path.isfile(fpath) and os.access(fpath, os.X_OK)):
        logger.debug("This is not standard file")
        if not exit:
            logger.debug("Resolving via 'which'")
            cmd = f"which {fpath}"
            cmds = shlex.split(cmd)
            proc = sb.run(cmds, capture_output=True)
            bout = proc.stdout.decode()
            # berr = proc.stderr.decode()
            if proc.returncode != 0:
                logger.debug('Process returned nonzero returncode')
                return False
            else:
                return is_exe(bout.strip(), logger.getChild('2nd'), exit=True)
        else:
            return False
    else:
        return True


# class config(dict[KT, VT]):
#     def __init__(self, *args, **kwargs) -> None:
#         super().__init__(*args, **kwargs)
#         self.placeholders: Dict[str, List[KT]] = {}
#         for k, v in super().items():
#             self.add_ph(k, v)

#     def add_ph(self, __key: KT, __value: VT) -> None:
#         if isinstance(__value, str):
#             phs = [tup[1] for tup in string.Formatter().parse(__value) if tup[1] is not None]
#             if len(phs) != 0:
#                 for ph in phs:
#                     if len(ph) != 0:
#                         if ph in self.placeholders:
#                             self.placeholders[ph] += [__key]
#                         else:
#                             self.placeholders[ph] = [__key]
#                     else:
#                         pass

#     def __setitem__(self, __key: KT, __value: VT) -> None:
#         self.add_ph(__key, __value)
#         return super().__setitem__(__key, __value)

#     def reconf(self, **kwargs) -> None:
#         for ph, value in kwargs.items():
#             for key in self.placeholders[ph]:
#                 obj: VT = super().__getitem__(key)
#                 if isinstance(obj, str):
#                     obj = obj.format(**{ph: value})  # type: ignore
#                 else:
#                     raise Exception
#                 super().__setitem__(key, obj)  # type: ignore

#     def sreconf(self) -> None:
#         for ph, keys in self.placeholders.items():
#             if super().__contains__(ph):
#                 for key in keys:
#                     obj: VT = super().__getitem__(key)
#                     if isinstance(obj, str):
#                         obj = obj.format(**{ph: super().__getitem__(ph)})  # type: ignore
#                     else:
#                         raise Exception
#                     super().__setitem__(key, obj)  # type: ignore


class config(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.placeholders: Dict[str, List] = {}
        for k, v in super().items():
            self.add_ph(k, v)

    def add_ph(self, __key, __value) -> None:
        if isinstance(__value, str):
            phs = [tup[1] for tup in string.Formatter().parse(__value) if tup[1] is not None]
            if len(phs) != 0:
                for ph in phs:
                    if len(ph) != 0:
                        if ph in self.placeholders:
                            self.placeholders[ph] += [__key]
                        else:
                            self.placeholders[ph] = [__key]
                    else:
                        pass

    def __setitem__(self, __key, __value) -> None:
        self.add_ph(__key, __value)
        return super().__setitem__(__key, __value)

    def reconf(self, **kwargs) -> None:
        for ph, value in kwargs.items():
            for key in self.placeholders[ph]:
                obj = super().__getitem__(key)
                if isinstance(obj, str):
                    obj = obj.format(**{ph: value})  # type: ignore
                else:
                    raise Exception
                super().__setitem__(key, obj)  # type: ignore

    def sreconf(self) -> None:
        for ph, keys in self.placeholders.items():
            if super().__contains__(ph):
                for key in keys:
                    obj = super().__getitem__(key)
                    if isinstance(obj, str):
                        obj = obj.format(**{ph: super().__getitem__(ph)})  # type: ignore
                    else:
                        raise Exception
                    super().__setitem__(key, obj)  # type: ignore


if __name__ == "__main__":
    pass
