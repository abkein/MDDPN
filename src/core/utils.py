#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

from typing import Union, Iterable


def is_iter(arr: Union[Iterable[float], float]) -> bool:
    try:
        iter(arr)  # type: ignore
        return True
    except Exception:
        return False


if __name__ == "__main__":
    pass
