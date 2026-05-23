#!/usr/bin/env python3
"""Deprecated legacy wrapper; prefer `python -m carla_testbed run ...`."""

import warnings
from carla_testbed.cli import main

warnings.warn("deprecated legacy wrapper: use `python -m carla_testbed run ...`", DeprecationWarning)

if __name__ == "__main__":
    main()
