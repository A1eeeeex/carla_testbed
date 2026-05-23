#!/usr/bin/env python3
"""Deprecated legacy wrapper; prefer the canonical carla_testbed CLI."""

import warnings
from tbio.scripts.stop import main

warnings.warn("deprecated legacy wrapper: use `python -m carla_testbed ...`", DeprecationWarning)

if __name__ == "__main__":
    main()
