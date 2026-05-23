#!/usr/bin/env python3
"""Deprecated legacy wrapper; prefer `python -m carla_testbed smoke ...`."""

import warnings
from tbio.scripts.smoke_test import main

warnings.warn("deprecated legacy wrapper: use `python -m carla_testbed smoke ...`", DeprecationWarning)

if __name__ == "__main__":
    main()
