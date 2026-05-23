#!/usr/bin/env python3
"""Deprecated legacy wrapper; prefer the canonical carla_testbed CLI."""

import warnings
from tbio.scripts.start_carla import main

warnings.warn(
    "deprecated legacy wrapper: use `python -m carla_testbed run --start-carla` or another canonical CLI",
    DeprecationWarning,
)

if __name__ == "__main__":
    main()
