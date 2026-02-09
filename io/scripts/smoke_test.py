#!/usr/bin/env python3
import warnings
from tbio.scripts.smoke_test import main

warnings.warn("兼容入口：建议使用 `python -m carla_testbed smoke ...`", DeprecationWarning)

if __name__ == "__main__":
    main()
