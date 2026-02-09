#!/usr/bin/env python3
import warnings
from tbio.scripts.stop import main

warnings.warn("兼容入口：建议使用 `python -m carla_testbed ...`", DeprecationWarning)

if __name__ == "__main__":
    main()
