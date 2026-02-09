#!/usr/bin/env python3
import warnings
from tbio.scripts.start_carla import main

warnings.warn("兼容入口：建议使用 `python -m carla_testbed run --start-carla` 等正式 CLI", DeprecationWarning)

if __name__ == "__main__":
    main()
