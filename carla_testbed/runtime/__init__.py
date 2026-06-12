"""Runtime dispatch helpers.

Core runtime dispatch lives in package modules so CLI tools can stay thin.
These helpers must remain import-safe in CI and therefore do not import CARLA,
CyberRT, or Apollo protobufs at module import time.
"""

