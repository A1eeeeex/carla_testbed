"""Offline platform planning primitives.

This package is CI-safe: it does not import CARLA, ROS2, CyberRT, Autoware, or
Apollo protobuf modules. Runtime wrappers should consume the resolved RunPlan
later instead of making the compiler import runtime stacks.
"""

from .compiler import PlatformCompileError, compile_run_plan, compile_suite_matrix
from .plan import RunPlan
from .registry import PlatformRegistry, default_platform_registry

__all__ = [
    "PlatformCompileError",
    "PlatformRegistry",
    "RunPlan",
    "compile_run_plan",
    "compile_suite_matrix",
    "default_platform_registry",
]
