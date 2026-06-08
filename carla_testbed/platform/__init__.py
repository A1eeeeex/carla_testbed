"""Offline platform planning primitives.

This package is CI-safe: it does not import CARLA, ROS2, CyberRT, Autoware, or
Apollo protobuf modules. Runtime wrappers should consume the resolved RunPlan
later instead of making the compiler import runtime stacks.
"""

from .compiler import PlatformCompileError, compile_run_plan, compile_suite_matrix
from .evidence_resolver import EvidenceResolution, resolve_evidence_for_plan
from .plan import RunPlan
from .registry import PlatformRegistry, default_platform_registry

_LAZY_EXPORTS = {
    "ExecutionResult": (".executor", "ExecutionResult"),
    "execute_run_plan": (".executor", "execute_run_plan"),
}


def __getattr__(name: str):
    try:
        module_name, attr_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    import importlib

    module = importlib.import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


__all__ = [
    "PlatformCompileError",
    "PlatformRegistry",
    "EvidenceResolution",
    "ExecutionResult",
    "RunPlan",
    "compile_run_plan",
    "compile_suite_matrix",
    "default_platform_registry",
    "execute_run_plan",
    "resolve_evidence_for_plan",
]
