"""Core runtime primitives with no simulator/backend dependencies."""

from .lifecycle import CleanupError, LifecycleManager, ManagedResource, cleanup_methods
from .run_context import RunContext
from .time import FrameStamp, is_monotonic

__all__ = [
    "CleanupError",
    "FrameStamp",
    "LifecycleManager",
    "ManagedResource",
    "RunContext",
    "cleanup_methods",
    "is_monotonic",
]
