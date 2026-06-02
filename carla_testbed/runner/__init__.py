from .hooks import FrameContext, RunHook

__all__ = ["FrameContext", "RunHook", "get_test_harness_class"]


def get_test_harness_class():
    """Return the CARLA-backed TestHarness without importing it at package load."""
    from .harness import TestHarness

    return TestHarness


def __getattr__(name: str):
    if name == "TestHarness":
        return get_test_harness_class()
    raise AttributeError(name)
