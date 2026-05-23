from __future__ import annotations

from carla_testbed.core.lifecycle import LifecycleManager


def test_cleanup_all_runs_in_reverse_registration_order() -> None:
    manager = LifecycleManager()
    calls: list[str] = []

    manager.register("first", "a", lambda resource: calls.append(resource))
    manager.register("second", "b", lambda resource: calls.append(resource))
    manager.register("third", "c", lambda resource: calls.append(resource))

    errors = manager.cleanup_all()

    assert calls == ["c", "b", "a"]
    assert errors == ()
    assert manager.errors == ()
    assert manager.resources == ()


def test_cleanup_all_continues_after_error() -> None:
    manager = LifecycleManager()
    calls: list[str] = []

    def fail(resource: str) -> None:
        calls.append(resource)
        raise RuntimeError("cleanup failed")

    manager.register("first", "a", lambda resource: calls.append(resource))
    manager.register("bad", "b", fail)
    manager.register("last", "c", lambda resource: calls.append(resource))

    errors = manager.cleanup_all()

    assert calls == ["c", "b", "a"]
    assert len(errors) == 1
    assert errors[0].name == "bad"
    assert isinstance(errors[0].error, RuntimeError)
    assert manager.resources == ()
