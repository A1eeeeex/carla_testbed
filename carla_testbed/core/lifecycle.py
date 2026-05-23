from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class CleanupCallable(Protocol):
    def __call__(self, resource: Any) -> None:
        ...


@dataclass(frozen=True)
class ManagedResource:
    name: str
    resource: Any
    cleanup: CleanupCallable


@dataclass(frozen=True)
class CleanupError:
    name: str
    error: Exception


class LifecycleManager:
    def __init__(self) -> None:
        self._resources: list[ManagedResource] = []
        self._errors: list[CleanupError] = []

    @property
    def errors(self) -> tuple[CleanupError, ...]:
        return tuple(self._errors)

    @property
    def resources(self) -> tuple[ManagedResource, ...]:
        return tuple(self._resources)

    def register(self, name: str, resource: Any, cleanup: CleanupCallable) -> ManagedResource:
        item = ManagedResource(name=name, resource=resource, cleanup=cleanup)
        self._resources.append(item)
        return item

    def register_methods(self, name: str, resource: Any, *method_names: str) -> ManagedResource:
        return self.register(name=name, resource=resource, cleanup=cleanup_methods(*method_names))

    def cleanup_all(self) -> tuple[CleanupError, ...]:
        self._errors.clear()
        while self._resources:
            item = self._resources.pop()
            try:
                item.cleanup(item.resource)
            except Exception as exc:  # keep cleanup best-effort
                self._errors.append(CleanupError(name=item.name, error=exc))
        return self.errors


def cleanup_methods(*method_names: str) -> CleanupCallable:
    def _cleanup(resource: Any) -> None:
        for method_name in method_names:
            method = getattr(resource, method_name, None)
            if method is None:
                continue
            method()

    return _cleanup
