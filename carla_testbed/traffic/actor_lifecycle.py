from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TrafficActorRegistry:
    actors: list[Any] = field(default_factory=list)

    def register(self, actor: Any) -> None:
        self.actors.append(actor)

    def destroy_all(self) -> list[str]:
        errors: list[str] = []
        for actor in reversed(self.actors):
            try:
                actor.destroy()
            except Exception as exc:  # pragma: no cover - defensive runtime cleanup
                errors.append(f"{getattr(actor, 'id', 'unknown')}: {type(exc).__name__}: {exc}")
        self.actors.clear()
        return errors
