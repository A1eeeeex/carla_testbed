from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict


class Backend(ABC):
    """Minimal lifecycle for IO backends."""

    def __init__(self, profile: Dict[str, Any]):
        self.profile = profile or {}

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def health_check(self) -> bool:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...
