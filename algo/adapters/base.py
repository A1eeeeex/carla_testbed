from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict


class Adapter(ABC):
    @abstractmethod
    def prepare(self, profile: Dict[str, Any], run_dir):
        ...

    @abstractmethod
    def start(self, profile: Dict[str, Any], run_dir):
        ...

    @abstractmethod
    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        ...

    @abstractmethod
    def stop(self, profile: Dict[str, Any], run_dir):
        ...

    def get_control_topics(self, profile: Dict[str, Any]):
        return []
