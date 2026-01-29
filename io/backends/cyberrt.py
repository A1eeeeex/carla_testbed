from __future__ import annotations

from typing import Any, Dict

from .base import Backend


class CyberRTBackend(Backend):
    def __init__(self, profile: Dict[str, Any]):
        super().__init__(profile)

    def start(self) -> None:
        raise NotImplementedError("CyberRT backend is a placeholder; planned for future work.")

    def health_check(self) -> bool:
        return False

    def stop(self) -> None:
        return
