from __future__ import annotations

from typing import Any, Dict

from algo.adapters.base import Adapter


class DummyAdapter(Adapter):
    def prepare(self, profile: Dict[str, Any], run_dir):
        return

    def start(self, profile: Dict[str, Any], run_dir):
        print("[dummy] no algorithm started")

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        return True

    def stop(self, profile: Dict[str, Any], run_dir):
        return

    def get_control_topics(self, profile: Dict[str, Any]):
        return []
