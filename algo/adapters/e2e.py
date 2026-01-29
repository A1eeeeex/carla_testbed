from __future__ import annotations

from typing import Any, Dict

from algo.adapters.base import Adapter


class E2EAdapter(Adapter):
    """Placeholder for future end-to-end model container/launch."""

    def prepare(self, profile: Dict[str, Any], run_dir):
        print("[e2e] TODO: prepare model weights/config")

    def start(self, profile: Dict[str, Any], run_dir):
        print("[e2e] TODO: start e2e stack")

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        return False

    def stop(self, profile: Dict[str, Any], run_dir):
        return
