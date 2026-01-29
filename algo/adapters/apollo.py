from __future__ import annotations

from typing import Any, Dict

from algo.adapters.base import Adapter


class ApolloAdapter(Adapter):
    """Placeholder for Apollo integration."""

    def prepare(self, profile: Dict[str, Any], run_dir):
        print("[apollo] TODO prepare")

    def start(self, profile: Dict[str, Any], run_dir):
        print("[apollo] TODO start")

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        return False

    def stop(self, profile: Dict[str, Any], run_dir):
        return
