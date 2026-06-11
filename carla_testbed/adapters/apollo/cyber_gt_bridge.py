from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ApolloCyberGTBridgeEntrypoint:
    """Adapter-side facade for the current Apollo CyberRT GT bridge entrypoint.

    The runtime implementation still lives in ``tools/apollo10_cyber_bridge``
    for backward compatibility. New orchestration code should depend on this
    facade rather than hard-coding the tools path; semantic publishers and
    subscribers can then migrate under ``carla_testbed.adapters.apollo`` in
    small, testable steps.
    """

    repo_root: Path

    @property
    def bridge_script(self) -> Path:
        return self.repo_root / "tools" / "apollo10_cyber_bridge" / "bridge.py"

    @property
    def bridge_package_dir(self) -> Path:
        return self.repo_root / "tools" / "apollo10_cyber_bridge"

    @property
    def pb_root(self) -> Path:
        return self.bridge_package_dir / "pb"


def default_apollo_cyber_gt_bridge_entrypoint(repo_root: str | Path | None = None) -> ApolloCyberGTBridgeEntrypoint:
    root = Path(repo_root).expanduser().resolve() if repo_root else Path(__file__).resolve().parents[3]
    return ApolloCyberGTBridgeEntrypoint(repo_root=root)


__all__ = [
    "ApolloCyberGTBridgeEntrypoint",
    "default_apollo_cyber_gt_bridge_entrypoint",
]
