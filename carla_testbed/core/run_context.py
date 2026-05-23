from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class RunContext:
    run_id: str
    output_dir: Path
    start_wall_time_s: float
    config_path: Path
    resolved_config_path: Path | None = None
    git_sha: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        output_dir: str | Path,
        config_path: str | Path,
        resolved_config_path: str | Path | None = None,
        git_sha: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        start_wall_time_s: float | None = None,
    ) -> "RunContext":
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        return cls(
            run_id=run_id,
            output_dir=output_path,
            start_wall_time_s=time.time() if start_wall_time_s is None else float(start_wall_time_s),
            config_path=Path(config_path),
            resolved_config_path=Path(resolved_config_path) if resolved_config_path is not None else None,
            git_sha=git_sha,
            metadata=dict(metadata or {}),
        )
