from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class HarnessConfig:
    town: str = "Town01"
    dt: float = 0.05
    max_steps: int = 10
    out_dir: Path = Path("runs/default")
    fail_strategy: str = "fail_fast"  # or log_and_continue
    post_fail_steps: int = 400
    record_demo: bool = False
    make_hud: bool = False
    enable_ros2_bridge: bool = False
    ros2_contract_path: Optional[Path] = None
