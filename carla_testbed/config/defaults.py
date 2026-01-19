from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple, List


@dataclass
class HarnessConfig:
    town: str = "Town01"
    dt: float = 0.05
    max_steps: int = 10
    out_dir: Path = Path("runs/default")
    fail_strategy: str = "fail_fast"  # or log_and_continue
    post_fail_steps: int = 400
    record_modes: List[str] = field(default_factory=list)
    record_output: Optional[Path] = None
    record_fps: Optional[float] = None
    record_resolution: Tuple[int, int] = (1920, 1080)
    record_chase_distance: float = 8.0
    record_chase_height: float = 3.0
    record_chase_pitch: float = -15.0
    record_max_lidar_points: int = 10000
    record_keep_frames: bool = False
    record_no_lidar: bool = False
    record_no_radar: bool = False
    record_no_hud: bool = False
    enable_ros2_bridge: bool = False
    ros2_contract_path: Optional[Path] = None
