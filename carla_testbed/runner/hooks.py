from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FrameContext:
    frame_id: int | None = None
    sim_time_s: float | None = None
    wall_time_s: float = field(default_factory=time.time)
    step: int | None = None
    ego_state: Any | None = None
    scene_state: Any | None = None
    control_command: Any | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class RunHook:
    """Optional staged callbacks for runner integrations.

    Hooks are intentionally transport-neutral. They must not depend on CARLA,
    ROS2, CyberRT, or Apollo types at the contract layer.
    """

    def on_run_start(self, context: Any) -> None:
        pass

    def before_tick(self, frame_context: FrameContext) -> None:
        pass

    def after_world_tick(self, frame_context: FrameContext) -> None:
        pass

    def after_state_collect(self, frame_context: FrameContext) -> None:
        pass

    def before_control_apply(self, frame_context: FrameContext) -> None:
        pass

    def after_control_apply(self, frame_context: FrameContext) -> None:
        pass

    def on_run_end(self, context: Any) -> None:
        pass

    def close(self) -> None:
        pass
