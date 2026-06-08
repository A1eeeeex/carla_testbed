from __future__ import annotations

from carla_testbed.scenario_player.controllers.base import ControllerInput, ControllerOutput, FixedSceneController


class LaneChangeController(FixedSceneController):
    controller_name = "lane_change"

    def step(self, control_input: ControllerInput) -> ControllerOutput:
        action = control_input.action
        return ControllerOutput(
            role=control_input.role,
            controller=self.controller_name,
            target_lane_offset=_int_or_none(action.get("target_lane_offset")),
            debug={"duration_s": action.get("duration_s")},
        )


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
