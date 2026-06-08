from __future__ import annotations

from carla_testbed.scenario_player.controllers.base import ControllerInput, ControllerOutput, FixedSceneController


class RouteFollowerController(FixedSceneController):
    controller_name = "route_follower"

    def step(self, control_input: ControllerInput) -> ControllerOutput:
        action = control_input.action
        return ControllerOutput(
            role=control_input.role,
            controller=self.controller_name,
            target_speed_mps=_float_or_none(action.get("target_speed_mps")),
            debug={"runtime_apply_required": True},
        )


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
