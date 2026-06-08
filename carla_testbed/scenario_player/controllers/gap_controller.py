from __future__ import annotations

from carla_testbed.scenario_player.controllers.base import ControllerInput, ControllerOutput, FixedSceneController


class GapController(FixedSceneController):
    controller_name = "gap_controller"

    def step(self, control_input: ControllerInput) -> ControllerOutput:
        return ControllerOutput(
            role=control_input.role,
            controller=self.controller_name,
            target_gap_m=_float_or_none(control_input.action.get("target_gap_m")),
            debug={"runtime_apply_required": True},
        )


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
