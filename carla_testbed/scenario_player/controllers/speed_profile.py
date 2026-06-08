from __future__ import annotations

from carla_testbed.scenario_player.controllers.base import ControllerInput, ControllerOutput, FixedSceneController


class SpeedProfileController(FixedSceneController):
    controller_name = "speed_profile"

    def step(self, control_input: ControllerInput) -> ControllerOutput:
        profile = control_input.action.get("profile")
        target_speed = control_input.action.get("target_speed_mps")
        if target_speed is None and isinstance(profile, list) and profile:
            target_speed = profile[-1].get("speed_mps") if isinstance(profile[-1], dict) else None
        return ControllerOutput(
            role=control_input.role,
            controller=self.controller_name,
            target_speed_mps=_float_or_none(target_speed),
            debug={"profile_points": len(profile) if isinstance(profile, list) else 0},
        )


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
