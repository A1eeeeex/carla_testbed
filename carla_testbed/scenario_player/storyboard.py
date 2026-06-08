from __future__ import annotations

from typing import Any, Mapping


def storyboard_phases(storyboard: Mapping[str, Any]) -> list[dict[str, Any]]:
    phases = storyboard.get("storyboard", {}).get("phases", [])
    return [dict(phase) for phase in phases if isinstance(phase, Mapping)]


def storyboard_roles(storyboard: Mapping[str, Any]) -> list[str]:
    roles = storyboard.get("roles", {})
    return sorted(str(role) for role in roles) if isinstance(roles, Mapping) else []


def phase_by_id(storyboard: Mapping[str, Any], phase_id: str) -> dict[str, Any] | None:
    for phase in storyboard_phases(storyboard):
        if str(phase.get("id")) == phase_id:
            return phase
    return None


def phase_action_types(storyboard: Mapping[str, Any]) -> list[str]:
    action_types: list[str] = []
    for phase in storyboard_phases(storyboard):
        for action in phase.get("actions", []):
            if isinstance(action, Mapping) and action.get("type") is not None:
                action_types.append(str(action["type"]))
    return action_types
