from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.actions import action_for_role
from carla_testbed.scenario_player.actor_registry import ScenarioActorRegistry, ScenarioActorState
from carla_testbed.scenario_player.schema import validate_fixed_scene_storyboard
from carla_testbed.scenario_player.storyboard import storyboard_phases, storyboard_roles
from carla_testbed.scenario_player.trace import (
    PhaseEventWriter,
    ScenarioActorTraceWriter,
    build_actor_trace_row,
    build_phase_event,
)
from carla_testbed.scenario_player.triggers import evaluate_trigger


@dataclass
class FixedSceneFrameContext:
    sim_time_sec: float
    world_frame: int
    actors: Mapping[str, ScenarioActorState | Mapping[str, Any]] = field(default_factory=dict)


class FixedScenePlayer:
    """Offline-safe fixed scene player skeleton.

    This class evaluates storyboard phase triggers and records intent/trace
    evidence. Runtime-specific subclasses may translate actions into CARLA actor
    controls, but this base class intentionally imports no CARLA runtime modules.
    """

    def __init__(self, *, trace_path: str | Path | None = None, phase_events_path: str | Path | None = None) -> None:
        self.storyboard: dict[str, Any] | None = None
        self.trace_writer = ScenarioActorTraceWriter(trace_path)
        self.event_writer = PhaseEventWriter(phase_events_path)
        self.started_phases: set[str] = set()
        self.completed_phases: set[str] = set()
        self.phase_start_times: dict[str, float] = {}
        self.current_phase_id: str | None = None
        self.actors = ScenarioActorRegistry()

    def setup(self, context: Mapping[str, Any] | None, storyboard: Mapping[str, Any]) -> None:
        resolved = dict(storyboard)
        validate_fixed_scene_storyboard(resolved)
        self.storyboard = resolved
        if context:
            trace_path = context.get("trace_path")
            phase_events_path = context.get("phase_events_path")
            if trace_path:
                self.trace_writer = ScenarioActorTraceWriter(trace_path)
            if phase_events_path:
                self.event_writer = PhaseEventWriter(phase_events_path)

    def tick(self, frame_context: FixedSceneFrameContext | Mapping[str, Any]) -> dict[str, Any]:
        if self.storyboard is None:
            raise RuntimeError("FixedScenePlayer.setup() must be called before tick()")
        frame = _normalize_frame_context(frame_context)
        self.actors.update(frame.actors)
        active_actions: dict[str, dict[str, Any]] = {}
        started_now: list[str] = []
        stopped = evaluate_trigger(
            self.storyboard.get("storyboard", {}).get("stop"),
            sim_time_sec=frame.sim_time_sec,
            world_frame=frame.world_frame,
            actors=self.actors,
            completed_phases=self.completed_phases,
        )
        for phase in storyboard_phases(self.storyboard):
            phase_id = str(phase["id"])
            if phase_id not in self.started_phases and evaluate_trigger(
                phase.get("start"),
                sim_time_sec=frame.sim_time_sec,
                world_frame=frame.world_frame,
                actors=self.actors,
                completed_phases=self.completed_phases,
            ):
                self.started_phases.add(phase_id)
                self.phase_start_times[phase_id] = frame.sim_time_sec
                self.current_phase_id = phase_id
                started_now.append(phase_id)
                self.event_writer.write(
                    build_phase_event(
                        scene_id=str(self.storyboard["scene_id"]),
                        phase=phase_id,
                        event="phase_started",
                        sim_time_sec=frame.sim_time_sec,
                        world_frame=frame.world_frame,
                        reason="start_trigger_satisfied",
                    )
                )
                previous = _previous_phase_id(storyboard_phases(self.storyboard), phase_id)
                if previous and previous not in self.completed_phases:
                    self.completed_phases.add(previous)
                    self.event_writer.write(
                        build_phase_event(
                            scene_id=str(self.storyboard["scene_id"]),
                            phase=previous,
                            event="phase_completed",
                            sim_time_sec=frame.sim_time_sec,
                            world_frame=frame.world_frame,
                            reason=f"next_phase_started:{phase_id}",
                        )
                    )
            if phase_id in self.started_phases and phase_id not in self.completed_phases:
                for action in phase.get("actions", []):
                    if isinstance(action, Mapping):
                        active_actions[str(action.get("role"))] = dict(action)
        if stopped and self.current_phase_id and self.current_phase_id not in self.completed_phases:
            self.completed_phases.add(self.current_phase_id)
            self.event_writer.write(
                build_phase_event(
                    scene_id=str(self.storyboard["scene_id"]),
                    phase=self.current_phase_id,
                    event="phase_completed",
                    sim_time_sec=frame.sim_time_sec,
                    world_frame=frame.world_frame,
                    reason="stop_trigger_satisfied",
                )
            )
        for role in storyboard_roles(self.storyboard):
            if role == "ego":
                continue
            actor = self.actors.get(role)
            if actor is None:
                continue
            phase = self.current_phase_id
            current = _phase_by_id(storyboard_phases(self.storyboard), phase) if phase else None
            action = active_actions.get(role)
            if action is None and phase:
                action = action_for_role(current.get("actions", []) if current else [], role)
            relative = self.actors.relative_longitudinal_lateral("ego", role)
            self.trace_writer.write(
                build_actor_trace_row(
                    scene_id=str(self.storyboard["scene_id"]),
                    sim_time_sec=frame.sim_time_sec,
                    world_frame=frame.world_frame,
                    phase=phase,
                    actor=actor,
                    action=action,
                    distance_to_ego_m=self.actors.distance("ego", role),
                    longitudinal_to_ego_m=relative[0] if relative is not None else None,
                    lateral_to_ego_m=relative[1] if relative is not None else None,
                    lane_change_progress=_lane_change_progress(
                        action,
                        current,
                        frame.sim_time_sec,
                        self.phase_start_times,
                    ),
                )
            )
        return {
            "scene_id": self.storyboard["scene_id"],
            "started_now": started_now,
            "active_roles": self.active_roles(),
            "current_phase": self.current_phase_id,
            "active_actions": active_actions,
            "stopped": stopped,
        }

    def teardown(self) -> None:
        self.storyboard = None
        self.started_phases.clear()
        self.completed_phases.clear()
        self.phase_start_times.clear()
        self.current_phase_id = None
        self.actors = ScenarioActorRegistry()

    def active_roles(self) -> list[str]:
        return [role for role in self.actors.roles() if role != "ego"]


def _normalize_frame_context(frame_context: FixedSceneFrameContext | Mapping[str, Any]) -> FixedSceneFrameContext:
    if isinstance(frame_context, FixedSceneFrameContext):
        return frame_context
    return FixedSceneFrameContext(
        sim_time_sec=float(frame_context.get("sim_time_sec", frame_context.get("time", 0.0))),
        world_frame=int(frame_context.get("world_frame", frame_context.get("frame", 0))),
        actors=frame_context.get("actors", {}),
    )


def _previous_phase_id(phases: list[dict[str, Any]], phase_id: str) -> str | None:
    previous: str | None = None
    for phase in phases:
        current = str(phase.get("id"))
        if current == phase_id:
            return previous
        previous = current
    return None


def _phase_by_id(phases: list[dict[str, Any]], phase_id: str) -> dict[str, Any] | None:
    for phase in phases:
        if str(phase.get("id")) == phase_id:
            return phase
    return None


def _lane_change_progress(
    action: Mapping[str, Any] | None,
    phase: Mapping[str, Any] | None,
    sim_time_sec: float,
    phase_start_times: Mapping[str, float],
) -> float | None:
    if not action or action.get("type") != "lane_change" or not phase:
        return None
    duration = action.get("duration_s")
    if duration in {None, ""}:
        return None
    try:
        duration_s = float(duration)
    except (TypeError, ValueError):
        return None
    if duration_s <= 0.0:
        return 1.0
    phase_id = str(phase.get("id"))
    started_at = phase_start_times.get(phase_id, sim_time_sec)
    return max(0.0, min(1.0, (sim_time_sec - float(started_at)) / duration_s))
