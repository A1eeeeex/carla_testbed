from __future__ import annotations

import json

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.player import FixedSceneFrameContext, FixedScenePlayer
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_player_emits_phase_events_and_trace(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml")
    storyboard = compile_fixed_scene_template(template)
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    player = FixedScenePlayer(trace_path=trace_path, phase_events_path=events_path)
    player.setup({}, storyboard)

    player.tick(
        FixedSceneFrameContext(
            sim_time_sec=0.0,
            world_frame=1,
            actors={
                "ego": {"x": 0.0, "y": 0.0, "speed_mps": 10.0},
                "lead_vehicle": {"actor_id": "lead", "x": 300.0, "y": 0.0, "speed_mps": 16.0},
            },
        )
    )
    player.tick(
        {
            "sim_time_sec": 10.0,
            "world_frame": 2,
            "actors": {
                "ego": {"x": 230.0, "y": 0.0, "speed_mps": 22.0},
                "lead_vehicle": {"actor_id": "lead", "x": 300.0, "y": 0.0, "speed_mps": 8.0},
            },
        }
    )
    player.tick(
        {
            "sim_time_sec": 15.0,
            "world_frame": 3,
            "actors": {
                "ego": {"x": 240.0, "y": 0.0, "speed_mps": 2.0},
                "lead_vehicle": {"actor_id": "lead", "x": 300.0, "y": 0.0, "speed_mps": 0.0},
            },
        }
    )

    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    rows = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert [event["phase"] for event in events if event["event"] == "phase_started"] == [
        "lead_cruise",
        "lead_brake_to_stop",
        "lead_hold_stop",
    ]
    assert rows[-1]["phase"] == "lead_hold_stop"
    assert rows[-1]["control_source"] == "fixed_scene_player"
    assert rows[-1]["target_speed_mps"] == 0.0


def test_player_records_lane_change_progress(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenario_templates/cut_in.yaml")
    storyboard = compile_fixed_scene_template(template)
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    player = FixedScenePlayer(trace_path=trace_path, phase_events_path=events_path)
    player.setup({}, storyboard)

    for sim_time in (0.0, 4.0, 5.5, 7.0):
        player.tick(
            {
                "sim_time_sec": sim_time,
                "world_frame": int(sim_time * 10 + 1),
                "actors": {
                    "ego": {"x": 0.0, "y": 0.0, "speed_mps": 8.0},
                    "lead_vehicle": {"actor_id": "cutin", "x": 25.0, "y": 3.5, "speed_mps": 10.0},
                },
            }
        )

    rows = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    lane_rows = [row for row in rows if row["action_type"] == "lane_change"]
    assert lane_rows
    assert lane_rows[-1]["lane_change_progress"] == 1.0
    assert lane_rows[-1]["target_lane_id"] == "offset:1"


def test_player_records_current_speed_profile_target(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    storyboard = compile_fixed_scene_template(template)
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    player = FixedScenePlayer(trace_path=trace_path)
    player.setup({}, storyboard)

    for sim_time in (0.0, 6.0, 10.0):
        player.tick(
            {
                "sim_time_sec": sim_time,
                "world_frame": int(sim_time + 1),
                "actors": {
                    "ego": {"x": 0.0, "y": 0.0, "speed_mps": 11.11},
                    "lead_vehicle": {"actor_id": "lead", "x": 20.0, "y": 0.0, "speed_mps": 11.11},
                },
            }
        )

    rows = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["target_speed_mps"] == 11.11
    assert round(rows[1]["target_speed_mps"], 3) == 12.776
    assert rows[-1]["target_speed_mps"] == 19.44
