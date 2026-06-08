from __future__ import annotations

from pathlib import Path

from carla_testbed.traffic.demo_recording import compute_displacements, movement_status, write_json, write_jsonl


def test_movement_status_blocks_static_visual_demo() -> None:
    movement = movement_status(
        {"100": 0.0, "101": 1.0, "102": 2.5},
        min_moving_actors=2,
        min_displacement_m=5.0,
    )

    assert movement["status"] == "fail"
    assert movement["blocking_reasons"] == ["traffic_actors_not_moving_enough_for_demo_recording"]


def test_movement_status_accepts_multiple_moving_traffic_actors() -> None:
    movement = movement_status(
        {"100": 8.0, "101": 12.0, "102": 0.5},
        min_moving_actors=2,
        min_displacement_m=5.0,
    )

    assert movement["status"] == "pass"
    assert movement["moving_actor_count"] == 2
    assert movement["blocking_reasons"] == []


def test_compute_displacements_uses_actor_ids() -> None:
    displacements = compute_displacements(
        {100: (0.0, 0.0), 101: (10.0, 10.0)},
        {100: (3.0, 4.0), 101: (10.0, 10.0)},
    )

    assert displacements == {"100": 5.0, "101": 0.0}


def test_recording_helpers_write_json_artifacts(tmp_path: Path) -> None:
    write_json(tmp_path / "status.json", {"status": "ready"})
    write_jsonl(tmp_path / "events.jsonl", [{"event": "spawned"}, {"event": "autopilot_enabled"}])

    assert '"status": "ready"' in (tmp_path / "status.json").read_text(encoding="utf-8")
    assert len((tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()) == 2
