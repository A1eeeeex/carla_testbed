from __future__ import annotations

from pathlib import Path

from tools.run_town01_capability_online_pack import build_online_command


def _entry() -> dict[str, object]:
    return {
        "capability_profile": "lane_keep",
        "seed_route_ids_file": "routes_seed.txt",
        "route_ids_file": "routes_full.txt",
        "seed_comparison_label": "lane_keep_seed",
        "comparison_label": "lane_keep_full",
        "overrides_file": "overrides.yaml",
        "ticks": 100,
    }


def test_build_online_command_forwards_no_enable_guard() -> None:
    argv = build_online_command(
        _entry(),
        mode="seed",
        batch_root_parent=Path("/tmp/batch"),
        comparison_label_suffix="noguard",
        startup_profile="render_offscreen_no_ros2",
        config_path=None,
        ticks_override=50,
        post_fail_steps_override=None,
        launch_attempts=1,
        world_ready_timeout_sec=10.0,
        retry_delay_sec=1.0,
        stop_carla_on_exit=False,
        enable_lateral=True,
        enable_guard=False,
        force_fresh_start=False,
    )

    assert "--enable-lateral" in argv
    assert "--no-enable-guard" in argv
    assert "--enable-guard" not in argv


def test_build_online_command_forwards_enable_guard() -> None:
    argv = build_online_command(
        _entry(),
        mode="seed",
        batch_root_parent=Path("/tmp/batch"),
        comparison_label_suffix="guarded",
        startup_profile="render_offscreen_no_ros2",
        config_path=None,
        ticks_override=50,
        post_fail_steps_override=None,
        launch_attempts=1,
        world_ready_timeout_sec=10.0,
        retry_delay_sec=1.0,
        stop_carla_on_exit=False,
        enable_lateral=True,
        enable_guard=True,
        force_fresh_start=False,
    )

    assert "--enable-guard" in argv
    assert "--no-enable-guard" not in argv
