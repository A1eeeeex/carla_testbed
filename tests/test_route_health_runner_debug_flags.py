from __future__ import annotations

from pathlib import Path

from tools import run_town01_route_health


def _flags(*, enable_lateral_debug: bool = False) -> dict[str, bool]:
    return {
        "enable_lateral": True,
        "enable_guard": False,
        "enable_lateral_debug": enable_lateral_debug,
        "enable_recording": False,
        "enable_strict_observation": False,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    }


def _overrides(*, enable_lateral_debug: bool = False) -> list[str]:
    return run_town01_route_health._overrides_for_flags(
        _flags(enable_lateral_debug=enable_lateral_debug),
        corpus_path=Path("tests/fixtures/routes"),
        route_id="lane_keep_097",
        ticks=100,
        comparison_label="test",
    )


def test_route_health_runner_respects_configured_control_raw_dump_by_default() -> None:
    overrides = _overrides(enable_lateral_debug=False)

    assert "algo.apollo.bridge.debug_dump_control_raw=true" not in overrides


def test_route_health_runner_enables_heavy_control_dump_only_for_lateral_debug() -> None:
    overrides = _overrides(enable_lateral_debug=True)

    assert overrides.count("algo.apollo.bridge.debug_dump_control_raw=true") == 1
