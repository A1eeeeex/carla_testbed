from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.curve_pair_semantics import compare_curve_pair, summarize_curve_route


SCRIPT = Path("tools/analyze_curve_pair.py")


def _route_health(
    route_id: str,
    *,
    high_steer: int | None = 10,
    matched: list[int] | None = None,
    target: list[int] | None = None,
    lateral_guard: int = 0,
    missing_inputs: list[str] | None = None,
) -> dict:
    return {
        "schema_version": "route_health.v1",
        "route_id": route_id,
        "route_geometry": {"curve_segments_count": 1},
        "run_metrics": {
            "lateral_error_p95_m": 1.2,
            "lateral_error_max_m": 1.8,
            "heading_error_p95_rad": 0.2,
            "heading_error_max_rad": 0.3,
        },
        "apollo_semantics": {
            "matched_point_anomaly_locations": matched or [],
            "target_point_anomaly_locations": target or [],
            "first_high_steer": None if high_steer is None else {"seq": high_steer, "value": 0.97},
        },
        "control_semantics": {
            "raw_mapped_applied_steer_available": True,
            "guard_apply_counts": {
                "lateral_guard": lateral_guard,
                "trajectory_contract_lateral_guard": 0,
                "low_speed_steer_guard": 0,
            },
        },
        "missing_fields": [],
        "missing_inputs": missing_inputs or [],
        "verdict": {"status": "diagnostic_ready"},
    }


def test_curve_route_summary_identifies_apollo_lateral_semantics() -> None:
    summary = summarize_curve_route(_route_health("curve217", high_steer=12, matched=[15]))

    assert summary["failure_family"] == "apollo_lateral_semantics_primary"
    assert summary["onset_seq"] == 12
    assert summary["first_matched_point_anomaly_seq"] == 15
    assert summary["guard_apply_counts"]["lateral_guard"] == 0


def test_curve_pair_shared_family_similar_onset() -> None:
    report = compare_curve_pair(
        [
            _route_health("curve217", high_steer=12, matched=[18]),
            _route_health("curve213", high_steer=20, target=[23]),
        ],
        onset_delta_threshold_seq=20,
    )

    assert report["status"] == "shared_family_similar_onset"
    assert report["routes"][0]["failure_family"] == "apollo_lateral_semantics_primary"


def test_curve_pair_shared_family_different_onset() -> None:
    report = compare_curve_pair(
        [
            _route_health("curve217", high_steer=5),
            _route_health("curve213", high_steer=80),
        ],
        onset_delta_threshold_seq=20,
    )

    assert report["status"] == "shared_family_different_onset"


def test_curve_pair_heterogeneous_when_bridge_guard_confounds_one_route() -> None:
    report = compare_curve_pair(
        [
            _route_health("curve217", high_steer=5, lateral_guard=3),
            _route_health("curve213", high_steer=8),
        ]
    )

    assert report["status"] == "heterogeneous_blocker"
    assert {route["failure_family"] for route in report["routes"]} == {
        "bridge_policy_confounded",
        "apollo_lateral_semantics_primary",
    }


def test_curve_pair_insufficient_when_route_health_inputs_missing() -> None:
    report = compare_curve_pair(
        [
            _route_health("curve217", missing_inputs=["timeseries"]),
            _route_health("curve213", high_steer=8),
        ]
    )

    assert report["status"] == "insufficient_data"


def test_curve_pair_cli_writes_outputs(tmp_path: Path) -> None:
    one = tmp_path / "curve217_route_health.json"
    two = tmp_path / "curve213_route_health.json"
    one.write_text(json.dumps(_route_health("curve217", high_steer=5)), encoding="utf-8")
    two.write_text(json.dumps(_route_health("curve213", high_steer=80)), encoding="utf-8")
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-health",
            str(one),
            "--route-health",
            str(two),
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["report"]["status"] == "shared_family_different_onset"
    assert (out / "curve_pair_semantics.json").is_file()
    assert "Curve Pair Semantic Comparison" in (out / "curve_pair_semantics.md").read_text(encoding="utf-8")
