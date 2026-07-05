from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.control_attribution import (
    analyze_control_attribution,
    analyze_control_attribution_run_dir,
    write_control_attribution_report,
)

FIXTURE = Path("tests/fixtures/control/simple_control_chain.csv")


def _write_rows(path: Path, rows: list[dict[str, object]]) -> Path:
    fieldnames = [
        "run_id",
        "route_id",
        "backend",
        "sim_time",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "ego_yaw_rate",
        "throttle_raw",
        "throttle_mapped",
        "throttle_applied",
        "brake_raw",
        "brake_mapped",
        "brake_applied",
        "control_latency_ms",
        "steer_scale",
        "steering_sign",
        "actuator_mapping_mode",
        "calibration_profile_id",
        "throttle_brake_mutual_exclusion_applied",
        "throttle_brake_hysteresis_held",
        "startup_boost_applied",
        "terminal_stop_hold_active",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    return path


def _base_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, raw in enumerate([0.20, 0.24, 0.16, 0.12]):
        mapped = raw * 0.25
        rows.append(
            {
                "run_id": "fixture_run",
                "route_id": "lane_keep_097",
                "backend": "carla_direct",
                "sim_time": index * 0.05,
                "apollo_steer_raw": raw,
                "bridge_steer_mapped": mapped,
                "carla_steer_applied": mapped,
                "ego_yaw_rate": 0.01 + index * 0.001,
                "throttle_raw": 0.2,
                "throttle_mapped": 0.2,
                "throttle_applied": 0.2,
                "brake_raw": 0.0,
                "brake_mapped": 0.0,
                "brake_applied": 0.0,
                "control_latency_ms": 50,
                "steer_scale": 0.25,
                "steering_sign": 1,
                "actuator_mapping_mode": "legacy",
                "calibration_profile_id": "control_actuation_draft",
                "throttle_brake_mutual_exclusion_applied": False,
                "throttle_brake_hysteresis_held": False,
                "startup_boost_applied": False,
                "terminal_stop_hold_active": False,
            }
        )
    return rows


def _analyze(tmp_path: Path, rows: list[dict[str, object]]) -> dict:
    path = _write_rows(tmp_path / "timeseries.csv", rows)
    return analyze_control_attribution(path)


def test_fixture_generates_report_and_summary(tmp_path: Path) -> None:
    report = analyze_control_attribution(FIXTURE)
    assert report["schema_version"] == "control_attribution.v1"
    assert report["verdict"]["status"] == "pass"
    assert report["attribution"]["dominant_breakpoint"] == "none"
    assert report["control_chain_status"] == "apollo_control_attributed"
    assert report["attribution"]["control_chain_status"] == "apollo_control_attributed"
    assert report["raw_control_available"] is True
    assert report["mapped_control_available"] is True
    assert report["applied_control_available"] is True
    assert report["vehicle_response_available"] is True
    assert "source_control_semantics does not prove Apollo algorithm limitation" in report["interpretation_caveat"]

    outputs = write_control_attribution_report(report, tmp_path / "out")
    assert Path(outputs["control_attribution_report"]).is_file()
    summary = Path(outputs["control_attribution_summary"]).read_text(encoding="utf-8")
    assert "Control Attribution Report" in summary
    assert "dominant_breakpoint: `none`" in summary


def test_source_control_semantics_strange_is_attributed_to_source(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["apollo_steer_raw"] = 1.0
        row["bridge_steer_mapped"] = 0.25
        row["carla_steer_applied"] = 0.25
        row["ego_yaw_rate"] = 0.04
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "source_control_semantics"
    assert report["verdict"]["status"] == "fail"


def test_high_amplitude_raw_steer_sign_switching_is_source_control_semantics(
    tmp_path: Path,
) -> None:
    rows = _base_rows()
    raw_values = [0.52, -0.50, 0.56, -0.48, 0.51, -0.46]
    rows = (rows * 2)[: len(raw_values)]
    for row, raw in zip(rows, raw_values):
        row["apollo_steer_raw"] = raw
        row["steer_scale"] = 1.0
        row["steering_sign"] = -1.0
        row["bridge_steer_mapped"] = -raw
        row["carla_steer_applied"] = -raw
        row["ego_yaw_rate"] = -raw * 0.2

    report = _analyze(tmp_path, rows)

    source = report["attribution"]["source_control_semantics"]
    assert report["attribution"]["dominant_breakpoint"] == "source_control_semantics"
    assert report["verdict"]["failure_reason"] == "source_control_semantics"
    assert source["reason"] == "source_steer_high_amplitude_sign_switching"
    assert source["apollo_steer_raw_sign_switch_count"] == 5
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["mapped_to_applied_steer_consistency"]["status"] == "pass"


def test_raw_ok_mapped_wrong_is_bridge_mapping(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["bridge_steer_mapped"] = 0.50
        row["carla_steer_applied"] = 0.50
        row["ego_yaw_rate"] = 0.04
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "bridge_mapping"
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["status"] == "fail"


def test_raw_mapped_mismatch_with_guard_is_guard_intervention(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["apollo_steer_raw"] = 1.0
        row["bridge_steer_mapped"] = 0.08
        row["carla_steer_applied"] = 0.08
        row["ego_yaw_rate"] = 0.04
        row["steer_scale"] = 1.0
    path = _write_rows(tmp_path / "timeseries.csv", rows)
    stats = tmp_path / "cyber_bridge_stats.json"
    stats.write_text(
        json.dumps(
            {
                "control_tx_count": 4,
                "lateral_guard_apply_count": 2,
                "trajectory_contract_lateral_guard_apply_count": 1,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_control_attribution(path, cyber_bridge_stats_json=stats)

    assert report["attribution"]["dominant_breakpoint"] == "bridge_guard_intervention"
    assert report["verdict"]["status"] == "fail"
    assert report["verdict"]["failure_reason"] == "bridge_guard_intervention"
    guard = report["attribution"]["steer_guard_intervention"]
    assert guard["status"] == "active"
    assert guard["source"] == "cyber_bridge_stats"
    assert guard["stats_guard_count"] == 3
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["guard_intervention_likely"] is True


def test_mapped_ok_applied_wrong_is_carla_apply(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["carla_steer_applied"] = 0.50
        row["ego_yaw_rate"] = 0.04
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "carla_apply"
    assert report["attribution"]["mapped_to_applied_steer_consistency"]["status"] == "fail"


def test_longitudinal_raw_mapped_mismatch_is_bridge_mapping(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["throttle_raw"] = 0.2
        row["throttle_mapped"] = 0.6
        row["throttle_applied"] = 0.6
        row["ego_yaw_rate"] = 0.04
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "bridge_mapping"
    assert report["attribution"]["throttle_raw_mapped_applied_consistency"]["status"] == "fail"
    assert report["verdict"]["status"] == "fail"


def test_longitudinal_raw_mapped_mismatch_with_policy_is_longitudinal_policy(tmp_path: Path) -> None:
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["throttle_raw"] = 0.2
        row["throttle_mapped"] = 0.6
        row["throttle_applied"] = 0.6
        row["ego_yaw_rate"] = 0.04
        row["throttle_brake_mutual_exclusion_applied"] = index < 2
        row["throttle_brake_hysteresis_held"] = index < 2
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "bridge_longitudinal_policy"
    assert report["attribution"]["control_policy_intervention"]["status"] == "active"
    assert report["attribution"]["control_policy_intervention"]["source"] == "control_trace"
    assert report["attribution"]["control_policy_intervention"]["row_policy_count"] == 4
    assert report["verdict"]["failure_reason"] == "bridge_longitudinal_policy"


def test_async_control_apply_trace_uses_auxiliary_decode_for_longitudinal_mapping(tmp_path: Path) -> None:
    primary = _write_jsonl(
        tmp_path / "control_apply_trace.jsonl",
        [
            {
                "apollo_raw": {"throttle": 0.2, "brake": 0.0, "steer": 0.1},
                "bridge_mapped": {
                    "mapped_throttle_cmd": 0.0,
                    "mapped_brake_cmd": 0.145,
                    "mapped_carla_steer_cmd": 0.1,
                    "throttle_brake_mutual_exclusion_applied": True,
                    "throttle_brake_hysteresis_held": True,
                },
                "carla_steer_applied": 0.1,
                "ego_yaw_rate": 0.02,
                "throttle_applied": 0.0,
                "brake_applied": 0.145,
                "steer_scale": 1.0,
                "steering_sign": 1.0,
                "throttle_scale": 1.5,
                "brake_scale": 1.0,
                "brake_deadzone": 0.05,
            }
        ],
    )
    auxiliary = _write_jsonl(
        tmp_path / "control_decode_debug.jsonl",
        [
            {
                "parsed_control": {"throttle": 0.2, "brake": 0.0, "steer": 0.1},
                "output_to_carla": {
                    "throttle_before_mutual_exclusion": 0.3,
                    "brake_before_mutual_exclusion": 0.0,
                    "mapped_throttle_cmd": 0.3,
                    "mapped_brake_cmd": 0.0,
                    "mapped_carla_steer_cmd": 0.1,
                    "throttle_brake_mutual_exclusion_applied": False,
                    "throttle_brake_hysteresis_held": False,
                },
                "steer_scale": 1.0,
                "steering_sign": 1.0,
                "throttle_scale": 1.5,
                "brake_scale": 1.0,
                "brake_deadzone": 0.05,
            }
        ],
    )
    bridge_health = tmp_path / "bridge_health_summary.json"
    bridge_health.write_text(json.dumps({"control_apply_path": "ros2_control_bridge"}), encoding="utf-8")

    report = analyze_control_attribution(
        primary,
        bridge_health_json=bridge_health,
        auxiliary_control_decode_json=auxiliary,
    )

    assert report["attribution"]["dominant_breakpoint"] == "none"
    assert report["verdict"]["status"] == "warn"
    assert "throttle_raw_to_mapped_consistency_from_auxiliary_control_decode" in report["warnings"]
    policy = report["attribution"]["control_policy_intervention"]
    assert policy["status"] == "none"
    assert policy["primary_async_observation"]["status"] == "active"
    throttle = report["attribution"]["throttle_raw_mapped_applied_consistency"]
    assert throttle["source"] == "auxiliary_control_decode"
    assert throttle["primary_trace_status"] == "fail"
    assert throttle["raw_to_mapped_status"] == "pass"


def test_applied_ok_yaw_rate_no_response_is_vehicle_response(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["ego_yaw_rate"] = 0.0
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "vehicle_response"
    assert report["attribution"]["applied_steer_to_yaw_rate_response"]["status"] == "fail"


def test_missing_raw_is_insufficient_data(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["apollo_steer_raw"] = ""
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "insufficient_data"
    assert report["control_chain_status"] == "control_missing"
    assert report["verdict"]["status"] == "insufficient_data"
    assert "apollo_steer_raw" in report["missing_fields"]


def test_missing_yaw_rate_keeps_mapping_analysis_available(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["ego_yaw_rate"] = ""
    report = _analyze(tmp_path, rows)
    assert report["vehicle_response_available"] is False
    assert report["attribution"]["dominant_breakpoint"] == "none"
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["mapped_to_applied_steer_consistency"]["status"] == "pass"


def test_apollo_control_source_is_normalized_for_claim_gate(tmp_path: Path) -> None:
    rows = _base_rows()
    trace = _write_rows(tmp_path / "timeseries.csv", rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")

    report = analyze_control_attribution(trace, manifest_json=manifest)

    assert report["control_source"] == "/apollo/control"
    assert report["applied_control_source"] == "apollo_control"
    assert report["control_chain_status"] == "apollo_control_attributed"


def test_non_apollo_applied_control_source_is_not_apollo_attributed(tmp_path: Path) -> None:
    rows = _base_rows()
    trace = _write_rows(tmp_path / "timeseries.csv", rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "carla_route_follower"}), encoding="utf-8")

    report = analyze_control_attribution(trace, manifest_json=manifest)

    assert report["applied_control_source"] == "carla_route_follower"
    assert report["control_chain_status"] == "applied_not_apollo"
    assert report["verdict"]["status"] == "fail"


def test_generic_external_stack_is_promoted_only_with_apollo_control_evidence(tmp_path: Path) -> None:
    rows = _base_rows()
    trace = _write_rows(tmp_path / "timeseries.csv", rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "external_stack"}), encoding="utf-8")
    handoff = tmp_path / "apollo_control_handoff_report.json"
    handoff.write_text(
        json.dumps(
            {
                "status": "warn",
                "control_channel": {"name": "/apollo/control", "status": "pass"},
                "bridge_receive": {"control_rx_count": 12},
                "mapping_and_apply": {"apply_control_count": 11},
            }
        ),
        encoding="utf-8",
    )

    report = analyze_control_attribution(trace, manifest_json=manifest, control_handoff_json=handoff)

    assert report["control_source"] == "/apollo/control"
    assert report["applied_control_source"] == "apollo_control"
    assert report["control_source_evidence"]["status"] == "pass"
    assert report["control_chain_status"] == "apollo_control_attributed"


def test_runtime_control_evidence_can_promote_when_handoff_report_is_incomplete(tmp_path: Path) -> None:
    rows = _base_rows()
    trace = _write_rows(tmp_path / "timeseries.csv", rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "external_stack"}), encoding="utf-8")
    handoff = tmp_path / "apollo_control_handoff_report.json"
    handoff.write_text(
        json.dumps(
            {
                "status": "insufficient_data",
                "control_channel": {"name": "/apollo/control"},
            }
        ),
        encoding="utf-8",
    )
    bridge = tmp_path / "bridge_health_summary.json"
    bridge.write_text(json.dumps({"control_apply_path": "ros2_control_bridge"}), encoding="utf-8")
    stats = tmp_path / "cyber_bridge_stats.json"
    stats.write_text(json.dumps({"control_rx_count": 12, "control_tx_count": 11}), encoding="utf-8")

    report = analyze_control_attribution(
        trace,
        manifest_json=manifest,
        control_handoff_json=handoff,
        bridge_health_json=bridge,
        cyber_bridge_stats_json=stats,
    )

    assert report["control_source"] == "/apollo/control"
    assert report["applied_control_source"] == "apollo_control"
    assert report["control_chain_status"] == "apollo_control_attributed"
    assert report["control_source_evidence"]["status"] == "warn"
    assert "control_handoff_status_not_claim_grade_but_runtime_control_seen" in report["control_source_evidence"]["warnings"]


def test_none_control_source_is_promoted_with_apollo_control_evidence(tmp_path: Path) -> None:
    rows = _base_rows()
    trace = _write_rows(tmp_path / "timeseries.csv", rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "none"}), encoding="utf-8")
    handoff = tmp_path / "apollo_control_handoff_report.json"
    handoff.write_text(
        json.dumps(
            {
                "status": "warn",
                "control_channel": {"name": "/apollo/control", "status": "pass"},
                "bridge_receive": {"control_rx_count": 12},
                "mapping_and_apply": {"apply_control_count": 11},
            }
        ),
        encoding="utf-8",
    )

    report = analyze_control_attribution(trace, manifest_json=manifest, control_handoff_json=handoff)

    assert report["control_source"] == "/apollo/control"
    assert report["applied_control_source"] == "apollo_control"
    assert report["control_chain_status"] == "apollo_control_attributed"


def test_explicit_non_apollo_control_source_is_not_promoted_by_apollo_evidence(tmp_path: Path) -> None:
    rows = _base_rows()
    trace = _write_rows(tmp_path / "timeseries.csv", rows)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "carla_route_follower"}), encoding="utf-8")
    handoff = tmp_path / "apollo_control_handoff_report.json"
    handoff.write_text(
        json.dumps(
            {
                "status": "warn",
                "control_channel": {"name": "/apollo/control", "status": "pass"},
                "bridge_receive": {"control_rx_count": 12},
                "mapping_and_apply": {"apply_control_count": 11},
            }
        ),
        encoding="utf-8",
    )

    report = analyze_control_attribution(trace, manifest_json=manifest, control_handoff_json=handoff)

    assert report["control_source"] == "carla_route_follower"
    assert report["applied_control_source"] == "carla_route_follower"
    assert report["control_source_evidence"]["status"] == "pass"
    assert report["control_chain_status"] == "applied_not_apollo"


def test_debug_timeseries_alias_fields_are_attributed(tmp_path: Path) -> None:
    path = tmp_path / "debug_timeseries.csv"
    fieldnames = [
        "run_id",
        "route_id",
        "backend",
        "apollo_desired_steer",
        "mapped_carla_steer_cmd",
        "measured_steer",
        "ego_yaw_rate_rad_s",
        "apollo_desired_throttle",
        "mapped_throttle_cmd",
        "measured_throttle",
        "apollo_desired_brake",
        "mapped_brake_cmd",
        "measured_brake",
        "control_latency_ms",
        "steer_sign",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index, raw in enumerate([0.20, 0.24, 0.16, 0.12]):
            mapped = raw * 0.25
            writer.writerow(
                {
                    "run_id": "debug_alias",
                    "route_id": "lane_keep_097",
                    "backend": "apollo_cyberrt",
                    "apollo_desired_steer": raw,
                    "mapped_carla_steer_cmd": mapped,
                    "measured_steer": mapped,
                    "ego_yaw_rate_rad_s": 0.01 + index * 0.001,
                    "apollo_desired_throttle": 0.2,
                    "mapped_throttle_cmd": 0.2,
                    "measured_throttle": 0.2,
                    "apollo_desired_brake": 0.0,
                    "mapped_brake_cmd": 0.0,
                    "measured_brake": 0.0,
                    "control_latency_ms": 5,
                    "steer_sign": 1,
                }
            )
    config = tmp_path / "config.resolved.yaml"
    config.write_text(
        "backend:\n"
        "  params:\n"
        "    legacy_algo:\n"
        "      apollo:\n"
        "        control_mapping:\n"
        "          steer_scale: 0.25\n"
        "          actuator_mapping_mode: legacy\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")

    report = analyze_control_attribution(path, manifest_json=manifest, config_yaml=config)

    assert report["verdict"]["status"] == "pass"
    assert report["resolved_fields"]["apollo_steer_raw"] == "apollo_desired_steer"
    assert report["resolved_fields"]["bridge_steer_mapped"] == "mapped_carla_steer_cmd"
    assert report["resolved_fields"]["carla_steer_applied"] == "measured_steer"
    assert report["steer_scale"] == 0.25
    assert report["actuator_mapping_mode"] == "legacy"
    assert report["control_chain_status"] == "apollo_control_attributed"


def test_run_dir_prefers_debug_timeseries_and_resolved_config(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    _write_rows(run_dir / "timeseries.csv", [{"run_id": "stale_plain_timeseries"}])
    debug_path = run_dir / "artifacts" / "debug_timeseries.csv"
    fieldnames = [
        "run_id",
        "route_id",
        "apollo_desired_steer",
        "mapped_carla_steer_cmd",
        "measured_steer",
        "ego_yaw_rate_rad_s",
        "apollo_desired_throttle",
        "mapped_throttle_cmd",
        "measured_throttle",
        "apollo_desired_brake",
        "mapped_brake_cmd",
        "measured_brake",
        "control_latency_ms",
    ]
    with debug_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "debug",
                "route_id": "lane_keep_097",
                "apollo_desired_steer": 0.2,
                "mapped_carla_steer_cmd": 0.05,
                "measured_steer": 0.05,
                "ego_yaw_rate_rad_s": 0.02,
                "apollo_desired_throttle": 0.1,
                "mapped_throttle_cmd": 0.1,
                "measured_throttle": 0.1,
                "apollo_desired_brake": 0.0,
                "mapped_brake_cmd": 0.0,
                "measured_brake": 0.0,
                "control_latency_ms": 5,
            }
        )
    (run_dir / "manifest.json").write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")
    (run_dir / "config.resolved.yaml").write_text(
        "backend:\n"
        "  params:\n"
        "    legacy_algo:\n"
        "      apollo:\n"
        "        control_mapping:\n"
        "          steer_scale: 0.25\n"
        "          steer_sign: 1.0\n"
        "          actuator_mapping_mode: legacy\n",
        encoding="utf-8",
    )

    report = analyze_control_attribution_run_dir(run_dir)

    assert report["source"]["control_input_path"].endswith("artifacts/debug_timeseries.csv")
    assert report["source"]["config_path"].endswith("config.resolved.yaml")
    assert report["resolved_fields"]["bridge_steer_mapped"] == "mapped_carla_steer_cmd"
    assert report["control_chain_status"] == "apollo_control_attributed"


def test_cli_writes_report(tmp_path: Path) -> None:
    out = tmp_path / "cli"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_control_attribution.py",
            "--timeseries",
            str(FIXTURE),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert (out / "control_attribution_report.json").is_file()
    assert (out / "control_attribution_summary.md").is_file()


def test_cli_run_dir_writes_report_from_debug_timeseries(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    debug_path = run_dir / "artifacts" / "debug_timeseries.csv"
    fieldnames = [
        "apollo_desired_steer",
        "mapped_carla_steer_cmd",
        "measured_steer",
        "ego_yaw_rate_rad_s",
        "apollo_desired_throttle",
        "mapped_throttle_cmd",
        "measured_throttle",
        "apollo_desired_brake",
        "mapped_brake_cmd",
        "measured_brake",
    ]
    with debug_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "apollo_desired_steer": 0.2,
                "mapped_carla_steer_cmd": 0.05,
                "measured_steer": 0.05,
                "ego_yaw_rate_rad_s": 0.02,
                "apollo_desired_throttle": 0.1,
                "mapped_throttle_cmd": 0.1,
                "measured_throttle": 0.1,
                "apollo_desired_brake": 0.0,
                "mapped_brake_cmd": 0.0,
                "measured_brake": 0.0,
            }
        )
    (run_dir / "manifest.json").write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")
    (run_dir / "config.resolved.yaml").write_text(
        "backend:\n"
        "  params:\n"
        "    legacy_algo:\n"
        "      apollo:\n"
        "        control_mapping:\n"
        "          steer_scale: 0.25\n",
        encoding="utf-8",
    )
    out = tmp_path / "cli_run_dir"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_control_attribution.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] in {"pass", "warn"}
    report = json.loads((out / "control_attribution_report.json").read_text(encoding="utf-8"))
    assert report["resolved_fields"]["apollo_steer_raw"] == "apollo_desired_steer"


def test_run_dir_prefers_control_apply_trace_over_sparse_timeseries(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    _write_rows(
        run_dir / "timeseries.csv",
        [
            {
                **_base_rows()[0],
                "apollo_steer_raw": "",
                "bridge_steer_mapped": "",
                "carla_steer_applied": "",
                "ego_yaw_rate": "",
            }
        ],
    )
    (run_dir / "artifacts" / "control_apply_trace.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "schema_version": "control_apply_trace.v1",
                    "timestamp": 10.0 + index,
                    "control_latency_ms": 5.0,
                    "apollo_raw": {"throttle": 0.2, "brake": 0.0, "steer": 0.2},
                    "bridge_mapped": {
                        "throttle": 0.2,
                        "brake": 0.0,
                        "steer": 0.05,
                        "mapped_throttle_cmd": 0.2,
                        "mapped_brake_cmd": 0.0,
                        "mapped_carla_steer_cmd": 0.05,
                    },
                    "carla_applied": {"throttle": 0.2, "brake": 0.0, "steer": 0.05},
                    "vehicle_response": {"yaw_rate_rad_s": 0.02},
                }
            )
            for index in range(3)
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")
    (run_dir / "config.resolved.yaml").write_text(
        "backend:\n"
        "  params:\n"
        "    legacy_algo:\n"
        "      apollo:\n"
        "        control_mapping:\n"
        "          steer_scale: 0.25\n"
        "          steer_sign: 1.0\n",
        encoding="utf-8",
    )

    report = analyze_control_attribution_run_dir(run_dir)

    assert report["source"]["control_input_path"].endswith("artifacts/control_apply_trace.jsonl")
    assert report["resolved_fields"]["apollo_steer_raw"] == "apollo_raw.steer"
    assert report["resolved_fields"]["bridge_steer_mapped"] == "bridge_mapped.mapped_carla_steer_cmd"
    assert report["control_chain_status"] == "apollo_control_attributed"
    assert report["verdict"]["status"] == "pass"


def test_ros2_control_apply_trace_marks_same_row_applied_longitudinal_diagnostic(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    (run_dir / "artifacts" / "control_apply_trace.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "schema_version": "control_apply_trace.v1",
                    "timestamp": 10.0 + index,
                    "control_latency_ms": 5.0,
                    "apollo_raw": {"throttle": 0.4, "brake": 0.0, "steer": 0.2},
                    "bridge_mapped": {
                        "throttle": 0.6,
                        "brake": 0.0,
                        "steer": 0.05,
                        "mapped_throttle_cmd": 0.6,
                        "mapped_brake_cmd": 0.0,
                        "mapped_carla_steer_cmd": 0.05,
                    },
                    "carla_applied": {"throttle": 0.0, "brake": 0.0, "steer": 0.05},
                    "vehicle_response": {"yaw_rate_rad_s": 0.02},
                    "throttle_scale": 1.5,
                    "steer_scale": 0.25,
                    "steering_sign": 1.0,
                }
            )
            for index in range(3)
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")
    (run_dir / "artifacts" / "bridge_health_summary.json").write_text(
        json.dumps({"control_apply_path": "ros2_control_bridge"}), encoding="utf-8"
    )

    report = analyze_control_attribution_run_dir(run_dir)

    throttle = report["attribution"]["throttle_raw_mapped_applied_consistency"]
    assert throttle["status"] == "pass"
    assert throttle["mapped_to_applied_status"] == "diagnostic_only_async_ros2_control_bridge"
    assert report["attribution"]["dominant_breakpoint"] == "none"


def test_control_apply_trace_uses_final_carla_command_not_stale_flat_mapped_field(tmp_path: Path) -> None:
    trace = tmp_path / "control_apply_trace.jsonl"
    rows = []
    for index in range(3):
        rows.append(
            {
                "schema_version": "control_apply_trace.v1",
                "timestamp": 20.0 + index,
                "apollo_steer_raw": 0.2,
                # Legacy traces can contain a stale flat value if live bridge
                # stats updated while the row snapshot was being serialized.
                "bridge_steer_mapped": -1.0,
                "carla_steer_applied": 0.31,
                "ego_yaw_rate": 0.02,
                "steer_scale": 1.0,
                "steering_sign": -1.0,
                "bridge_mapped": {
                    "steer_pre_policy": -0.2,
                    "steer": -1.0,
                    "mapped_carla_steer_cmd": 0.31,
                },
                "carla_applied": {"steer": 0.31},
                "apollo_raw": {"steer": 0.2, "throttle": 0.1, "brake": 0.0},
                "throttle_raw": 0.1,
                "throttle_mapped": 0.1,
                "throttle_applied": 0.1,
                "brake_raw": 0.0,
                "brake_mapped": 0.0,
                "brake_applied": 0.0,
            }
        )
    trace.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    report = analyze_control_attribution(trace)

    assert report["resolved_fields"]["bridge_steer_pre_policy"] == "bridge_mapped.steer_pre_policy"
    assert report["resolved_fields"]["bridge_steer_mapped"] == "bridge_mapped.mapped_carla_steer_cmd"
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["mapped_to_applied_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["dominant_breakpoint"] == "none"


def test_bridge_control_decode_honors_recorded_steering_sign(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    rows = []
    for index in range(4):
        rows.append(
            {
                "ts_sec": 30.0 + index,
                "raw_steer": -0.2,
                "commanded_steer_pre_lateral_guards": 0.2,
                "commanded_steer": 0.2,
                "mapped_carla_steer_cmd": 0.2,
                "carla_steer_applied": 0.2,
                "ego_yaw_rate": 0.02,
                "raw_throttle": 0.1,
                "mapped_throttle_cmd": 0.1,
                "throttle_applied": 0.1,
                "raw_brake": 0.0,
                "mapped_brake_cmd": 0.0,
                "brake_applied": 0.0,
                "steer_scale": 1.0,
                "steering_sign": -1.0,
            }
        )
    (artifacts / "bridge_control_decode.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")

    report = analyze_control_attribution_run_dir(run_dir)

    assert report["source"]["control_input_path"].endswith("artifacts/bridge_control_decode.jsonl")
    assert report["steer_scale"] == 1.0
    assert report["steering_sign"] == -1.0
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["mapped_to_applied_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["dominant_breakpoint"] == "none"


def test_run_dir_uses_bridge_decode_for_raw_to_mapped_when_apply_trace_is_misaligned(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "control_apply_trace.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "schema_version": "control_apply_trace.v1",
                    "timestamp": 40.0 + index,
                    "apollo_steer_raw": -0.05,
                    "bridge_steer_pre_policy": 0.45,
                    "bridge_steer_mapped": 0.45,
                    "carla_steer_applied": 0.45,
                    "ego_yaw_rate": 0.02,
                    "steer_scale": 1.0,
                    "steering_sign": -1.0,
                    "throttle_raw": 0.1,
                    "throttle_mapped": 0.1,
                    "throttle_applied": 0.1,
                    "brake_raw": 0.0,
                    "brake_mapped": 0.0,
                    "brake_applied": 0.0,
                }
            )
            for index in range(4)
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "bridge_control_decode.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "ts_sec": 40.0 + index,
                    "raw_steer": -0.45,
                    "commanded_steer_pre_lateral_guards": 0.45,
                    "mapped_carla_steer_cmd": 0.45,
                    "steer_scale": 1.0,
                    "steering_sign": -1.0,
                }
            )
            for index in range(4)
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(json.dumps({"control_source": "/apollo/control"}), encoding="utf-8")

    report = analyze_control_attribution_run_dir(run_dir)

    raw_to_mapped = report["attribution"]["raw_to_mapped_steer_consistency"]
    assert report["source"]["control_input_path"].endswith("artifacts/control_apply_trace.jsonl")
    assert report["source"]["auxiliary_control_decode_path"].endswith("artifacts/bridge_control_decode.jsonl")
    assert raw_to_mapped["status"] == "pass"
    assert raw_to_mapped["source"] == "auxiliary_bridge_control_decode"
    assert raw_to_mapped["primary_trace_status"] == "fail"
    assert report["attribution"]["mapped_to_applied_steer_consistency"]["status"] == "pass"
    assert report["attribution"]["dominant_breakpoint"] == "none"
