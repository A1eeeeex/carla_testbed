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
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
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


def test_raw_ok_mapped_wrong_is_bridge_mapping(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["bridge_steer_mapped"] = 0.50
        row["carla_steer_applied"] = 0.50
        row["ego_yaw_rate"] = 0.04
    report = _analyze(tmp_path, rows)
    assert report["attribution"]["dominant_breakpoint"] == "bridge_mapping"
    assert report["attribution"]["raw_to_mapped_steer_consistency"]["status"] == "fail"


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
