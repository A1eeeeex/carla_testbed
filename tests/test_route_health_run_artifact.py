from __future__ import annotations

import json
import shutil
from pathlib import Path

from carla_testbed.analysis.route_health_report import analyze_route_health_run_dir, route_health_inspect_summary
from carla_testbed.cli import main as cli_main


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _copy_fixture_run(tmp_path: Path) -> Path:
    src = FIXTURES / "runs" / "minimal_route_run"
    dst = tmp_path / "minimal_route_run"
    shutil.copytree(src, dst)
    return dst


def test_run_dir_generates_route_health_analysis_files(tmp_path: Path) -> None:
    run_dir = _copy_fixture_run(tmp_path)

    result = analyze_route_health_run_dir(run_dir)

    out_dir = run_dir / "analysis" / "route_health"
    for name in ["route_health.json", "route_health.csv", "curve_segments.csv", "route_health_summary.md"]:
        assert (out_dir / name).is_file()
    payload = json.loads((out_dir / "route_health.json").read_text(encoding="utf-8"))
    assert payload["route_id"] == "minimal_route_fixture"
    assert payload["source"]["run_dir"] == str(run_dir)
    assert payload["source"]["manifest_path"] == str(run_dir / "manifest.json")
    assert payload["source"]["summary_path"] == str(run_dir / "summary.json")
    assert payload["source"]["timeseries_path"] == str(run_dir / "timeseries.csv")
    assert payload["source"]["route_path"] == str(run_dir / "route.json")
    assert payload["route_source"] == "configured_route_file"
    assert payload["evidence_level"] == "claim_grade"
    assert payload["hard_gate_eligible"] is True
    assert payload["route_geometry"]["curve_segments_count"] == 1
    assert payload["run_metrics"]["lateral_error_max_m"] == 0.3
    assert result["outputs"]["route_health_json"] == str(out_dir / "route_health.json")


def test_run_dir_missing_route_graceful_degrade(tmp_path: Path) -> None:
    run_dir = _copy_fixture_run(tmp_path)
    (run_dir / "route.json").unlink()

    analyze_route_health_run_dir(run_dir)

    out_dir = run_dir / "analysis" / "route_health"
    payload = json.loads((out_dir / "route_health.json").read_text(encoding="utf-8"))
    assert payload["verdict"]["status"] == "insufficient_data"
    assert payload["route_source"] == "missing"
    assert payload["evidence_level"] == "insufficient"
    assert payload["hard_gate_eligible"] is False
    assert "route" in payload["missing_inputs"]
    assert payload["source"]["route_path"] is None
    assert (out_dir / "route_health.csv").read_text(encoding="utf-8").startswith("route_id,index,s")
    assert (out_dir / "curve_segments.csv").read_text(encoding="utf-8").startswith("route_id,curve_segment_id")


def test_run_dir_reconstructs_route_from_p0_timeseries(tmp_path: Path) -> None:
    run_dir = tmp_path / "p0_route_run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "summary.json").write_text("{}", encoding="utf-8")
    (run_dir / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame_id,sim_time,route_id,nearest_route_index,route_s,route_x,route_y,route_z,route_heading,route_curvature,ego_x,ego_y,ego_heading,cross_track_error,heading_error",
                "0,0.00,town01_fixture,0,0.0,0.0,0.0,0.0,0.0,0.0,0.1,0.0,0.0,0.1,0.01",
                "1,0.05,town01_fixture,1,1.0,1.0,0.0,0.0,0.0,0.04,1.0,0.2,0.0,0.2,0.02",
                "2,0.10,town01_fixture,2,2.0,2.0,0.1,0.0,0.1,0.04,2.0,0.4,0.1,0.3,0.03",
            ]
        ),
        encoding="utf-8",
    )

    analyze_route_health_run_dir(run_dir)

    payload = json.loads((run_dir / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    assert payload["route_id"] == "town01_fixture"
    assert payload["route_source"] == "reconstructed_from_timeseries"
    assert payload["evidence_level"] == "diagnostic_only"
    assert payload["hard_gate_eligible"] is False
    assert payload["route_evidence_reason"] == "reconstructed_from_timeseries_cannot_support_hard_gate"
    assert payload["source"]["route_path"] is None
    assert "route" not in payload["missing_inputs"]
    assert payload["route_geometry"]["point_count"] == 3
    assert payload["run_metrics"]["lateral_error_max_m"] == 0.3
    assert "route reconstructed from timeseries P0 route_curve fields" in payload["warnings"]


def test_run_dir_uses_manifest_route_trace_for_carla_route_geometry(tmp_path: Path) -> None:
    run_dir = tmp_path / "route_trace_run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "route_trace_run",
                "route_id": "town01_route_trace",
                "map_name": "Town01",
                "metadata": {
                    "scenario_metadata": {
                        "route_id": "town01_route_trace",
                        "map_name": "Town01",
                        "route_trace": [
                            {"location": {"x": 0.0, "y": 0.0, "z": 0.0}, "heading": 0.0},
                            {"location": {"x": 10.0, "y": 0.0, "z": 0.0}, "heading": 0.0},
                            {"location": {"x": 20.0, "y": 1.0, "z": 0.0}, "heading": 0.1},
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"route_id": "town01_route_trace"}), encoding="utf-8")
    (run_dir / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame_id,sim_time,route_id,ego_x,ego_y,ego_heading,cross_track_error,heading_error",
                "0,0.00,town01_route_trace,0.0,0.0,0.0,0.02,0.01",
                "1,0.05,town01_route_trace,1.0,0.0,0.0,0.02,0.01",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    analyze_route_health_run_dir(run_dir)

    payload = json.loads((run_dir / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    assert payload["route_source"] == "manifest_route_trace"
    assert payload["evidence_level"] == "claim_grade_for_carla_route_geometry"
    assert payload["hard_gate_eligible"] is True
    assert payload["route_evidence_reason"] == "manifest_route_trace"
    assert payload["route_geometry"]["point_count"] == 3
    assert payload["route_geometry"]["length_m"] > 20.0
    assert payload["reference_line_verified"] is False
    assert payload["apollo_reference_line_claim_grade"] is False
    assert payload["source"]["route_trace_source_key"] == "manifest.metadata.scenario_metadata.route_trace"


def test_run_dir_inline_route_definition_can_be_claim_grade(tmp_path: Path) -> None:
    run_dir = tmp_path / "inline_claim_grade"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "inline_claim_grade",
                "route_definition": {
                    "route_id": "inline_route_001",
                    "map": "Town01",
                    "points": [
                        {"x": 0.0, "y": 0.0},
                        {"x": 10.0, "y": 0.0},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"route_id": "inline_route_001"}), encoding="utf-8")

    analyze_route_health_run_dir(run_dir)

    payload = json.loads((run_dir / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    assert payload["route_id"] == "inline_route_001"
    assert payload["route_source"] == "inline_route"
    assert payload["evidence_level"] == "claim_grade"
    assert payload["hard_gate_eligible"] is True
    assert payload["route_evidence_reason"] == "inline_route_definition_with_valid_route_identity_and_geometry"
    assert payload["source"]["inline_route_source_key"] == "route_definition"


def test_run_dir_ambiguous_inline_route_is_diagnostic_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "inline_ambiguous"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "inline_ambiguous",
                "route": {
                    "route_id": "ambiguous_inline_route",
                    "map": "Town01",
                    "points": [
                        {"x": 0.0, "y": 0.0},
                        {"x": 10.0, "y": 0.0},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"route_id": "ambiguous_inline_route"}), encoding="utf-8")

    analyze_route_health_run_dir(run_dir)

    payload = json.loads((run_dir / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    assert payload["route_id"] == "ambiguous_inline_route"
    assert payload["route_source"] == "inline_route"
    assert payload["evidence_level"] == "diagnostic_only"
    assert payload["hard_gate_eligible"] is False
    assert payload["route_evidence_reason"] == "inline_route_source_ambiguous_cannot_support_hard_gate"
    assert payload["source"]["inline_route_source_key"] == "route"


def test_run_dir_enriches_curve_semantics_from_online_summary(tmp_path: Path) -> None:
    run_dir = _copy_fixture_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "first_high_steer_seq": 4,
            "first_high_steer_at": 2.5,
            "first_matched_point_too_large_seq": 5,
            "first_matched_point_too_large_at": 3.0,
            "apollo_simple_lat_target_point_kappa_abs_p95": 0.12,
        }
    )
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    timeseries_path = run_dir / "timeseries.csv"
    lines = timeseries_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    keep = [index for index, name in enumerate(header) if name != "apollo_steer_raw"]
    rewritten = [
        ",".join(header[index] for index in keep),
        *(",".join(row.split(",")[index] for index in keep) for row in lines[1:]),
    ]
    timeseries_path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    analyze_route_health_run_dir(run_dir)

    payload = json.loads((run_dir / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    apollo = payload["apollo_semantics"]
    assert apollo["matched_point_anomaly_locations"] == [5]
    assert apollo["first_matched_point_too_large"]["seq"] == 5
    assert apollo["first_high_steer"] == {"seq": 4, "at": 2.5, "value": None}
    assert apollo["target_point_kappa_abs_p95"] == 0.12
    assert {"matched_point", "target_point", "apollo_raw_steer"}.isdisjoint(payload["missing_fields"])
    assert "summary.first_matched_point_too_large_seq" in apollo["summary_sources"]
    assert "summary.first_high_steer_seq" in apollo["summary_sources"]


def test_inspect_summary_reads_route_health_json(tmp_path: Path, capsys) -> None:
    run_dir = _copy_fixture_run(tmp_path)
    analyze_route_health_run_dir(run_dir)

    summary = route_health_inspect_summary(run_dir)
    assert summary is not None
    assert summary["status"] == "diagnostic_ready"
    assert summary["route_id"] == "minimal_route_fixture"
    assert summary["lateral_error_max_m"] == 0.3
    assert summary["curve_segments_count"] == 1

    rc = cli_main(["inspect-run", str(run_dir)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "route_health status=diagnostic_ready" in captured.out
    assert "route_id=minimal_route_fixture" in captured.out
