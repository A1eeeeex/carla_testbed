from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_hdmap_projection import (
    HDMAP_PROJECTION_REPORT_SCHEMA_VERSION,
    analyze_apollo_hdmap_projection_file,
    write_apollo_hdmap_projection_report,
)
from carla_testbed.analysis.apollo_hdmap_projection_export import (
    LocalizationProjectionSample,
    MapXyslConfig,
    build_map_xysl_command,
    export_apollo_hdmap_projection_jsonl,
    load_localization_projection_samples,
    parse_map_xysl_xy_to_sl_output,
)


SCRIPT = Path("tools/analyze_apollo_hdmap_projection.py")


def test_missing_projection_is_insufficient_data(tmp_path: Path) -> None:
    report = analyze_apollo_hdmap_projection_file(tmp_path / "missing.jsonl")

    assert report["schema_version"] == HDMAP_PROJECTION_REPORT_SCHEMA_VERSION
    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert report["artifact_status"] == "artifact_missing"
    assert report["artifact_file_exists"] is False
    assert "apollo_hdmap_projection_missing" in report["warnings"]
    assert "apollo_hdmap_projection" in report["missing_fields"]


def test_empty_projection_artifact_is_distinct_from_missing(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    path.write_text("", encoding="utf-8")

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert report["artifact_status"] == "artifact_empty"
    assert report["artifact_file_exists"] is True
    assert report["projection"]["file_present"] is True
    assert "apollo_hdmap_projection_empty" in report["warnings"]
    assert "apollo_hdmap_projection_rows" in report["missing_fields"]


def test_unofficial_projection_rows_are_not_claim_grade(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    _write_jsonl(
        path,
        [
            {
                "timestamp": 0.0,
                "heading_error_rad": 0.01,
                "lateral_error_m": 0.05,
                "nearest_lane_id": "lane_097",
                "source": "bridge_nearest_lane_debug",
                "status": "ok",
            }
        ],
    )

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert report["artifact_file_exists"] is True
    assert "apollo_hdmap_projection_source_not_official" in report["warnings"]
    assert "source=apollo_hdmap_api" in report["missing_fields"]


def test_official_projection_passes_claim_grade(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    _write_jsonl(path, _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05))

    report = analyze_apollo_hdmap_projection_file(path)
    outputs = write_apollo_hdmap_projection_report(report, tmp_path / "out")

    assert report["status"] == "pass"
    assert report["claim_grade"] is True
    assert report["projection"]["official_source_available"] is True
    assert report["projection"]["ok_ratio"] == 1.0
    assert Path(outputs["apollo_hdmap_projection_report"]).is_file()
    assert Path(outputs["apollo_hdmap_projection_summary"]).is_file()


def test_high_heading_error_fails_with_suspected_layers(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    _write_jsonl(path, _projection_rows(heading_error_rad=0.35, lateral_error_m=0.05))

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert report["claim_grade"] is False
    assert "apollo_hdmap_projection_heading_error_high" in report["blocking_reasons"]
    assert "lane_direction" in report["suspected_failure_layers"]


def test_claim_grade_projection_uses_strict_heading_and_lateral_thresholds(tmp_path: Path) -> None:
    heading_path = tmp_path / "heading.jsonl"
    lateral_path = tmp_path / "lateral.jsonl"
    _write_jsonl(heading_path, _projection_rows(heading_error_rad=0.06, lateral_error_m=0.05))
    _write_jsonl(lateral_path, _projection_rows(heading_error_rad=0.01, lateral_error_m=0.60))

    heading_report = analyze_apollo_hdmap_projection_file(heading_path)
    lateral_report = analyze_apollo_hdmap_projection_file(lateral_path)

    assert heading_report["status"] == "fail"
    assert "apollo_hdmap_projection_heading_error_high" in heading_report["blocking_reasons"]
    assert lateral_report["status"] == "fail"
    assert "apollo_hdmap_projection_lateral_error_high" in lateral_report["blocking_reasons"]


def test_claim_grade_projection_requires_minimum_sample_count(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    _write_jsonl(path, _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)[:1])

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert report["claim_grade"] is False
    assert "apollo_hdmap_projection_sample_count_low" in report["blocking_reasons"]


def test_claim_grade_projection_requires_route_s_coverage(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    for index, row in enumerate(rows):
        row["projection_s"] = 10.0 + index * 0.1

    _write_jsonl(path, rows)
    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert report["claim_grade"] is False
    assert "apollo_hdmap_projection_route_s_coverage_low" in report["blocking_reasons"]


def test_claim_grade_projection_fails_inconsistent_map_identity(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    rows[-1]["map_name"] = "Town02"

    _write_jsonl(path, rows)
    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert "apollo_hdmap_projection_map_identity_inconsistent" in report["blocking_reasons"]


def test_cli_writes_projection_report(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    out = tmp_path / "out"
    _write_jsonl(path, _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05))

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--projection",
            str(path),
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "pass"
    assert payload["claim_grade"] is True
    assert (out / "apollo_hdmap_projection_report.json").is_file()
    assert (out / "apollo_hdmap_projection_summary.md").is_file()


def test_parse_map_xysl_output() -> None:
    parsed = parse_map_xysl_xy_to_sl_output(
        "lane_id[15_1_1], s[66.746709], l[0.508728], heading[1.570666]\n"
    )

    assert parsed == {
        "nearest_lane_id": "15_1_1",
        "projection_s": 66.746709,
        "projection_l": 0.508728,
        "lane_heading_at_s": 1.570666,
    }


def test_build_map_xysl_command_uses_docker_by_default() -> None:
    command = build_map_xysl_command(
        LocalizationProjectionSample(
            timestamp=1.0,
            x=1.5,
            y=-250.8,
            heading=1.57,
            source_artifact="sample.jsonl",
            source_index=0,
        ),
        MapXyslConfig(docker_container="apollo_container"),
    )

    assert command[:4] == ["docker", "exec", "apollo_container", "bash"]
    assert "timeout --kill-after=2s" in command[-1]
    assert "/opt/apollo/neo/bin/map_xysl" in command[-1]
    assert "--xy_to_sl" in command[-1]
    assert "--map_dir=/apollo/modules/map/data/carla_town01" in command[-1]


def test_load_projection_samples_from_reference_line_contract(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    rows = [
        {
            "timestamp": 1.0,
            "localization": {
                "x": 1.5,
                "y": -250.8,
                "heading": 1.57,
            },
        },
        {
            "timestamp": 1.05,
            "localization": {
                "x": 1.6,
                "y": -250.7,
                "heading": 1.58,
            },
        },
    ]
    _write_jsonl(run_dir / "artifacts" / "apollo_reference_line_contract.jsonl", rows)

    samples = load_localization_projection_samples(run_dir=run_dir)

    assert len(samples) == 2
    assert samples[0].x == 1.5
    assert samples[0].y == -250.8
    assert samples[0].heading == 1.57


def test_export_projection_jsonl_with_mocked_apollo_map_xysl(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(
        run_dir / "artifacts" / "apollo_reference_line_contract.jsonl",
        [
            {
                "timestamp": 1.0,
                "localization": {
                    "x": 1.509986955,
                    "y": -250.853492675,
                    "heading": 1.570800987,
                },
            }
        ],
    )

    def fake_runner(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="lane_id[15_1_1], s[66.746709], l[0.508728], heading[1.570666]\n",
            stderr="",
        )

    out_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    status = export_apollo_hdmap_projection_jsonl(
        run_dir=run_dir,
        out_path=out_path,
        command_runner=fake_runner,
    )
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]

    assert status["status"] == "pass"
    assert status["source"] == "apollo_hdmap_api"
    assert rows[0]["source"] == "apollo_hdmap_api"
    assert rows[0]["status"] == "ok"
    assert rows[0]["nearest_lane_id"] == "15_1_1"
    assert rows[0]["projection_s"] == 66.746709
    assert rows[0]["lateral_error_m"] == 0.508728
    assert abs(rows[0]["heading_error_rad"] - (1.570800987 - 1.570666)) < 1e-9


def test_export_projection_jsonl_records_map_xysl_timeout(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(
        run_dir / "artifacts" / "apollo_reference_line_contract.jsonl",
        [
            {
                "timestamp": 1.0,
                "localization": {
                    "x": 1.509986955,
                    "y": -250.853492675,
                    "heading": 1.570800987,
                },
            }
        ],
    )

    def timeout_runner(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout"), stderr="hung")

    out_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    status = export_apollo_hdmap_projection_jsonl(
        run_dir=run_dir,
        out_path=out_path,
        command_runner=timeout_runner,
    )
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]

    assert status["status"] == "warn"
    assert status["non_ok_row_count"] == 1
    assert rows[0]["status"] == "error"
    assert rows[0]["command_timeout"] is True


def _projection_rows(*, heading_error_rad: float, lateral_error_m: float) -> list[dict]:
    rows = []
    for index in range(60):
        rows.append(
            {
                "timestamp": index * 0.05,
                "localization_x": 10.0 + index * 0.5,
                "localization_y": 2.0,
                "localization_heading": 0.0,
                "nearest_lane_id": "lane_097",
                "projection_s": 3.0 + index * 0.6,
                "projection_l": lateral_error_m,
                "lane_heading_at_s": 0.0,
                "heading_error_rad": heading_error_rad,
                "lateral_error_m": lateral_error_m,
                "road_id": "road_1",
                "junction_id": None,
                "source": "apollo_hdmap_api",
                "map_name": "Town01",
                "map_dir": "/apollo/modules/map/data/town01",
                "status": "ok",
            }
        )
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
