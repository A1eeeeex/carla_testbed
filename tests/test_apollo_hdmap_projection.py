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
    project_sample_with_map_xysl,
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
    assert report["projection"]["empty_reason"] == "apollo_hdmap_projection_artifact_empty_no_exported_rows"
    assert "export_apollo_hdmap_projection.py" in report["projection"]["next_action"]
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
    _write_jsonl(heading_path, _projection_rows(heading_error_rad=0.031, lateral_error_m=0.05))
    _write_jsonl(lateral_path, _projection_rows(heading_error_rad=0.01, lateral_error_m=0.31))

    heading_report = analyze_apollo_hdmap_projection_file(heading_path)
    lateral_report = analyze_apollo_hdmap_projection_file(lateral_path)

    assert heading_report["status"] == "fail"
    assert "apollo_hdmap_projection_heading_error_high" in heading_report["blocking_reasons"]
    assert lateral_report["status"] == "fail"
    assert "apollo_hdmap_projection_lateral_error_high" in lateral_report["blocking_reasons"]


def test_start_goal_projection_failure_blocks_alignment_evidence(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    rows[0]["sample_type"] = "start"
    rows[0]["status"] = "no_lane"
    rows[0]["projection_status"] = "no_lane"
    _write_jsonl(path, rows)

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert "start_goal_projection_failed" in report["blocking_reasons"]


def test_route_projection_low_ok_ratio_blocks_coverage(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    for row in rows:
        row["sample_type"] = "route"
    rows[-1]["status"] = "out_of_map"
    rows[-1]["projection_status"] = "out_of_map"
    rows[-2]["status"] = "no_lane"
    rows[-2]["projection_status"] = "no_lane"
    rows[-3]["status"] = "error"
    rows[-3]["projection_status"] = "error"
    rows[-4]["status"] = "no_lane"
    rows[-4]["projection_status"] = "no_lane"
    _write_jsonl(path, rows)

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert "route_projection_coverage_low" in report["blocking_reasons"]


def test_claim_grade_projection_requires_minimum_sample_count(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    _write_jsonl(path, _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)[:1])

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert "apollo_hdmap_projection_sample_count_low" in report["insufficient_reasons"]
    assert "apollo_hdmap_projection_sample_count_low" not in report["blocking_reasons"]


def test_claim_grade_projection_requires_route_s_coverage(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    for index, row in enumerate(rows):
        row["projection_s"] = 10.0 + index * 0.1

    _write_jsonl(path, rows)
    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert "apollo_hdmap_projection_route_s_coverage_low" in report["insufficient_reasons"]
    assert "apollo_hdmap_projection_route_s_coverage_low" not in report["blocking_reasons"]


def test_claim_grade_projection_fails_inconsistent_map_identity(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    rows[-1]["map_name"] = "Town02"

    _write_jsonl(path, rows)
    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "fail"
    assert "apollo_hdmap_projection_map_identity_inconsistent" in report["blocking_reasons"]


def test_environment_unavailable_projection_is_insufficient_not_map_fail(tmp_path: Path) -> None:
    path = tmp_path / "apollo_hdmap_projection.jsonl"
    rows = _projection_rows(heading_error_rad=0.01, lateral_error_m=0.05)
    for row in rows:
        row.update(
            {
                "status": "environment_unavailable",
                "nearest_lane_id": None,
                "projection_s": None,
                "projection_l": None,
                "heading_error_rad": None,
                "lateral_error_m": None,
                "raw_output_excerpt": "Error response from daemon: container abc is not running",
            }
        )
    _write_jsonl(path, rows)

    report = analyze_apollo_hdmap_projection_file(path)

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert report["projection"]["environment_unavailable_count"] == len(rows)
    assert "apollo_hdmap_projection_runtime_unavailable" in report["insufficient_reasons"]
    assert "apollo_map_xysl_runtime" in report["missing_fields"]
    assert "apollo_hdmap_projection_no_ok_rows" not in report["blocking_reasons"]
    assert "map_alignment" not in report["suspected_failure_layers"]


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


def test_project_sample_emits_alignment_schema_aliases() -> None:
    sample = LocalizationProjectionSample(
        timestamp=12.5,
        x=1.5,
        y=-250.8,
        heading=1.57,
        source_artifact="sample.jsonl",
        source_index=3,
        sample_type="route",
        metadata={"carla_lane_id": "117:0:1", "carla_lane_key": "117:1", "route_s": 281.0},
    )

    row = project_sample_with_map_xysl(
        sample,
        MapXyslConfig(docker_container=None),
        command_runner=lambda *args, **kwargs: subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="lane_id[15_1_1], s[66.746709], l[0.050000], heading[1.570000]\n",
            stderr="",
        ),
    )

    assert row["sample_type"] == "route"
    assert row["sample_index"] == 3
    assert row["x_apollo"] == 1.5
    assert row["y_apollo"] == -250.8
    assert row["heading_apollo"] == 1.57
    assert row["projection_status"] == "ok"
    assert row["carla_lane_id"] == "117:0:1"
    assert row["carla_lane_key"] == "117:1"
    assert row["route_s"] == 281.0


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


def test_projection_samples_prefer_sim_time_over_wall_timestamp(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(
        run_dir / "artifacts" / "apollo_reference_line_contract.jsonl",
        [
            {
                "timestamp": 1_781_250_000.0,
                "sim_time_sec": 12.5,
                "localization": {
                    "x": 1.5,
                    "y": -250.8,
                    "heading": 1.57,
                },
            }
        ],
    )

    samples = load_localization_projection_samples(run_dir=run_dir)

    assert len(samples) == 1
    assert samples[0].timestamp == 12.5


def test_projection_sample_limit_evenly_covers_source_window(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    rows = [
        {
            "timestamp": float(index),
            "localization": {
                "x": float(index),
                "y": -250.0 + float(index),
                "heading": 1.57,
            },
        }
        for index in range(101)
    ]
    _write_jsonl(run_dir / "artifacts" / "apollo_reference_line_contract.jsonl", rows)

    samples = load_localization_projection_samples(run_dir=run_dir, max_samples=5)

    assert [sample.source_index for sample in samples] == [0, 25, 50, 75, 100]


def test_projection_samples_can_include_route_json_start_goal(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    route = {
        "points": [
                {"x": 1.0, "y": 2.0, "heading": 0.1, "lane_id": "117:0:1", "s": 10.0},
                {"x": 2.0, "y": 2.5, "heading": 0.2, "lane_id": "117:0:1", "s": 15.0},
                {"x": 3.0, "y": 3.0, "heading": 0.3, "lane_id": "83:0:1", "s": 20.0},
        ]
    }
    (run_dir / "route.json").parent.mkdir(parents=True, exist_ok=True)
    (run_dir / "route.json").write_text(json.dumps(route), encoding="utf-8")

    samples = load_localization_projection_samples(
        run_dir=run_dir,
        include_start_goal=True,
        include_route_samples=True,
        max_samples=0,
    )

    assert [(sample.x, sample.y, sample.heading) for sample in samples] == [
        (1.0, 2.0, 0.1),
        (3.0, 3.0, 0.3),
        (2.0, 2.5, 0.2),
    ]
    assert all(sample.source_artifact.endswith("route.json") for sample in samples)
    assert [sample.metadata.get("carla_lane_key") for sample in samples] == ["117:1", "83:1", "117:1"]


def test_projection_samples_can_include_manifest_route_trace_with_frame_transform(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "manifest.json",
        {
            "metadata": {
                "scenario_metadata": {
                    "route_trace": [
                        {"x": 2.0, "y": 208.0, "z": 0.0, "heading": -1.57079632679, "s": 0.0, "lane_id": "15:0:1"},
                        {"x": 2.0, "y": 108.0, "z": 0.0, "heading": -1.57079632679, "s": 100.0, "lane_id": "15:0:1"},
                        {"x": 2.0, "y": 8.0, "z": 0.0, "heading": -1.57079632679, "s": 200.0, "lane_id": "13:0:-1"},
                    ]
                }
            }
        },
    )
    transform_path = tmp_path / "apollo_frame_transform.yaml"
    _write_json(
        transform_path,
        {
            "map_name": "Town01",
            "source_frame": "carla_world",
            "target_frame": "apollo_map",
            "transform": {
                "tx": 0.0,
                "ty": 0.0,
                "tz": 0.0,
                "yaw_rad": 0.0,
                "scale": 1.0,
                "y_flip": True,
            },
        },
    )

    samples = load_localization_projection_samples(
        run_dir=run_dir,
        include_route_samples=True,
        include_start_goal=True,
        frame_transform_path=transform_path,
        max_samples=0,
    )

    assert [(round(sample.x, 3), round(sample.y, 3)) for sample in samples] == [
        (2.0, -208.0),
        (2.0, -8.0),
        (2.0, -108.0),
    ]
    assert all(abs((sample.heading or 0.0) - 1.57079632679) < 1e-9 for sample in samples)
    assert all("manifest.json:metadata.scenario_metadata.route_trace" in sample.source_artifact for sample in samples)
    assert [sample.metadata.get("carla_lane_key") for sample in samples] == ["15:1", "13:-1", "15:1"]


def test_projection_samples_transform_runtime_route_json_carla_frame(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "route.json",
        {
            "schema_version": "runtime_route_trace.v1",
            "source": "artifacts/scenario_metadata.json:route_trace",
            "coordinate_frame": "carla_world",
            "target_projection_frame": "apollo_map",
            "points": [
                {"x": 2.0, "y": 208.0, "z": 0.0, "heading": -1.57079632679, "s": 0.0},
                {"x": 2.0, "y": 108.0, "z": 0.0, "heading": -1.57079632679, "s": 100.0},
                {"x": 2.0, "y": 8.0, "z": 0.0, "heading": -1.57079632679, "s": 200.0},
            ],
        },
    )
    transform_path = tmp_path / "apollo_frame_transform.yaml"
    _write_json(
        transform_path,
        {
            "transform": {
                "tx": 0.0,
                "ty": 0.0,
                "tz": 0.0,
                "yaw_rad": 0.0,
                "scale": 1.0,
                "y_flip": True,
            }
        },
    )

    samples = load_localization_projection_samples(
        run_dir=run_dir,
        include_start_goal=True,
        include_route_samples=True,
        frame_transform_path=transform_path,
        max_samples=0,
    )

    assert [(round(sample.x, 3), round(sample.y, 3)) for sample in samples] == [
        (2.0, -208.0),
        (2.0, -8.0),
        (2.0, -108.0),
    ]
    assert all(abs((sample.heading or 0.0) - 1.57079632679) < 1e-9 for sample in samples)
    assert all(sample.source_artifact.endswith("route.json") for sample in samples)


def test_projection_samples_skip_runtime_route_json_carla_frame_without_transform(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "route.json",
        {
            "schema_version": "runtime_route_trace.v1",
            "source": "artifacts/scenario_metadata.json:route_trace",
            "coordinate_frame": "carla_world",
            "points": [
                {"x": 2.0, "y": 208.0, "heading": -1.57079632679},
                {"x": 2.0, "y": 108.0, "heading": -1.57079632679},
            ],
        },
    )

    samples = load_localization_projection_samples(
        run_dir=run_dir,
        include_start_goal=True,
        include_route_samples=True,
        max_samples=0,
    )

    assert samples == []


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


def test_export_route_projection_records_trace_and_chord_heading_errors(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "route.json",
        {
            "schema_version": "runtime_route_trace.v1",
            "source": "artifacts/scenario_metadata.json:route_trace",
            "coordinate_frame": "carla_world",
            "points": [
                {"x": 0.0, "y": 0.0, "z": 0.0, "heading": 0.0, "s": 0.0, "lane_id": "13:0:-1"},
                {"x": 10.0, "y": 0.0, "z": 0.0, "heading": 0.0, "s": 10.0, "lane_id": "13:0:-1"},
                {"x": 10.0, "y": 10.0, "z": 0.0, "heading": 0.0, "s": 20.0, "lane_id": "13:0:-1"},
            ],
        },
    )
    transform_path = tmp_path / "apollo_frame_transform.yaml"
    _write_json(
        transform_path,
        {
            "transform": {
                "tx": 0.0,
                "ty": 0.0,
                "tz": 0.0,
                "yaw_rad": 0.0,
                "scale": 1.0,
                "y_flip": True,
            }
        },
    )

    def fake_runner(*args, **kwargs):
        command = args[0]
        command_text = command[-1] if command[:2] == ["docker", "exec"] else " ".join(command)
        lane_heading = -0.785398163397 if "--x=10" in command_text and "--y=-10" not in command_text else 0.0
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=f"lane_id[13_1_-1], s[10.0], l[0.1], heading[{lane_heading}]\n",
            stderr="",
        )

    out_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    status = export_apollo_hdmap_projection_jsonl(
        run_dir=run_dir,
        out_path=out_path,
        include_route_samples=True,
        frame_transform_path=transform_path,
        command_runner=fake_runner,
    )
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    middle = rows[1]

    assert status["status"] == "pass"
    assert middle["route_heading_source"] == "route_chord"
    assert abs(middle["heading_apollo"] + 0.785398163397) < 1e-9
    assert abs(middle["route_chord_heading_apollo"] + 0.785398163397) < 1e-9
    assert middle["route_trace_heading_apollo"] == 0.0
    assert abs(middle["route_chord_heading_error_rad"]) < 1e-9
    assert abs(middle["route_trace_heading_error_rad"] - 0.785398163397) < 1e-9


def test_export_projection_records_requested_route_s_coverage_gap(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "route.json").parent.mkdir(parents=True, exist_ok=True)
    (run_dir / "route.json").write_text(
        json.dumps({"points": [{"x": 1.0, "y": 2.0, "heading": 0.0}, {"x": 2.0, "y": 2.0, "heading": 0.0}]}),
        encoding="utf-8",
    )

    def fake_runner(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="lane_id[15_1_1], s[10.0], l[0.1], heading[0.0]\n",
            stderr="",
        )

    out_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    status = export_apollo_hdmap_projection_jsonl(
        run_dir=run_dir,
        out_path=out_path,
        include_route_samples=True,
        min_route_s_coverage=20.0,
        command_runner=fake_runner,
    )

    assert status["status"] == "warn"
    assert status["failure_reason"] == "route_s_coverage_below_requested_min"
    assert status["projection_s_coverage_m"] == 0.0
    assert "apollo_hdmap_projection_route_s_coverage_below_requested_min" in status["warnings"]


def test_export_projection_uses_manifest_route_trace_for_requested_coverage(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "manifest.json",
        {
            "metadata": {
                "scenario_metadata": {
                    "route_trace": [
                        {"x": 2.0, "y": 208.0, "heading": -1.57079632679},
                        {"x": 2.0, "y": 108.0, "heading": -1.57079632679},
                    ]
                }
            }
        },
    )
    transform_path = tmp_path / "apollo_frame_transform.yaml"
    _write_json(
        transform_path,
        {
            "transform": {
                "tx": 0.0,
                "ty": 0.0,
                "tz": 0.0,
                "yaw_rad": 0.0,
                "scale": 1.0,
                "y_flip": True,
            }
        },
    )

    def fake_runner(*args, **kwargs):
        command = args[0]
        command_text = command[-1] if command[:2] == ["docker", "exec"] else " ".join(command)
        if "--y=-208" in command_text:
            s = 0.0
        elif "--y=-108" in command_text:
            s = 100.0
        else:
            s = 50.0
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=f"lane_id[15_1_1], s[{s}], l[0.1], heading[1.570796]\n",
            stderr="",
        )

    out_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    status = export_apollo_hdmap_projection_jsonl(
        run_dir=run_dir,
        out_path=out_path,
        include_route_samples=True,
        include_start_goal=True,
        frame_transform_path=transform_path,
        min_route_s_coverage=90.0,
        command_runner=fake_runner,
    )
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]

    assert status["status"] == "pass"
    assert status["projection_s_coverage_m"] == 100.0
    assert status["frame_transform_path"] == str(transform_path)
    assert len(rows) == 2


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


def test_project_sample_classifies_docker_container_unavailable() -> None:
    sample = LocalizationProjectionSample(
        timestamp=1.0,
        x=1.0,
        y=2.0,
        heading=0.0,
        source_artifact="sample.jsonl",
        source_index=0,
    )

    def fake_runner(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=1,
            stdout="",
            stderr="Error response from daemon: container abc is not running\n",
        )

    row = project_sample_with_map_xysl(
        sample,
        MapXyslConfig(docker_container="apollo_container"),
        command_runner=fake_runner,
    )

    assert row["status"] == "environment_unavailable"
    assert row["command_exit_code"] == 1
    assert "container abc is not running" in row["raw_output_excerpt"]


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


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
