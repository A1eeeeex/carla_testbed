from __future__ import annotations

import json
import subprocess
from pathlib import Path

PYTHON = "/home/ubuntu/miniconda3/envs/carla16/bin/python3"
SCRIPT = Path("tools/check_apollo_reproduction.py")
BASE_ARGS = [
    PYTHON,
    str(SCRIPT),
    "--inventory",
    "configs/algorithms/apollo_inventory.yaml",
    "--variant",
    "configs/algorithms/apollo_variant.carla_gt.example.yaml",
    "--reproduction-report",
    "configs/algorithms/apollo_reproduction_report.example.yaml",
]


def test_cli_generates_check_json_and_markdown_with_warn_exit(tmp_path: Path) -> None:
    out_dir = tmp_path / "check"

    proc = subprocess.run(
        [*BASE_ARGS, "--out", str(out_dir)],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 1
    report_path = out_dir / "apollo_reproduction_check.json"
    md_path = out_dir / "apollo_reproduction_check.md"
    assert report_path.exists()
    assert md_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["schema_version"] == "apollo_reproduction_check.v1"
    assert report["status"] == "warn"
    assert "route_health_report" in report["gate"]["missing_artifacts"]
    assert "ab_report" in report["gate"]["missing_artifacts"]


def test_cli_allow_warn_as_success_returns_zero(tmp_path: Path) -> None:
    out_dir = tmp_path / "check_warn_ok"

    proc = subprocess.run(
        [*BASE_ARGS, "--out", str(out_dir), "--allow-warn-as-success"],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 0
    assert (out_dir / "apollo_reproduction_check.json").exists()


def test_cli_generates_optional_subreports(tmp_path: Path) -> None:
    out_dir = tmp_path / "check_full"

    proc = subprocess.run(
        [
            *BASE_ARGS,
            "--channel-stats",
            "tests/fixtures/apollo/channel_stats_valid.json",
            "--replay-golden",
            "tests/fixtures/apollo/replay_digest_golden.json",
            "--replay-candidate",
            "tests/fixtures/apollo/replay_digest_candidate.json",
            "--shadow-timeseries",
            "tests/fixtures/apollo/shadow_mode_timeseries.csv",
            "--out",
            str(out_dir),
            "--allow-warn-as-success",
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 0
    report = json.loads((out_dir / "apollo_reproduction_check.json").read_text(encoding="utf-8"))
    assert report["component_statuses"]["adapter_contract_report"] == "pass"
    assert report["component_statuses"]["replay_comparison_report"] == "pass"
    assert report["component_statuses"]["shadow_mode_report"] == "pass"
    assert (out_dir / "adapter_contract_report.json").exists()
    assert (out_dir / "replay_digest" / "replay_comparison_report.json").exists()
    assert (out_dir / "shadow_mode" / "shadow_mode_report.json").exists()


def test_cli_fail_blocked_returns_nonzero(tmp_path: Path) -> None:
    reproduction = json.loads(
        json.dumps(
            {
                "schema_version": "algorithm_reproduction.v1",
                "algorithm": "apollo",
                "variant_id": "apollo_10_0_carla_gt_town01_reference",
                "created_at": "2026-05-24T00:00:00Z",
                "overall_status": "blocked",
                "missing_artifacts": [],
                "blocking_failures": [],
                "notes": "synthetic blocked report",
                "levels": {
                    "L0_environment_frozen": {
                        "status": "fail",
                        "required_artifacts": [
                            "algorithm_variant_manifest.yaml",
                            "environment_manifest.json",
                            "explicit_no_patch.marker",
                        ],
                        "observed_artifacts": [],
                        "checks": [],
                        "failure_reason": "missing_environment",
                        "next_action": "freeze environment",
                    },
                    "L1_upstream_demo_or_record_replay": {
                        "status": "blocked",
                        "required_artifacts": [
                            "record_info.txt",
                            "dreamview_screenshot.png",
                            "cyber_channel_stats.json",
                        ],
                        "observed_artifacts": [],
                        "checks": [],
                        "failure_reason": "blocked",
                        "next_action": "run replay",
                    },
                    "L2_module_golden_replay": {
                        "status": "blocked",
                        "required_artifacts": [
                            "apollo_output.record",
                            "planning_digest.json",
                            "control_digest.json",
                            "replay_report.json",
                        ],
                        "observed_artifacts": [],
                        "checks": [],
                        "failure_reason": "blocked",
                        "next_action": "run replay",
                    },
                    "L3_carla_to_apollo_adapter_contract": {
                        "status": "blocked",
                        "required_artifacts": [
                            "adapter_contract_report.json",
                            "frame_transform_report.json",
                            "routing_contract_report.json",
                            "channel_stats.json",
                        ],
                        "observed_artifacts": [],
                        "checks": [],
                        "failure_reason": "blocked",
                        "next_action": "run L3",
                    },
                    "L4_shadow_mode": {
                        "status": "blocked",
                        "required_artifacts": [
                            "shadow_mode_report.json",
                            "planning_vs_route.csv",
                            "control_shadow_timeseries.csv",
                            "route_health.json",
                        ],
                        "observed_artifacts": [],
                        "checks": [],
                        "failure_reason": "blocked",
                        "next_action": "run L4",
                    },
                    "L5_closed_loop": {
                        "status": "blocked",
                        "required_artifacts": [
                            "summary.json",
                            "manifest.json",
                            "timeseries.csv",
                            "route_health.json",
                        ],
                        "observed_artifacts": [],
                        "checks": [],
                        "failure_reason": "blocked",
                        "next_action": "run L5",
                    },
                },
            }
        )
    )
    repro_path = tmp_path / "blocked_reproduction.json"
    repro_path.write_text(json.dumps(reproduction), encoding="utf-8")
    out_dir = tmp_path / "blocked_check"

    proc = subprocess.run(
        [
            PYTHON,
            str(SCRIPT),
            "--inventory",
            "configs/algorithms/apollo_inventory.yaml",
            "--variant",
            "configs/algorithms/apollo_variant.carla_gt.example.yaml",
            "--reproduction-report",
            str(repro_path),
            "--out",
            str(out_dir),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode != 0
    report = json.loads((out_dir / "apollo_reproduction_check.json").read_text(encoding="utf-8"))
    assert report["status"] == "blocked"
    assert report["gate"]["can_run_closed_loop_eval"] is False
