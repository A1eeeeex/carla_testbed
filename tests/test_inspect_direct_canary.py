from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("tools/inspect_town01_direct_canary.py")
FIXTURE_BATCH = Path("tests/fixtures/ab/simple_batch")


def test_inspect_direct_canary_reports_missing_marker(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--marker",
            str(tmp_path / "missing_marker.txt"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "missing_run_root"


def test_inspect_direct_canary_can_pass_filtered_lane097_fixture(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--run-root",
            str(FIXTURE_BATCH),
            "--routes",
            "lane097",
            "--out",
            str(tmp_path / "analysis"),
            "--require-steering-normalization-mode",
            "single_percent_at_select",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "passed"
    assert payload["route_filter"] == ["lane097"]
    assert payload["requirement_check"]["passed"] is True
    assert payload["direct_rows"][0]["direct_transport_contract_status"] == "aligned"
    assert payload["comparisons"][0]["cadence_comparison"]["bridge_loc_hz_ratio"] == 0.9
    assert (tmp_path / "analysis" / "ab_report.json").is_file()


def test_inspect_direct_canary_default_steering_requirement_is_strict(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--run-root",
            str(FIXTURE_BATCH),
            "--routes",
            "lane097",
            "--out",
            str(tmp_path / "analysis"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert any("steering_normalization_mode mismatch" in item for item in payload["requirement_check"]["failures"])
