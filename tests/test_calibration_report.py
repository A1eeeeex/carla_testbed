from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.calibration.control_actuation import CALIBRATION_TRIAL_FIELDS, load_calibration_trials_csv
from carla_testbed.calibration.profile import load_calibration_profile
from carla_testbed.calibration.report import analyze_calibration_report, build_calibration_report


PROFILE = Path("configs/calibration/control_actuation.yaml")
TRIALS = Path("tests/fixtures/calibration/trials.csv")
GATES = Path("tests/fixtures/calibration/gate_results.json")
SCRIPT = Path("tools/analyze_calibration_report.py")


def test_fake_trials_generate_report() -> None:
    profile = load_calibration_profile(PROFILE)
    trials = load_calibration_trials_csv(TRIALS)
    report = build_calibration_report(profile=profile, trials=trials, trials_path=TRIALS, gate_results_path=GATES)

    assert report["schema_version"] == "calibration_report.v1"
    assert report["profile_id"] == "town01_control_actuation_legacy_draft"
    assert report["results"]["throttle_response"]["supported"] is True
    assert report["results"]["brake_response"]["supported"] is True
    assert report["results"]["steer_response"]["supported"] is True
    assert report["results"]["steer_response"]["steering_sign_verified"] is True


def test_missing_gate_results_disable_promotion() -> None:
    report, _outputs = analyze_calibration_report(profile_path=PROFILE, trials_path=TRIALS)

    assert report["no_regression"]["promotion_allowed"] is False
    assert set(report["no_regression"]["missing_gates"]) == {"097", "217", "031"}
    assert report["recommendation"]["keep_legacy_steer_scale_025"] is None


def test_physical_mapping_recommendation_defaults_false_even_with_passed_gates() -> None:
    report, _outputs = analyze_calibration_report(profile_path=PROFILE, trials_path=TRIALS, gate_results_path=GATES)

    assert report["no_regression"]["promotion_allowed"] is True
    assert report["recommendation"]["enable_physical_mapping"] is False
    assert "physical" in "; ".join(report["recommendation"]["required_next_steps"])


def test_legacy_steer_scale_025_is_explained_not_modified() -> None:
    report, _outputs = analyze_calibration_report(profile_path=PROFILE, trials_path=TRIALS, gate_results_path=GATES)

    assert report["results"]["steer_response"]["legacy_steer_scale_025_supported"] is True
    profile = load_calibration_profile(PROFILE)
    assert profile.control_mapping.steer_scale == 0.25
    assert report["recommendation"]["keep_legacy_steer_scale_025"] is True


def test_sensor_calibration_fields_are_not_in_schema() -> None:
    report, _outputs = analyze_calibration_report(profile_path=PROFILE, trials_path=TRIALS, gate_results_path=GATES)
    encoded = json.dumps(report, sort_keys=True)

    assert "camera_intrinsics" not in encoded
    assert "lidar_extrinsics" not in encoded
    assert "sensor_calibration" not in encoded


def test_missing_steer_trials_gracefully_degrades(tmp_path: Path) -> None:
    source_rows = load_calibration_trials_csv(TRIALS)
    reduced = [row for row in source_rows if row["command_type"] != "steer"]
    path = tmp_path / "no_steer.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CALIBRATION_TRIAL_FIELDS)
        writer.writeheader()
        writer.writerows(reduced)

    report, _outputs = analyze_calibration_report(profile_path=PROFILE, trials_path=path, gate_results_path=GATES)

    assert report["results"]["steer_response"]["supported"] is False
    assert "steer_trials" in report["missing_fields"]
    assert report["recommendation"]["enable_physical_mapping"] is False


def test_cli_writes_standard_outputs(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--profile",
            str(PROFILE),
            "--trials",
            str(TRIALS),
            "--gate-results",
            str(GATES),
            "--out",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["promotion_allowed"] is True
    assert (tmp_path / "calibration_report.json").is_file()
    assert (tmp_path / "calibration_trials.csv").is_file()
    assert (tmp_path / "calibration_plots" / "README.md").is_file()

    report = json.loads((tmp_path / "calibration_report.json").read_text(encoding="utf-8"))
    assert report["recommendation"]["enable_physical_mapping"] is False
