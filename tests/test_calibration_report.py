from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from carla_testbed.calibration.control_actuation import CALIBRATION_TRIAL_FIELDS, load_calibration_trials_csv
from carla_testbed.calibration.profile import load_calibration_profile
from carla_testbed.calibration.report import analyze_calibration_report, build_calibration_report
from tools import calibrate_carla_actuators as actuator_calibration


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


class _TickWorld:
    def __init__(self) -> None:
        self.tick_count = 0

    def tick(self) -> None:
        self.tick_count += 1


def _synchronous_probe() -> actuator_calibration.CarlaProbe:
    probe = object.__new__(actuator_calibration.CarlaProbe)
    probe.sync_mode = True
    probe.step_sec = 0.05
    probe.world = _TickWorld()
    return probe


def test_carla_probe_synchronous_step_uses_simulation_duration() -> None:
    probe = _synchronous_probe()

    probe._step(0.11)

    assert probe.world.tick_count == 3


def test_carla_probe_synchronous_sample_has_deterministic_ticks_and_timestamps() -> None:
    probe = _synchronous_probe()
    controls = []
    probe.settle = lambda **_kwargs: None
    probe.apply = lambda **kwargs: controls.append(kwargs)
    probe.state = lambda: {"speed_mps": 1.0}

    samples = probe.hold_and_sample(
        throttle=0.0,
        brake=0.1,
        steer=0.0,
        settle_sec=0.0,
        sample_sec=0.11,
    )

    assert probe.world.tick_count == 3
    assert len(controls) == 3
    assert [item["sample_index"] for item in samples] == [0, 1, 2]
    assert [item["elapsed_sec"] for item in samples] == pytest.approx([0.05, 0.10, 0.15])


def test_carla_probe_synchronous_acceleration_stops_at_target_speed() -> None:
    probe = _synchronous_probe()
    speeds = iter((0.4, 1.1))
    controls = []
    probe.apply = lambda **kwargs: controls.append(kwargs)
    probe.state = lambda: {"speed_mps": next(speeds)}

    result = probe.accelerate_to_speed(target_speed_mps=1.0, throttle=0.5, timeout_sec=1.0)

    assert result == {"target_speed_mps": 1.0, "final_speed_mps": 1.1, "reached": True}
    assert probe.world.tick_count == 2
    assert len(controls) == 2


def test_effective_brake_decel_uses_declared_window_and_rejects_stop_spike() -> None:
    samples = [
        {"elapsed_sec": 0.05, "speed_mps": 5.0, "forward_accel_mps2": 1.0},
        {"elapsed_sec": 0.10, "speed_mps": 4.9, "forward_accel_mps2": -2.0},
        {"elapsed_sec": 0.20, "speed_mps": 4.8, "forward_accel_mps2": -2.2},
        {"elapsed_sec": 0.30, "speed_mps": 4.6, "forward_accel_mps2": -2.4},
        {"elapsed_sec": 0.40, "speed_mps": 4.4, "forward_accel_mps2": -2.5},
        {"elapsed_sec": 0.90, "speed_mps": 0.8, "forward_accel_mps2": -25.0},
    ]

    decel = actuator_calibration._effective_brake_decel(samples)

    assert decel == pytest.approx(2.0)


def test_carla_probe_state_records_longitudinal_and_drivetrain_context() -> None:
    probe = object.__new__(actuator_calibration.CarlaProbe)
    transform = SimpleNamespace(
        location=SimpleNamespace(x=1.0, y=2.0, z=3.0),
        rotation=SimpleNamespace(roll=0.1, pitch=0.2, yaw=180.0),
        get_forward_vector=lambda: SimpleNamespace(x=-1.0, y=0.0, z=0.0),
    )
    actor = SimpleNamespace(
        get_velocity=lambda: SimpleNamespace(x=-4.0, y=0.0, z=0.0),
        get_acceleration=lambda: SimpleNamespace(x=2.0, y=0.0, z=0.0),
        get_angular_velocity=lambda: SimpleNamespace(z=5.0),
        get_transform=lambda: transform,
        get_control=lambda: SimpleNamespace(
            throttle=0.0,
            brake=0.1,
            steer=0.0,
            gear=2,
            manual_gear_shift=False,
            reverse=False,
            hand_brake=False,
        ),
    )
    probe.vehicle = lambda: actor

    state = probe.state()

    assert state["speed_mps"] == pytest.approx(4.0)
    assert state["forward_speed_mps"] == pytest.approx(4.0)
    assert state["forward_accel_mps2"] == pytest.approx(-2.0)
    assert state["gear"] == 2
    assert state["location_x_m"] == pytest.approx(1.0)
    assert state["yaw_deg"] == pytest.approx(180.0)


def test_calibration_axis_selection_rejects_unknown_or_empty_axes() -> None:
    assert actuator_calibration._parse_axes("brake,steering,brake") == ("steering", "brake")
    with pytest.raises(ValueError, match="unsupported calibration axes"):
        actuator_calibration._parse_axes("brake,rudder")
    with pytest.raises(ValueError, match="at least one"):
        actuator_calibration._parse_axes("")


def test_brake_default_commands_include_zero_brake_coast_baseline() -> None:
    args = actuator_calibration.parse_args([])

    assert actuator_calibration._parse_float_list(args.brake_commands)[0] == 0.0
    assert actuator_calibration._parse_float_list(args.low_speed_brake_commands)[0] == 0.0
    assert args.brake_response_start_sec == pytest.approx(0.2)
    assert args.brake_effective_window_sec == pytest.approx(0.4)


def test_brake_quality_uses_increment_above_zero_brake_coast() -> None:
    bucket = {
        "measurements": [
            {"brake_cmd": 0.0, "target_decel_mps2": 2.5, "sample_count": 10},
            {"brake_cmd": 0.05, "target_decel_mps2": 2.7, "sample_count": 10},
            {"brake_cmd": 0.10, "target_decel_mps2": 3.0, "sample_count": 10},
            {"brake_cmd": 0.20, "target_decel_mps2": 3.5, "sample_count": 10},
        ],
        "inverse": {},
        "raw_measurements": [
            {"brake_cmd": 0.0, "target_decel_mps2": 2.5, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.05, "target_decel_mps2": 2.7, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.10, "target_decel_mps2": 3.0, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.20, "target_decel_mps2": 3.5, "observed_speed_mps": 8.0, "sample_count": 10},
        ],
    }

    actuator_calibration._annotate_brake_coast_baseline([bucket])
    actuator_calibration._annotate_axis_reliability([bucket], axis="brake")

    assert bucket["coast_baseline"]["target_decel_mps2"] == pytest.approx(2.5)
    assert [item["incremental_brake_decel_mps2"] for item in bucket["measurements"]] == pytest.approx(
        [0.0, 0.2, 0.5, 1.0]
    )
    assert bucket["quality"]["reliable"] is True
    assert bucket["quality"]["max_incremental_brake_decel_mps2"] == pytest.approx(1.0)
    assert bucket["inverse"]["target_incremental_brake_decel_mps2_to_brake_cmd"][-1] == {
        "target_incremental_brake_decel_mps2": 1.0,
        "brake_cmd": 0.2,
    }


def test_brake_quality_rejects_total_decel_without_coast_baseline() -> None:
    bucket = {
        "measurements": [
            {"brake_cmd": 0.05, "target_decel_mps2": 3.0, "sample_count": 10},
            {"brake_cmd": 0.10, "target_decel_mps2": 3.2, "sample_count": 10},
            {"brake_cmd": 0.20, "target_decel_mps2": 3.5, "sample_count": 10},
        ],
        "inverse": {},
        "raw_measurements": [
            {"brake_cmd": 0.05, "target_decel_mps2": 3.0, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.10, "target_decel_mps2": 3.2, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.20, "target_decel_mps2": 3.5, "observed_speed_mps": 8.0, "sample_count": 10},
        ],
    }

    actuator_calibration._annotate_brake_coast_baseline([bucket])
    actuator_calibration._annotate_axis_reliability([bucket], axis="brake")

    assert bucket["quality"]["reliable"] is False
    assert "missing_coast_baseline" in bucket["quality"]["reasons"]
    assert "weak_incremental_brake_signal" in bucket["quality"]["reasons"]


def test_brake_quality_rejects_repeatable_non_monotonic_increment() -> None:
    bucket = {
        "measurements": [
            {"brake_cmd": 0.0, "target_decel_mps2": 2.5, "sample_count": 10},
            {"brake_cmd": 0.05, "target_decel_mps2": 2.8, "sample_count": 10},
            {"brake_cmd": 0.10, "target_decel_mps2": 2.5, "sample_count": 10},
            {"brake_cmd": 0.20, "target_decel_mps2": 2.9, "sample_count": 10},
        ],
        "inverse": {},
        "raw_measurements": [
            {"brake_cmd": 0.0, "target_decel_mps2": 2.5, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.05, "target_decel_mps2": 2.8, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.10, "target_decel_mps2": 2.5, "observed_speed_mps": 8.0, "sample_count": 10},
            {"brake_cmd": 0.20, "target_decel_mps2": 2.9, "observed_speed_mps": 8.0, "sample_count": 10},
        ],
    }

    actuator_calibration._annotate_brake_coast_baseline([bucket])
    actuator_calibration._annotate_axis_reliability([bucket], axis="brake")

    assert bucket["quality"]["reliable"] is False
    assert "non_monotonic_incremental_brake_response" in bucket["quality"]["reasons"]
    assert bucket["quality"]["incremental_monotonic_tolerance_mps2"] == pytest.approx(0.1)


def test_brake_calibration_excludes_bin_below_entry_speed_floor() -> None:
    class Probe:
        def reset_to_reference_pose(self, *, settle_sec: float) -> None:
            assert settle_sec >= 0.0

        def accelerate_to_speed(
            self,
            *,
            target_speed_mps: float,
            throttle: float,
            timeout_sec: float,
        ) -> dict[str, float | bool]:
            assert throttle > 0.0
            assert timeout_sec > 0.0
            return {
                "target_speed_mps": target_speed_mps,
                "final_speed_mps": target_speed_mps,
                "reached": True,
            }

        def state(self) -> dict[str, float | int]:
            return {"speed_mps": 3.5, "forward_speed_mps": 3.5, "gear": 1}

        def hold_and_sample(self, **_kwargs: object) -> list[dict[str, float]]:
            return [
                {
                    "elapsed_sec": 0.2,
                    "speed_mps": 3.4,
                    "forward_speed_mps": 3.4,
                    "forward_accel_mps2": -1.0,
                },
                {
                    "elapsed_sec": 0.4,
                    "speed_mps": 3.2,
                    "forward_speed_mps": 3.2,
                    "forward_accel_mps2": -1.0,
                },
            ]

    args = actuator_calibration.parse_args(
        [
            "--speed-bins",
            "0,2,5",
            "--brake-commands",
            "0.0",
            "--brake-entry-speed-mps-min",
            "2.0",
        ]
    )

    result = actuator_calibration.calibrate_brake(Probe(), args)

    assert [
        (item["speed_min_mps"], item["speed_max_mps"])
        for item in result["speed_bins"]
    ] == [(2.0, 5.0)]
    assert result["unavailable_speed_bins"] == [
        {
            "speed_min_mps": 0.0,
            "speed_max_mps": 2.0,
            "target_entry_speed_mps": 0.7,
            "effective_entry_speed_mps": 2.0,
            "reason": "entry_speed_floor_outside_bin",
        }
    ]
    assert result["summary"]["full_declared_speed_range_covered"] is False
    assert result["summary"]["materialized_speed_bin_count"] == 1


def test_low_speed_brake_calibration_separates_stop_from_stationary_hold() -> None:
    class Probe:
        def __init__(self) -> None:
            self.mode = "stationary"

        def reset_to_reference_pose(self, *, settle_sec: float) -> None:
            assert settle_sec >= 0.0
            self.mode = "stationary"

        def accelerate_to_speed(
            self,
            *,
            target_speed_mps: float,
            throttle: float,
            timeout_sec: float,
        ) -> dict[str, float | bool]:
            self.mode = "moving"
            return {
                "target_speed_mps": target_speed_mps,
                "final_speed_mps": target_speed_mps,
                "reached": True,
            }

        def state(self) -> dict[str, float]:
            return {
                "speed_mps": 0.0 if self.mode == "stationary" else 1.0,
                "location_x_m": 0.0,
                "location_y_m": 0.0,
            }

        def hold_and_sample(self, **kwargs: object) -> list[dict[str, float]]:
            brake = float(kwargs["brake"])
            if self.mode == "stationary":
                return [
                    {
                        "elapsed_sec": 0.05,
                        "speed_mps": 0.0,
                        "location_x_m": 0.0,
                        "location_y_m": 0.0,
                    },
                    {
                        "elapsed_sec": 0.8,
                        "speed_mps": 0.0,
                        "location_x_m": 0.0,
                        "location_y_m": 0.0,
                    },
                ]
            end_speed = 1.0 if brake == 0.0 else 0.0
            return [
                {
                    "elapsed_sec": 0.05,
                    "speed_mps": 1.0,
                    "forward_speed_mps": 1.0,
                },
                {
                    "elapsed_sec": 0.1,
                    "speed_mps": end_speed,
                    "forward_speed_mps": end_speed,
                },
                {
                    "elapsed_sec": 0.8,
                    "speed_mps": end_speed,
                    "forward_speed_mps": end_speed,
                },
            ]

    args = actuator_calibration.parse_args(
        [
            "--low-speed-brake-commands",
            "0.0,0.01,0.02,0.03",
            "--low-speed-rolling-brake-entry-speeds",
            "1.5",
        ]
    )

    result = actuator_calibration.calibrate_low_speed_brake(Probe(), args)

    assert result["stop"]["summary"]["stop_cmd"] == pytest.approx(0.01)
    assert result["stop"]["quality"]["reliable"] is True
    assert result["hold"]["summary"]["hold_cmd"] == pytest.approx(0.0)
    assert result["hold"]["quality"]["reliable"] is True
    assert all(
        item["initial_speed_mps"] == pytest.approx(0.0)
        for item in result["hold"]["measurements"]
    )
    assert result["rolling"][0]["quality"]["state_transition_excluded_count"] == 3
    assert result["rolling"][0]["quality"]["reliable"] is False


def test_brake_only_payload_preserves_base_steering_without_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_path = tmp_path / "steering.json"
    base_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "calibration_id": "steering-base",
                "provenance": {"source": "fixture"},
                "vehicle": {"type_id": "vehicle.lincoln.mkz_2020"},
                "steering": {"inverse": {"fixture": [1]}},
            }
        ),
        encoding="utf-8",
    )
    args = actuator_calibration.parse_args(
        [
            "--axes",
            "brake",
            "--base-calibration-file",
            str(base_path),
            "--calibration-id",
            "brake-diagnostic",
        ]
    )
    probe = _synchronous_probe()
    probe.vehicle_characteristics = lambda: {"type_id": "vehicle.lincoln.mkz_2020"}
    monkeypatch.setattr(actuator_calibration, "calibrate_brake", lambda _probe, _args: {"speed_bins": [1]})
    monkeypatch.setattr(actuator_calibration, "calibrate_low_speed_brake", lambda _probe, _args: {"hold": {}})

    payload = actuator_calibration.build_calibration_payload(probe, args, axes=("brake",))

    assert payload["steering"] == {"inverse": {"fixture": [1]}}
    assert payload["brake"] == {"speed_bins": [1], "low_speed": {"hold": {}}}
    assert "throttle" not in payload
    assert payload["generator"]["axes"] == ["brake"]
    assert payload["claim_boundary"]["evidence_owner"] == "diagnostic_carla_direct"
    assert payload["claim_boundary"]["mapping_promotion_allowed"] is False
    assert payload["recommendation_policy"]["automatic_promotion"] is False
    assert payload["provenance"]["base_calibration"]["calibration_id"] == "steering-base"


def test_brake_only_payload_can_skip_unrelated_low_speed_probes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = actuator_calibration.parse_args(
        ["--axes", "brake", "--skip-low-speed-probes"]
    )
    probe = _synchronous_probe()
    probe.vehicle_characteristics = lambda: {"type_id": "vehicle.lincoln.mkz_2020"}
    monkeypatch.setattr(
        actuator_calibration,
        "calibrate_brake",
        lambda _probe, _args: {"speed_bins": []},
    )

    def _unexpected_low_speed(_probe, _args):
        raise AssertionError("low-speed brake probe should have been skipped")

    monkeypatch.setattr(
        actuator_calibration,
        "calibrate_low_speed_brake",
        _unexpected_low_speed,
    )

    payload = actuator_calibration.build_calibration_payload(
        probe,
        args,
        axes=("brake",),
    )

    assert payload["brake"] == {"speed_bins": []}


def test_brake_low_speed_only_payload_preserves_base_main_bins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_path = tmp_path / "main_brake.json"
    base_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "calibration_id": "main-brake-base",
                "vehicle": {"type_id": "vehicle.lincoln.mkz_2020"},
                "steering": {"inverse": {"fixture": [1]}},
                "brake": {
                    "speed_bins": [{"speed_min_mps": 2.0, "speed_max_mps": 5.0}],
                    "summary": {"full_declared_speed_range_covered": False},
                },
            }
        ),
        encoding="utf-8",
    )
    args = actuator_calibration.parse_args(
        [
            "--axes",
            "brake",
            "--base-calibration-file",
            str(base_path),
            "--brake-low-speed-probes-only",
        ]
    )
    probe = _synchronous_probe()
    probe.vehicle_characteristics = lambda: {"type_id": "vehicle.lincoln.mkz_2020"}

    def _unexpected_main_brake(_probe, _args):
        raise AssertionError("main brake bins must be preserved")

    monkeypatch.setattr(actuator_calibration, "calibrate_brake", _unexpected_main_brake)
    monkeypatch.setattr(
        actuator_calibration,
        "calibrate_low_speed_brake",
        lambda _probe, _args: {"hold": {"summary": {"hold_cmd": 0.0}}},
    )

    payload = actuator_calibration.build_calibration_payload(
        probe,
        args,
        axes=("brake",),
    )

    assert payload["brake"] == {
        "speed_bins": [{"speed_min_mps": 2.0, "speed_max_mps": 5.0}],
        "summary": {"full_declared_speed_range_covered": False},
        "low_speed": {"hold": {"summary": {"hold_cmd": 0.0}}},
    }
    assert payload["steering"] == {"inverse": {"fixture": [1]}}
    assert payload["generator"]["brake_low_speed_probes_only"] is True
