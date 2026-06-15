from __future__ import annotations

from carla_testbed.config.sensor_capture import sensor_capture_enabled_from_config


def test_sensor_capture_disabled_from_legacy_record_section() -> None:
    enabled, source = sensor_capture_enabled_from_config({"record": {"sensors": {"enable": False}}})

    assert enabled is False
    assert source == "record.sensors.enable"


def test_sensor_capture_disabled_from_folded_legacy_record_section() -> None:
    enabled, source = sensor_capture_enabled_from_config(
        {
            "recording": {
                "artifacts": {
                    "legacy_record": {
                        "sensors": {
                            "enable": False,
                        }
                    }
                }
            }
        }
    )

    assert enabled is False
    assert source == "recording.artifacts.legacy_record.sensors.enable"


def test_sensor_capture_defaults_enabled_without_explicit_disable() -> None:
    enabled, source = sensor_capture_enabled_from_config({"record": {"sensors": {"enable": True}}})

    assert enabled is True
    assert source == "default_enabled"
