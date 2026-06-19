from __future__ import annotations

from carla_testbed.config.sensor_capture import legacy_record_from_config, sensor_capture_enabled_from_config


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


def test_legacy_record_from_config_prefers_raw_record() -> None:
    record = legacy_record_from_config(
        {
            "record": {"probe": {"require_clock": False}},
            "recording": {"artifacts": {"legacy_record": {"probe": {"require_clock": True}}}},
        }
    )

    assert record["probe"]["require_clock"] is False


def test_legacy_record_from_config_reads_folded_legacy_record() -> None:
    record = legacy_record_from_config(
        {
            "recording": {
                "artifacts": {
                    "legacy_record": {
                        "probe": {
                            "require_clock": False,
                            "topics": ["/tf", "/carla/hero/odom"],
                        }
                    }
                }
            }
        }
    )

    assert record["probe"]["require_clock"] is False
    assert record["probe"]["topics"] == ["/tf", "/carla/hero/odom"]
