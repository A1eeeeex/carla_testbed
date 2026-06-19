import json
from pathlib import Path

import pytest

from tools.run_apollo_control_runtime_smoke import (
    _load_exact_overlay_records,
    _parse_exact_overlay_stats,
    _shell_script,
)


def _write_manifest(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "name": "libcontrol_component.so",
                        "source": "/backup/libcontrol_component.so",
                        "target": "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so",
                    },
                    {
                        "name": "lib_control_cmd_proto_mcc_bin.so",
                        "source": "/backup/lib_control_cmd_proto_mcc_bin.so",
                        "target": "/opt/apollo/neo/lib/modules/common_msgs/control_msgs/lib_control_cmd_proto_mcc_bin.so",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def test_exact_overlay_manifest_filtering(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path / "overlay_manifest.json")

    records = _load_exact_overlay_records(
        str(manifest),
        target_regex=r"control_component/libcontrol_component",
    )

    assert records == [
        {
            "name": "libcontrol_component.so",
            "source": "/backup/libcontrol_component.so",
            "target": "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so",
        }
    ]


def test_exact_overlay_manifest_filter_must_select_records(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path / "overlay_manifest.json")

    with pytest.raises(ValueError, match="selected no records"):
        _load_exact_overlay_records(str(manifest), name_regex="does_not_exist")


def test_exact_overlay_shell_uses_exact_targets_and_restores() -> None:
    records = [
        {
            "name": "libcontrol_component.so",
            "source": "/backup/libcontrol_component.so",
            "target": "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so",
        }
    ]

    script = _shell_script(
        timeout_sec=1.0,
        legacy=False,
        legacy_backup_path="",
        control_runtime_source_dir="",
        exact_overlay_records=records,
        runner="mainboard",
        variant="exact_test",
    )

    assert "trap restore_exact_overlay EXIT" in script
    assert "/backup/libcontrol_component.so" in script
    assert "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so" in script
    assert 'TB_EXACT_OVERLAY_RESULT=" + json.dumps' in script
    assert "LD_LIBRARY_PATH=/opt/apollo/neo/lib/cyber" in script


def test_parse_exact_overlay_stats_merges_apply_and_restore() -> None:
    text = "\n".join(
        [
            'TB_EXACT_OVERLAY_RESULT={"stage":"apply","selected_count":17,"missing_count":0}',
            'TB_EXACT_OVERLAY_RESULT={"stage":"restore","restored_count":17}',
        ]
    )

    stats = _parse_exact_overlay_stats(text)

    assert stats["selected_count"] == 17
    assert stats["missing_count"] == 0
    assert stats["restored_count"] == 17
