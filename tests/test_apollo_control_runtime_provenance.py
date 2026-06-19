from __future__ import annotations

import json
import subprocess
from pathlib import Path

from carla_testbed.analysis.apollo_control_runtime_provenance import (
    analyze_apollo_control_runtime_overlay_set_provenance,
    analyze_apollo_control_runtime_provenance,
    write_apollo_control_runtime_overlay_set_provenance,
    write_apollo_control_runtime_provenance,
)


def test_local_provenance_detects_overlay_source_target_difference(tmp_path: Path) -> None:
    source = tmp_path / "backup" / "lib_control_cmd_proto_mcc_bin.so"
    target = tmp_path / "target" / "lib_control_cmd_proto_mcc_bin.so"
    source.parent.mkdir()
    target.parent.mkdir()
    source.write_bytes(b"\x7fELF source RPATH control command proto")
    target.write_bytes(b"\x7fELF target RUNPATH control command proto with more bytes")
    manifest = tmp_path / "apollo_control_runtime_overlay_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "name": "lib_control_cmd_proto_mcc_bin.so",
                        "source": str(source),
                        "target": str(target),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    report = analyze_apollo_control_runtime_provenance(overlay_manifest=manifest)

    assert report["schema_version"] == "apollo_control_runtime_provenance.v1"
    assert report["overlay_record"]["source"] == str(source)
    assert report["source_target_same_content_current_after_restore"] is False
    assert report["diagnostic_interpretation"]["finding"] == "overlay_source_differs_from_current_target"
    assert report["source_probe"]["elf_path_style"] == "rpath"
    assert report["target_probe_current_after_restore"]["elf_path_style"] == "runpath"
    assert "sha256_differs" in report["source_target_differences"]
    assert "size_differs" in report["source_target_differences"]
    assert "diagnostic_only_not_capability_evidence" in report["warnings"]


def test_provenance_writer_outputs_json_and_summary(tmp_path: Path) -> None:
    source = tmp_path / "source.so"
    target = tmp_path / "target.so"
    source.write_bytes(b"same")
    target.write_bytes(b"same")
    report = analyze_apollo_control_runtime_provenance(
        source_path=source,
        target_path=target,
    )

    outputs = write_apollo_control_runtime_provenance(report, tmp_path / "out")

    report_path = Path(outputs["report"])
    summary_path = Path(outputs["summary"])
    assert report_path.exists()
    assert summary_path.exists()
    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert written["source_target_same_content_current_after_restore"] is True
    assert "Control Cmd Proto Runtime Provenance" in summary_path.read_text(encoding="utf-8")


def test_missing_target_is_insufficient_data(tmp_path: Path) -> None:
    source = tmp_path / "source.so"
    source.write_bytes(b"source")

    report = analyze_apollo_control_runtime_provenance(
        source_path=source,
        target_path=tmp_path / "missing.so",
    )

    assert report["diagnostic_interpretation"]["status"] == "insufficient_data"
    assert report["diagnostic_interpretation"]["finding"] == "source_or_target_missing"
    assert report["target_probe_current_after_restore"]["exists"] is False


def test_docker_probe_can_be_mocked_without_apollo_runtime(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "name": "lib_control_cmd_proto_mcc_bin.so",
                        "source": "/backup/lib_control_cmd_proto_mcc_bin.so",
                        "target": "/live/lib_control_cmd_proto_mcc_bin.so",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    def fake_runner(argv: list[str]) -> subprocess.CompletedProcess[str]:
        command = argv[-1]
        is_source = "/backup/" in command
        stdout = ""
        returncode = 0
        if command.startswith("test -e"):
            stdout = ""
        elif command.startswith("stat"):
            path = "/backup/lib_control_cmd_proto_mcc_bin.so" if is_source else "/live/lib_control_cmd_proto_mcc_bin.so"
            size = 10 if is_source else 20
            stdout = f"{path}|{size}|regular file|555|ubuntu|ubuntu|123\n"
        elif command.startswith("sha256sum"):
            sha = "a" * 64 if is_source else "b" * 64
            path = "/backup/lib_control_cmd_proto_mcc_bin.so" if is_source else "/live/lib_control_cmd_proto_mcc_bin.so"
            stdout = f"{sha}  {path}\n"
        elif command.startswith("file"):
            stdout = "ELF 64-bit LSB shared object\n"
        elif command.startswith("readelf -d"):
            style = "RPATH" if is_source else "RUNPATH"
            stdout = (
                "0x0000000000000001 (NEEDED) Shared library: [libprotobuf.so]\n"
                f"0x000000000000000f ({style}) Library path: [$ORIGIN]\n"
            )
        elif "nm -D" in command or command.startswith("strings"):
            stdout = "ControlCommand\n"
        return subprocess.CompletedProcess(argv, returncode, stdout, "")

    report = analyze_apollo_control_runtime_provenance(
        overlay_manifest=manifest,
        container="apollo",
        runner=fake_runner,
    )

    assert report["container"] == "apollo"
    assert report["source_probe"]["sha256"] == "a" * 64
    assert report["target_probe_current_after_restore"]["sha256"] == "b" * 64
    assert report["source_probe"]["needed_libraries"] == ["libprotobuf.so"]
    assert report["source_probe"]["elf_path_style"] == "rpath"
    assert report["target_probe_current_after_restore"]["elf_path_style"] == "runpath"
    assert report["source_target_same_content_current_after_restore"] is False


def test_overlay_set_provenance_summarizes_all_records(tmp_path: Path) -> None:
    source_a = tmp_path / "backup" / "a.so"
    target_a = tmp_path / "target" / "a.so"
    source_b = tmp_path / "backup" / "b.so"
    target_b = tmp_path / "target" / "b.so"
    source_c = tmp_path / "backup" / "c.so"
    target_c = tmp_path / "target" / "missing.so"
    source_a.parent.mkdir()
    target_a.parent.mkdir()
    source_a.write_bytes(b"same")
    target_a.write_bytes(b"same")
    source_b.write_bytes(b"\x7fELF source RPATH")
    target_b.write_bytes(b"\x7fELF target RUNPATH")
    source_c.write_bytes(b"source only")
    manifest = tmp_path / "apollo_control_runtime_overlay_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "records": [
                    {"name": "a.so", "source": str(source_a), "target": str(target_a)},
                    {"name": "b.so", "source": str(source_b), "target": str(target_b)},
                    {"name": "c.so", "source": str(source_c), "target": str(target_c)},
                ]
            }
        ),
        encoding="utf-8",
    )

    report = analyze_apollo_control_runtime_overlay_set_provenance(overlay_manifest=manifest)

    assert report["schema_version"] == "apollo_control_runtime_overlay_set_provenance.v1"
    assert report["record_count"] == 3
    assert report["summary"]["same_content_count"] == 1
    assert report["summary"]["different_content_count"] == 1
    assert report["summary"]["missing_pair_count"] == 1
    assert report["summary"]["records_with_elf_path_style_diff"] == 1
    assert report["diagnostic_interpretation"]["status"] == "insufficient_data"
    assert report["diagnostic_interpretation"]["finding"] == "overlay_set_partially_missing"


def test_overlay_set_provenance_writer_outputs_json_and_summary(tmp_path: Path) -> None:
    source = tmp_path / "source.so"
    target = tmp_path / "target.so"
    source.write_bytes(b"same")
    target.write_bytes(b"same")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"records": [{"name": "source.so", "source": str(source), "target": str(target)}]}),
        encoding="utf-8",
    )
    report = analyze_apollo_control_runtime_overlay_set_provenance(overlay_manifest=manifest)

    outputs = write_apollo_control_runtime_overlay_set_provenance(report, tmp_path / "out")

    report_path = Path(outputs["report"])
    summary_path = Path(outputs["summary"])
    assert report_path.exists()
    assert summary_path.exists()
    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert written["summary"]["same_content_count"] == 1
    assert "Apollo Control Runtime Overlay Set Provenance" in summary_path.read_text(encoding="utf-8")
