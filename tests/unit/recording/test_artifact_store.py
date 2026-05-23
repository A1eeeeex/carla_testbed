from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from carla_testbed.record.artifact_store import RunArtifactStore, build_manifest, build_summary


def test_artifact_store_writes_standard_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "smoke"
    store = RunArtifactStore(run_dir).ensure()

    store.write_manifest(
        build_manifest(
            run_id="smoke",
            start_time_wall_s=12.5,
            config_path="configs/examples/smoke.yaml",
            carla_host="localhost",
            carla_port=2000,
            carla_town="Town01",
            scenario_name="smoke_empty",
            backend_name="dummy",
            metadata={"profile": "unit"},
        )
    )
    store.write_resolved_config(
        {
            "run": {"id": "smoke", "output_root": tmp_path / "runs"},
            "backend": {"name": "dummy"},
        }
    )
    store.write_summary(
        build_summary(
            success=True,
            exit_reason="success",
            frames=3,
            sim_duration_s=0.15,
            wall_duration_s=0.2,
            cleanup_errors_count=0,
        )
    )

    events = store.open_events()
    events.append({"event_type": "run_start", "frame": 0})
    events.append({"event_type": "run_end", "frame": 3})
    events.close()

    timeseries = store.open_timeseries_jsonl()
    timeseries.append({"frame": 1, "speed_mps": 0.0})
    timeseries.close()

    assert (run_dir / "manifest.json").is_file()
    assert (run_dir / "config.resolved.yaml").is_file()
    assert (run_dir / "summary.json").is_file()
    assert (run_dir / "events.jsonl").is_file()
    assert (run_dir / "timeseries.jsonl").is_file()
    assert (run_dir / "logs").is_dir()

    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["run_id"] == "smoke"
    assert manifest["carla"] == {"host": "localhost", "port": 2000, "town": "Town01"}
    assert manifest["scenario_name"] == "smoke_empty"
    assert manifest["backend_name"] == "dummy"

    resolved = yaml.safe_load((run_dir / "config.resolved.yaml").read_text())
    assert resolved["run"]["output_root"].endswith("/runs")

    summary = json.loads((run_dir / "summary.json").read_text())
    assert summary["success"] is True
    assert summary["frames"] == 3

    event_lines = [json.loads(line) for line in (run_dir / "events.jsonl").read_text().splitlines()]
    assert [line["event_type"] for line in event_lines] == ["run_start", "run_end"]


def test_update_manifest_preserves_existing_fields(tmp_path: Path) -> None:
    store = RunArtifactStore(tmp_path / "run").ensure()
    store.write_manifest(build_manifest(run_id="case", start_time_wall_s=1.0))

    manifest = store.update_manifest({"end_time_wall_s": 2.0})

    assert manifest["run_id"] == "case"
    assert manifest["start_time_wall_s"] == 1.0
    assert manifest["end_time_wall_s"] == 2.0


def test_jsonl_writer_rejects_append_after_close(tmp_path: Path) -> None:
    writer = RunArtifactStore(tmp_path / "run").ensure().open_events()
    writer.append({"event_type": "run_start"})
    writer.close()

    with pytest.raises(RuntimeError):
        writer.append({"event_type": "late_event"})
