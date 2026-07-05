from __future__ import annotations

import json
from pathlib import Path

from tools import run_town01_capability_online_chain as chain


def _manifest(path: Path) -> Path:
    routes = path / "routes.txt"
    routes.write_text("town01_rh_spawn097_goal046\n", encoding="utf-8")
    overrides = path / "overrides.txt"
    overrides.write_text("", encoding="utf-8")
    manifest = path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "capability_profile": "lane_keep",
                        "seed_route_ids_file": str(routes),
                        "route_ids_file": str(routes),
                        "overrides_file": str(overrides),
                        "seed_comparison_label": "lane_keep_seed",
                        "comparison_label": "lane_keep_full",
                        "ticks": 100,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return manifest


def test_chain_plan_preserves_existing_carla_without_force_fresh(tmp_path: Path) -> None:
    plan = chain.build_online_chain_plan(
        manifest_path=_manifest(tmp_path),
        route_steps=[("lane_keep", "town01_rh_spawn097_goal046")],
        capability_profiles=[],
        mode="seed",
        config_path=None,
        batch_root_parent=tmp_path / "runs",
        comparison_label_suffix="reuse",
        startup_profile="render_offscreen_no_ros2",
        ticks_override=50,
        post_fail_steps_override=None,
        launch_attempts=1,
        world_ready_timeout_sec=10.0,
        retry_delay_sec=1.0,
        keep_carla_alive_at_end=True,
        preserve_existing_carla_at_end=True,
        prewarmed_carla=False,
    )

    assert len(plan) == 1
    assert plan[0]["force_fresh_start"] is False
    assert plan[0]["stop_carla_on_exit"] is False
    assert "--carla-force-fresh-start" not in plan[0]["command_argv"]
    assert "--stop-carla-on-exit" not in plan[0]["command_argv"]


def test_existing_carla_probe_uses_carla16_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        chain,
        "_probe_existing_carla_world",
        lambda *_args, **_kwargs: {
            "ready": False,
            "town": "",
            "error": "ModuleNotFoundError(\"No module named 'carla'\")",
        },
    )
    monkeypatch.setattr(
        chain,
        "_detect_existing_ready_carla_with_carla16",
        lambda *, host, port: {
            "host": host,
            "port": port,
            "ready": True,
            "town": "Town01",
            "error": "",
            "probe_source": "carla16_subprocess",
        },
    )

    result = chain._detect_existing_ready_carla(host="127.0.0.1", port=2000)

    assert result["ready"] is True
    assert result["town"] == "Town01"
    assert result["probe_source"] == "carla16_subprocess"


def test_phase1_postprocess_uses_latest_effective_run(tmp_path: Path, monkeypatch) -> None:
    batch_root = tmp_path / "batch"
    run_dir = batch_root / "route" / "route__01"
    run_dir.mkdir(parents=True)
    (run_dir / "summary.json").write_text("{}", encoding="utf-8")
    (batch_root / "LATEST.txt").write_text(str(run_dir), encoding="utf-8")

    calls: list[Path] = []

    def fake_postprocess(path: Path):
        calls.append(Path(path))
        return {
            "phase1_status": "failed",
            "apollo_hdmap_projection_status": "pass",
            "apollo_hdmap_projection_auto_export_status": "pass",
            "apollo_reference_line_contract_status": "pass",
            "apollo_link_health_primary_blocker": "control_mapping_apply:bridge_longitudinal_policy",
        }

    import carla_testbed.analysis.phase1_postprocess as phase1_postprocess

    monkeypatch.setattr(phase1_postprocess, "run_phase1_postprocess", fake_postprocess)

    report = chain._postprocess_phase1_runs_for_item(
        {"command_argv": ["python", "runner.py", "--batch-root", str(batch_root)]}
    )

    assert calls == [run_dir]
    assert report["status"] == "pass"
    assert report["run_count"] == 1
    assert report["reports"][0]["postprocess_status"] == "pass"
    assert report["reports"][0]["phase1_status"] == "failed"
    assert report["reports"][0]["apollo_hdmap_projection_status"] == "pass"
    assert report["reports"][0]["apollo_hdmap_projection_auto_export_status"] == "pass"
    persisted = json.loads(
        (batch_root / "artifacts" / "online_chain_phase1_postprocess.json").read_text(
            encoding="utf-8"
        )
    )
    assert persisted == report


def test_phase1_postprocess_reports_missing_batch_root() -> None:
    report = chain._postprocess_phase1_runs_for_item({"command_argv": ["python", "runner.py"]})

    assert report["status"] == "skipped"
    assert report["reason"] == "batch_root_missing"
