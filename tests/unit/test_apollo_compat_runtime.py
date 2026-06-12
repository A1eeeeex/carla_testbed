from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import yaml

from carla_testbed.config import load_config
from carla_testbed.runtime import apollo_compat


def _write_transition_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "town01_apollo_route_only_claim_probe.yaml"
    config_path.write_text(
        "\n".join(
            [
                "run:",
                "  id: claim_runtime",
                "  max_ticks: 3",
                "  fixed_dt_s: 0.05",
                f"  output_root: {tmp_path.as_posix()}",
                "  claim_profile: true",
                "  profile_name: unit_claim_transition",
                "sim:",
                "  town: Town01",
                "scenario:",
                "  name: carla_town01_route_health",
                "  driver: carla_town01_route_health",
                "  route_health:",
                "    route_id: town01_rh_spawn097_goal046",
                "backend:",
                "  name: apollo_cyberrt",
                "algo:",
                "  stack: apollo",
                "  apollo:",
                "    transport_mode: ros2_gt",
                "io:",
                "  mode: ros2_native",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def test_typed_transition_backend_uses_resolved_config_and_preserves_reports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_transition_config(tmp_path)
    cfg = load_config(config_path)
    run_dir = tmp_path / "claim_transition"
    invoked: dict[str, str] = {}

    def fake_invoke(effective_path: Path, root: Path) -> int:
        invoked["effective_path"] = str(effective_path)
        payload = yaml.safe_load(effective_path.read_text(encoding="utf-8"))
        assert payload["typed_runtime"]["runtime_dispatch_kind"] == "typed_apollo_claim_runtime"
        assert payload["typed_runtime"]["legacy_fallback_used"] is False
        assert payload["scenario"]["driver"] == "carla_town01_route_health"
        assert payload["run"]["ticks"] == 3
        report_path = root / "analysis" / "routing_response_decoded" / "routing_response_decoded_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "schema_version": "routing_response_decoded.v1",
                    "status": "pass",
                    "lane_segment_count": 1,
                    "lane_segments": [{"lane_id": "lane_1", "length_m": 42.0}],
                    "total_length_m": 42.0,
                }
            ),
            encoding="utf-8",
        )
        (root / "summary.json").write_text(
            json.dumps({"success": True, "exit_reason": "fake_transition_ok", "frames": 1}),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(apollo_compat, "_invoke_tbio_transition_backend", fake_invoke)

    result = apollo_compat.run_compat_apollo_cyber_gt_runtime(
        cfg,
        config_path=config_path,
        run_dir=run_dir,
        resolved_config=dataclasses.asdict(cfg),
    )

    assert result.exit_code == 0
    assert "typed_apollo_claim_runtime" in result.message
    assert Path(invoked["effective_path"]).name == "typed_runtime.effective_legacy.yaml"
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    boundary = json.loads(
        (run_dir / "analysis" / "runtime_claim_boundary" / "runtime_claim_boundary_report.json").read_text(
            encoding="utf-8"
        )
    )
    routing = json.loads(
        (
            run_dir
            / "analysis"
            / "routing_response_decoded"
            / "routing_response_decoded_report.json"
        ).read_text(encoding="utf-8")
    )
    assert manifest["runtime_dispatch_kind"] == "typed_apollo_claim_runtime"
    assert manifest["legacy_fallback_used"] is False
    assert manifest["compat_layers"] == ["ros2_gt_transition", "legacy_route_health_transition"]
    assert manifest["transport_mode"] == "apollo_cyberrt_gt_over_ros2_transition"
    assert manifest["typed_runtime_effective_config_path"] == "typed_runtime.effective_legacy.yaml"
    assert summary["runtime_dispatch_kind"] == "typed_apollo_claim_runtime"
    assert summary["can_claim_unassisted_natural_driving"] is False
    assert boundary["status"] == "pass"
    assert routing["status"] == "pass"
    assert routing["lane_segment_count"] == 1


def test_transition_python_resolver_does_not_use_unexpanded_placeholder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_python = tmp_path / "carla16" / "bin" / "python"
    fake_python.parent.mkdir(parents=True)
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    effective = tmp_path / "typed_runtime.effective_legacy.yaml"
    effective.write_text(
        "\n".join(
            [
                "algo:",
                "  apollo:",
                "    docker:",
                "      host_python_exec: ${CARLA16_PYTHON}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("CARLA16_PYTHON", raising=False)
    monkeypatch.setattr(
        apollo_compat,
        "_is_executable_file",
        lambda candidate: str(candidate) == fake_python.as_posix(),
    )
    monkeypatch.setattr(
        apollo_compat,
        "_expand_runtime_python_candidate",
        lambda raw: fake_python.as_posix()
        if str(raw or "") == "/home/ubuntu/miniconda3/envs/carla16/bin/python"
        else "",
    )

    resolved = apollo_compat._resolve_transition_python(effective)

    assert resolved == fake_python.as_posix()
    assert "${CARLA16_PYTHON}" not in resolved


def test_transition_backend_invocation_does_not_prewrite_target_run_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    effective = tmp_path / "staging" / "typed_runtime.effective_legacy.yaml"
    effective.parent.mkdir(parents=True)
    effective.write_text(
        "\n".join(
            [
                "run:",
                "  map: Town01",
                "runtime:",
                "  carla:",
                "    start: false",
                "",
            ]
        ),
        encoding="utf-8",
    )
    run_dir = tmp_path / "online_run"
    captured: dict[str, list[str]] = {}

    class _Completed:
        returncode = 0

    def fake_run(command, **_kwargs):
        captured["command"] = list(command)
        assert not run_dir.exists()
        return _Completed()

    monkeypatch.setattr(apollo_compat, "_resolve_transition_python", lambda _path: fake_python.as_posix())
    monkeypatch.setattr(apollo_compat.subprocess, "run", fake_run)

    rc = apollo_compat._invoke_tbio_transition_backend(effective, run_dir)

    assert rc == 0
    assert not run_dir.exists()
    assert "examples.run_followstop" in captured["command"]
    assert "tbio.scripts.run" not in captured["command"]
    runtime = yaml.safe_load((effective.parent / "typed_transition_runtime.json").read_text(encoding="utf-8"))
    assert runtime["transition_entrypoint"] == "examples.run_followstop"
