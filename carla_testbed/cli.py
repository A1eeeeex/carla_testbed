from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.config import ConfigError, TestbedConfig, load_config
from carla_testbed.analysis.artifact_completeness import check_run_artifact_completeness
from carla_testbed.analysis.route_health_report import route_health_inspect_summary
from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.contracts import EgoState, FrameStamp, SceneTruth
from carla_testbed.control import DummyController
from carla_testbed.doctor import doctor_main
from carla_testbed.evidence import (
    build_and_write_evidence_bundle,
    package_run_evidence,
    run_and_write_gate,
)
from carla_testbed.platform.compiler import (
    PlatformCompileError,
    compile_run_plan,
    compile_suite_matrix,
    plan_to_yaml,
    write_run_plan,
)
from carla_testbed.platform.executor import execute_run_plan
from carla_testbed.platform.phase1_pair_runner import run_phase1_pair
from carla_testbed.platform.plan import RunPlan
from carla_testbed.platform.registry import PlatformRegistry, PlatformRegistryError
from carla_testbed.record import RunArtifactStore, build_manifest, build_summary
from carla_testbed.record.registry import default_recorder_registry
from carla_testbed.runtime.apollo_compat import run_compat_apollo_cyber_gt_runtime
from carla_testbed.scenario_player.manifest_contract import fixed_scene_manifest_fields_from_template_path
from carla_testbed.utils.env import resolve_repo_root

try:
    from dotenv import load_dotenv as _load_dotenv
except Exception:  # python-dotenv is optional
    _load_dotenv = None


def _load_env_file(env_path: Path, *, protected_keys: set[str], override_loaded: bool = False) -> None:
    if not env_path.exists():
        return
    if _load_dotenv is not None:
        _load_dotenv(env_path, override=override_loaded)
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        if key in protected_keys:
            continue
        if (not override_loaded) and key in os.environ:
            continue
        os.environ[key] = value


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="carla_testbed", description="CARLA testbed CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="run a configured scenario or dispatch a legacy run config")
    run_p.add_argument("--config", type=Path)
    run_p.add_argument("--plan", type=Path, help="resolved run_plan.v1 YAML")
    run_p.add_argument("--override", action="append", default=[])
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--plan-only", action="store_true")
    run_p.add_argument("--legacy-dispatch", action="store_true")
    run_p.add_argument("--print-effective-config", action="store_true")
    run_p.add_argument("--run-dir", type=Path)
    run_p.add_argument("--log-level", default=None)
    run_p.add_argument("--no-healthcheck", action="store_true")
    run_p.add_argument("--follow-logs", action="store_true")
    run_p.add_argument("--compose-clean", action="store_true")
    run_p.add_argument("--timeout-s", type=float, default=None)

    smoke_p = sub.add_parser("smoke", help="CI-friendly config smoke; does not start CARLA/Apollo")
    smoke_p.add_argument("--config", required=True, type=Path)
    smoke_p.add_argument("--run-dir", type=Path)

    validate_p = sub.add_parser("config-validate", help="validate a typed v0 config")
    validate_p.add_argument("path", type=Path)

    inspect_p = sub.add_parser("inspect-run", help="inspect manifest.json and summary.json in a run dir")
    inspect_p.add_argument("output_dir", type=Path)

    sub.add_parser("doctor", help="环境检查")

    list_p = sub.add_parser("list", help="list platform profile registry entries")
    list_p.add_argument(
        "kind",
        choices=(
            "platforms",
            "algorithms",
            "scenarios",
            "recording",
            "recorders",
            "traffic",
            "gates",
            "suites",
            "fixed-scenes",
            "backends",
        ),
    )
    list_p.add_argument("--json", action="store_true", help="emit JSON instead of text")

    plan_p = sub.add_parser("plan", help="compile an offline RunPlan; does not start runtime")
    plan_p.add_argument("--request", type=Path, help="run_request.v1 YAML with include profiles")
    plan_p.add_argument("--platform")
    plan_p.add_argument("--algorithm")
    plan_p.add_argument("--scenario")
    plan_p.add_argument("--traffic", default="none")
    plan_p.add_argument("--record", "--recording", dest="recording", default="none")
    plan_p.add_argument("--gate", default="smoke")
    plan_p.add_argument("--suite", type=Path, help="compile suite matrix instead of a single run")
    plan_p.add_argument("--out", type=Path, help="output plan YAML path or directory for suite matrix")
    plan_p.add_argument("--print", action="store_true", dest="print_plan")
    plan_p.add_argument("--show-launch", action="store_true", help="include backend LaunchPlan preview")

    suite_p = sub.add_parser("suite", help="compile or dry-run a RunPlan suite matrix")
    suite_p.add_argument("action", nargs="?", default="dry-run", choices=("dry-run", "run"))
    suite_p.add_argument("--suite", required=True, type=Path)
    suite_p.add_argument("--out", required=True, type=Path)
    suite_p.add_argument("--dry-run", action="store_true")
    suite_p.add_argument("--legacy-dispatch", action="store_true")

    analyze_p = sub.add_parser("analyze", help="build evidence bundle and gate report for a run dir")
    analyze_p.add_argument("--run-dir", required=True, type=Path)
    analyze_p.add_argument("--plan", type=Path)
    analyze_p.add_argument("--gate", help="gate profile name; defaults to plan gate if a plan is provided")
    analyze_p.add_argument("--out", type=Path, help="analysis output root; defaults to run_dir/analysis")

    pack_p = sub.add_parser("pack", help="package run evidence for review")
    pack_p.add_argument("--run-dir", required=True, type=Path)
    pack_p.add_argument("--out", required=True, type=Path)
    pack_p.add_argument("--profile", default="claim", choices=("metrics", "debug", "demo", "claim"))
    pack_p.add_argument("--include-large-artifacts", action="store_true")

    phase1_p = sub.add_parser("phase1", help="Phase 1 scenario-platform helpers")
    phase1_sub = phase1_p.add_subparsers(dest="phase1_cmd", required=True)
    pair_p = phase1_sub.add_parser("run-pair", help="run PlanningControlBackend and ApolloBackend for one scenario")
    pair_p.add_argument("--scenario", required=True, help="scenario registry name or YAML path")
    pair_p.add_argument("--out", required=True, type=Path)
    pair_p.add_argument("--pair-id", default=None)
    pair_p.add_argument("--apollo-profile", default="apollo/apollo10_carla_gt")
    pair_p.add_argument("--planning-profile", default="builtin/simple_acc_route_follower")
    pair_p.add_argument("--apollo-platform", default="apollo_cyberrt")
    pair_p.add_argument("--planning-platform", default="carla_builtin")
    pair_p.add_argument("--record", "--recording", dest="recording", default="metrics")
    pair_p.add_argument("--gate", default="scenario_validation")
    pair_p.add_argument("--dry-run", action="store_true")
    pair_p.add_argument("--timeout-s", type=float, default=None)
    return ap


def _parse_override_pairs(pairs: list[str] | None) -> dict[str, Any]:
    import yaml

    out: dict[str, Any] = {}
    for item in pairs or []:
        if "=" not in item:
            raise ConfigError(f"override must use key=value form: {item}")
        key, value = item.split("=", 1)
        cursor = out
        parts = [part for part in key.split(".") if part]
        if not parts:
            raise ConfigError(f"override key is empty: {item}")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = yaml.safe_load(value)
    return out


def _config_to_dict(cfg: TestbedConfig) -> dict[str, Any]:
    payload = dataclasses.asdict(cfg)
    if cfg.source_path is not None:
        payload["source_path"] = str(cfg.source_path)
    return payload


def _run_dir_for_config(cfg: TestbedConfig, explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit
    return Path(cfg.run.output_root) / cfg.run.id


def _print_config_loaded(cfg: TestbedConfig, *, prefix: str = "config") -> None:
    print(
        f"[{prefix}] ok path={cfg.source_path} run_id={cfg.run.id} "
        f"scenario={cfg.scenario.name} backend={cfg.backend.name} "
        f"town={cfg.sim.town} max_ticks={cfg.run.max_ticks}"
    )


def _cmd_config_validate(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.path)
    except ConfigError as exc:
        print(f"[config-validate] failed: {exc}", file=sys.stderr)
        return 2
    _print_config_loaded(cfg, prefix="config-validate")
    return 0


def _cmd_smoke(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"[smoke] config failed: {exc}", file=sys.stderr)
        return 2

    start_wall_time_s = time.time()
    run_dir = _run_dir_for_config(cfg, args.run_dir)
    store = RunArtifactStore(run_dir).ensure()
    store.write_manifest(
        build_manifest(
            run_id=cfg.run.id,
            start_time_wall_s=start_wall_time_s,
            config_path=args.config,
            carla_host=cfg.sim.host,
            carla_port=cfg.sim.port,
            carla_town=cfg.sim.town,
            scenario_name=cfg.scenario.name,
            backend_name=cfg.backend.name,
            metadata={"mode": "smoke-config", "starts_carla": False, "starts_apollo": False},
        )
    )
    store.write_resolved_config(_config_to_dict(cfg))
    events = store.open_events()
    events.append({"event_type": "smoke_start", "run_id": cfg.run.id, "wall_time_s": start_wall_time_s})

    frame = FrameStamp(frame_id=0, sim_time_s=0.0)
    controller = DummyController()
    command = controller.step(frame, EgoState(stamp=frame), SceneTruth(stamp=frame))
    command.validate()

    end_wall_time_s = time.time()
    summary = build_summary(
        success=True,
        exit_reason="smoke_config_ok",
        frames=0,
        sim_duration_s=0.0,
        wall_duration_s=end_wall_time_s - start_wall_time_s,
        cleanup_errors_count=0,
        metadata={
            "mode": "smoke-config",
            "dummy_control": command.to_dict(),
            "starts_carla": False,
            "starts_apollo": False,
        },
    )
    events.append({"event_type": "smoke_end", "success": True, "wall_time_s": end_wall_time_s})
    events.close()
    store.update_manifest({"end_time_wall_s": end_wall_time_s})
    store.write_summary(summary)
    print(f"[smoke] ok run_dir={run_dir} scenario={cfg.scenario.name} backend={cfg.backend.name}")
    print("[smoke] no CARLA/Apollo runtime was started")
    return 0


def _cmd_inspect_run(args: argparse.Namespace) -> int:
    run_dir = args.output_dir
    manifest_path = run_dir / "manifest.json"
    summary_path = run_dir / "summary.json"
    manifest = _read_json_optional(manifest_path)
    summary = _read_json_optional(summary_path)
    if manifest is None and summary is None:
        print(f"[inspect-run] no manifest.json or summary.json found in {run_dir}", file=sys.stderr)
        return 2

    run_id = (manifest or {}).get("run_id") or run_dir.name
    scenario = (manifest or {}).get("scenario_name")
    backend = (manifest or {}).get("backend_name")
    success = (summary or {}).get("success")
    exit_reason = (summary or {}).get("exit_reason") or (summary or {}).get("fail_reason")
    frames = (summary or {}).get("frames")
    metrics = (summary or {}).get("metrics") or {}

    print(f"[inspect-run] run_id={run_id} path={run_dir}")
    if scenario or backend:
        print(f"[inspect-run] scenario={scenario or 'unknown'} backend={backend or 'unknown'}")
    if summary is not None:
        print(f"[inspect-run] success={success} exit_reason={exit_reason} frames={frames}")
        if metrics:
            print(
                "[inspect-run] metrics "
                f"avg_speed_mps={metrics.get('avg_speed_mps')} "
                f"max_speed_mps={metrics.get('max_speed_mps')} "
                f"collision_count={metrics.get('collision_count')}"
            )
    carla_world = (manifest or {}).get("carla_world") or (summary or {}).get("carla_world")
    if isinstance(carla_world, Mapping):
        print(
            "[inspect-run] carla_world "
            f"configured_town={carla_world.get('configured_town')} "
            f"loaded_map_name={carla_world.get('loaded_map_name')} "
            f"matches_configured_town={carla_world.get('matches_configured_town')} "
            f"spawn_point_count={carla_world.get('spawn_point_count')} "
            f"source={carla_world.get('source')}"
        )
    route_health = route_health_inspect_summary(run_dir)
    if route_health is not None:
        print(
            "[inspect-run] route_health "
            f"status={route_health.get('status')} "
            f"route_id={route_health.get('route_id')} "
            f"lateral_error_max_m={route_health.get('lateral_error_max_m')} "
            f"heading_error_max_rad={route_health.get('heading_error_max_rad')} "
            f"curve_segments_count={route_health.get('curve_segments_count')} "
            f"missing_inputs_count={route_health.get('missing_inputs_count')} "
            f"missing_fields_count={route_health.get('missing_fields_count')}"
        )
    artifact_completeness = check_run_artifact_completeness(run_dir)
    print(
        "[inspect-run] artifact_completeness "
        f"status={artifact_completeness.get('status')} "
        f"scenario_class={artifact_completeness.get('scenario_class') or 'unknown'} "
        f"missing_artifacts_count={len(artifact_completeness.get('missing_artifacts') or [])} "
        f"missing_manifest_fields_count={len(artifact_completeness.get('missing_manifest_fields') or [])} "
        f"invalid_manifest_source_fields_count="
        f"{len(artifact_completeness.get('invalid_manifest_source_fields') or [])} "
        f"invalid_report_source_fields_count="
        f"{len(artifact_completeness.get('invalid_report_source_fields') or [])} "
        f"missing_control_trace_fields_count="
        f"{len(artifact_completeness.get('missing_control_trace_fields') or [])}"
    )
    missing_artifacts = artifact_completeness.get("missing_artifacts") or []
    if missing_artifacts:
        print(f"[inspect-run] missing_artifacts={','.join(missing_artifacts)}")
    missing_raw_artifacts = artifact_completeness.get("missing_raw_evidence_artifacts") or []
    if missing_raw_artifacts:
        print(f"[inspect-run] missing_raw_evidence_artifacts={','.join(missing_raw_artifacts)}")
    missing_manifest_fields = artifact_completeness.get("missing_manifest_fields") or []
    if missing_manifest_fields:
        print(f"[inspect-run] missing_manifest_fields={','.join(missing_manifest_fields)}")
    invalid_manifest_source_fields = artifact_completeness.get("invalid_manifest_source_fields") or []
    if invalid_manifest_source_fields:
        print(f"[inspect-run] invalid_manifest_source_fields={','.join(invalid_manifest_source_fields)}")
    invalid_report_source_fields = artifact_completeness.get("invalid_report_source_fields") or []
    if invalid_report_source_fields:
        print(f"[inspect-run] invalid_report_source_fields={','.join(invalid_report_source_fields)}")
    missing_control_trace_fields = artifact_completeness.get("missing_control_trace_fields") or []
    if missing_control_trace_fields:
        print(f"[inspect-run] missing_control_trace_fields={','.join(missing_control_trace_fields)}")
    blocker_stack = _inspect_run_blocker_stack(run_dir)
    first_blocker = _inspect_first_blocker(blocker_stack)
    print(f"[inspect-run] blocker_stack={','.join(item['name'] for item in blocker_stack)}")
    print(f"[inspect-run] first_blocker={first_blocker or 'none'}")
    for item in blocker_stack:
        print(
            "[inspect-run] blocker_stage "
            f"name={item['name']} status={item['status']} "
            f"reason={item['reason'] or 'none'}"
        )
    return 0


def _inspect_run_blocker_stack(run_dir: Path) -> list[dict[str, str]]:
    artifacts = run_dir / "artifacts"
    analysis = run_dir / "analysis"
    cyber_stats = _read_json_optional(artifacts / "cyber_bridge_stats.json") or {}
    planning_debug = _read_json_optional(artifacts / "planning_topic_debug_summary.json") or {}
    return [
        _inspect_goal_stage(artifacts / "goal_validity_report.json"),
        _inspect_map_stage(artifacts),
        _inspect_routing_stage(artifacts, cyber_stats),
        _inspect_hdmap_projection_stage(run_dir),
        _inspect_planning_stage(analysis, planning_debug),
        _inspect_control_stage(analysis, cyber_stats),
        _inspect_attribution_stage(analysis, run_dir),
    ]


def _inspect_goal_stage(path: Path) -> dict[str, str]:
    report = _read_json_optional(path)
    if report is None:
        return _inspect_stage("goal", "insufficient_data", "goal_validity_report_missing")
    invalid = report.get("invalid_goal") is True
    status = str(report.get("status") or "").strip().lower()
    fallback_applied = report.get("fallback_applied") is True
    if status == "fail" or invalid:
        return _inspect_stage(
            "goal",
            "fail",
            str(report.get("invalid_goal_reason") or "invalid_goal"),
        )
    if fallback_applied and bool(report.get("claim_profile_enabled") or report.get("materialization_probe_enabled")):
        return _inspect_stage("goal", "fail", "fallback_route_applied_in_claim_profile")
    return _inspect_stage("goal", status or "pass", "")


def _inspect_map_stage(artifacts: Path) -> dict[str, str]:
    report = _read_json_optional(artifacts / "map_contract_guard.json") or _read_json_optional(
        artifacts / "stage5_map_contract_guard.json"
    )
    if report is None:
        return _inspect_stage("map", "insufficient_data", "map_contract_guard_missing")
    if report.get("map_contract_invalid") is True or report.get("high_risk_mismatch") is True:
        reasons = [str(item) for item in (report.get("mismatch_reasons") or []) if item]
        reason = reasons[0] if reasons else str(report.get("mismatch_classification") or "map_contract_invalid")
        return _inspect_stage("map", "fail", reason)
    return _inspect_stage("map", "pass", "")


def _inspect_routing_stage(artifacts: Path, cyber_stats: Mapping[str, Any]) -> dict[str, str]:
    decoded = _read_json_optional(artifacts / "routing_response_decoded.json")
    lane_segments = decoded.get("lane_segments") if isinstance(decoded, Mapping) else None
    if isinstance(lane_segments, list) and lane_segments:
        return _inspect_stage("routing", "pass", "")
    request_count = _numeric(cyber_stats.get("routing_request_count"))
    response_count = _numeric(cyber_stats.get("routing_response_count"))
    success_count = _numeric(cyber_stats.get("routing_success_count"))
    if request_count is not None and request_count < 1:
        return _inspect_stage("routing", "fail", "routing_request_missing")
    if response_count is not None and response_count < 1:
        return _inspect_stage("routing", "fail", "routing_response_missing")
    if success_count is not None and success_count < 1:
        return _inspect_stage("routing", "fail", "routing_success_missing")
    return _inspect_stage("routing", "insufficient_data", "routing_response_decoded_missing")


def _inspect_hdmap_projection_stage(run_dir: Path) -> dict[str, str]:
    report = _read_json_optional(
        run_dir / "analysis" / "apollo_hdmap_projection" / "apollo_hdmap_projection_report.json"
    )
    raw = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    if report is None and not raw.exists():
        return _inspect_stage("hdmap_projection", "insufficient_data", "apollo_hdmap_projection_missing")
    if report is None:
        return _inspect_stage("hdmap_projection", "warn", "apollo_hdmap_projection_raw_unanalyzed")
    status = str(report.get("status") or report.get("verdict") or "insufficient_data").strip().lower()
    reasons = report.get("blocking_reasons") or report.get("errors") or []
    reason = str(reasons[0]) if reasons else ""
    return _inspect_stage("hdmap_projection", status or "insufficient_data", reason)


def _inspect_planning_stage(analysis: Path, planning_debug: Mapping[str, Any]) -> dict[str, str]:
    report = _read_json_optional(analysis / "planning_materialization" / "planning_materialization_report.json")
    if report is not None:
        status = str(report.get("verdict") or report.get("status") or "insufficient_data").strip().lower()
        reasons = [str(item) for item in (report.get("blocking_reasons") or []) if item]
        reason = reasons[0] if reasons else str(report.get("materialization_status") or "")
        return _inspect_stage("planning", status or "insufficient_data", reason)
    total = _numeric(planning_debug.get("total_messages_received"))
    nonempty = _numeric(planning_debug.get("messages_with_nonzero_trajectory_points"))
    if total is None:
        return _inspect_stage("planning", "insufficient_data", "planning_materialization_missing")
    if total > 0 and (nonempty or 0) < 1:
        return _inspect_stage("planning", "fail", "planning_trajectory_empty")
    return _inspect_stage("planning", "pass", "")


def _inspect_control_stage(analysis: Path, cyber_stats: Mapping[str, Any]) -> dict[str, str]:
    report = _read_json_optional(analysis / "apollo_control_handoff" / "apollo_control_handoff_report.json")
    if report is not None:
        status = str(report.get("verdict") or report.get("status") or "insufficient_data").strip().lower()
        reasons = [str(item) for item in (report.get("blocking_reasons") or []) if item]
        return _inspect_stage("control", status or "insufficient_data", reasons[0] if reasons else "")
    control_rx = _numeric(cyber_stats.get("control_rx_count"))
    control_tx = _numeric(cyber_stats.get("control_tx_count"))
    if control_rx is not None and control_rx < 1:
        return _inspect_stage("control", "fail", "control_rx_missing")
    if control_tx is not None and control_tx < 1:
        return _inspect_stage("control", "fail", "control_tx_missing")
    return _inspect_stage("control", "insufficient_data", "apollo_control_handoff_report_missing")


def _inspect_attribution_stage(analysis: Path, run_dir: Path) -> dict[str, str]:
    report = _read_json_optional(analysis / "control_attribution" / "control_attribution_report.json")
    if report is not None:
        verdict = report.get("verdict")
        status = verdict.get("status") if isinstance(verdict, Mapping) else verdict
        status = str(status or report.get("status") or "insufficient_data").strip().lower()
        reasons = [str(item) for item in (report.get("blocking_reasons") or []) if item]
        return _inspect_stage("attribution", status or "insufficient_data", reasons[0] if reasons else "")
    if not (run_dir / "artifacts" / "control_apply_trace.jsonl").exists():
        return _inspect_stage("attribution", "insufficient_data", "control_apply_trace_missing")
    return _inspect_stage("attribution", "insufficient_data", "control_attribution_report_missing")


def _inspect_stage(name: str, status: str, reason: str) -> dict[str, str]:
    return {"name": name, "status": status or "insufficient_data", "reason": reason or ""}


def _inspect_first_blocker(stack: list[dict[str, str]]) -> str | None:
    by_name = {item["name"]: item for item in stack}
    goal = by_name.get("goal", {})
    map_stage = by_name.get("map", {})
    if goal.get("reason") == "goal_projection_unavailable" and map_stage.get("status") == "fail":
        return "goal_projection_unavailable/map_contract_invalid"
    for item in stack:
        if item["status"] in {"fail", "insufficient_data"}:
            return f"{item['name']}:{item['reason'] or item['status']}"
    return None


def _numeric(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_json_optional(path: Path) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(data, Mapping):
        raise SystemExit(f"{path}: JSON root must be an object")
    return data


def _config_declares_claim_profile(path: Path | None) -> bool:
    if path is None:
        return False
    path_declares_claim = "claim" in str(path).lower()
    try:
        import yaml

        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except Exception:
        # A malformed claim-profile config must not escape into legacy fallback
        # merely because the lightweight detector could not parse it.
        return path_declares_claim
    if not isinstance(payload, Mapping):
        return path_declares_claim
    run = payload.get("run") if isinstance(payload.get("run"), Mapping) else {}
    profile_name = str(run.get("profile_name") or payload.get("profile_name") or "").lower()
    if bool(run.get("claim_profile") or payload.get("claim_profile")):
        return True
    return path_declares_claim or "claim" in profile_name


def _cmd_run(args: argparse.Namespace) -> int:
    if args.plan:
        try:
            plan = _read_run_plan(args.plan)
        except PlatformCompileError as exc:
            print(f"[run] failed to read plan: {exc}", file=sys.stderr)
            return 2
        backend = default_backend_registry().for_plan(plan)
        preflight = backend.preflight(plan).to_dict()
        launch_plan = backend.build_launch_plan(plan).to_dict()
        print(
            json.dumps(
                {
                    "schema_version": "run_dispatch_preview.v1",
                    "run_id": plan.identity.run_id,
                    "plan": str(args.plan),
                    "backend": plan.platform.name,
                    "plan_only": bool(args.plan_only),
                    "dry_run": bool(args.dry_run),
                    "legacy_dispatch": bool(args.legacy_dispatch),
                    "preflight": preflight,
                    "launch_plan": launch_plan,
                    "legacy_dispatch_hint": dict(backend.legacy_dispatch_hint(plan)),
                },
                indent=2,
                sort_keys=True,
            )
        )
        if args.plan_only:
            return 0
        result = execute_run_plan(
            plan,
            run_dir=args.run_dir,
            dry_run=bool(args.dry_run),
            legacy_dispatch=bool(args.legacy_dispatch),
            timeout_s=args.timeout_s,
        )
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return result.exit_code

    if not args.config:
        print("[run] either --config or --plan is required", file=sys.stderr)
        return 2
    try:
        cfg = load_config(args.config, overrides=_parse_override_pairs(args.override))
    except ConfigError as exc:
        if _config_declares_claim_profile(args.config):
            print(
                f"[run] claim profile typed config load failed; legacy fallback is forbidden: {exc}",
                file=sys.stderr,
            )
            return 2
        if not (args.legacy_dispatch or args.dry_run):
            print(
                f"[run] typed config load failed and --legacy-dispatch was not set: {exc}",
                file=sys.stderr,
            )
            return 2
        print(f"[run] typed config load failed, falling back to legacy runner: {exc}", file=sys.stderr)
        from tbio.scripts.run import main as legacy_run_main

        legacy_run_main(args)
        return 0

    _print_config_loaded(cfg, prefix="run")
    if args.print_effective_config:
        import yaml

        print(yaml.safe_dump(_config_to_dict(cfg), sort_keys=False))
    if args.dry_run:
        print("[run] dry-run ok; typed runner not executed")
        return 0

    if cfg.backend.name == "apollo_cyberrt" and (cfg.run.claim_profile or _config_declares_claim_profile(args.config)):
        run_dir = _run_dir_for_config(cfg, args.run_dir)
        result = run_compat_apollo_cyber_gt_runtime(
            cfg,
            config_path=args.config,
            run_dir=run_dir,
            resolved_config=_config_to_dict(cfg),
            legacy_dispatch_requested=bool(args.legacy_dispatch),
        )
        print(f"[run] {result.message}", file=sys.stderr)
        print(json.dumps({"run_dir": str(result.run_dir), "outputs": result.outputs}, indent=2, sort_keys=True))
        return result.exit_code

    print(
        "[run] typed v0 runner is not wired to CARLA yet. "
        "Use `python -m carla_testbed smoke --config ...` for a no-runtime check, "
        "or use a legacy configs/io config for the existing runner.",
        file=sys.stderr,
    )
    return 2


def _cmd_list(args: argparse.Namespace) -> int:
    if args.kind == "backends":
        registry = default_backend_registry()
        names = registry.names()
        if args.json:
            print(json.dumps([{"name": name} for name in names], indent=2, sort_keys=True))
        else:
            for name in names:
                print(name)
        return 0
    if args.kind == "recorders":
        registry = default_recorder_registry()
        names = registry.names()
        if args.json:
            print(json.dumps([{"name": name} for name in names], indent=2, sort_keys=True))
        else:
            for name in names:
                print(name)
        return 0
    registry = PlatformRegistry(repo_root=resolve_repo_root())
    try:
        entries = registry.list(args.kind)
    except PlatformRegistryError as exc:
        print(f"[list] failed: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(
            json.dumps(
                [
                    {
                        "kind": entry.kind,
                        "name": entry.name,
                        "path": str(entry.path),
                        "schema_version": entry.payload.get("schema_version"),
                    }
                    for entry in entries
                ],
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    for entry in entries:
        print(f"{entry.name}\t{entry.path}")
    return 0


def _cmd_plan(args: argparse.Namespace) -> int:
    registry = PlatformRegistry(repo_root=resolve_repo_root())
    try:
        if args.suite:
            plans = compile_suite_matrix(args.suite, registry=registry)
            if args.out:
                output = args.out
                output.mkdir(parents=True, exist_ok=True)
                for plan in plans:
                    write_run_plan(plan, output / f"{plan.identity.run_id}.plan.resolved.yaml")
            payload = {
                "schema_version": "run_plan_matrix.v1",
                "suite": str(args.suite),
                "plan_count": len(plans),
                "run_ids": [plan.identity.run_id for plan in plans],
                "output_dir": str(args.out) if args.out else None,
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0

        plan = compile_run_plan(
            args.request,
            platform=args.platform,
            algorithm=args.algorithm,
            scenario=args.scenario,
            traffic=args.traffic,
            recording=args.recording,
            gate=args.gate,
            registry=registry,
        )
        if args.out:
            write_run_plan(plan, args.out)
            print(f"[plan] wrote {args.out}")
        if args.show_launch:
            backend = default_backend_registry().for_plan(plan)
            launch = backend.build_launch_plan(plan).to_dict()
            print(
                json.dumps(
                    {
                        "schema_version": "launch_plan_preview.v1",
                        "backend_contract": backend.contract(plan).to_dict(),
                        "phase1_manifest_contract": fixed_scene_manifest_fields_from_template_path(
                            plan.source_profiles.get("scenario")
                        ),
                        "launch_plan": launch,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        if args.print_plan or not args.out:
            print(plan_to_yaml(plan))
        return 0
    except PlatformCompileError as exc:
        print(f"[plan] failed: {exc}", file=sys.stderr)
        return 2


def _cmd_suite(args: argparse.Namespace) -> int:
    registry = PlatformRegistry(repo_root=resolve_repo_root())
    try:
        plans = compile_suite_matrix(args.suite, registry=registry)
    except PlatformCompileError as exc:
        print(f"[suite] failed: {exc}", file=sys.stderr)
        return 2
    out = args.out.expanduser()
    out.mkdir(parents=True, exist_ok=True)
    plan_dir = out / "plans"
    plan_dir.mkdir(parents=True, exist_ok=True)
    for plan in plans:
        write_run_plan(plan, plan_dir / f"{plan.identity.run_id}.plan.resolved.yaml")
    manifest_path = out / "suite_manifest.json"
    matrix_path = out / "run_matrix.csv"
    manifest = {
        "schema_version": "suite_dry_run_manifest.v1",
        "suite": str(args.suite),
        "action": args.action,
        "dry_run": True,
        "legacy_dispatch": bool(args.legacy_dispatch),
        "plan_count": len(plans),
        "plan_dir": str(plan_dir),
        "run_ids": [plan.identity.run_id for plan in plans],
        "starts_runtime": False,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with matrix_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "run_id",
                "suite_id",
                "platform",
                "algorithm",
                "scenario_id",
                "scenario_class",
                "recording",
                "gate",
                "plan_path",
            ],
        )
        writer.writeheader()
        for plan in plans:
            writer.writerow(
                {
                    "run_id": plan.identity.run_id,
                    "suite_id": plan.identity.suite_id,
                    "platform": plan.platform.name,
                    "algorithm": plan.algorithm.variant_id,
                    "scenario_id": plan.scenario.scenario_id,
                    "scenario_class": plan.scenario.scenario_class,
                    "recording": plan.recording.profile,
                    "gate": plan.gate.profile,
                    "plan_path": str(plan_dir / f"{plan.identity.run_id}.plan.resolved.yaml"),
                }
            )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    if args.action == "run" and not (args.dry_run or args.legacy_dispatch):
        print("[suite] real RunPlan suite dispatch is not implemented; dry-run artifacts were written", file=sys.stderr)
        return 2
    return 0


def _cmd_analyze(args: argparse.Namespace) -> int:
    plan = _read_run_plan(args.plan) if args.plan else None
    gate_payload = None
    if args.gate:
        try:
            gate_entry = PlatformRegistry(repo_root=resolve_repo_root()).get("gate", args.gate)
        except PlatformRegistryError as exc:
            print(f"[analyze] failed: {exc}", file=sys.stderr)
            return 2
        gate_payload = {
            "profile": gate_entry.payload.get("name") or args.gate,
            **dict(gate_entry.payload.get("gate") or {}),
        }
    root = args.run_dir.expanduser()
    output_root = args.out.expanduser() if args.out else root / "analysis"
    bundle_paths = build_and_write_evidence_bundle(
        root,
        out_dir=output_root / "evidence_bundle",
        plan=plan,
    )
    gate_paths = run_and_write_gate(
        root,
        out_dir=output_root / "gate",
        plan=plan,
        gate=gate_payload,
    )
    print(json.dumps({"evidence_bundle": bundle_paths, "gate": gate_paths}, indent=2, sort_keys=True))
    return 0


def _cmd_pack(args: argparse.Namespace) -> int:
    result = package_run_evidence(
        args.run_dir,
        out_path=args.out,
        profile=args.profile,
        include_large_artifacts=args.include_large_artifacts,
    )
    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    return 0


def _cmd_phase1(args: argparse.Namespace) -> int:
    if args.phase1_cmd == "run-pair":
        result = run_phase1_pair(
            scenario=args.scenario,
            out_dir=args.out,
            pair_id=args.pair_id,
            apollo_profile=args.apollo_profile,
            planning_profile=args.planning_profile,
            apollo_platform=args.apollo_platform,
            planning_platform=args.planning_platform,
            recording=args.recording,
            gate=args.gate,
            dry_run=bool(args.dry_run),
            timeout_s=args.timeout_s,
            registry=PlatformRegistry(repo_root=resolve_repo_root()),
        )
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return max((item.exit_code for item in result.execution_results), default=0)
    return 2


def _read_run_plan(path: Path | None) -> RunPlan:
    import yaml

    if path is None:
        raise PlatformCompileError("plan path is required")
    try:
        payload = yaml.safe_load(path.expanduser().read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise PlatformCompileError(f"plan file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise PlatformCompileError(f"failed to parse plan YAML {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise PlatformCompileError(f"{path}: root must be a mapping")
    return RunPlan.from_dict(payload)


def main(argv=None) -> int:
    # load env files with a clear priority:
    # process env > .env.<hostname> > .env.local > .env
    repo_root = resolve_repo_root()
    protected = set(os.environ.keys())
    hostname = socket.gethostname().strip()
    candidates = [
        (repo_root / ".env", False),
        (repo_root / ".env.local", True),
    ]
    if hostname:
        candidates.append((repo_root / f".env.{hostname}", True))
    for path, override_loaded in candidates:
        _load_env_file(path, protected_keys=protected, override_loaded=override_loaded)
    argv = argv or sys.argv[1:]
    ap = build_parser()
    args = ap.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args)
    elif args.cmd == "smoke":
        return _cmd_smoke(args)
    elif args.cmd == "config-validate":
        return _cmd_config_validate(args)
    elif args.cmd == "inspect-run":
        return _cmd_inspect_run(args)
    elif args.cmd == "doctor":
        doctor_main()
        return 0
    elif args.cmd == "list":
        return _cmd_list(args)
    elif args.cmd == "plan":
        return _cmd_plan(args)
    elif args.cmd == "suite":
        return _cmd_suite(args)
    elif args.cmd == "analyze":
        return _cmd_analyze(args)
    elif args.cmd == "pack":
        return _cmd_pack(args)
    elif args.cmd == "phase1":
        return _cmd_phase1(args)
    else:
        ap.print_help()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
