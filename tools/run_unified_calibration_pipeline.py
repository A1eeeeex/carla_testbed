#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.calibration_pipeline_common import (
    calibration_metadata_payload,
    compare_validation_payloads,
    deep_update,
    demo_ready_summary_markdown,
    dump_yaml,
    load_yaml,
    versioning_policy_markdown,
    write_comparison_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _info(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _warn(msg: str) -> None:
    print(f"[{_ts()}][WARN] {msg}", flush=True)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _sha256_file(path: Optional[Path]) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _artifact_targets(pipeline_dir: Path) -> List[Path]:
    targets = [(pipeline_dir / "artifacts").resolve()]
    repo_artifacts = (REPO_ROOT / "artifacts").resolve()
    if repo_artifacts not in targets:
        targets.append(repo_artifacts)
    return targets


def _write_json_to_targets(targets: Sequence[Path], relative_name: str, payload: Dict[str, Any]) -> None:
    for root in targets:
        _write_json(root / relative_name, payload)


def _write_text_to_targets(targets: Sequence[Path], relative_name: str, text: str) -> None:
    for root in targets:
        _write_text(root / relative_name, text)


def _read_env_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def _child_env() -> Dict[str, str]:
    env = dict(os.environ)
    env.update(_read_env_file(REPO_ROOT / ".env"))
    return env


def _run_logged(cmd: Sequence[str], *, log_path: Path, cwd: Path, env: Dict[str, str]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as fp:
        fp.write("$ %s\n\n" % " ".join(cmd))
        fp.flush()
        proc = subprocess.Popen(
            list(cmd),
            cwd=str(cwd),
            env=env,
            stdout=fp,
            stderr=subprocess.STDOUT,
            text=True,
        )
        rc = proc.wait()
        if rc != 0:
            raise subprocess.CalledProcessError(rc, list(cmd))


def _resolve_output_dir(raw: str, *, prefix: str) -> Path:
    if raw:
        out = Path(raw).expanduser()
        return out if out.is_absolute() else (REPO_ROOT / out).resolve()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (REPO_ROOT / "runs" / f"{prefix}_{ts}").resolve()


def _resolve_repo_path(raw: str) -> Path:
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def _valid_capture_entry_payload(raw_run_dir: str) -> Dict[str, Any]:
    resolved_run_dir = _resolve_repo_path(str(raw_run_dir))
    exists = resolved_run_dir.exists()
    summary_exists = (resolved_run_dir / "summary.json").exists()
    artifacts_dir_exists = (resolved_run_dir / "artifacts").exists()
    return {
        "run_dir": str(resolved_run_dir),
        "exists": exists,
        "summary_exists": summary_exists,
        "artifacts_dir_exists": artifacts_dir_exists,
        "ready": bool(exists and summary_exists and artifacts_dir_exists),
    }


def _fallback_capture_entry_payload(raw_cfg: str) -> Dict[str, Any]:
    resolved_cfg = _resolve_repo_path(str(raw_cfg))
    exists = resolved_cfg.exists()
    return {
        "capture_config": str(raw_cfg),
        "resolved_path": str(resolved_cfg),
        "exists": exists,
        "ready": bool(exists),
    }


def _mode_from_args(args: argparse.Namespace) -> str:
    if args.capture_only:
        return "capture_only"
    if args.infer_only:
        return "infer_only"
    if args.validate_only:
        return "validate_only"
    return "full"


def _nested_dict(base: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    current = base
    for key in keys:
        value = current.get(key)
        if not isinstance(value, dict):
            value = {}
            current[key] = value
        current = value
    return current


def _build_config(args: argparse.Namespace) -> Dict[str, Any]:
    config_path = Path(args.config).expanduser()
    if not config_path.is_absolute():
        config_path = (REPO_ROOT / config_path).resolve()
    cfg = load_yaml(config_path)
    cfg.setdefault("pipeline", {})
    cfg["config_path"] = str(config_path)
    return cfg


def _run_suite_stage(
    *,
    pipeline_dir: Path,
    pipeline_cfg: Dict[str, Any],
    mode: str,
    resume: bool,
    env: Dict[str, str],
) -> Tuple[Path, Dict[str, Any]]:
    suite_cfg = _nested_dict(pipeline_cfg, "pipeline", "suite")
    suite_dir = pipeline_dir / "suite"
    manifest_path = suite_dir / "suite_manifest.json"
    if resume and manifest_path.exists():
        manifest = _load_json(manifest_path)
        if manifest:
            _info(f"[suite] resume existing suite at {suite_dir}")
            return suite_dir, manifest
    suite_dir.mkdir(parents=True, exist_ok=True)
    suite_mode = "semantic" if mode == "capture_only" else "full"
    cmd = [
        sys.executable,
        "tools/run_apollo_actuator_semantic_suite.py",
        "--mode",
        suite_mode,
        "--output-dir",
        str(suite_dir),
        "--apollo-profile",
        str(suite_cfg.get("apollo_profile", "configs/io/examples/apollo_semantic_suite_base.yaml")),
        "--map",
        str(suite_cfg.get("map", "Town01")),
        "--carla-host",
        str(suite_cfg.get("carla_host", "127.0.0.1")),
        "--carla-port",
        str(int(suite_cfg.get("carla_port", 2000))),
        "--heartbeat-sec",
        str(float(suite_cfg.get("heartbeat_sec", 20.0))),
        "--scene-timeout-sec",
        str(float(suite_cfg.get("scene_timeout_sec", 600.0))),
        "--calibration-detail",
        str(suite_cfg.get("calibration_detail", "exhaustive")),
        "--calibration-library-dir",
        str(suite_cfg.get("calibration_library_dir", "artifacts/actuator_calibration_library")),
        "--policy-config",
        str(pipeline_cfg.get("config_path", "")),
    ]
    vehicle_profile = str(suite_cfg.get("vehicle_profile", "") or "").strip()
    if vehicle_profile:
        cmd.extend(["--vehicle-profile", vehicle_profile])
    if bool(suite_cfg.get("cleanup_port_on_failure", False)):
        cmd.append("--cleanup-port-on-failure")
    if bool(suite_cfg.get("cleanup_port_before_start", False)):
        cmd.append("--cleanup-port-before-start")
    if bool(suite_cfg.get("force_recalibration", False)):
        cmd.append("--force-recalibration")
    max_scenes = int(suite_cfg.get("max_scenes", 0) or 0)
    if max_scenes > 0:
        cmd.extend(["--max-scenes", str(max_scenes)])
    _info(f"[suite] start mode={suite_mode}")
    _run_logged(cmd, log_path=pipeline_dir / "logs" / "suite.log", cwd=REPO_ROOT, env=env)
    manifest = _load_json(manifest_path)
    return suite_dir, manifest


def _run_infer_stage(
    *,
    suite_dir: Path,
    pipeline_dir: Path,
    candidate_calibration_path: Path,
    resume: bool,
    env: Dict[str, str],
) -> Dict[str, Any]:
    out_path = suite_dir / "artifacts" / "control_semantics_report.json"
    if resume and out_path.exists():
        payload = _load_json(out_path)
        if payload:
            _info("[infer] reuse existing control semantics report")
            return payload
    cmd = [
        sys.executable,
        "tools/infer_apollo_control_semantics.py",
        "--input-dir",
        str(suite_dir),
        "--output-dir",
        str(suite_dir / "artifacts"),
        "--calibration-path",
        str(candidate_calibration_path),
        "--capture-validity-summary",
        str(suite_dir / "artifacts" / "calibration_capture_validity_summary.json"),
    ]
    _info("[infer] start")
    _run_logged(cmd, log_path=pipeline_dir / "logs" / "infer.log", cwd=REPO_ROOT, env=env)
    return _load_json(out_path)


def _run_replay_validation(
    *,
    pipeline_dir: Path,
    pipeline_cfg: Dict[str, Any],
    calibration_path: Path,
    output_subdir: str,
    capture_run_dirs: Optional[Sequence[str]],
    resume: bool,
    env: Dict[str, str],
) -> Dict[str, Any]:
    replay_cfg = _nested_dict(pipeline_cfg, "pipeline", "replay_validation")
    output_dir = pipeline_dir / output_subdir
    out_json = output_dir / "artifacts" / "apollo_control_replay_validation.json"
    if resume and out_json.exists():
        payload = _load_json(out_json)
        if payload:
            _info(f"[replay] reuse {output_subdir}")
            return payload
    cmd = [
        sys.executable,
        "tools/run_apollo_control_replay_validation.py",
        "--validation-config",
        str(replay_cfg.get("validation_config", "configs/io/examples/apollo_actuator_tracking_validation.yaml")),
        "--calibration-file",
        str(calibration_path),
        "--output-dir",
        str(output_dir),
        "--policy-config",
        str(pipeline_cfg.get("config_path", "")),
    ]
    if capture_run_dirs:
        for run_dir in capture_run_dirs:
            cmd.extend(["--capture-run-dir", str(run_dir)])
    else:
        for capture_cfg in replay_cfg.get("capture_configs", []) or []:
            cmd.extend(["--capture-config", str(capture_cfg)])
    _info(f"[replay] start {output_subdir}")
    _run_logged(cmd, log_path=pipeline_dir / "logs" / f"{output_subdir}.log", cwd=REPO_ROOT, env=env)
    return _load_json(out_json)


def _run_tracking_validation(
    *,
    pipeline_dir: Path,
    pipeline_cfg: Dict[str, Any],
    calibration_path: Path,
    output_subdir: str,
    resume: bool,
    env: Dict[str, str],
) -> Dict[str, Any]:
    tracking_cfg = _nested_dict(pipeline_cfg, "pipeline", "tracking_validation")
    output_dir = pipeline_dir / output_subdir
    out_json = output_dir / "artifacts" / "actuator_tracking_validation.json"
    if resume and out_json.exists():
        payload = _load_json(out_json)
        if payload:
            _info(f"[tracking] reuse {output_subdir}")
            return payload
    cmd = [
        sys.executable,
        "tools/validate_apollo_carla_actuator_tracking.py",
        "--config",
        str(tracking_cfg.get("config", "configs/io/examples/apollo_actuator_tracking_validation.yaml")),
        "--calibration-file",
        str(calibration_path),
        "--output-dir",
        str(output_dir),
    ]
    _info(f"[tracking] start {output_subdir}")
    _run_logged(cmd, log_path=pipeline_dir / "logs" / f"{output_subdir}.log", cwd=REPO_ROOT, env=env)
    return _load_json(out_json)


def _resolve_candidate_calibration(suite_dir: Optional[Path], args: argparse.Namespace) -> Optional[Path]:
    if args.candidate_calibration_file:
        path = Path(args.candidate_calibration_file).expanduser()
        return path if path.is_absolute() else (REPO_ROOT / path).resolve()
    if suite_dir is not None:
        path = suite_dir / "artifacts" / "carla_actuator_calibration.json"
        if path.exists():
            return path
    return None


def _resolve_baseline_reference_details(pipeline_cfg: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    baseline_cfg = _nested_dict(pipeline_cfg, "pipeline", "baseline")
    details: Dict[str, Any] = {
        "source": "none",
        "resolution_mode": "none",
        "resolved_calibration_path": "",
        "requested_calibration_file": "",
        "requested_library_entry_id": "",
        "requested_library_dir": "",
        "library_index_path": "",
        "library_index_exists": False,
        "library_index_entry_found": False,
        "library_entry_request_fingerprint": "",
        "library_entry_map": "",
        "library_entry_metadata_path": "",
    }
    if args.baseline_calibration_file:
        path = Path(str(args.baseline_calibration_file)).expanduser()
        resolved = path if path.is_absolute() else (REPO_ROOT / path).resolve()
        details.update(
            {
                "source": "cli_arg",
                "resolution_mode": "direct_file",
                "requested_calibration_file": str(args.baseline_calibration_file),
                "resolved_calibration_path": str(resolved),
            }
        )
        return details

    raw = str(baseline_cfg.get("calibration_file", "") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        resolved = path if path.is_absolute() else (REPO_ROOT / path).resolve()
        details.update(
            {
                "source": "pipeline_config",
                "resolution_mode": "direct_file",
                "requested_calibration_file": raw,
                "resolved_calibration_path": str(resolved),
            }
        )
        return details

    entry_id = str(baseline_cfg.get("library_entry_id", "") or "").strip()
    if not entry_id:
        return details
    library_dir_raw = str(
        baseline_cfg.get("library_dir", _nested_dict(pipeline_cfg, "pipeline", "suite").get("calibration_library_dir", "artifacts/actuator_calibration_library"))
        or ""
    ).strip()
    if not library_dir_raw:
        details.update(
            {
                "source": "pipeline_library_entry",
                "resolution_mode": "library_missing_dir",
                "requested_library_entry_id": entry_id,
            }
        )
        return details
    library_dir = Path(library_dir_raw).expanduser()
    if not library_dir.is_absolute():
        library_dir = (REPO_ROOT / library_dir).resolve()
    index_path = library_dir / "index.json"
    details.update(
        {
            "source": "pipeline_library_entry",
            "requested_library_entry_id": entry_id,
            "requested_library_dir": str(library_dir),
            "library_index_path": str(index_path),
            "library_index_exists": index_path.exists(),
        }
    )
    index_payload = _load_json(index_path)
    for item in index_payload.get("entries", []) or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("entry_id", "") or "").strip() != entry_id:
            continue
        calibration_path = Path(str(item.get("calibration_path", "") or "")).expanduser()
        if not calibration_path.is_absolute():
            calibration_path = (REPO_ROOT / calibration_path).resolve()
        details.update(
            {
                "resolution_mode": "library_index",
                "resolved_calibration_path": str(calibration_path),
                "library_index_entry_found": True,
                "library_entry_request_fingerprint": str(item.get("request_fingerprint", "") or ""),
                "library_entry_map": str(item.get("map", "") or ""),
                "library_entry_metadata_path": str(item.get("metadata_path", "") or ""),
            }
        )
        return details
    conventional = library_dir / entry_id / "carla_actuator_calibration.json"
    details.update(
        {
            "resolution_mode": "library_conventional_path",
            "resolved_calibration_path": str(conventional.resolve()),
        }
    )
    return details


def _recommend_demo_case(comparison: Dict[str, Any]) -> str:
    improved = set(str(item) for item in comparison.get("improved_metrics", []))
    if any("steer" in item for item in improved):
        return "configs/io/examples/followstop_apollo_gt_case2_locked.yaml"
    if any("accel" in item or "decel" in item for item in improved):
        return "configs/io/examples/followstop_apollo_gt_case3_locked.yaml"
    return "configs/io/examples/followstop_apollo_gt_validation.yaml"


def _calibration_reference_payload(
    *,
    calibration_path: Optional[Path],
    source: str,
    candidate_calibration: Optional[Path] = None,
    resolution_details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resolved = calibration_path.resolve() if calibration_path is not None and calibration_path.exists() else calibration_path
    metadata_path = None
    if resolved is not None:
        candidate_metadata = resolved.with_suffix(".metadata.json")
        if candidate_metadata.exists():
            metadata_path = candidate_metadata
    if metadata_path is None and resolution_details:
        library_metadata = str(resolution_details.get("library_entry_metadata_path", "") or "").strip()
        if library_metadata:
            candidate_library_metadata = Path(library_metadata).expanduser()
            metadata_path = candidate_library_metadata if candidate_library_metadata.exists() else None
    same_as_candidate = False
    if resolved is not None and candidate_calibration is not None and candidate_calibration.exists():
        try:
            same_as_candidate = resolved.resolve() == candidate_calibration.resolve()
        except Exception:
            same_as_candidate = False
    payload = {
        "source": source,
        "exists": bool(resolved is not None and resolved.exists()),
        "calibration_path": str(resolved) if resolved is not None else "",
        "calibration_sha256": _sha256_file(resolved if isinstance(resolved, Path) else None),
        "metadata_path": str(metadata_path) if metadata_path is not None else "",
        "same_as_candidate": same_as_candidate,
    }
    if resolution_details:
        for key, value in resolution_details.items():
            if key == "resolved_calibration_path":
                continue
            payload[key] = value
    return payload


def _replay_input_contract_payload(
    *,
    capture_summary: Dict[str, Any],
    valid_capture_run_dirs: Sequence[str],
    replay_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    captures = [item for item in (capture_summary.get("captures", []) or []) if isinstance(item, dict)]
    invalid = [item for item in captures if not bool(item.get("capture_valid"))]
    valid_capture_entries = [_valid_capture_entry_payload(str(raw_run_dir)) for raw_run_dir in valid_capture_run_dirs]
    invalid_capture_entries = []
    for item in invalid:
        raw_run_dir = str(item.get("run_dir", "") or "")
        resolved_run_dir = _resolve_repo_path(raw_run_dir) if raw_run_dir else None
        invalid_capture_entries.append(
            {
                "capture_id": str(item.get("capture_id", "") or ""),
                "run_dir": str(resolved_run_dir) if resolved_run_dir is not None else "",
                "run_dir_exists": bool(resolved_run_dir is not None and resolved_run_dir.exists()),
                "summary_path": str(item.get("summary_path", "") or ""),
                "summary_exists": bool(item.get("summary_path") and Path(str(item.get("summary_path"))).exists()),
            }
        )
    fallback_capture_entries = [
        _fallback_capture_entry_payload(str(raw_cfg))
        for raw_cfg in (replay_cfg.get("capture_configs", []) or [])
    ]
    replay_source_mode = "suite_valid_capture_run_dirs" if valid_capture_run_dirs else "replay_config_capture_configs"
    selected_source_ready = bool(valid_capture_entries or fallback_capture_entries) and (
        all(bool(item.get("ready")) for item in valid_capture_entries)
        if valid_capture_entries
        else all(bool(item.get("ready")) for item in fallback_capture_entries)
    )
    return {
        "schema_version": 1,
        "source": "suite_manifest.capture_summary",
        "valid_capture_run_dirs": list(valid_capture_run_dirs),
        "valid_capture_entries": valid_capture_entries,
        "valid_capture_count": len(valid_capture_run_dirs),
        "invalid_capture_count": len(invalid),
        "valid_capture_ids": [str(item.get("capture_id", "") or "") for item in captures if bool(item.get("capture_valid"))],
        "invalid_capture_ids": [str(item.get("capture_id", "") or "") for item in invalid],
        "invalid_capture_entries": invalid_capture_entries,
        "fallback_capture_configs": [str(item) for item in (replay_cfg.get("capture_configs", []) or [])],
        "fallback_capture_entries": fallback_capture_entries,
        "replay_source_mode": replay_source_mode,
        "selected_source_ready": bool(selected_source_ready),
    }


def _replay_input_contract_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        "# Calibration Replay Input Contract",
        "",
        f"- replay_source_mode: `{payload.get('replay_source_mode', '')}`",
        f"- selected_source_ready: `{payload.get('selected_source_ready', False)}`",
        f"- valid_capture_count: `{payload.get('valid_capture_count', 0)}`",
        f"- invalid_capture_count: `{payload.get('invalid_capture_count', 0)}`",
        "",
        "## Valid Capture Run Dirs",
        "",
    ]
    valid_entries = list(payload.get("valid_capture_entries", []) or [])
    if valid_entries:
        for item in valid_entries:
            lines.append(
                "- `{}`\n  exists=`{}` summary_exists=`{}` artifacts_dir_exists=`{}` ready=`{}`".format(
                    item.get("run_dir", ""),
                    item.get("exists", False),
                    item.get("summary_exists", False),
                    item.get("artifacts_dir_exists", False),
                    item.get("ready", False),
                )
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Invalid Capture Entries", ""])
    invalid_entries = list(payload.get("invalid_capture_entries", []) or [])
    if invalid_entries:
        for item in invalid_entries:
            lines.append(
                "- `{}`\n  run_dir=`{}` run_dir_exists=`{}` summary_exists=`{}`".format(
                    item.get("capture_id", ""),
                    item.get("run_dir", ""),
                    item.get("run_dir_exists", False),
                    item.get("summary_exists", False),
                )
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Fallback Capture Configs", ""])
    fallback_entries = list(payload.get("fallback_capture_entries", []) or [])
    if fallback_entries:
        for item in fallback_entries:
            lines.append(
                "- `{}`\n  resolved_path=`{}` exists=`{}` ready=`{}`".format(
                    item.get("capture_config", ""),
                    item.get("resolved_path", ""),
                    item.get("exists", False),
                    item.get("ready", False),
                )
            )
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _baseline_reference_summary_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": str(payload.get("source", "") or ""),
        "exists": bool(payload.get("exists")),
        "same_as_candidate": bool(payload.get("same_as_candidate")),
        "resolution_mode": str(payload.get("resolution_mode", "") or ""),
        "requested_calibration_file": str(payload.get("requested_calibration_file", "") or ""),
        "requested_library_entry_id": str(payload.get("requested_library_entry_id", "") or ""),
        "library_index_entry_found": bool(payload.get("library_index_entry_found")),
        "library_entry_map": str(payload.get("library_entry_map", "") or ""),
    }


def _baseline_reference_summary_markdown(payload: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Baseline Reference Summary",
            "",
            f"- source: `{payload.get('source', '')}`",
            f"- exists: `{payload.get('exists', False)}`",
            f"- same_as_candidate: `{payload.get('same_as_candidate', False)}`",
            f"- resolution_mode: `{payload.get('resolution_mode', 'none')}`",
            f"- requested_calibration_file: `{payload.get('requested_calibration_file', '')}`",
            f"- requested_library_entry_id: `{payload.get('requested_library_entry_id', '')}`",
            f"- library_index_entry_found: `{payload.get('library_index_entry_found', False)}`",
            f"- library_entry_map: `{payload.get('library_entry_map', '')}`",
            "",
        ]
    )


def _emit_replay_input_contract_artifacts(
    *,
    artifact_targets: Sequence[Path],
    suite_manifest: Dict[str, Any],
    replay_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    capture_summary = suite_manifest.get("capture_summary", {}) if suite_manifest else {}
    valid_capture_run_dirs = [
        str(item.get("run_dir", "") or "")
        for item in (capture_summary.get("captures", []) or [])
        if isinstance(item, dict) and bool(item.get("capture_valid"))
    ]
    payload = _replay_input_contract_payload(
        capture_summary=capture_summary,
        valid_capture_run_dirs=valid_capture_run_dirs,
        replay_cfg=replay_cfg,
    )
    _write_json_to_targets(artifact_targets, "replay_input_contract.json", payload)
    _write_text_to_targets(
        artifact_targets,
        "replay_input_contract.md",
        _replay_input_contract_markdown(payload),
    )
    return payload


def _manifest_markdown(manifest: Dict[str, Any]) -> str:
    final_recommendation = manifest.get("final_recommendation", {}) or {}
    capture_summary = manifest.get("capture_summary", {}) or {}
    inference_summary = manifest.get("inference_summary", {}) or {}
    replay_input_contract_summary = manifest.get("replay_input_contract_summary", {}) or {}
    baseline_reference_summary = manifest.get("baseline_reference_summary", {}) or {}
    replay_summary = manifest.get("replay_validation_summary", {}) or {}
    tracking_summary = manifest.get("tracking_validation_summary", {}) or {}
    return "\n".join(
        [
            "# Unified Calibration Manifest",
            "",
            f"- calibration_run_id: `{manifest.get('calibration_run_id', '')}`",
            f"- scenario_config: `{manifest.get('scenario_config', '')}`",
            f"- capture_summary: `valid={capture_summary.get('valid_capture_count', 0)} invalid={capture_summary.get('invalid_capture_count', 0)}`",
            f"- replay_input_contract_summary: `source_mode={replay_input_contract_summary.get('replay_source_mode', 'n/a')} ready={replay_input_contract_summary.get('selected_source_ready', False)} valid={replay_input_contract_summary.get('valid_capture_count', 0)} invalid={replay_input_contract_summary.get('invalid_capture_count', 0)}`",
            f"- replay_input_contract_bundle_embedded: `{bool(manifest.get('replay_input_contract_bundle'))}`",
            f"- baseline_reference_summary: `source={baseline_reference_summary.get('source', 'n/a')} exists={baseline_reference_summary.get('exists', False)} mode={baseline_reference_summary.get('resolution_mode', 'none')} entry={baseline_reference_summary.get('requested_library_entry_id', '') or 'n/a'} index_hit={baseline_reference_summary.get('library_index_entry_found', False)} map={baseline_reference_summary.get('library_entry_map', '') or 'n/a'}`",
            f"- inference_summary: `lateral={((inference_summary.get('lateral') or {}).get('recommended_steering_field') or 'n/a')} longitudinal={((inference_summary.get('longitudinal') or {}).get('recommended_signed_acceleration_field') or 'n/a')}`",
            f"- calibration_output_path: `{manifest.get('calibration_output_path', '')}`",
            f"- replay_validation_summary: `candidate_pass={replay_summary.get('candidate_pass')} baseline_pass={replay_summary.get('baseline_pass')}`",
            f"- tracking_validation_summary: `candidate_pass={tracking_summary.get('candidate_pass')} baseline_pass={tracking_summary.get('baseline_pass')}`",
            f"- final_recommendation: `recommended_for_default={final_recommendation.get('recommended_for_default')} status={final_recommendation.get('status')}`",
            "",
        ]
    )


def _validation_gate_replay(payload: Dict[str, Any]) -> bool:
    quality = payload.get("quality", {}) or {}
    steer = float(quality.get("mean_abs_steer_error_deg", 999.0) or 999.0)
    accel = float(quality.get("mean_abs_accel_error_mps2", 999.0) or 999.0)
    decel = float(quality.get("mean_abs_decel_error_mps2", 999.0) or 999.0)
    return steer <= 5.0 and accel <= 2.5 and decel <= 2.5


def _validation_gate_tracking(payload: Dict[str, Any]) -> bool:
    steering = float((((payload.get("steering") or {}).get("quality") or {}).get("mean_abs_error_deg", 999.0)) or 999.0)
    throttle = float((((payload.get("throttle") or {}).get("quality") or {}).get("mean_abs_error_mps2", 999.0)) or 999.0)
    brake = float((((payload.get("brake") or {}).get("quality") or {}).get("mean_abs_error_mps2", 999.0)) or 999.0)
    return steering <= 5.0 and throttle <= 2.5 and brake <= 2.5


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the unified calibration capture/infer/validate/recommend pipeline")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--capture-only", action="store_true")
    mode.add_argument("--infer-only", action="store_true")
    mode.add_argument("--validate-only", action="store_true")
    mode.add_argument("--full", action="store_true")
    ap.add_argument("--config", default="configs/io/examples/unified_calibration_pipeline.yaml")
    ap.add_argument("--output-dir", default="")
    ap.add_argument("--suite-dir", default="")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--candidate-calibration-file", default="")
    ap.add_argument("--baseline-calibration-file", default="")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    mode = _mode_from_args(args)
    pipeline_cfg = _build_config(args)
    pipeline_dir = _resolve_output_dir(args.output_dir, prefix="unified_calibration")
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    (pipeline_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (pipeline_dir / "logs").mkdir(parents=True, exist_ok=True)
    dump_yaml(pipeline_dir / "artifacts" / "effective_pipeline_config.yaml", pipeline_cfg)
    artifact_targets = _artifact_targets(pipeline_dir)
    env = _child_env()

    suite_dir: Optional[Path] = None
    suite_manifest: Dict[str, Any] = {}
    inference_summary: Dict[str, Any] = {}
    candidate_replay: Dict[str, Any] = {}
    baseline_replay: Dict[str, Any] = {}
    candidate_tracking: Dict[str, Any] = {}
    baseline_tracking: Dict[str, Any] = {}
    suite_stage_failed = False
    suite_stage_error = ""
    suite_stage_returncode: Optional[int] = None

    try:
        if args.suite_dir:
            suite_dir = Path(args.suite_dir).expanduser()
            if not suite_dir.is_absolute():
                suite_dir = (REPO_ROOT / suite_dir).resolve()
            suite_manifest = _load_json(suite_dir / "suite_manifest.json")

        if mode in {"capture_only", "full"}:
            try:
                suite_dir, suite_manifest = _run_suite_stage(
                    pipeline_dir=pipeline_dir,
                    pipeline_cfg=pipeline_cfg,
                    mode=mode,
                    resume=bool(args.resume),
                    env=env,
                )
            except subprocess.CalledProcessError as exc:
                suite_stage_failed = True
                suite_stage_returncode = int(exc.returncode or 1)
                suite_stage_error = (
                    f"suite stage failed rc={suite_stage_returncode}: "
                    + " ".join(exc.cmd if isinstance(exc.cmd, list) else [str(exc.cmd)])
                )
                suite_dir = (pipeline_dir / "suite").resolve()
                suite_manifest = _load_json(suite_dir / "suite_manifest.json")
                _warn(suite_stage_error)

        candidate_calibration = _resolve_candidate_calibration(suite_dir, args)
        baseline_reference_details = _resolve_baseline_reference_details(pipeline_cfg, args)
        baseline_source = str(baseline_reference_details.get("source", "none") or "none")
        baseline_calibration_path = str(baseline_reference_details.get("resolved_calibration_path", "") or "").strip()
        baseline_calibration = Path(baseline_calibration_path) if baseline_calibration_path else None
        baseline_reference = _calibration_reference_payload(
            calibration_path=baseline_calibration,
            source=baseline_source,
            candidate_calibration=candidate_calibration,
            resolution_details=baseline_reference_details,
        )
        baseline_reference_summary = _baseline_reference_summary_payload(baseline_reference)
        _write_json_to_targets(artifact_targets, "baseline_calibration_reference.json", baseline_reference)
        _write_text_to_targets(
            artifact_targets,
            "baseline_calibration_reference.md",
            "\n".join(
                [
                    "# Baseline Calibration Reference",
                    "",
                    f"- source: `{baseline_reference['source']}`",
                    f"- exists: `{baseline_reference['exists']}`",
                    f"- calibration_path: `{baseline_reference['calibration_path']}`",
                    f"- calibration_sha256: `{baseline_reference['calibration_sha256']}`",
                    f"- metadata_path: `{baseline_reference['metadata_path']}`",
                    f"- same_as_candidate: `{baseline_reference['same_as_candidate']}`",
                    f"- resolution_mode: `{baseline_reference.get('resolution_mode', 'none')}`",
                    f"- requested_calibration_file: `{baseline_reference.get('requested_calibration_file', '')}`",
                    f"- requested_library_entry_id: `{baseline_reference.get('requested_library_entry_id', '')}`",
                    f"- requested_library_dir: `{baseline_reference.get('requested_library_dir', '')}`",
                    f"- library_index_path: `{baseline_reference.get('library_index_path', '')}`",
                    f"- library_index_exists: `{baseline_reference.get('library_index_exists', False)}`",
                    f"- library_index_entry_found: `{baseline_reference.get('library_index_entry_found', False)}`",
                    f"- library_entry_request_fingerprint: `{baseline_reference.get('library_entry_request_fingerprint', '')}`",
                    f"- library_entry_map: `{baseline_reference.get('library_entry_map', '')}`",
                    f"- library_entry_metadata_path: `{baseline_reference.get('library_entry_metadata_path', '')}`",
                    "",
                ]
            ),
        )
        _write_json_to_targets(artifact_targets, "baseline_reference_summary.json", baseline_reference_summary)
        _write_text_to_targets(
            artifact_targets,
            "baseline_reference_summary.md",
            _baseline_reference_summary_markdown(baseline_reference_summary),
        )

        if mode in {"infer_only", "full"} and not suite_stage_failed:
            if suite_dir is None:
                raise RuntimeError("--infer-only and --full require a suite directory or a suite stage run")
            if candidate_calibration is None or not candidate_calibration.exists():
                raise RuntimeError("candidate calibration file not available for inference")
            inference_summary = _run_infer_stage(
                suite_dir=suite_dir,
                pipeline_dir=pipeline_dir,
                candidate_calibration_path=candidate_calibration,
                resume=bool(args.resume),
                env=env,
            )
        elif mode in {"infer_only", "full"} and suite_stage_failed:
            _warn("[infer] skipped because suite stage failed")
        elif suite_dir is not None:
            inference_summary = _load_json(suite_dir / "artifacts" / "control_semantics_report.json")

        replay_cfg = _nested_dict(pipeline_cfg, "pipeline", "replay_validation")
        replay_input_contract: Dict[str, Any] = {}
        if suite_manifest:
            replay_input_contract = _emit_replay_input_contract_artifacts(
                artifact_targets=artifact_targets,
                suite_manifest=suite_manifest,
                replay_cfg=replay_cfg,
            )

        if mode in {"validate_only", "full"} and not suite_stage_failed:
            if candidate_calibration is None or not candidate_calibration.exists():
                raise RuntimeError("candidate calibration file not available for validation")
            tracking_cfg = _nested_dict(pipeline_cfg, "pipeline", "tracking_validation")
            valid_capture_run_dirs = list(replay_input_contract.get("valid_capture_run_dirs", []) or [])
            candidate_replay = _run_replay_validation(
                pipeline_dir=pipeline_dir,
                pipeline_cfg=pipeline_cfg,
                calibration_path=candidate_calibration,
                output_subdir=str(replay_cfg.get("output_subdir_candidate", "replay_validation_candidate")),
                capture_run_dirs=valid_capture_run_dirs,
                resume=bool(args.resume),
                env=env,
            )
            candidate_tracking = _run_tracking_validation(
                pipeline_dir=pipeline_dir,
                pipeline_cfg=pipeline_cfg,
                calibration_path=candidate_calibration,
                output_subdir=str(tracking_cfg.get("output_subdir_candidate", "tracking_validation_candidate")),
                resume=bool(args.resume),
                env=env,
            )
            if baseline_calibration is not None and baseline_calibration.exists():
                if baseline_calibration.resolve() == candidate_calibration.resolve():
                    baseline_replay = candidate_replay
                    baseline_tracking = candidate_tracking
                else:
                    baseline_replay = _run_replay_validation(
                        pipeline_dir=pipeline_dir,
                        pipeline_cfg=pipeline_cfg,
                        calibration_path=baseline_calibration,
                        output_subdir=str(replay_cfg.get("output_subdir_baseline", "replay_validation_baseline")),
                        capture_run_dirs=valid_capture_run_dirs,
                        resume=bool(args.resume),
                        env=env,
                    )
                    baseline_tracking = _run_tracking_validation(
                        pipeline_dir=pipeline_dir,
                        pipeline_cfg=pipeline_cfg,
                        calibration_path=baseline_calibration,
                        output_subdir=str(tracking_cfg.get("output_subdir_baseline", "tracking_validation_baseline")),
                        resume=bool(args.resume),
                        env=env,
                    )
        elif mode in {"validate_only", "full"} and suite_stage_failed:
            _warn("[validate] skipped because suite stage failed")

        candidate_replay_pass = _validation_gate_replay(candidate_replay) if candidate_replay else None
        baseline_replay_pass = _validation_gate_replay(baseline_replay) if baseline_replay else None
        candidate_tracking_pass = _validation_gate_tracking(candidate_tracking) if candidate_tracking else None
        baseline_tracking_pass = _validation_gate_tracking(baseline_tracking) if baseline_tracking else None
        candidate_pass = None
        baseline_pass = None
        if candidate_replay_pass is not None or candidate_tracking_pass is not None:
            candidate_pass = bool(candidate_replay_pass is not False and candidate_tracking_pass is not False)
        if baseline_replay_pass is not None or baseline_tracking_pass is not None:
            baseline_pass = bool(baseline_replay_pass is not False and baseline_tracking_pass is not False)
        comparison = compare_validation_payloads(
            baseline_replay=baseline_replay or None,
            candidate_replay=candidate_replay or None,
            baseline_tracking=baseline_tracking or None,
            candidate_tracking=candidate_tracking or None,
            baseline_pass=baseline_pass,
            candidate_pass=candidate_pass,
            policy=(pipeline_cfg.get("comparison_policy") if isinstance(pipeline_cfg.get("comparison_policy"), dict) else None),
        )
        for root in artifact_targets:
            write_comparison_artifacts(root, comparison)

        capture_summary = suite_manifest.get("capture_summary", {}) if suite_manifest else {}
        scenario_config = str(pipeline_cfg.get("config_path", ""))
        calibration_run_id = pipeline_dir.name
        calibration_output_path = str(candidate_calibration) if candidate_calibration else ""
        if candidate_calibration is not None and candidate_calibration.exists():
            metadata = calibration_metadata_payload(
                calibration_path=candidate_calibration,
                source_run_id=calibration_run_id,
                source_scenario_config=scenario_config,
                source_capture_ids=[
                    str(item.get("capture_id", ""))
                    for item in (capture_summary.get("captures", []) or [])
                    if isinstance(item, dict)
                ],
                source_inference_version="control_semantics_report.v1",
                validation_version="replay_v1+tracking_v1",
                vehicle_profile=str(_nested_dict(pipeline_cfg, "pipeline", "suite").get("vehicle_profile", "") or ""),
                map_name=str(_nested_dict(pipeline_cfg, "pipeline", "suite").get("map", "Town01")),
                recommended_for_default=bool(comparison.get("recommended_for_default")),
            )
            _write_json(candidate_calibration.with_suffix(".metadata.json"), metadata)
            _write_text_to_targets(
                artifact_targets,
                "calibration_versioning_policy.md",
                versioning_policy_markdown(metadata),
            )

        demo_summary = {
            "baseline_calibration_id": str(baseline_calibration) if baseline_calibration else "",
            "candidate_calibration_id": calibration_output_path,
            "improved_metrics": list(comparison.get("improved_metrics", [])),
            "regressed_metrics": list(comparison.get("regressed_metrics", [])),
            "recommended_calibration_id": calibration_output_path if comparison.get("recommended_for_default") else str(baseline_calibration or ""),
            "recommended_demo_case": _recommend_demo_case(comparison),
        }
        _write_json_to_targets(artifact_targets, "calibration_demo_ready_summary.json", demo_summary)
        _write_text_to_targets(
            artifact_targets,
            "calibration_demo_ready_summary.md",
            demo_ready_summary_markdown(demo_summary),
        )

        manifest = {
            "schema_version": 1,
            "calibration_run_id": calibration_run_id,
            "status": "failed" if suite_stage_failed else "completed",
            "suite_stage_failed": suite_stage_failed,
            "suite_stage_returncode": suite_stage_returncode,
            "suite_stage_error": suite_stage_error,
            "scenario_config": scenario_config,
            "capture_summary": capture_summary,
            "replay_input_contract_summary": {
                "replay_source_mode": replay_input_contract.get("replay_source_mode", ""),
                "selected_source_ready": replay_input_contract.get("selected_source_ready"),
                "valid_capture_count": replay_input_contract.get("valid_capture_count", 0),
                "invalid_capture_count": replay_input_contract.get("invalid_capture_count", 0),
            },
            "replay_input_contract_bundle": {
                "valid_capture_run_dirs": list(replay_input_contract.get("valid_capture_run_dirs", []) or []),
                "valid_capture_entries": list(replay_input_contract.get("valid_capture_entries", []) or []),
                "invalid_capture_entries": list(replay_input_contract.get("invalid_capture_entries", []) or []),
                "fallback_capture_entries": list(replay_input_contract.get("fallback_capture_entries", []) or []),
                "selected_source_ready": replay_input_contract.get("selected_source_ready"),
                "replay_source_mode": replay_input_contract.get("replay_source_mode", ""),
            },
            "baseline_reference_summary": baseline_reference_summary,
            "inference_summary": inference_summary,
            "calibration_output_path": calibration_output_path,
            "replay_validation_summary": {
                "candidate_pass": candidate_replay_pass,
                "baseline_pass": baseline_replay_pass,
                "candidate_output": str((pipeline_dir / _nested_dict(pipeline_cfg, "pipeline", "replay_validation").get("output_subdir_candidate", "replay_validation_candidate")).resolve()),
                "baseline_output": str((pipeline_dir / _nested_dict(pipeline_cfg, "pipeline", "replay_validation").get("output_subdir_baseline", "replay_validation_baseline")).resolve()),
            },
            "tracking_validation_summary": {
                "candidate_pass": candidate_tracking_pass,
                "baseline_pass": baseline_tracking_pass,
                "candidate_output": str((pipeline_dir / _nested_dict(pipeline_cfg, "pipeline", "tracking_validation").get("output_subdir_candidate", "tracking_validation_candidate")).resolve()),
                "baseline_output": str((pipeline_dir / _nested_dict(pipeline_cfg, "pipeline", "tracking_validation").get("output_subdir_baseline", "tracking_validation_baseline")).resolve()),
            },
            "final_recommendation": comparison,
        }
        _write_json_to_targets(artifact_targets, "unified_calibration_manifest.json", manifest)
        _write_text_to_targets(
            artifact_targets,
            "unified_calibration_manifest.md",
            _manifest_markdown(manifest),
        )

        compatibility_lines = [
            "# Calibration Backward Compatibility",
            "",
            "- Existing entrypoints are preserved: `run_apollo_actuator_semantic_suite.py`, `infer_apollo_control_semantics.py`, `run_apollo_control_replay_validation.py`, `validate_apollo_carla_actuator_tracking.py`.",
            "- The new unified entrypoint only orchestrates those scripts; it does not delete or rename them.",
            "- New capture validity behavior is driven by `configs/io/examples/unified_calibration_pipeline.yaml` and can be rolled back by changing the policy config or bypassing the unified entrypoint.",
            "",
            "## Protected Mainline Configs",
            "",
        ]
        for item in (_nested_dict(pipeline_cfg, "pipeline", "compatibility").get("protected_configs", []) or []):
            compatibility_lines.append(f"- `{item}`")
        compatibility_lines.append("")
        _write_text_to_targets(
            artifact_targets,
            "calibration_backward_compatibility.md",
            "\n".join(compatibility_lines),
        )

        if suite_stage_failed:
            _warn(f"[done-with-failure] partial unified pipeline artifacts written under {pipeline_dir}")
            return 1
        _info(f"[done] unified pipeline artifacts written under {pipeline_dir} (mirrored to repo artifacts for compatibility)")
        return 0
    except subprocess.CalledProcessError as exc:
        _warn(f"command failed rc={exc.returncode}: {' '.join(exc.cmd if isinstance(exc.cmd, list) else exc.cmd)}")
        return 1
    except Exception as exc:
        _warn(f"unified calibration pipeline failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
