#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Apollo actuator semantic runs.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis or carla_testbed.calibration.
# Migration target: carla_testbed.analysis.control_attribution and carla_testbed.calibration.
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.calibration_pipeline_common import (
    evaluate_capture_validity,
    load_policy_config,
    summarize_capture_validity,
    write_capture_validity_artifacts,
    write_policy_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _info(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _warn(msg: str) -> None:
    print(f"[{_ts()}][WARN] {msg}", flush=True)


def _tail_text(path: Path, *, max_lines: int = 40) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(errors="replace").splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-max_lines:])


def _deep_update(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _load_yaml(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return {}
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def _carla_server_ready(host: str, port: int, *, timeout_sec: float = 2.0) -> bool:
    try:
        import carla  # type: ignore
    except Exception:
        return _port_open(host, port)
    try:
        client = carla.Client(host, port)
        client.set_timeout(float(timeout_sec))
        client.get_server_version()
        world = client.get_world()
        _ = world.get_map().name
        return True
    except Exception:
        return False


def _write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


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


def _requested_calibration_context(*, map_name: str, apollo_profile: Path, vehicle_profile: Optional[Path]) -> Dict[str, Any]:
    payload = {
        "schema_version": 1,
        "map": str(map_name),
        "apollo_profile_path": str(apollo_profile),
        "apollo_profile_sha256": _sha256_file(apollo_profile),
        "vehicle_profile_path": str(vehicle_profile) if vehicle_profile else "",
        "vehicle_profile_sha256": _sha256_file(vehicle_profile),
    }
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    payload["fingerprint"] = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()[:16]
    return payload


def _load_json_dict(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_calibration_library_index(library_root: Path) -> Dict[str, Any]:
    path = library_root / "index.json"
    payload = _load_json_dict(path)
    entries = payload.get("entries")
    if not isinstance(entries, list):
        payload["entries"] = []
    return payload


def _write_calibration_library_index(library_root: Path, payload: Dict[str, Any]) -> None:
    payload = dict(payload)
    payload.setdefault("schema_version", 1)
    payload.setdefault("entries", [])
    _write_json(library_root / "index.json", payload)


def _find_saved_calibration_entry(library_root: Path, *, request_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = _load_calibration_library_index(library_root)
    fingerprint = str(request_context.get("fingerprint", "") or "")
    for item in payload.get("entries", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("request_fingerprint", "") or "") != fingerprint:
            continue
        calibration_path = Path(str(item.get("calibration_path", "") or ""))
        if calibration_path.exists():
            out = dict(item)
            out["calibration_path"] = str(calibration_path)
            return out
    return None


def _stage_saved_calibration_into_suite(*, suite_dir: Path, entry: Dict[str, Any]) -> Dict[str, Any]:
    artifacts_dir = suite_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    src = Path(str(entry.get("calibration_path", "") or ""))
    dst = artifacts_dir / "carla_actuator_calibration.json"
    shutil.copy2(src, dst)
    staged: Dict[str, Any] = {
        "reused": True,
        "source_entry_id": str(entry.get("entry_id", "") or ""),
        "source_calibration_path": str(src),
        "staged_calibration_path": str(dst),
        "request_fingerprint": str(entry.get("request_fingerprint", "") or ""),
    }
    recommended_src = Path(str(entry.get("recommended_mapping_path", "") or ""))
    if recommended_src.exists():
        recommended_dst = artifacts_dir / "recommended_physical_mapping.yaml"
        shutil.copy2(recommended_src, recommended_dst)
        staged["staged_recommended_mapping_path"] = str(recommended_dst)
    return staged


def _persist_calibration_entry(
    *,
    library_root: Path,
    request_context: Dict[str, Any],
    suite_dir: Path,
    calibration_path: Path,
    recommended_mapping_path: Optional[Path] = None,
) -> Dict[str, Any]:
    calibration_payload = _load_json_dict(calibration_path)
    vehicle_payload = calibration_payload.get("vehicle")
    if not isinstance(vehicle_payload, dict):
        vehicle_payload = {}
    entry_id = str(request_context.get("fingerprint", "") or "")
    entry_dir = library_root / entry_id
    entry_dir.mkdir(parents=True, exist_ok=True)
    stored_calibration = entry_dir / "carla_actuator_calibration.json"
    shutil.copy2(calibration_path, stored_calibration)
    stored_mapping = ""
    if recommended_mapping_path is not None and recommended_mapping_path.exists():
        mapping_dst = entry_dir / "recommended_physical_mapping.yaml"
        shutil.copy2(recommended_mapping_path, mapping_dst)
        stored_mapping = str(mapping_dst)
    metadata = {
        "schema_version": 1,
        "entry_id": entry_id,
        "request_context": dict(request_context),
        "vehicle": vehicle_payload,
        "source_suite_dir": str(suite_dir),
        "calibration_path": str(stored_calibration),
        "recommended_mapping_path": stored_mapping,
        "created_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
    }
    _write_json(entry_dir / "metadata.json", metadata)
    index = _load_calibration_library_index(library_root)
    entries = [item for item in index.get("entries", []) if isinstance(item, dict)]
    entries = [item for item in entries if str(item.get("entry_id", "")) != entry_id]
    entries.append(
        {
            "entry_id": entry_id,
            "request_fingerprint": entry_id,
            "map": str(request_context.get("map", "") or ""),
            "apollo_profile_sha256": str(request_context.get("apollo_profile_sha256", "") or ""),
            "vehicle_profile_sha256": str(request_context.get("vehicle_profile_sha256", "") or ""),
            "vehicle_type_id": str(vehicle_payload.get("type_id", "") or ""),
            "calibration_path": str(stored_calibration),
            "recommended_mapping_path": stored_mapping,
            "metadata_path": str(entry_dir / "metadata.json"),
            "created_at": metadata["created_at"],
        }
    )
    index["entries"] = entries
    _write_calibration_library_index(library_root, index)
    return metadata


def _find_pid_listening_on_port(port: int) -> Optional[int]:
    try:
        out = subprocess.check_output(["ss", "-ltnp"], text=True)
    except Exception:
        return None
    for line in out.splitlines():
        if f":{port}" not in line:
            continue
        match = re.search(r"pid=(\d+)", line)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                return None
    return None


def _cleanup_port_process(port: int) -> bool:
    pid = _find_pid_listening_on_port(port)
    if not pid:
        return False
    _warn(f"cleanup stale port owner: pid={pid} port={port}")
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except Exception as exc:
        _warn(f"SIGTERM failed for pid={pid}: {exc}")
        return False
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if not _port_open("127.0.0.1", port) and not _port_open("localhost", port):
            return True
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except Exception as exc:
        _warn(f"SIGKILL failed for pid={pid}: {exc}")
        return False
    time.sleep(0.5)
    return True


def _resolve_run_dir(run_dir: Path) -> Path:
    redirect = run_dir / "RUN_DIR_REDIRECT.txt"
    if redirect.exists():
        try:
            target = Path(redirect.read_text().strip())
            if target.exists():
                return target
        except Exception:
            return run_dir
    latest = run_dir.parent / "LATEST.txt"
    if latest.exists():
        try:
            target = Path(latest.read_text().strip())
            if target.exists() and target.parent == run_dir.parent:
                if target == run_dir or target.name.startswith(f"{run_dir.name}__"):
                    return target
        except Exception:
            pass
    try:
        candidates = sorted(
            (
                path
                for path in run_dir.parent.glob(f"{run_dir.name}__*")
                if path.is_dir()
            ),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        candidates = []
    for candidate in candidates:
        if (candidate / "effective.yaml").exists() or (candidate / "artifacts").exists():
            return candidate
    return run_dir


def _summarize_failure(*, scene_name: str, run_dir: Path, log_path: Path) -> str:
    resolved_run_dir = _resolve_run_dir(run_dir)
    parts = [
        f"scene={scene_name}",
        f"scene_log={log_path}",
        f"run_dir={resolved_run_dir}",
    ]
    for candidate in [
        log_path,
        resolved_run_dir / "logs" / "carla_server.log",
        resolved_run_dir / "artifacts" / "doctor.txt",
    ]:
        tail = _tail_text(candidate, max_lines=20)
        if tail:
            parts.append(f"--- tail: {candidate} ---\n{tail}")
    return "\n".join(parts)


def _run_logged(
    cmd: Sequence[str],
    *,
    log_path: Path,
    cwd: Path,
    label: str,
    heartbeat_sec: float = 20.0,
    timeout_sec: Optional[float] = None,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as fp:
        fp.write("$ %s\n\n" % " ".join(cmd))
        fp.flush()
        proc = subprocess.Popen(
            list(cmd),
            cwd=cwd,
            stdout=fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        start = time.time()
        next_heartbeat = start + max(5.0, heartbeat_sec)
        deadline = (start + float(timeout_sec)) if timeout_sec and timeout_sec > 0.0 else None
        while True:
            rc = proc.poll()
            if rc is not None:
                if rc != 0:
                    raise subprocess.CalledProcessError(rc, proc.args)
                return
            now = time.time()
            if deadline is not None and now >= deadline:
                elapsed = int(now - start)
                _warn(f"{label} timed out after {elapsed}s; stopping process group")
                _stop_process_group(proc)
                raise TimeoutError(f"{label} timed out after {float(timeout_sec):.1f}s")
            if now >= next_heartbeat:
                elapsed = int(now - start)
                _info(f"{label} still running: {elapsed}s elapsed, log={log_path}")
                next_heartbeat = now + max(5.0, heartbeat_sec)
            time.sleep(1.0)


def _start_logged(cmd: Sequence[str], *, log_path: Path, cwd: Path) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fp = log_path.open("w")
    fp.write("$ %s\n\n" % " ".join(cmd))
    fp.flush()
    return subprocess.Popen(
        list(cmd),
        cwd=cwd,
        stdout=fp,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def _stop_process_group(proc: subprocess.Popen, *, timeout_sec: float = 20.0) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        return
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.5)
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return


def _list_vehicles(host: str, port: int) -> List[Dict[str, Any]]:
    cmd = [
        sys.executable,
        "tools/calibrate_carla_actuators.py",
        "--carla-host",
        host,
        "--carla-port",
        str(port),
        "--list-vehicles",
    ]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        return []
    try:
        payload = json.loads(proc.stdout)
    except Exception:
        return []
    vehicles = payload.get("vehicles")
    return vehicles if isinstance(vehicles, list) else []


def _wait_for_carla_ready(
    host: str,
    port: int,
    *,
    timeout_sec: float = 300.0,
    poll_sec: float = 1.0,
    post_ready_settle_sec: float = 8.0,
) -> None:
    deadline = time.time() + timeout_sec
    next_heartbeat = time.time() + max(5.0, poll_sec * 5.0)
    while time.time() < deadline:
        if _carla_server_ready(host, port):
            _info(f"CARLA ready: host={host} port={port}")
            if post_ready_settle_sec > 0.0:
                _info(f"CARLA settle wait: {post_ready_settle_sec:.1f}s")
                time.sleep(post_ready_settle_sec)
            return
        if time.time() >= next_heartbeat:
            remain = max(0.0, deadline - time.time())
            _info(f"waiting CARLA ready: host={host} port={port} remain={remain:.0f}s")
            next_heartbeat = time.time() + max(5.0, poll_sec * 5.0)
        time.sleep(poll_sec)
    raise TimeoutError(f"carla server not ready, host={host} port={port}")


def _wait_for_vehicle(
    host: str,
    port: int,
    role_name: str,
    *,
    timeout_sec: float = 240.0,
    poll_sec: float = 2.0,
    stable_hits: int = 1,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_sec
    consecutive_hits = 0
    last_match: Dict[str, Any] = {}
    next_heartbeat = time.time() + max(5.0, poll_sec * 5.0)
    while time.time() < deadline:
        vehicles = _list_vehicles(host, port)
        matched = None
        for vehicle in vehicles:
            if str(vehicle.get("role_name", "") or "").strip() == role_name:
                matched = vehicle
                break
        if matched is not None:
            consecutive_hits += 1
            last_match = matched
            if consecutive_hits >= max(1, stable_hits):
                _info(
                    f"ego ready: role={role_name} actor_id={matched.get('actor_id')} "
                    f"stable_hits={consecutive_hits}"
                )
                return matched
        else:
            consecutive_hits = 0
        if time.time() >= next_heartbeat:
            remain = max(0.0, deadline - time.time())
            _info(
                f"waiting ego vehicle: role={role_name} host={host} port={port} "
                f"remain={remain:.0f}s visible={bool(last_match)} stable_hits={consecutive_hits}"
            )
            next_heartbeat = time.time() + max(5.0, poll_sec * 5.0)
        time.sleep(poll_sec)
    raise TimeoutError(f"ego vehicle not ready, role={role_name} host={host} port={port}")


def _summarize_calibration_failure(*, suite_dir: Path, run_dir: Path) -> str:
    logs_root = suite_dir / "logs"
    resolved_run_dir = _resolve_run_dir(run_dir)
    parts = [
        f"run_dir={resolved_run_dir}",
        f"probe_log={logs_root / 'actuator_calibration_probe.log'}",
        f"run_log={logs_root / 'actuator_calibration_run.log'}",
    ]
    for candidate in [
        logs_root / "actuator_calibration_probe.log",
        logs_root / "actuator_calibration_run.log",
        resolved_run_dir / "logs" / "carla_server.log",
        resolved_run_dir / "artifacts" / "doctor.txt",
    ]:
        tail = _tail_text(candidate, max_lines=25)
        if tail:
            parts.append(f"--- tail: {candidate} ---\n{tail}")
    return "\n".join(parts)


def _wait_for_ego_actor_id(
    run_dir: Path,
    *,
    timeout_sec: float = 120.0,
    poll_sec: float = 1.0,
) -> int:
    resolved_run_dir = _resolve_run_dir(run_dir)
    meta_path = resolved_run_dir / "artifacts" / "scenario_metadata.json"
    deadline = time.time() + timeout_sec
    next_heartbeat = time.time() + max(5.0, poll_sec * 5.0)
    while time.time() < deadline:
        if meta_path.exists():
            try:
                payload = json.loads(meta_path.read_text())
            except Exception:
                payload = {}
            actor_id = int((payload.get("ego_actor_id", 0) or 0))
            if actor_id > 0:
                _info(f"ego actor id ready: actor_id={actor_id} meta={meta_path}")
                return actor_id
        if time.time() >= next_heartbeat:
            remain = max(0.0, deadline - time.time())
            _info(f"waiting ego actor_id metadata: remain={remain:.0f}s path={meta_path}")
            next_heartbeat = time.time() + max(5.0, poll_sec * 5.0)
        time.sleep(poll_sec)
    raise TimeoutError(f"ego actor_id not found in scenario metadata: {meta_path}")


def _ensure_runtime_overrides(
    cfg: Dict[str, Any],
    *,
    map_name: str,
    carla_host: str,
    carla_port: int,
) -> Dict[str, Any]:
    runtime_carla = cfg.setdefault("runtime", {}).setdefault("carla", {})
    runtime_carla["host"] = carla_host
    runtime_carla["port"] = int(carla_port)
    if not _port_open(carla_host, carla_port):
        runtime_carla["start"] = True
    cfg.setdefault("carla", {})["host"] = carla_host
    cfg.setdefault("carla", {})["port"] = int(carla_port)
    cfg.setdefault("run", {})["map"] = map_name
    apollo_bridge = cfg.setdefault("algo", {}).setdefault("apollo", {}).setdefault("bridge", {})
    apollo_bridge.setdefault("carla_feedback", {})["host"] = carla_host
    apollo_bridge.setdefault("carla_feedback", {})["port"] = int(carla_port)
    return cfg


def _build_semantic_scene_specs() -> List[Dict[str, Any]]:
    return [
        {
            "name": "lat_straight_track",
            "fragment": REPO_ROOT / "configs/io/examples/apollo_semantic_capture_lateral.yaml",
            "overrides": {
                "run": {"ticks": 460},
                "scenario": {
                    "semantic_suite": {"scene_type": "lateral_straight_track"},
                    "ego_pose_offset": {"y_m": 0.10, "yaw_deg": 0.6},
                },
            },
            "purpose": "直道轻微跟踪，尽量采小幅 steering 修正。",
        },
        {
            "name": "lat_left_curve",
            "fragment": REPO_ROOT / "configs/io/examples/apollo_semantic_capture_lateral.yaml",
            "overrides": {
                "run": {"ticks": 520},
                "scenario": {
                    "semantic_suite": {"scene_type": "lateral_left_curve"},
                    "ego_pose_offset": {"y_m": 0.20, "yaw_deg": 1.5},
                },
            },
            "purpose": "温和左弯场景，采从小到中的左向 steering。",
        },
        {
            "name": "lat_right_curve",
            "fragment": REPO_ROOT / "configs/io/examples/apollo_semantic_capture_lateral.yaml",
            "overrides": {
                "run": {"ticks": 520},
                "scenario": {
                    "semantic_suite": {"scene_type": "lateral_right_curve"},
                    "ego_pose_offset": {"y_m": -0.20, "yaw_deg": -1.5},
                },
            },
            "purpose": "温和右弯场景，采从小到中的右向 steering。",
        },
        {
            "name": "lon_launch_cruise",
            "fragment": REPO_ROOT / "configs/io/examples/apollo_semantic_capture_longitudinal.yaml",
            "overrides": {
                "run": {"ticks": 560},
                "scenario": {"semantic_suite": {"scene_type": "longitudinal_launch_cruise"}},
            },
            "purpose": "起步和短时巡航，尽量暴露正向驱动字段。",
        },
        {
            "name": "lon_follow_decel",
            "fragment": REPO_ROOT / "configs/io/examples/apollo_semantic_capture_longitudinal.yaml",
            "overrides": {
                "run": {"ticks": 760},
                "scenario": {"semantic_suite": {"scene_type": "longitudinal_follow_decel"}},
            },
            "purpose": "跟车减速直到接近静止，暴露 brake/减速度字段。",
        },
        {
            "name": "lon_stop_and_go",
            "fragment": REPO_ROOT / "configs/io/examples/apollo_semantic_capture_longitudinal.yaml",
            "overrides": {
                "run": {"ticks": 520},
                "scenario": {
                    "semantic_suite": {
                        "scene_type": "longitudinal_stop_and_go",
                        "lead_profile": {
                            "mode": "stop_and_go",
                            "cruise_speed_mps": 6.0,
                            "hold_before_move_sec": 2.0,
                            "cruise_hold_sec": 4.0,
                            "stop_hold_sec": 3.0,
                        },
                    },
                },
            },
            "purpose": "动态前车 stop-and-go 场景，采 throttle/brake/accel 的切换关系。",
        },
    ]


def _build_scene_config(
    *,
    base_config_path: Path,
    fragment_path: Path,
    vehicle_profile_path: Optional[Path],
    map_name: str,
    carla_host: str,
    carla_port: int,
    carla_wait_cfg: Dict[str, Any],
    extra_overrides: Dict[str, Any],
) -> Dict[str, Any]:
    cfg = _load_yaml(base_config_path)
    _deep_update(cfg, _load_yaml(fragment_path))
    if vehicle_profile_path is not None:
        _deep_update(cfg, _load_yaml(vehicle_profile_path))
    _deep_update(cfg, {"runtime": {"carla": dict(carla_wait_cfg)}})
    _deep_update(cfg, extra_overrides)
    return _ensure_runtime_overrides(cfg, map_name=map_name, carla_host=carla_host, carla_port=carla_port)


def _semantic_base_config(path: Path) -> Path:
    return path


def _run_semantic_scenes(
    *,
    suite_dir: Path,
    base_apollo_profile: Path,
    vehicle_profile_path: Optional[Path],
    map_name: str,
    carla_host: str,
    carla_port: int,
    carla_wait_cfg: Dict[str, Any],
    heartbeat_sec: float,
    cleanup_port_on_failure: bool,
    capture_policy: Dict[str, Any],
    scene_timeout_sec: Optional[float],
    max_scenes: int,
) -> Dict[str, Any]:
    scenes_root = suite_dir / "scenes"
    generated_cfg_root = suite_dir / "generated_configs"
    logs_root = suite_dir / "logs"
    specs = _build_semantic_scene_specs()
    if max_scenes > 0:
        specs = specs[:max_scenes]
    results: List[Dict[str, Any]] = []
    capture_details: List[Dict[str, Any]] = []
    total = len(specs)
    for index, spec in enumerate(specs, start=1):
        run_dir = scenes_root / spec["name"]
        config_path = generated_cfg_root / f"{spec['name']}.yaml"
        scene_log_path = logs_root / f"{spec['name']}.log"
        _info(f"[scene {index}/{total}] start {spec['name']} - {spec['purpose']}")
        cfg = _build_scene_config(
            base_config_path=_semantic_base_config(base_apollo_profile),
            fragment_path=spec["fragment"],
            vehicle_profile_path=vehicle_profile_path,
            map_name=map_name,
            carla_host=carla_host,
            carla_port=carla_port,
            carla_wait_cfg=carla_wait_cfg,
            extra_overrides=spec["overrides"],
        )
        cfg.setdefault("run", {})["profile_name"] = spec["name"]
        _write_yaml(config_path, cfg)
        cmd = [
            sys.executable,
            "-m",
            "carla_testbed",
            "run",
            "--config",
            str(config_path),
            "--run-dir",
            str(run_dir),
            "--no-healthcheck",
        ]
        exit_code = 0
        failure_summary = ""
        try:
            _run_logged(
                cmd,
                log_path=scene_log_path,
                cwd=REPO_ROOT,
                label=f"scene {spec['name']}",
                heartbeat_sec=heartbeat_sec,
                timeout_sec=scene_timeout_sec,
            )
        except subprocess.CalledProcessError as exc:
            exit_code = int(exc.returncode or 0)
            if cleanup_port_on_failure:
                _cleanup_port_process(carla_port)
            failure_summary = _summarize_failure(scene_name=spec["name"], run_dir=run_dir, log_path=scene_log_path)
            _warn(
                f"[scene {index}/{total}] failed {spec['name']} rc={exc.returncode}; "
                "capturing validity summary before deciding suite outcome"
            )
        except TimeoutError as exc:
            exit_code = 124
            if cleanup_port_on_failure:
                _cleanup_port_process(carla_port)
            failure_summary = _summarize_failure(scene_name=spec["name"], run_dir=run_dir, log_path=scene_log_path)
            failure_summary = f"{exc}\n{failure_summary}"
            _warn(
                f"[scene {index}/{total}] timed out {spec['name']} after {float(scene_timeout_sec or 0.0):.1f}s; "
                "capturing validity summary before deciding suite outcome"
            )
        else:
            _info(f"[scene {index}/{total}] done {spec['name']}")
        resolved_run_dir = _resolve_run_dir(run_dir)
        raw_path = resolved_run_dir / "artifacts" / "apollo_control_raw.jsonl"
        detail = evaluate_capture_validity(
            capture_id=spec["name"],
            raw_path=raw_path if raw_path.exists() else None,
            exit_code=exit_code,
            policy=capture_policy,
            summary_path=resolved_run_dir / "summary.json",
            metadata_path=resolved_run_dir / "artifacts" / "scenario_metadata.json",
            log_path=scene_log_path,
        )
        detail.update(
            {
                "purpose": spec["purpose"],
                "config_path": str(config_path),
                "run_dir": str(resolved_run_dir),
                "failure_summary": failure_summary,
            }
        )
        capture_details.append(detail)
        results.append(
            {
                "name": spec["name"],
                "purpose": spec["purpose"],
                "run_dir": str(resolved_run_dir),
                "config_path": str(config_path),
                "exit_code": exit_code,
                "capture_valid": bool(detail.get("capture_valid")),
            }
        )
    return {
        "scenes": results,
        "capture_summary": summarize_capture_validity(capture_details, policy=capture_policy),
    }


def _calibration_probe_extra_args(detail: str) -> List[str]:
    mode = str(detail or "exhaustive").strip().lower()
    if mode != "exhaustive":
        return []
    return [
        "--speed-bins",
        "0,1,2,3,5,7.5,10,12.5,15,20,25",
        "--max-raw-series-per-axis",
        "2000",
        "--steering-commands",
        "-1.0,-0.9,-0.8,-0.7,-0.6,-0.5,-0.4,-0.3,-0.2,-0.1,0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0",
        "--steering-sample-sec",
        "2.0",
        "--throttle-commands",
        "0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6,0.7,0.8,0.9,1.0",
        "--throttle-sample-sec",
        "3.2",
        "--throttle-effective-window-sec",
        "1.2",
        "--brake-commands",
        "0.02,0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.35,0.4,0.5,0.6,0.7,0.8,1.0",
        "--brake-sample-sec",
        "3.0",
        "--brake-effective-window-sec",
        "0.8",
        "--low-speed-throttle-commands",
        "0.03,0.05,0.08,0.1,0.12,0.15,0.18,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.6,0.7,0.8,0.9,1.0",
        "--low-speed-launch-sample-sec",
        "2.0",
        "--low-speed-launch-eval-sec",
        "1.0",
        "--low-speed-crawl-entry-speeds",
        "1.5,2.0,3.0,4.0,6.0,8.0",
        "--low-speed-crawl-sample-sec",
        "1.6",
        "--low-speed-crawl-eval-sec",
        "1.0",
        "--low-speed-brake-commands",
        "0.02,0.03,0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.4,0.5",
        "--low-speed-hold-entry-speed-mps",
        "1.0",
        "--low-speed-hold-sample-sec",
        "1.8",
        "--low-speed-hold-eval-sec",
        "0.8",
        "--low-speed-rolling-brake-entry-speeds",
        "1.5,2.0,3.0,4.0,6.0,8.0",
        "--low-speed-rolling-brake-sample-sec",
        "1.2",
        "--low-speed-rolling-brake-eval-sec",
        "0.4",
    ]


def _run_calibration_stage(
    *,
    suite_dir: Path,
    vehicle_profile_path: Optional[Path],
    map_name: str,
    carla_host: str,
    carla_port: int,
    carla_wait_cfg: Dict[str, Any],
    carla_ready_timeout_sec: float,
    carla_ready_poll_sec: float,
    carla_post_ready_settle_sec: float,
    hero_ready_timeout_sec: float,
    hero_ready_poll_sec: float,
    calibration_detail: str,
    heartbeat_sec: float,
    cleanup_port_on_failure: bool,
) -> Dict[str, Any]:
    config_path = suite_dir / "generated_configs" / "actuator_calibration.yaml"
    run_dir = suite_dir / "calibration_run"
    logs_root = suite_dir / "logs"
    calibration_base = REPO_ROOT / "configs/io/examples/followstop_dummy.yaml"
    calibration_fragment = REPO_ROOT / "configs/io/examples/apollo_actuator_calibration.yaml"
    cfg = _build_scene_config(
        base_config_path=calibration_base,
        fragment_path=calibration_fragment,
        vehicle_profile_path=vehicle_profile_path,
        map_name=map_name,
        carla_host=carla_host,
        carla_port=carla_port,
        carla_wait_cfg=carla_wait_cfg,
        extra_overrides={},
    )
    _write_yaml(config_path, cfg)
    if _port_open(carla_host, carla_port):
        _warn(f"[calibration] pre-clean existing CARLA port owner on {carla_host}:{carla_port}")
        _cleanup_port_process(carla_port)
        time.sleep(2.0)
    run_cmd = [
        sys.executable,
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(config_path),
        "--run-dir",
        str(run_dir),
        "--no-healthcheck",
    ]
    proc = _start_logged(run_cmd, log_path=logs_root / "actuator_calibration_run.log", cwd=REPO_ROOT)
    calibration_path = suite_dir / "artifacts" / "carla_actuator_calibration.json"
    try:
        _info("[calibration] waiting CARLA and ego vehicle")
        _wait_for_carla_ready(
            carla_host,
            carla_port,
            timeout_sec=carla_ready_timeout_sec,
            poll_sec=carla_ready_poll_sec,
            post_ready_settle_sec=carla_post_ready_settle_sec,
        )
        ego_actor_id = _wait_for_ego_actor_id(
            run_dir,
            timeout_sec=hero_ready_timeout_sec,
            poll_sec=1.0,
        )
        _wait_for_carla_ready(
            carla_host,
            carla_port,
            timeout_sec=min(60.0, carla_ready_timeout_sec),
            poll_sec=carla_ready_poll_sec,
            post_ready_settle_sec=0.0,
        )
        time.sleep(2.0)
        calib_cmd = [
            sys.executable,
            "tools/calibrate_carla_actuators.py",
            "--carla-host",
            carla_host,
            "--carla-port",
            str(carla_port),
            "--ego-role-name",
            "hero",
            "--actor-id",
            str(ego_actor_id),
            "--timeout-sec",
            "20",
            "--ego-discovery-timeout-sec",
            "30",
            "--ego-discovery-poll-sec",
            "1.0",
            "--output",
            str(calibration_path),
        ]
        calib_cmd.extend(_calibration_probe_extra_args(calibration_detail))
        _info("[calibration] start open-loop probe")
        _run_logged(
            calib_cmd,
            log_path=logs_root / "actuator_calibration_probe.log",
            cwd=REPO_ROOT,
            label="actuator calibration probe",
            heartbeat_sec=heartbeat_sec,
        )
        _info(f"[calibration] done -> {calibration_path}")
    except Exception as exc:
        if cleanup_port_on_failure:
            _cleanup_port_process(carla_port)
        summary = _summarize_calibration_failure(suite_dir=suite_dir, run_dir=run_dir)
        raise RuntimeError(f"calibration stage failed: {exc}\n{summary}") from exc
    finally:
        _stop_process_group(proc)
    return {
        "run_dir": str(run_dir),
        "config_path": str(config_path),
        "calibration_path": str(calibration_path),
    }


def _run_semantic_inference(
    *,
    suite_dir: Path,
    calibration_relpath: str,
    heartbeat_sec: float,
    capture_validity_summary_path: Optional[Path] = None,
) -> Dict[str, Any]:
    logs_root = suite_dir / "logs"
    cmd = [
        sys.executable,
        "tools/infer_apollo_control_semantics.py",
        "--input-dir",
        str(suite_dir),
        "--output-dir",
        str(suite_dir / "artifacts"),
        "--calibration-path",
        calibration_relpath,
    ]
    if capture_validity_summary_path is not None:
        cmd.extend(["--capture-validity-summary", str(capture_validity_summary_path)])
    _info("[inference] start control semantics inference")
    _run_logged(
        cmd,
        log_path=logs_root / "semantic_inference.log",
        cwd=REPO_ROOT,
        label="semantic inference",
        heartbeat_sec=heartbeat_sec,
    )
    report_path = suite_dir / "artifacts" / "control_semantics_report.json"
    payload = json.loads(report_path.read_text()) if report_path.exists() else {}
    _info(f"[inference] done -> {report_path}")
    return payload if isinstance(payload, dict) else {}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run Apollo↔CARLA semantic capture and actuator calibration suite")
    ap.add_argument(
        "--vehicle-profile",
        default="",
        help="可选 YAML overlay，用于车辆蓝图、spawn、姿态偏置等场景/车型差异",
    )
    ap.add_argument(
        "--map",
        default="Town01",
        help="CARLA town 名称；会覆盖 base profile 的 run.map",
    )
    ap.add_argument("--output-dir", default="")
    ap.add_argument("--mode", choices=("semantic", "calibration", "full"), default="full")
    ap.add_argument(
        "--apollo-profile",
        default="configs/io/examples/apollo_semantic_suite_base.yaml",
        help="Apollo semantic suite 专用 base config",
    )
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--carla-ready-timeout-sec", type=float, default=300.0)
    ap.add_argument("--carla-ready-poll-sec", type=float, default=1.0)
    ap.add_argument("--carla-post-ready-settle-sec", type=float, default=8.0)
    ap.add_argument("--hero-ready-timeout-sec", type=float, default=240.0)
    ap.add_argument("--hero-ready-poll-sec", type=float, default=2.0)
    ap.add_argument("--carla-get-world-attempts", type=int, default=8)
    ap.add_argument("--carla-get-world-delay-sec", type=float, default=5.0)
    ap.add_argument("--carla-load-world-attempts", type=int, default=5)
    ap.add_argument("--carla-load-world-delay-sec", type=float, default=5.0)
    ap.add_argument("--carla-load-world-timeout-sec", type=float, default=120.0)
    ap.add_argument("--heartbeat-sec", type=float, default=20.0, help="长任务进度心跳打印间隔")
    ap.add_argument(
        "--scene-timeout-sec",
        type=float,
        default=600.0,
        help="单个 semantic scene 的最大执行时长；超时后会停止该 scene 并按 capture invalid 记录",
    )
    ap.add_argument(
        "--max-scenes",
        type=int,
        default=0,
        help="仅运行前 N 个 semantic scenes；0 表示运行全部场景",
    )
    ap.add_argument(
        "--cleanup-port-on-failure",
        action="store_true",
        help="scene/calibration 失败时尝试清理 CARLA 端口占用进程",
    )
    ap.add_argument(
        "--cleanup-port-before-start",
        action="store_true",
        help="suite 启动前先尝试清理目标 CARLA 端口占用进程",
    )
    ap.add_argument(
        "--calibration-detail",
        choices=("standard", "exhaustive"),
        default="exhaustive",
        help="calibration probe 细致程度；exhaustive 会使用更密的命令/速度网格和更长采样窗口",
    )
    ap.add_argument(
        "--calibration-library-dir",
        default="artifacts/actuator_calibration_library",
        help="保存与复用标定结果的目录；相同指纹命中时可直接复用",
    )
    ap.add_argument(
        "--force-recalibration",
        action="store_true",
        help="即使命中已有保存标定，也强制重新跑 calibration",
    )
    ap.add_argument(
        "--policy-config",
        default="configs/io/examples/unified_calibration_pipeline.yaml",
        help="capture validity / minimum coverage policy yaml",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (REPO_ROOT / "runs" / f"semantic_suite_{ts}").resolve()
    )
    suite_dir.mkdir(parents=True, exist_ok=True)
    (suite_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    base_apollo_profile = Path(args.apollo_profile).expanduser()
    if not base_apollo_profile.is_absolute():
        base_apollo_profile = (REPO_ROOT / base_apollo_profile).resolve()
    vehicle_profile_path = None
    if args.vehicle_profile:
        vehicle_profile_path = Path(args.vehicle_profile).expanduser()
        if not vehicle_profile_path.is_absolute():
            vehicle_profile_path = (REPO_ROOT / vehicle_profile_path).resolve()
    calibration_library_root = Path(args.calibration_library_dir).expanduser()
    if not calibration_library_root.is_absolute():
        calibration_library_root = (REPO_ROOT / calibration_library_root).resolve()
    policy_path = Path(args.policy_config).expanduser()
    if not policy_path.is_absolute():
        policy_path = (REPO_ROOT / policy_path).resolve()
    capture_policy = load_policy_config(policy_path if policy_path.exists() else None)
    request_context = _requested_calibration_context(
        map_name=args.map,
        apollo_profile=base_apollo_profile,
        vehicle_profile=vehicle_profile_path,
    )
    saved_entry = _find_saved_calibration_entry(calibration_library_root, request_context=request_context)
    carla_wait_cfg: Dict[str, Any] = {
        "ready_timeout_sec": float(args.carla_ready_timeout_sec),
        "ready_poll_sec": float(args.carla_ready_poll_sec),
        "post_ready_settle_sec": float(args.carla_post_ready_settle_sec),
        "get_world_attempts": int(args.carla_get_world_attempts),
        "get_world_delay_sec": float(args.carla_get_world_delay_sec),
        "load_world_attempts": int(args.carla_load_world_attempts),
        "load_world_delay_sec": float(args.carla_load_world_delay_sec),
        "load_world_timeout_sec": float(args.carla_load_world_timeout_sec),
    }

    suite_meta: Dict[str, Any] = {
        "schema_version": 1,
        "created_at": ts,
        "suite_dir": str(suite_dir),
        "mode": args.mode,
        "apollo_profile": str(base_apollo_profile),
        "vehicle_profile": str(vehicle_profile_path) if vehicle_profile_path else "",
        "map": args.map,
        "carla_host": args.carla_host,
        "carla_port": args.carla_port,
        "carla_wait": carla_wait_cfg,
        "hero_ready_timeout_sec": float(args.hero_ready_timeout_sec),
        "hero_ready_poll_sec": float(args.hero_ready_poll_sec),
        "heartbeat_sec": float(args.heartbeat_sec),
        "scene_timeout_sec": float(args.scene_timeout_sec),
        "max_scenes": int(args.max_scenes),
        "cleanup_port_on_failure": bool(args.cleanup_port_on_failure),
        "cleanup_port_before_start": bool(args.cleanup_port_before_start),
        "calibration_detail": str(args.calibration_detail),
        "calibration_library_dir": str(calibration_library_root),
        "force_recalibration": bool(args.force_recalibration),
        "policy_config": str(policy_path),
        "capture_policy": capture_policy,
        "requested_calibration_context": request_context,
    }
    _info(f"suite start: mode={args.mode} map={args.map} out={suite_dir}")
    _info(f"suite base profile: {base_apollo_profile}")
    if args.vehicle_profile:
        _info(f"suite vehicle overlay: {vehicle_profile_path}")
    if saved_entry is not None:
        _info(
            "[calibration-cache] matched saved calibration: "
            f"entry_id={saved_entry.get('entry_id','')} path={saved_entry.get('calibration_path','')}"
        )
    else:
        _warn(
            "[calibration-cache] no compatible saved calibration for current map/profile/vehicle overlay; "
            "recommend one calibration run before relying on physical mapping"
        )
    if args.cleanup_port_before_start:
        _cleanup_port_process(int(args.carla_port))

    manifest_path = suite_dir / "suite_manifest.json"
    suite_meta["status"] = "running"
    suite_meta["calibration_cache"] = {
        "library_dir": str(calibration_library_root),
        "request_context": request_context,
        "matched_entry": saved_entry or {},
    }
    _write_json(manifest_path, suite_meta)
    try:
        if args.mode in {"semantic", "full"}:
            semantic_result = _run_semantic_scenes(
                suite_dir=suite_dir,
                base_apollo_profile=base_apollo_profile,
                vehicle_profile_path=vehicle_profile_path,
                map_name=args.map,
                carla_host=args.carla_host,
                carla_port=args.carla_port,
                carla_wait_cfg=carla_wait_cfg,
                heartbeat_sec=float(args.heartbeat_sec),
                cleanup_port_on_failure=bool(args.cleanup_port_on_failure),
                capture_policy=capture_policy,
                scene_timeout_sec=float(args.scene_timeout_sec),
                max_scenes=int(args.max_scenes),
            )
            suite_meta["semantic_scenes"] = semantic_result.get("scenes", [])
            suite_meta["capture_summary"] = semantic_result.get("capture_summary", {})
            write_capture_validity_artifacts(suite_dir / "artifacts", suite_meta["capture_summary"])
            write_policy_artifacts(suite_dir / "artifacts", suite_meta["capture_summary"])
            _write_json(manifest_path, suite_meta)
            if not bool((suite_meta.get("capture_summary") or {}).get("minimum_coverage_ok")):
                raise RuntimeError(
                    "semantic capture minimum coverage not met: "
                    + ", ".join((suite_meta.get("capture_summary") or {}).get("minimum_coverage_reasons", []))
                )
        reused_calibration = None
        if args.mode in {"semantic", "full"} and saved_entry is not None and not args.force_recalibration:
            reused_calibration = _stage_saved_calibration_into_suite(suite_dir=suite_dir, entry=saved_entry)
            suite_meta["calibration"] = dict(reused_calibration)
            _info(
                "[calibration-cache] staged saved calibration into suite artifacts: "
                f"{reused_calibration['staged_calibration_path']}"
            )
            _write_json(manifest_path, suite_meta)
        if args.mode in {"calibration", "full"} and not (args.mode == "full" and reused_calibration is not None):
            suite_meta["calibration"] = _run_calibration_stage(
                suite_dir=suite_dir,
                vehicle_profile_path=vehicle_profile_path,
                map_name=args.map,
                carla_host=args.carla_host,
                carla_port=args.carla_port,
                carla_wait_cfg=carla_wait_cfg,
                carla_ready_timeout_sec=float(args.carla_ready_timeout_sec),
                carla_ready_poll_sec=float(args.carla_ready_poll_sec),
                carla_post_ready_settle_sec=float(args.carla_post_ready_settle_sec),
                hero_ready_timeout_sec=float(args.hero_ready_timeout_sec),
                hero_ready_poll_sec=float(args.hero_ready_poll_sec),
                calibration_detail=str(args.calibration_detail),
                heartbeat_sec=float(args.heartbeat_sec),
                cleanup_port_on_failure=bool(args.cleanup_port_on_failure),
            )
            _write_json(manifest_path, suite_meta)
        calibration_relpath = "artifacts/carla_actuator_calibration.json"
        recommended_mapping_path: Optional[Path] = None
        calibration_artifact_path = suite_dir / "artifacts" / "carla_actuator_calibration.json"
        if args.mode in {"semantic", "full"} and calibration_artifact_path.exists():
            suite_meta["semantic_report"] = _run_semantic_inference(
                suite_dir=suite_dir,
                calibration_relpath=calibration_relpath,
                heartbeat_sec=float(args.heartbeat_sec),
                capture_validity_summary_path=suite_dir / "artifacts" / "calibration_capture_validity_summary.json",
            )
            recommended_mapping_path = suite_dir / "artifacts" / "recommended_physical_mapping.yaml"
        elif args.mode in {"semantic", "full"}:
            _warn(
                "[calibration-cache] no calibration artifact available for semantic inference; "
                "run --mode calibration or --mode full once, or keep a compatible saved calibration in the library"
            )
        if calibration_artifact_path.exists():
            persisted = _persist_calibration_entry(
                library_root=calibration_library_root,
                request_context=request_context,
                suite_dir=suite_dir,
                calibration_path=calibration_artifact_path,
                recommended_mapping_path=recommended_mapping_path,
            )
            suite_meta["calibration_cache"]["persisted_entry"] = persisted
        suite_meta["status"] = "completed"
        _write_json(manifest_path, suite_meta)
        _info(f"suite done: {suite_dir}")
        return 0
    except Exception as exc:
        suite_meta["status"] = "failed"
        suite_meta["error"] = str(exc)
        _write_json(manifest_path, suite_meta)
        _warn(f"suite failed: {exc}")
        _warn(f"manifest written: {manifest_path}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
