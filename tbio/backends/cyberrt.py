from __future__ import annotations

import json
import math
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import html
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from carla_testbed.adapters.apollo.cyber_gt_bridge import default_apollo_cyber_gt_bridge_entrypoint

from .base import Backend


class CyberRTBackend(Backend):
    def __init__(self, profile: Dict[str, Any]):
        super().__init__(profile)
        self.repo_root = Path(__file__).resolve().parents[2]
        self.bridge_proc: Optional[subprocess.Popen] = None
        self.control_proc: Optional[subprocess.Popen] = None
        self._docker_container_name: Optional[str] = None
        self._docker_stage_dir: Optional[str] = None
        self._docker_stats_path: Optional[str] = None
        self._runtime_source_prefix: Optional[str] = None
        self._bridge_out = None
        self._bridge_err = None
        self._control_out = None
        self._control_err = None
        self._dreamview_started_by_backend = False
        self._dreamview_started_mode: Optional[str] = None
        self._dreamview_record_proc: Optional[subprocess.Popen] = None
        self._dreamview_record_out = None
        self._dreamview_record_err = None
        self._dreamview_record_mode: str = "ffmpeg_realtime"
        self._dreamview_tick_capture_enabled = False
        self._dreamview_tick_capture_region: Optional[tuple[int, int, int, int]] = None
        self._dreamview_tick_capture_dir: Optional[Path] = None
        self._dreamview_tick_capture_index_path: Optional[Path] = None
        self._dreamview_tick_capture_count = 0
        self._dreamview_tick_capture_fail_count = 0
        self._dreamview_tick_capture_last_error = ""
        self._dreamview_tick_capture_last_ts: Optional[float] = None
        self._dreamview_tick_capture_every_n = 1
        self._dreamview_capture_state: Dict[str, Any] = {}
        self._apollo_log_offsets: Dict[str, int] = {}
        self._carla_vehicle_param_override: Dict[str, Any] = {}
        self._last_docker_host_lib_cache_status: Dict[str, Any] = {}
        self._deferred_control_pending = False
        self._deferred_control_started = False
        self._deferred_control_overall_start_sec: Optional[float] = None
        self._deferred_control_route_seen_sec: Optional[float] = None
        self._deferred_control_route_established_sec: Optional[float] = None
        self._deferred_control_last_poll_sec: float = 0.0
        self._deferred_control_snapshots: list[str] = []
        self._deferred_control_failure: Optional[str] = None
        self._deferred_control_start_thread: Optional[threading.Thread] = None
        self._deferred_control_start_in_progress = False
        self._deferred_control_start_async_failure: Optional[str] = None
        self._control_runtime_overlay_active = False

    def _apollo_cfg(self) -> Dict[str, Any]:
        return (self.profile.get("algo", {}) or {}).get("apollo", {}) or {}

    def _run_cfg(self) -> Dict[str, Any]:
        return (self.profile.get("run", {}) or {})

    def _sim_dt_seconds(self) -> float:
        run_cfg = self._run_cfg()
        runtime_carla_cfg = (self.profile.get("runtime", {}) or {}).get("carla", {}) or {}
        raw_dt = run_cfg.get("dt", runtime_carla_cfg.get("fixed_delta_seconds", 0.05))
        try:
            dt = float(raw_dt)
        except Exception:
            dt = 0.05
        return dt if dt > 1e-6 else 0.05

    def _default_control_apply_hz(self) -> float:
        return max(1.0, 1.0 / self._sim_dt_seconds())

    def _docker_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("docker", {}) or {})

    def _transport_mode(self) -> str:
        raw = str(self._apollo_cfg().get("transport_mode") or "ros2_gt").strip().lower()
        return raw or "ros2_gt"

    def _direct_bridge_cfg(self) -> Dict[str, Any]:
        return dict(self._apollo_cfg().get("direct_bridge", {}) or {})

    def _direct_require_no_ros2_runtime(self) -> bool:
        return bool(self._direct_bridge_cfg().get("require_no_ros2_runtime", False))

    def _direct_route_command_mode(self) -> str:
        raw = str(self._direct_bridge_cfg().get("route_command_mode") or "cyber_direct").strip().lower()
        return raw or "cyber_direct"

    def _uses_ros2_gt(self) -> bool:
        return self._transport_mode() == "ros2_gt"

    def _uses_ros2_control_bridge(self) -> bool:
        if self._transport_mode() != "ros2_gt":
            return False
        return bool((self._apollo_cfg().get("carla_control_bridge", {}) or {}).get("enabled", True))

    def _requires_ros2_reexec(self) -> bool:
        return False

    def _docker_enabled(self) -> bool:
        cfg = self._docker_cfg()
        if "enabled" in cfg:
            return bool(cfg.get("enabled"))
        return bool(cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER"))

    def _discover_apollo_container(self) -> str:
        try:
            proc = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--format",
                    "{{.Names}}\t{{.Image}}\t{{.State}}",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            return ""
        if proc.returncode != 0:
            return ""

        running_candidates: list[str] = []
        all_candidates: list[str] = []
        for raw_line in proc.stdout.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t", 2)
            name = parts[0].strip()
            image = parts[1].strip().lower() if len(parts) > 1 else ""
            state = parts[2].strip().lower() if len(parts) > 2 else ""
            name_l = name.lower()
            if (
                name_l.startswith("apollo")
                or "apollo" in name_l
                or "apollo" in image
            ):
                all_candidates.append(name)
                if state == "running":
                    running_candidates.append(name)
        if len(running_candidates) == 1:
            return running_candidates[0]
        if len(all_candidates) == 1:
            return all_candidates[0]
        return ""

    def _docker_container(self) -> str:
        cfg = self._docker_cfg()
        name = str(cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER", "")).strip()
        if not name:
            name = self._discover_apollo_container()
            if name:
                self._docker_container_name = name
                return name
            raise RuntimeError(
                "Apollo docker mode enabled but container is missing; "
                "set algo.apollo.docker.container or APOLLO_DOCKER_CONTAINER"
            )
        return name

    def _docker_apollo_root(self) -> str:
        cfg = self._docker_cfg()
        return str(cfg.get("apollo_root_in_container") or "/apollo")

    def _docker_apollo_dist(self) -> str:
        cfg = self._docker_cfg()
        return str(cfg.get("apollo_distribution_home") or "/opt/apollo/neo")

    def _docker_python_exec(self) -> str:
        cfg = self._docker_cfg()
        return str(cfg.get("python_exec") or "python3")

    def _host_bridge_python_exec(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("host_python_exec") or "").strip()
        if not raw:
            return sys.executable
        if raw.startswith("${") and raw.endswith("}") and len(raw) > 3:
            env_name = raw[2:-1]
            env_value = os.environ.get(env_name, "").strip()
            if env_value:
                raw = env_value
            elif env_name == "CARLA16_PYTHON":
                return sys.executable
            else:
                raise RuntimeError(f"host_python_exec environment placeholder is not set: {raw}")
        expanded = os.path.expandvars(os.path.expanduser(raw))
        if expanded.startswith("${"):
            raise RuntimeError(f"host_python_exec environment placeholder is not resolved: {raw}")
        if os.path.sep in raw or expanded != raw:
            path = Path(expanded)
            if not path.exists():
                raise RuntimeError(f"host_python_exec not found: {path}")
            return str(path)
        resolved = shutil.which(raw)
        return resolved or raw

    def _docker_bridge_in_container(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("bridge_in_container", False))

    def _docker_require_ipc_host(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("require_ipc_host", True))

    def _docker_require_network_host(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("require_network_host", True))

    def _docker_auto_install_runtime_deps(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("auto_install_runtime_deps", True))

    def _docker_module_exec_user(self) -> str:
        cfg = self._docker_cfg()
        # Keep container module uid aligned with host user to avoid CyberRT shm permission mismatches.
        return str(cfg.get("module_exec_user") or "1000:1000")

    def _host_ros2_setup_script(self) -> Optional[str]:
        apollo_cfg = self._apollo_cfg()
        script = str(apollo_cfg.get("ros2_setup_script") or "").strip()
        if script:
            p = Path(script).expanduser()
            if not p.is_absolute():
                p = (self.repo_root / p).resolve()
            return str(p)
        distro = str(os.environ.get("ROS_DISTRO", "")).strip()
        candidates = []
        if distro:
            candidates.append(Path(f"/opt/ros/{distro}/setup.bash"))
        candidates.append(Path("/opt/ros/humble/setup.bash"))
        for cand in candidates:
            if cand.exists():
                return str(cand)
        return None

    def _cyber_domain_id(self) -> str:
        apollo_cfg = self._apollo_cfg()
        value = str(apollo_cfg.get("cyber_domain_id") or os.environ.get("CYBER_DOMAIN_ID", "")).strip()
        if value:
            return value
        if self._docker_enabled():
            # Apollo Docker setup.sh defaults to 80; keep host bridge aligned.
            return "80"
        return ""

    def _cyber_ip(self) -> str:
        apollo_cfg = self._apollo_cfg()
        return str(apollo_cfg.get("cyber_ip") or os.environ.get("CYBER_IP", "")).strip()

    def _docker_env_script(self) -> str:
        cfg = self._docker_cfg()
        script = cfg.get("apollo_env_script")
        if script:
            return str(script)
        return f"{self._docker_apollo_root()}/cyber/setup.bash"

    def _docker_auto_start_container(self) -> bool:
        cfg = self._docker_cfg()
        if "auto_start_container" in cfg:
            return bool(cfg.get("auto_start_container"))
        return True

    def _dreamview_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("dreamview", {}) or {})

    def _dreamview_enabled(self) -> bool:
        cfg = self._dreamview_cfg()
        if "enabled" in cfg:
            return bool(cfg.get("enabled"))
        return False

    def _dreamview_url(self) -> str:
        cfg = self._dreamview_cfg()
        host = str(cfg.get("host") or "localhost").strip() or "localhost"
        port = int(cfg.get("port") or 8888)
        scheme = str(cfg.get("scheme") or "http").strip() or "http"
        return str(cfg.get("url") or f"{scheme}://{host}:{port}")

    def _dreamview_host_port(self) -> tuple[str, int]:
        cfg = self._dreamview_cfg()
        host = str(cfg.get("host") or "localhost").strip() or "localhost"
        port = int(cfg.get("port") or 8888)
        return host, port

    def _apollo_application_core_root(self) -> Optional[Path]:
        cfg = self._dreamview_cfg()
        explicit = str(cfg.get("application_core_root") or "").strip()
        candidates: list[Path] = []
        if explicit:
            candidates.append(Path(explicit).expanduser())
        root_cfg = self._apollo_cfg()
        for key in ("apollo_application_core", "application_core_root"):
            value = str(root_cfg.get(key) or "").strip()
            if value:
                candidates.append(Path(value).expanduser())
        apollo_root = str(root_cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT", "")).strip()
        if apollo_root:
            p = Path(apollo_root).expanduser()
            for parent in [p] + list(p.parents):
                if parent.name == ".aem":
                    candidates.append(parent.parent)
                    break
                if (parent / ".aem").exists():
                    candidates.append(parent)
                    break
        for cand in candidates:
            resolved = cand.resolve()
            if resolved.exists() and (resolved / ".aem").exists():
                return resolved
        return None

    def _dreamview_use_aem_bootstrap(self) -> bool:
        cfg = self._dreamview_cfg()
        if "use_aem_bootstrap" in cfg:
            return bool(cfg.get("use_aem_bootstrap"))
        return True

    def _dreamview_stop_on_exit(self) -> bool:
        cfg = self._dreamview_cfg()
        if "stop_on_exit" in cfg:
            return bool(cfg.get("stop_on_exit"))
        return True

    def _dreamview_record_cfg(self) -> Dict[str, Any]:
        cfg = self._dreamview_cfg()
        return (cfg.get("record", {}) or {})

    def _dreamview_record_enabled(self) -> bool:
        cfg = self._dreamview_record_cfg()
        return bool(cfg.get("enabled", False))

    def _dreamview_record_capture_mode(self) -> str:
        cfg = self._dreamview_record_cfg()
        raw = str(cfg.get("capture_mode") or cfg.get("method") or "ffmpeg_realtime").strip().lower()
        if raw in {"tick", "tick_snapshot", "per_tick", "frame_by_tick"}:
            return "tick_snapshot"
        return "ffmpeg_realtime"

    def _dreamview_case_name(self) -> str:
        run_cfg = self._run_cfg()
        return str(run_cfg.get("profile_name") or self._run_dir().name)

    def _dreamview_config_name(self) -> str:
        raw = str(self.profile.get("_profile_config_path") or "").strip()
        return Path(raw).name if raw else ""

    @staticmethod
    def _normalize_dreamview_display_name(value: str) -> str:
        display = str(value or "").strip()
        if display.endswith(".0"):
            display = display[:-2]
        return display or ":0"

    def _dreamview_display_name(self) -> str:
        cfg = self._dreamview_record_cfg()
        raw = str(cfg.get("display") or os.environ.get("DISPLAY") or ":0.0").strip() or ":0.0"
        return self._normalize_dreamview_display_name(raw)

    def _dreamview_video_root(self, artifacts: Path) -> Path:
        root = artifacts.parent / "video" / "dreamview"
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _dreamview_default_output_path(self, artifacts: Path) -> Path:
        cfg = self._dreamview_record_cfg()
        output = str(cfg.get("output") or "").strip()
        out = Path(output).expanduser() if output else (self._dreamview_video_root(artifacts) / "dreamview_capture.mp4")
        if not out.is_absolute():
            out = (self.repo_root / out).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    def _dreamview_legacy_output_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_capture.mp4"

    def _dreamview_status_json_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_recording_status.json"

    def _dreamview_status_md_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_recording_status.md"

    def _dreamview_manifest_json_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_capture_manifest.json"

    def _dreamview_manifest_md_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_capture_manifest.md"

    def _dreamview_runtime_snapshot_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_runtime_config_snapshot.json"

    def _dreamview_region_cache_repo_path(self) -> Path:
        cfg = self._dreamview_record_cfg()
        raw = str(
            cfg.get("region_cache_path")
            or cfg.get("capture_region_cache_path")
            or "artifacts/dreamview_capture_region_cache.json"
        ).strip()
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _dreamview_region_cache_run_path(self, artifacts: Path) -> Path:
        return artifacts / "dreamview_capture_region_cache.json"

    @staticmethod
    def _dreamview_region_to_dict(region: Optional[tuple[int, int, int, int]]) -> Optional[Dict[str, int]]:
        if region is None:
            return None
        width, height, offset_x, offset_y = region
        return {
            "x": int(offset_x),
            "y": int(offset_y),
            "width": int(width),
            "height": int(height),
        }

    def _dreamview_region_from_dict(self, payload: Any) -> Optional[tuple[int, int, int, int]]:
        if not isinstance(payload, dict):
            return None
        try:
            width = int(payload.get("width"))
            height = int(payload.get("height"))
            raw_x = payload.get("x") if "x" in payload else payload.get("offset_x")
            raw_y = payload.get("y") if "y" in payload else payload.get("offset_y")
            offset_x = int(raw_x)
            offset_y = int(raw_y)
        except Exception:
            return None
        if width <= 0 or height <= 0:
            return None
        return width, height, offset_x, offset_y

    def _dreamview_manual_region(self) -> Optional[tuple[int, int, int, int]]:
        cfg = self._dreamview_record_cfg()
        manual = self._dreamview_region_from_dict(cfg.get("capture_region"))
        if manual is not None:
            return manual
        has_legacy = any(
            key in cfg
            for key in ("width", "height", "offset_x", "offset_y")
        )
        if not has_legacy:
            return None
        try:
            width = int(cfg.get("width") or 0)
            height = int(cfg.get("height") or 0)
            offset_x = int(cfg.get("offset_x") or 0)
            offset_y = int(cfg.get("offset_y") or 0)
        except Exception:
            return None
        if width <= 0 or height <= 0:
            return None
        return width, height, offset_x, offset_y

    def _dreamview_display_geometry(self) -> Dict[str, Any]:
        xwininfo = shutil.which("xwininfo")
        if not xwininfo:
            return {}
        try:
            out = subprocess.run(
                [xwininfo, "-root"],
                capture_output=True,
                text=True,
                check=False,
            )
            text = out.stdout
            import re

            def _match(pattern: str) -> Optional[int]:
                match = re.search(pattern, text)
                return int(match.group(1)) if match else None

            width = _match(r"Width:\s+(\d+)")
            height = _match(r"Height:\s+(\d+)")
            if width is None or height is None or width <= 0 or height <= 0:
                return {}
            return {
                "display": self._dreamview_display_name(),
                "width": int(width),
                "height": int(height),
            }
        except Exception:
            return {}

    def _dreamview_validate_region(
        self,
        region: Optional[tuple[int, int, int, int]],
        *,
        display_geometry: Optional[Dict[str, Any]] = None,
    ) -> tuple[Optional[tuple[int, int, int, int]], Optional[str]]:
        if region is None:
            return None, "region_missing"
        width, height, offset_x, offset_y = region
        if width <= 0 or height <= 0:
            return None, "region_invalid"
        if display_geometry:
            max_width = int(display_geometry.get("width") or 0)
            max_height = int(display_geometry.get("height") or 0)
            if max_width > 0 and max_height > 0:
                if offset_x < 0 or offset_y < 0:
                    return None, "region_invalid"
                if (offset_x + width) > max_width or (offset_y + height) > max_height:
                    return None, "region_invalid"
        return (int(width), int(height), int(offset_x), int(offset_y)), None

    def _load_dreamview_region_cache(self) -> Dict[str, Any]:
        path = self._dreamview_region_cache_repo_path()
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text())
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _write_dreamview_region_cache_copy(self, artifacts: Path, payload: Dict[str, Any]) -> None:
        run_path = self._dreamview_region_cache_run_path(artifacts)
        run_path.write_text(json.dumps(payload, indent=2))

    def _remember_dreamview_region(
        self,
        artifacts: Path,
        region: tuple[int, int, int, int],
        *,
        region_source: str,
        window_title_matched: str = "",
        window_id: str = "",
    ) -> None:
        if region_source not in {"manual", "remembered", "auto-detected"}:
            return
        payload = {
            "saved_at_sec": time.time(),
            "display": self._dreamview_display_name(),
            "display_geometry": self._dreamview_display_geometry(),
            "region_source": region_source,
            "window_title_requested": str(self._dreamview_record_cfg().get("window_title") or "Dreamview"),
            "window_title_matched": window_title_matched,
            "window_id": window_id,
            "region": self._dreamview_region_to_dict(region),
            "run_dir": str(self._run_dir()),
        }
        path = self._dreamview_region_cache_repo_path()
        path.write_text(json.dumps(payload, indent=2))
        self._write_dreamview_region_cache_copy(artifacts, payload)

    def _resolve_dreamview_remembered_region(
        self,
        artifacts: Path,
        *,
        require_display_match: bool,
    ) -> tuple[Optional[tuple[int, int, int, int]], Optional[str]]:
        payload = self._load_dreamview_region_cache()
        if not payload:
            return None, None
        self._write_dreamview_region_cache_copy(artifacts, payload)
        region = self._dreamview_region_from_dict(payload.get("region"))
        current_display = self._dreamview_display_name()
        cached_display = self._normalize_dreamview_display_name(str(payload.get("display") or "").strip())
        current_geometry = self._dreamview_display_geometry()
        cached_geometry = payload.get("display_geometry") if isinstance(payload.get("display_geometry"), dict) else {}
        if require_display_match and cached_display and cached_display != current_display:
            return None, "remembered_region_display_conflict"
        if current_geometry and cached_geometry:
            if (
                int(current_geometry.get("width") or 0) != int(cached_geometry.get("width") or 0)
                or int(current_geometry.get("height") or 0) != int(cached_geometry.get("height") or 0)
            ):
                return None, "remembered_region_display_conflict"
        region, error = self._dreamview_validate_region(region, display_geometry=current_geometry or None)
        if error:
            return None, error
        return region, None

    def _ensure_dreamview_capture_state(self, artifacts: Path) -> None:
        if self._dreamview_capture_state:
            return
        cfg = self._dreamview_record_cfg()
        state = {
            "case_name": self._dreamview_case_name(),
            "config_name": self._dreamview_config_name(),
            "capture_mode_requested": self._dreamview_record_capture_mode(),
            "capture_mode_used": "",
            "recording_status": "initialized",
            "recording_success": False,
            "failure_types": [],
            "failure_messages": [],
            "window_title_requested": str(cfg.get("window_title") or "Dreamview").strip() or "Dreamview",
            "window_title_matched": "",
            "window_detect_success": False,
            "fallback_used": False,
            "used_fixed_region": False,
            "region_source_requested": str(cfg.get("region_source") or "").strip() or None,
            "region_source": "",
            "region": None,
            "output_video_path": str(self._dreamview_default_output_path(artifacts)),
            "output_video_generated": False,
            "frame_count": 0,
            "duration_sec": None,
            "snapshot_dir": "",
            "runtime_config_snapshot_path": str(self._dreamview_runtime_snapshot_path(artifacts)),
            "status_json_path": str(self._dreamview_status_json_path(artifacts)),
            "manifest_json_path": str(self._dreamview_manifest_json_path(artifacts)),
            "started_at_sec": None,
            "finished_at_sec": None,
            "recording_enabled": bool(self._dreamview_record_enabled()),
        }
        self._dreamview_capture_state = state
        self._write_dreamview_runtime_config_snapshot(artifacts)
        self._write_dreamview_capture_artifacts(artifacts)

    def _append_dreamview_failure(
        self,
        artifacts: Path,
        failure_type: str,
        message: str,
    ) -> None:
        self._ensure_dreamview_capture_state(artifacts)
        state = self._dreamview_capture_state
        if failure_type not in state["failure_types"]:
            state["failure_types"].append(failure_type)
        if message:
            state["failure_messages"].append(message)
        if failure_type == "fallback_to_screen_triggered":
            state["fallback_used"] = True
            if state["recording_status"] not in {"failed", "success"}:
                state["recording_status"] = "success_with_fallback"
        else:
            state["recording_status"] = "failed"
            state["recording_success"] = False
        self._write_dreamview_capture_artifacts(artifacts)

    def _mark_dreamview_region_usage(
        self,
        artifacts: Path,
        region: tuple[int, int, int, int],
        *,
        source: str,
        window_title_matched: str = "",
        window_detect_success: bool = False,
    ) -> None:
        self._ensure_dreamview_capture_state(artifacts)
        state = self._dreamview_capture_state
        state["region"] = self._dreamview_region_to_dict(region)
        state["region_source"] = source
        state["used_fixed_region"] = source in {"manual", "remembered"}
        state["window_title_matched"] = window_title_matched
        state["window_detect_success"] = bool(window_detect_success)
        self._write_dreamview_capture_artifacts(artifacts)

    def _dreamview_manifest_payload(self, artifacts: Path) -> Dict[str, Any]:
        self._ensure_dreamview_capture_state(artifacts)
        state = self._dreamview_capture_state
        return {
            "case_name": state["case_name"],
            "config_name": state["config_name"],
            "capture_mode": state["capture_mode_used"] or state["capture_mode_requested"],
            "used_capture_mode": state["capture_mode_used"] or state["capture_mode_requested"],
            "used_fixed_region": bool(state["used_fixed_region"]),
            "region": state["region"],
            "region_source": state["region_source"] or state["region_source_requested"],
            "window_title_requested": state["window_title_requested"],
            "window_title_matched": state["window_title_matched"],
            "window_detect_success": bool(state["window_detect_success"]),
            "fallback_used": bool(state["fallback_used"]),
            "output_video_path": state["output_video_path"],
            "output_video_generated": bool(state["output_video_generated"]),
            "frame_count": int(state["frame_count"] or 0),
            "duration_sec": state["duration_sec"],
            "snapshot_dir": state["snapshot_dir"] or None,
            "recording_status": state["recording_status"],
            "runtime_config_snapshot_path": state["runtime_config_snapshot_path"],
        }

    def _dreamview_status_payload(self, artifacts: Path) -> Dict[str, Any]:
        self._ensure_dreamview_capture_state(artifacts)
        state = self._dreamview_capture_state
        payload = dict(state)
        payload["capture_mode"] = state["capture_mode_used"] or state["capture_mode_requested"]
        return payload

    def _write_dreamview_runtime_config_snapshot(self, artifacts: Path) -> None:
        dreamview_cfg = self._dreamview_cfg()
        cfg = self._dreamview_record_cfg()
        payload = {
            "enabled": bool(self._dreamview_enabled()),
            "auto_start": bool(dreamview_cfg.get("auto_start", True)),
            "record_enabled": bool(self._dreamview_record_enabled()),
            "capture_mode_requested": self._dreamview_record_capture_mode(),
            "recommended_priority_order": [
                "fixed_region+tick_snapshot",
                "fixed_region+ffmpeg_realtime",
                "auto_detected_window_region",
                "fallback_screen_region",
            ],
            "legacy_mode": str(cfg.get("region_mode") or cfg.get("mode") or "window").strip().lower() or "window",
            "use_fixed_region": bool(cfg.get("use_fixed_region", False)),
            "capture_region": cfg.get("capture_region"),
            "region_source": cfg.get("region_source"),
            "remember_last_region": bool(cfg.get("remember_last_region", True)),
            "prefer_remembered_region": bool(cfg.get("prefer_remembered_region", True)),
            "fallback_to_screen": bool(cfg.get("fallback_to_screen", True)),
            "window_title": str(cfg.get("window_title") or "Dreamview"),
            "window_search_timeout_sec": float(cfg.get("window_search_timeout_sec") or 8.0),
            "fps": float(cfg.get("fps") or max(1.0, round(self._default_control_apply_hz()))),
            "tick_every_n": int(max(1, int(cfg.get("tick_every_n") or 1))),
            "display": self._dreamview_display_name(),
            "region_cache_path": str(self._dreamview_region_cache_repo_path()),
            "default_output_video_path": str(self._dreamview_default_output_path(artifacts)),
        }
        self._dreamview_runtime_snapshot_path(artifacts).write_text(json.dumps(payload, indent=2))

    def _write_dreamview_capture_artifacts(self, artifacts: Path) -> None:
        manifest = self._dreamview_manifest_payload(artifacts)
        status = self._dreamview_status_payload(artifacts)
        self._dreamview_manifest_json_path(artifacts).write_text(json.dumps(manifest, indent=2))
        self._dreamview_status_json_path(artifacts).write_text(json.dumps(status, indent=2))
        manifest_lines = [
            "# Dreamview Capture Manifest",
            "",
            f"- case_name: `{manifest['case_name']}`",
            f"- config_name: `{manifest['config_name'] or 'unknown'}`",
            f"- capture_mode: `{manifest['capture_mode']}`",
            f"- used_fixed_region: `{manifest['used_fixed_region']}`",
            f"- region_source: `{manifest['region_source'] or 'unknown'}`",
            f"- region: `{json.dumps(manifest['region'], ensure_ascii=False) if manifest['region'] else 'null'}`",
            f"- window_title_requested: `{manifest['window_title_requested']}`",
            f"- window_title_matched: `{manifest['window_title_matched'] or ''}`",
            f"- window_detect_success: `{manifest['window_detect_success']}`",
            f"- fallback_used: `{manifest['fallback_used']}`",
            f"- output_video_path: `{manifest['output_video_path']}`",
            f"- output_video_generated: `{manifest['output_video_generated']}`",
            f"- frame_count: `{manifest['frame_count']}`",
            f"- duration_sec: `{manifest['duration_sec']}`",
            f"- snapshot_dir: `{manifest['snapshot_dir'] or ''}`",
            f"- recording_status: `{manifest['recording_status']}`",
        ]
        self._dreamview_manifest_md_path(artifacts).write_text("\n".join(manifest_lines) + "\n")
        status_lines = [
            "# Dreamview Recording Status",
            "",
            f"- recording_status: `{status['recording_status']}`",
            f"- capture_mode: `{status['capture_mode']}`",
            f"- recording_success: `{status['recording_success']}`",
            f"- failure_types: `{json.dumps(status['failure_types'], ensure_ascii=False)}`",
            f"- failure_messages: `{json.dumps(status['failure_messages'], ensure_ascii=False)}`",
            f"- fallback_used: `{status['fallback_used']}`",
            f"- window_detect_success: `{status['window_detect_success']}`",
            f"- region_source: `{status['region_source'] or ''}`",
            f"- region: `{json.dumps(status['region'], ensure_ascii=False) if status['region'] else 'null'}`",
            f"- output_video_path: `{status['output_video_path']}`",
            f"- output_video_generated: `{status['output_video_generated']}`",
            f"- frame_count: `{status['frame_count']}`",
            f"- duration_sec: `{status['duration_sec']}`",
            f"- snapshot_dir: `{status['snapshot_dir'] or ''}`",
        ]
        self._dreamview_status_md_path(artifacts).write_text("\n".join(status_lines) + "\n")

    def _export_dreamview_legacy_output(self, artifacts: Path) -> None:
        src = self._dreamview_default_output_path(artifacts)
        dst = self._dreamview_legacy_output_path(artifacts)
        if src == dst or not src.exists():
            return
        try:
            if dst.exists():
                dst.unlink()
            os.link(src, dst)
        except Exception:
            try:
                shutil.copyfile(src, dst)
            except Exception:
                pass

    def _sync_dreamview_recording_summary(self, artifacts: Path) -> None:
        if not self._dreamview_capture_state:
            return
        provisional_path = self._run_dir() / "summary.provisional.json"
        summary_path = provisional_path if provisional_path.exists() else (self._run_dir() / "summary.json")
        summary: Dict[str, Any] = {}
        if summary_path.exists():
            try:
                loaded = json.loads(summary_path.read_text())
                if isinstance(loaded, dict):
                    summary = loaded
            except Exception:
                summary = {}
        summary["dreamview_recording"] = self._dreamview_manifest_payload(artifacts)
        summary["dreamview_recording_status_path"] = str(self._dreamview_status_json_path(artifacts))
        summary["dreamview_capture_manifest_path"] = str(self._dreamview_manifest_json_path(artifacts))
        summary["dreamview_runtime_config_snapshot_path"] = str(self._dreamview_runtime_snapshot_path(artifacts))
        summary_path.write_text(json.dumps(summary, indent=2))

    def _dreamview_has_fatal_failures(self) -> bool:
        fatal = {"ffmpeg_start_failed", "snapshot_failed", "output_not_generated", "unknown"}
        return any(item in fatal for item in self._dreamview_capture_state.get("failure_types", []))

    def _apollo_host_log_dir(self) -> Optional[Path]:
        app_root = self._apollo_application_core_root()
        if app_root:
            log_dir = (app_root / "data" / "log").resolve()
            if log_dir.exists():
                return log_dir
        try:
            apollo_root = self._apollo_root()
        except Exception:
            return None
        for parent in [apollo_root] + list(apollo_root.parents):
            if parent.name == "application-core":
                log_dir = (parent / "data" / "log").resolve()
                if log_dir.exists():
                    return log_dir
                break
        return None

    def _apollo_host_dumps_dir(self) -> Optional[Path]:
        app_root = self._apollo_application_core_root()
        if app_root:
            dumps_dir = (app_root / "dumps").resolve()
            if dumps_dir.exists():
                return dumps_dir
        try:
            apollo_root = self._apollo_root()
        except Exception:
            return None
        for parent in [apollo_root] + list(apollo_root.parents):
            if parent.name == "application-core":
                dumps_dir = (parent / "dumps").resolve()
                if dumps_dir.exists():
                    return dumps_dir
                break
        return None

    def _apollo_internal_debug_dir_host(self) -> Optional[Path]:
        app_root = self._apollo_application_core_root()
        if not app_root:
            return None
        return (
            app_root
            / "dumps"
            / "carla_testbed_apollo_internal"
            / self._safe_run_tag(self._run_dir().name)
        ).resolve()

    def _docker_apollo_internal_debug_dir(self) -> str:
        return (
            "/apollo_workspace/dumps/carla_testbed_apollo_internal/"
            + self._safe_run_tag(self._run_dir().name)
        )

    def _apollo_stage6_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("stage6_reference_line", {}) or {})

    def _apollo_internal_debug_env(self, *, docker: bool) -> Dict[str, str]:
        stage6_cfg = self._apollo_stage6_cfg()
        enabled = bool(stage6_cfg.get("enabled", False))
        env: Dict[str, str] = {
            "TB_APOLLO_INTERNAL_DEBUG_ENABLED": "1" if enabled else "0",
            "TB_STAGE6_CLEAR_LANEFOLLOW_CACHE_ON_NEW_COMMAND": (
                "1" if bool(stage6_cfg.get("clear_lane_follow_cache_on_new_command", False)) else "0"
            ),
            "TB_STAGE6_REFLINE_GENERATION_GUARD": (
                "1" if bool(stage6_cfg.get("reference_line_generation_guard", False)) else "0"
            ),
        }
        if enabled:
            env["TB_APOLLO_INTERNAL_DEBUG_DIR"] = (
                self._docker_apollo_internal_debug_dir()
                if docker
                else str(self._apollo_internal_debug_dir_host() or (self._artifacts_dir() / "apollo_internal"))
            )
        return env

    def _apollo_internal_debug_shell_exports(self, *, docker: bool) -> str:
        env = self._apollo_internal_debug_env(docker=docker)
        parts: list[str] = []
        debug_dir = env.get("TB_APOLLO_INTERNAL_DEBUG_DIR", "")
        if debug_dir:
            parts.append(f"mkdir -p {shlex.quote(debug_dir)}")
        for key, value in env.items():
            parts.append(f"export {key}={shlex.quote(str(value))}")
        return "; ".join(parts)

    def _prepare_apollo_internal_debug_dir(self, artifacts: Path) -> None:
        stage6_cfg = self._apollo_stage6_cfg()
        if not bool(stage6_cfg.get("enabled", False)):
            return
        host_dir = self._apollo_internal_debug_dir_host()
        if host_dir is not None:
            host_dir.mkdir(parents=True, exist_ok=True)
            for path in host_dir.glob("stage6_*.jsonl"):
                try:
                    path.unlink()
                except Exception:
                    pass
            (artifacts / "apollo_internal_debug_dir.txt").write_text(str(host_dir) + "\n")
        if self._docker_container_name:
            try:
                docker_dir = self._docker_apollo_internal_debug_dir()
                self._docker_exec(
                    f"mkdir -p {shlex.quote(docker_dir)} && rm -f {shlex.quote(docker_dir)}/stage6_*.jsonl",
                    check=False,
                )
            except Exception:
                pass

    def _snapshot_apollo_internal_debug(self, artifacts: Path) -> None:
        stage6_cfg = self._apollo_stage6_cfg()
        summary: Dict[str, Any] = {
            "enabled": bool(stage6_cfg.get("enabled", False)),
            "snapshotted_at_sec": time.time(),
            "files": {},
        }
        if not bool(stage6_cfg.get("enabled", False)):
            (artifacts / "apollo_internal_debug_snapshot_meta.json").write_text(json.dumps(summary, indent=2))
            return
        source_dir = self._apollo_internal_debug_dir_host()
        if source_dir is None or not source_dir.exists():
            summary["status"] = "missing_source_dir"
            summary["source_dir"] = str(source_dir) if source_dir is not None else ""
            (artifacts / "apollo_internal_debug_snapshot_meta.json").write_text(json.dumps(summary, indent=2))
            return
        summary["source_dir"] = str(source_dir)
        for path in sorted(source_dir.glob("stage6_*.jsonl")):
            out = artifacts / path.name
            try:
                out.write_bytes(path.read_bytes())
                summary["files"][path.name] = {
                    "source": str(path),
                    "snapshot": str(out),
                    "bytes": out.stat().st_size,
                }
            except Exception as exc:
                summary["files"][path.name] = {
                    "source": str(path),
                    "error": str(exc),
                }
        (artifacts / "apollo_internal_debug_snapshot_meta.json").write_text(json.dumps(summary, indent=2))

    @staticmethod
    def _apollo_core_log_files() -> tuple[str, ...]:
        return (
            "planning.INFO",
            "prediction.INFO",
            "control.INFO",
            "routing.INFO",
            "external_command.INFO",
        )

    @staticmethod
    def _apollo_log_tiny_delta_threshold_bytes() -> int:
        return 64

    @staticmethod
    def _apollo_log_tail_capture_bytes() -> int:
        return 64 * 1024

    def _capture_apollo_log_offsets(self, artifacts: Path) -> None:
        log_dir = self._apollo_host_log_dir()
        meta: Dict[str, Any] = {
            "log_dir": str(log_dir) if log_dir else "",
            "captured_at_sec": time.time(),
            "files": {},
        }
        self._apollo_log_offsets = {}
        if not log_dir:
            (artifacts / "apollo_log_capture_meta.json").write_text(json.dumps(meta, indent=2))
            return
        for name in self._apollo_core_log_files():
            path = log_dir / name
            try:
                offset = path.stat().st_size if path.exists() else 0
            except Exception:
                offset = 0
            self._apollo_log_offsets[name] = offset
            meta["files"][name] = {"path": str(path), "start_offset": offset}
        (artifacts / "apollo_log_capture_meta.json").write_text(json.dumps(meta, indent=2))

    def _snapshot_apollo_logs(self, artifacts: Path) -> None:
        log_dir = self._apollo_host_log_dir()
        summary: Dict[str, Any] = {
            "log_dir": str(log_dir) if log_dir else "",
            "snapshotted_at_sec": time.time(),
            "files": {},
        }
        if not log_dir:
            (artifacts / "apollo_log_snapshot_meta.json").write_text(json.dumps(summary, indent=2))
            self._apollo_log_offsets = {}
            return
        for name in self._apollo_core_log_files():
            src = log_dir / name
            out = artifacts / f"apollo_{name}"
            start_offset = int(self._apollo_log_offsets.get(name, 0) or 0)
            item: Dict[str, Any] = {"source": str(src), "start_offset": start_offset}
            if not src.exists():
                item["status"] = "missing"
                summary["files"][name] = item
                continue
            try:
                end_offset = src.stat().st_size
                item["end_offset"] = end_offset
                if end_offset < start_offset:
                    start_offset = 0
                    item["status"] = "rotated"
                else:
                    item["status"] = "ok"
                with src.open("rb") as fp:
                    fp.seek(start_offset)
                    data = fp.read()
                out.write_bytes(data)
                item["snapshot"] = str(out)
                item["bytes"] = len(data)
                if item["status"] == "ok" and 0 < len(data) <= self._apollo_log_tiny_delta_threshold_bytes():
                    tail_start_offset = max(0, end_offset - self._apollo_log_tail_capture_bytes())
                    with src.open("rb") as fp:
                        fp.seek(tail_start_offset)
                        tail_data = fp.read()
                    tail_out = artifacts / f"apollo_{name}.tail"
                    tail_out.write_bytes(tail_data)
                    item["tail_snapshot"] = str(tail_out)
                    item["tail_bytes"] = len(tail_data)
                    item["tail_start_offset"] = tail_start_offset
                    item["tail_end_offset"] = end_offset
                    item["tail_reason"] = "delta_below_threshold"
            except Exception as exc:
                item["status"] = "error"
                item["error"] = str(exc)
            summary["files"][name] = item
        (artifacts / "apollo_log_snapshot_meta.json").write_text(json.dumps(summary, indent=2))
        self._apollo_log_offsets = {}

    @staticmethod
    def _apollo_core_bvar_dump_files() -> tuple[str, ...]:
        return (
            "planning.data",
            "planning.dag_external_command_process.dag.data",
        )

    def _snapshot_apollo_bvar_dumps(self, artifacts: Path) -> None:
        artifacts.mkdir(parents=True, exist_ok=True)
        dumps_dir = self._apollo_host_dumps_dir()
        summary: Dict[str, Any] = {
            "dumps_dir": str(dumps_dir) if dumps_dir else "",
            "snapshotted_at_sec": time.time(),
            "files": {},
        }
        if not dumps_dir:
            (artifacts / "apollo_bvar_dump_snapshot_meta.json").write_text(json.dumps(summary, indent=2))
            return
        for name in self._apollo_core_bvar_dump_files():
            src = dumps_dir / name
            safe_name = "apollo_" + name.replace(".", "_", 1)
            if name == "planning.data":
                safe_name = "apollo_planning.data"
            out = artifacts / safe_name
            item: Dict[str, Any] = {"source": str(src)}
            if not src.exists():
                item["status"] = "missing"
                summary["files"][name] = item
                continue
            try:
                shutil.copyfile(src, out)
                item["status"] = "ok"
                item["snapshot"] = str(out)
                item["bytes"] = out.stat().st_size
            except Exception as exc:
                item["status"] = "error"
                item["error"] = str(exc)
            summary["files"][name] = item
        (artifacts / "apollo_bvar_dump_snapshot_meta.json").write_text(json.dumps(summary, indent=2))

    def _resolve_dreamview_record_region(self, artifacts: Path) -> Optional[tuple[int, int, int, int]]:
        self._ensure_dreamview_capture_state(artifacts)
        cfg = self._dreamview_record_cfg()
        region_mode = str(cfg.get("region_mode") or cfg.get("mode") or "window").strip().lower()
        log_path = artifacts / "dreamview_record.log"
        title = str(cfg.get("window_title") or "Dreamview").strip() or "Dreamview"
        timeout_sec = float(cfg.get("window_search_timeout_sec") or 8.0)
        prefer_remembered = bool(cfg.get("prefer_remembered_region", True))
        remember_last = bool(cfg.get("remember_last_region", True))
        use_fixed_region = bool(cfg.get("use_fixed_region", False))
        preferred_source = str(cfg.get("region_source") or "").strip().lower()
        display_geometry = self._dreamview_display_geometry()
        manual_region, manual_error = self._dreamview_validate_region(
            self._dreamview_manual_region(),
            display_geometry=display_geometry or None,
        )
        if manual_error and use_fixed_region:
            self._append_dreamview_failure(artifacts, "region_invalid", f"manual capture_region invalid: {manual_error}")
            return None
        if manual_region is not None and (use_fixed_region or preferred_source == "manual" or region_mode != "window"):
            self._mark_dreamview_region_usage(artifacts, manual_region, source="manual")
            if remember_last:
                self._remember_dreamview_region(artifacts, manual_region, region_source="manual")
            with log_path.open("a") as fp:
                fp.write(
                    "mode=fixed_region\n"
                    f"window_title={title}\n"
                    f"region={manual_region[0]}x{manual_region[1]}+{manual_region[2]},{manual_region[3]}\n"
                    "region_source=manual\n"
                )
            return manual_region

        remembered_region = None
        remembered_error = None
        if remember_last:
            remembered_region, remembered_error = self._resolve_dreamview_remembered_region(
                artifacts,
                require_display_match=bool(cfg.get("validate_remembered_region", True)),
            )
            if remembered_error:
                self._append_dreamview_failure(artifacts, "region_invalid", remembered_error)
            if remembered_region is not None and (preferred_source == "remembered" or prefer_remembered):
                self._mark_dreamview_region_usage(artifacts, remembered_region, source="remembered")
                with log_path.open("a") as fp:
                    fp.write(
                        "mode=remembered_region\n"
                        f"window_title={title}\n"
                        f"region={remembered_region[0]}x{remembered_region[1]}+{remembered_region[2]},{remembered_region[3]}\n"
                        "region_source=remembered\n"
                    )
                return remembered_region

        if region_mode != "window":
            fallback_region = manual_region
            if fallback_region is None:
                self._append_dreamview_failure(artifacts, "region_invalid", "screen mode requested without valid region")
                return None
            self._mark_dreamview_region_usage(artifacts, fallback_region, source="manual")
            return fallback_region

        xdotool = shutil.which("xdotool")
        xwininfo = shutil.which("xwininfo")
        wmctrl = shutil.which("wmctrl")
        if not xwininfo:
            fallback = bool(cfg.get("fallback_to_screen", True))
            if fallback:
                fallback_region = manual_region
                if fallback_region is None:
                    self._append_dreamview_failure(artifacts, "region_invalid", "fallback_to_screen requested without valid manual region")
                    return None
                log_path.write_text(
                    f"mode=window\nwindow_title={title}\nmissing_tools=xwininfo\n"
                    f"fallback_to_screen=true\nregion={fallback_region[0]}x{fallback_region[1]}+{fallback_region[2]},{fallback_region[3]}\n"
                )
                self._mark_dreamview_region_usage(artifacts, fallback_region, source="manual")
                self._append_dreamview_failure(artifacts, "fallback_to_screen_triggered", "missing xwininfo")
                return fallback_region
            log_path.write_text(
                f"mode=window\nwindow_title={title}\nerror=missing xwininfo and fallback disabled\n"
            )
            self._append_dreamview_failure(artifacts, "window_not_found", "missing xwininfo and fallback disabled")
            return None

        def _window_ids_from_root_tree() -> list[str]:
            try:
                out = subprocess.run(
                    [xwininfo, "-root", "-tree"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                ids: list[str] = []
                for line in out.stdout.splitlines():
                    if title.lower() not in line.lower():
                        continue
                    token = line.strip().split()[0] if line.strip() else ""
                    if token.startswith("0x"):
                        ids.append(token)
                return ids
            except Exception:
                return []

        def _window_ids() -> list[str]:
            if xdotool:
                try:
                    search = subprocess.run(
                        [xdotool, "search", "--onlyvisible", "--name", title],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    ids = [line.strip() for line in search.stdout.splitlines() if line.strip()]
                    if ids:
                        return ids
                except Exception:
                    pass
            if wmctrl:
                try:
                    out = subprocess.run(
                        [wmctrl, "-lx"],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    ids: list[str] = []
                    for line in out.stdout.splitlines():
                        if title.lower() not in line.lower():
                            continue
                        token = line.strip().split()[0] if line.strip() else ""
                        if token.startswith("0x"):
                            ids.append(token)
                    if ids:
                        return ids
                except Exception:
                    pass
            return _window_ids_from_root_tree()

        deadline = time.time() + max(timeout_sec, 0.0)
        last_error = ""
        while time.time() < deadline:
            try:
                ids = _window_ids()
                if ids:
                    win_id = ids[-1]
                    info = subprocess.run(
                        [xwininfo, "-id", win_id],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    text = info.stdout
                    def _match(pattern: str) -> Optional[int]:
                        import re
                        m = re.search(pattern, text)
                        return int(m.group(1)) if m else None
                    offset_x = _match(r"Absolute upper-left X:\s+(-?\d+)")
                    offset_y = _match(r"Absolute upper-left Y:\s+(-?\d+)")
                    width = _match(r"Width:\s+(\d+)")
                    height = _match(r"Height:\s+(\d+)")
                    if None not in (offset_x, offset_y, width, height) and width > 0 and height > 0:
                        log_path.write_text(
                            f"mode=window\nwindow_title={title}\nwindow_id={win_id}\n"
                            f"region={width}x{height}+{offset_x},{offset_y}\n"
                        )
                        region = (int(width), int(height), int(offset_x), int(offset_y))
                        region, error = self._dreamview_validate_region(region, display_geometry=display_geometry or None)
                        if error is None and region is not None:
                            self._mark_dreamview_region_usage(
                                artifacts,
                                region,
                                source="auto-detected",
                                window_title_matched=title,
                                window_detect_success=True,
                            )
                            if remember_last:
                                self._remember_dreamview_region(
                                    artifacts,
                                    region,
                                    region_source="auto-detected",
                                    window_title_matched=title,
                                    window_id=win_id,
                                )
                            return region
                        last_error = error or "failed_to_parse_window_geometry"
                        self._append_dreamview_failure(artifacts, "region_invalid", last_error)
                    else:
                        last_error = "failed_to_parse_window_geometry"
                else:
                    last_error = "window_not_found"
            except Exception as exc:
                last_error = str(exc)
            time.sleep(0.5)

        if remembered_region is not None:
            self._mark_dreamview_region_usage(artifacts, remembered_region, source="remembered")
            with log_path.open("a") as fp:
                fp.write(
                    f"mode=window\nwindow_title={title}\nerror={last_error}\n"
                    "remembered_region_reused=true\n"
                    f"region={remembered_region[0]}x{remembered_region[1]}+{remembered_region[2]},{remembered_region[3]}\n"
                )
            return remembered_region

        fallback = bool(cfg.get("fallback_to_screen", True))
        if fallback:
            fallback_region = manual_region
            if fallback_region is None:
                self._append_dreamview_failure(artifacts, "window_not_found", last_error or "window_not_found")
                self._append_dreamview_failure(artifacts, "region_invalid", "fallback_to_screen requested without valid manual region")
                return None
            log_path.write_text(
                f"mode=window\nwindow_title={title}\nerror={last_error}\n"
                f"fallback_to_screen=true\nregion={fallback_region[0]}x{fallback_region[1]}+{fallback_region[2]},{fallback_region[3]}\n"
            )
            self._mark_dreamview_region_usage(artifacts, fallback_region, source="manual")
            self._append_dreamview_failure(artifacts, "window_not_found", last_error or "window_not_found")
            self._append_dreamview_failure(artifacts, "fallback_to_screen_triggered", last_error or "window_not_found")
            return fallback_region
        log_path.write_text(
            f"mode=window\nwindow_title={title}\nerror={last_error}\nfallback_to_screen=false\n"
        )
        self._append_dreamview_failure(artifacts, "window_not_found", last_error or "window_not_found")
        return None

    def _prepare_dreamview_tick_capture(self, artifacts: Path) -> None:
        cfg = self._dreamview_record_cfg()
        self._ensure_dreamview_capture_state(artifacts)
        self._dreamview_tick_capture_enabled = False
        self._dreamview_tick_capture_region = None
        self._dreamview_tick_capture_dir = None
        self._dreamview_tick_capture_index_path = None
        self._dreamview_tick_capture_count = 0
        self._dreamview_tick_capture_fail_count = 0
        self._dreamview_tick_capture_last_error = ""
        self._dreamview_tick_capture_last_ts = None
        self._dreamview_tick_capture_every_n = max(1, int(cfg.get("tick_every_n") or 1))

        log_path = artifacts / "dreamview_record.log"
        region = self._resolve_dreamview_record_region(artifacts)
        if region is None:
            with log_path.open("a") as fp:
                fp.write("capture_mode=tick_snapshot\nstart_failed=region_unavailable\n")
            print("[cyberrt] Dreamview tick capture skipped: browser window not found")
            self._append_dreamview_failure(artifacts, "snapshot_failed", "tick_snapshot start failed: region unavailable")
            return

        video_root = self._dreamview_video_root(artifacts)
        frames_dir = cfg.get("frames_dir")
        out_dir = Path(str(frames_dir)).expanduser() if frames_dir else (video_root / "frames")
        if not out_dir.is_absolute():
            out_dir = (self.repo_root / out_dir).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        index_path = cfg.get("index_path")
        index_path = (
            Path(str(index_path)).expanduser()
            if index_path
            else (video_root / "dreamview_frames_index.jsonl")
        )
        if not index_path.is_absolute():
            index_path = (self.repo_root / index_path).resolve()
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text("")

        self._dreamview_tick_capture_enabled = True
        self._dreamview_tick_capture_region = region
        self._dreamview_tick_capture_dir = out_dir
        self._dreamview_tick_capture_index_path = index_path
        self._dreamview_capture_state["capture_mode_used"] = "tick_snapshot"
        self._dreamview_capture_state["recording_status"] = "recording"
        self._dreamview_capture_state["started_at_sec"] = time.time()
        self._dreamview_capture_state["snapshot_dir"] = str(out_dir)
        self._dreamview_capture_state["output_video_path"] = str(self._dreamview_default_output_path(artifacts))
        with log_path.open("a") as fp:
            w, h, x, y = region
            fp.write(
                "capture_mode=tick_snapshot\n"
                f"tick_every_n={self._dreamview_tick_capture_every_n}\n"
                f"frames_dir={out_dir}\n"
                f"region={w}x{h}+{x},{y}\n"
            )
        self._write_dreamview_capture_artifacts(artifacts)
        print(f"[cyberrt] Dreamview tick capture armed: {out_dir}")

    def _capture_dreamview_tick_snapshot(self, frame_id: int, timestamp: Optional[float]) -> None:
        if not self._dreamview_tick_capture_enabled:
            return
        if self._dreamview_tick_capture_every_n > 1 and (frame_id % self._dreamview_tick_capture_every_n) != 0:
            return
        region = self._dreamview_tick_capture_region
        out_dir = self._dreamview_tick_capture_dir
        if region is None or out_dir is None:
            return
        try:
            from PIL import ImageGrab

            width, height, offset_x, offset_y = region
            bbox = (
                int(offset_x),
                int(offset_y),
                int(offset_x + width),
                int(offset_y + height),
            )
            image = ImageGrab.grab(bbox=bbox)
            out_path = out_dir / f"{int(frame_id):06d}.png"
            image.save(out_path)
            self._dreamview_tick_capture_count += 1
            self._dreamview_tick_capture_last_ts = timestamp
            if self._dreamview_tick_capture_index_path is not None:
                rec = {
                    "frame_id": int(frame_id),
                    "timestamp": None if timestamp is None else float(timestamp),
                    "path": str(out_path),
                }
                with self._dreamview_tick_capture_index_path.open("a") as fp:
                    fp.write(json.dumps(rec) + "\n")
        except Exception as exc:
            self._dreamview_tick_capture_fail_count += 1
            self._dreamview_tick_capture_last_error = str(exc)
            if self._dreamview_tick_capture_fail_count <= 3:
                print(f"[cyberrt] Dreamview tick capture failed frame={frame_id}: {exc}")

    def _finalize_dreamview_tick_capture(self, artifacts: Path) -> None:
        cfg = self._dreamview_record_cfg()
        self._ensure_dreamview_capture_state(artifacts)
        log_path = artifacts / "dreamview_record.log"
        video_root = self._dreamview_video_root(artifacts)
        summary_raw = cfg.get("summary_path")
        summary_path = (
            Path(str(summary_raw)).expanduser()
            if summary_raw
            else (video_root / "dreamview_record_summary.json")
        )
        if not summary_path.is_absolute():
            summary_path = (self.repo_root / summary_path).resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        out_dir = self._dreamview_tick_capture_dir
        fps = float(cfg.get("fps") or max(1.0, round(self._default_control_apply_hz())))
        out_mp4 = self._dreamview_default_output_path(artifacts)
        ffmpeg = shutil.which(str(cfg.get("ffmpeg_bin") or "ffmpeg"))
        encoded = False
        encode_error = ""
        frame_count = 0
        if out_dir is not None and self._dreamview_tick_capture_count > 0:
            frames = sorted(out_dir.glob("*.png"))
            frame_count = len(frames)
            if ffmpeg:
                seq_dir = out_dir.parent / "_encode_seq"
                try:
                    if seq_dir.exists():
                        shutil.rmtree(seq_dir)
                except Exception:
                    pass
                seq_dir.mkdir(parents=True, exist_ok=True)
                for idx, src in enumerate(frames):
                    dst = seq_dir / f"{idx:06d}.png"
                    try:
                        os.link(src, dst)
                    except Exception:
                        shutil.copyfile(src, dst)
                cmd = [
                    ffmpeg,
                    "-y",
                    "-framerate",
                    str(fps),
                    "-start_number",
                    "0",
                    "-i",
                    str(seq_dir / "%06d.png"),
                    "-c:v",
                    "libx264",
                    "-pix_fmt",
                    "yuv420p",
                    "-profile:v",
                    "high",
                    "-preset",
                    str(cfg.get("preset") or "veryfast"),
                    "-crf",
                    str(cfg.get("crf") or "23"),
                    "-movflags",
                    "+faststart",
                    str(out_mp4),
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
                encoded = proc.returncode == 0
                if not encoded:
                    encode_error = proc.stderr.strip() or proc.stdout.strip() or f"ffmpeg_rc={proc.returncode}"
                    self._append_dreamview_failure(artifacts, "snapshot_failed", encode_error)
                try:
                    shutil.rmtree(seq_dir)
                except Exception:
                    pass
                with log_path.open("a") as fp:
                    fp.write(
                        "finalize_mode=tick_snapshot\n"
                        + "encode_cmd="
                        + " ".join(shlex.quote(x) for x in cmd)
                        + "\n"
                        + f"encode_returncode={proc.returncode}\n"
                    )
                    if proc.stderr:
                        fp.write("[encode_stderr]\n" + proc.stderr + "\n")
            else:
                encode_error = "ffmpeg_not_found"
                self._append_dreamview_failure(artifacts, "snapshot_failed", encode_error)
                with log_path.open("a") as fp:
                    fp.write("finalize_mode=tick_snapshot\nencode_skipped=ffmpeg_not_found\n")
        else:
            self._append_dreamview_failure(artifacts, "snapshot_failed", "tick_snapshot finalize skipped: no frames")
            with log_path.open("a") as fp:
                fp.write("finalize_mode=tick_snapshot\nencode_skipped=no_frames\n")

        summary = {
            "capture_mode": "tick_snapshot",
            "frames_dir": str(out_dir) if out_dir is not None else "",
            "frames_count_on_disk": frame_count,
            "frames_captured": int(self._dreamview_tick_capture_count),
            "frames_failed": int(self._dreamview_tick_capture_fail_count),
            "last_error": self._dreamview_tick_capture_last_error,
            "last_timestamp": self._dreamview_tick_capture_last_ts,
            "tick_every_n": int(self._dreamview_tick_capture_every_n),
            "fps": float(fps),
            "output_mp4": str(out_mp4),
            "encoded": bool(encoded),
            "encode_error": encode_error,
        }
        summary_path.write_text(json.dumps(summary, indent=2))
        output_generated = bool(encoded and out_mp4.exists() and out_mp4.stat().st_size > 0)
        if not output_generated:
            self._append_dreamview_failure(artifacts, "output_not_generated", "tick_snapshot output mp4 not generated")
        self._dreamview_capture_state["frame_count"] = int(frame_count)
        self._dreamview_capture_state["output_video_path"] = str(out_mp4)
        self._dreamview_capture_state["output_video_generated"] = output_generated
        self._dreamview_capture_state["finished_at_sec"] = time.time()
        self._dreamview_capture_state["duration_sec"] = (
            round(frame_count / fps, 3) if fps > 0 and frame_count > 0 else 0.0
        )
        if output_generated and not self._dreamview_has_fatal_failures():
            self._dreamview_capture_state["recording_success"] = True
            self._dreamview_capture_state["recording_status"] = (
                "success_with_fallback" if self._dreamview_capture_state["fallback_used"] else "success"
            )
        self._export_dreamview_legacy_output(artifacts)
        self._write_dreamview_capture_artifacts(artifacts)
        self._dreamview_tick_capture_enabled = False
        self._dreamview_tick_capture_region = None
        self._dreamview_tick_capture_dir = None
        self._dreamview_tick_capture_index_path = None

    def _start_dreamview_recording(self, artifacts: Path) -> None:
        if not self._dreamview_record_enabled():
            return
        self._ensure_dreamview_capture_state(artifacts)
        self._dreamview_record_mode = self._dreamview_record_capture_mode()
        if self._dreamview_record_mode == "tick_snapshot":
            self._prepare_dreamview_tick_capture(artifacts)
            return
        if self._dreamview_record_proc is not None and self._dreamview_record_proc.poll() is None:
            return
        ffmpeg = shutil.which(str(self._dreamview_record_cfg().get("ffmpeg_bin") or "ffmpeg"))
        log_path = artifacts / "dreamview_record.log"
        if not ffmpeg:
            log_path.write_text("ffmpeg not found; skip Dreamview recording\n")
            print("[cyberrt] Dreamview recording skipped: ffmpeg not found")
            self._append_dreamview_failure(artifacts, "ffmpeg_start_failed", "ffmpeg not found")
            return
        cfg = self._dreamview_record_cfg()
        fps = int(cfg.get("fps") or 20)
        region = self._resolve_dreamview_record_region(artifacts)
        if region is None:
            print("[cyberrt] Dreamview recording skipped: browser window not found")
            self._append_dreamview_failure(artifacts, "window_not_found", "Dreamview region unavailable for ffmpeg capture")
            return
        width, height, offset_x, offset_y = region
        # yuv420p requires even dimensions; clamp the capture region to a valid size.
        width_even = width if width % 2 == 0 else max(2, width - 1)
        height_even = height if height % 2 == 0 else max(2, height - 1)
        region_adjusted = (width_even != width) or (height_even != height)
        width, height = width_even, height_even
        display = str(cfg.get("display") or os.environ.get("DISPLAY") or ":0.0").strip() or ":0.0"
        preset = str(cfg.get("preset") or "veryfast").strip() or "veryfast"
        crf = str(cfg.get("crf") or "23").strip() or "23"
        out_path = self._dreamview_default_output_path(artifacts)
        stdout_path = artifacts / "dreamview_record.out.log"
        stderr_path = artifacts / "dreamview_record.err.log"
        self._dreamview_record_out = stdout_path.open("w")
        self._dreamview_record_err = stderr_path.open("w")
        cmd = [
            ffmpeg,
            "-y",
            "-video_size",
            f"{width}x{height}",
            "-framerate",
            str(fps),
            "-use_wallclock_as_timestamps",
            "1",
            "-f",
            "x11grab",
            "-i",
            f"{display}+{offset_x},{offset_y}",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "high",
            "-vsync",
            "cfr",
            "-r",
            str(fps),
            "-preset",
            preset,
            "-crf",
            crf,
            "-movflags",
            "+faststart",
            str(out_path),
        ]
        try:
            self._dreamview_record_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=self._dreamview_record_out,
                stderr=self._dreamview_record_err,
                start_new_session=True,
            )
            self._dreamview_capture_state["capture_mode_used"] = "ffmpeg_realtime"
            self._dreamview_capture_state["recording_status"] = "recording"
            self._dreamview_capture_state["started_at_sec"] = time.time()
            self._dreamview_capture_state["output_video_path"] = str(out_path)
            with log_path.open("a") as fp:
                fp.write(
                    "enabled=true\n"
                    + "cmd="
                    + " ".join(shlex.quote(x) for x in cmd)
                    + "\n"
                    + f"output={out_path}\n"
                )
                if region_adjusted:
                    fp.write(f"adjusted_region={width}x{height}+{offset_x},{offset_y}\n")
            self._write_dreamview_capture_artifacts(artifacts)
            print(f"[cyberrt] Dreamview recording started: {out_path}")
        except Exception as exc:
            log_path.write_text(f"enabled=true\nstart_failed={exc}\n")
            print(f"[cyberrt] Dreamview recording failed: {exc}")
            self._append_dreamview_failure(artifacts, "ffmpeg_start_failed", str(exc))
            for fp_attr in ("_dreamview_record_out", "_dreamview_record_err"):
                fp = getattr(self, fp_attr, None)
                if fp is not None:
                    try:
                        fp.close()
                    except Exception:
                        pass
                    setattr(self, fp_attr, None)
            self._dreamview_record_proc = None

    def _stop_dreamview_recording(self, artifacts: Path) -> None:
        if not self._dreamview_record_enabled():
            self._ensure_dreamview_capture_state(artifacts)
            log_path = artifacts / "dreamview_record.log"
            with log_path.open("a") as fp:
                fp.write("enabled=false\n")
                fp.write("stop_mode=recording_disabled\n")
            self._dreamview_capture_state["recording_status"] = "disabled"
            self._dreamview_capture_state["recording_success"] = False
            self._dreamview_capture_state["finished_at_sec"] = time.time()
            self._write_dreamview_capture_artifacts(artifacts)
            self._dreamview_record_mode = "ffmpeg_realtime"
            return
        if self._dreamview_record_mode == "tick_snapshot":
            self._finalize_dreamview_tick_capture(artifacts)
            self._dreamview_record_mode = "ffmpeg_realtime"
            return
        self._ensure_dreamview_capture_state(artifacts)
        proc = self._dreamview_record_proc
        log_path = artifacts / "dreamview_record.log"
        if proc is not None and proc.poll() is None:
            stop_mode = "stdin_q"
            try:
                if proc.stdin is None:
                    raise RuntimeError("stdin unavailable")
                proc.stdin.write(b"q\n")
                proc.stdin.flush()
                proc.wait(timeout=8.0)
            except Exception as exc:
                stop_mode = f"signal_fallback:{exc}"
                self._terminate(proc, timeout_s=5.0)
        else:
            stop_mode = "already_stopped"
        self._dreamview_record_proc = None
        with log_path.open("a") as fp:
            fp.write(f"stop_mode={stop_mode}\n")
            if proc is not None:
                fp.write(f"returncode={proc.poll()}\n")
        output_path = self._dreamview_default_output_path(artifacts)
        output_generated = bool(output_path.exists() and output_path.stat().st_size > 0)
        if proc is not None and (proc.poll() not in (0, None)):
            self._append_dreamview_failure(
                artifacts,
                "output_not_generated" if not output_generated else "unknown",
                f"ffmpeg returncode={proc.poll()}",
            )
        if not output_generated:
            self._append_dreamview_failure(artifacts, "output_not_generated", "ffmpeg output mp4 not generated")
        self._dreamview_capture_state["finished_at_sec"] = time.time()
        self._dreamview_capture_state["output_video_path"] = str(output_path)
        self._dreamview_capture_state["output_video_generated"] = output_generated
        started_at = self._dreamview_capture_state.get("started_at_sec")
        finished_at = self._dreamview_capture_state.get("finished_at_sec")
        if isinstance(started_at, (int, float)) and isinstance(finished_at, (int, float)):
            self._dreamview_capture_state["duration_sec"] = round(max(0.0, finished_at - started_at), 3)
        duration_sec = float(self._dreamview_capture_state.get("duration_sec") or 0.0)
        fps = float(self._dreamview_record_cfg().get("fps") or 20.0)
        if output_generated and duration_sec > 0.0 and fps > 0.0:
            self._dreamview_capture_state["frame_count"] = max(1, int(round(duration_sec * fps)))
        if output_generated and not self._dreamview_has_fatal_failures():
            self._dreamview_capture_state["recording_success"] = True
            self._dreamview_capture_state["recording_status"] = (
                "success_with_fallback" if self._dreamview_capture_state["fallback_used"] else "success"
            )
        self._export_dreamview_legacy_output(artifacts)
        for fp_attr in ("_dreamview_record_out", "_dreamview_record_err"):
            fp = getattr(self, fp_attr, None)
            if fp is not None:
                try:
                    fp.close()
                except Exception:
                    pass
                setattr(self, fp_attr, None)
        self._write_dreamview_capture_artifacts(artifacts)
        self._dreamview_record_mode = "ffmpeg_realtime"

    def _start_dreamview_via_aem(self, artifacts: Path, url: str, host: str, port: int) -> bool:
        app_root = self._apollo_application_core_root()
        if not app_root or not shutil.which("aem"):
            return False
        log_path = artifacts / "dreamview_launch.log"
        lines = [f"url={url}", "start_mode=host_aem_bootstrap", f"application_core_root={app_root}"]
        cmd = ["bash", "-lc", f"cd {shlex.quote(str(app_root))} && aem bootstrap restart --plus"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        lines.append(f"start_cmd=cd {app_root} && aem bootstrap restart --plus")
        lines.append(f"returncode={result.returncode}")
        if result.stdout:
            lines.extend(["[stdout]", result.stdout])
        if result.stderr:
            lines.extend(["[stderr]", result.stderr])
        ready_timeout = float(self._dreamview_cfg().get("ready_timeout_sec") or 15.0)
        ready = self._wait_for_tcp(host, port, ready_timeout)
        lines.append(f"ready={ready}")
        lines.append(f"ready_timeout_sec={ready_timeout}")
        log_path.write_text("\n".join(lines) + "\n")
        if ready:
            self._dreamview_started_by_backend = True
            self._dreamview_started_mode = "aem"
            if bool(self._dreamview_cfg().get("auto_open", True)):
                self._open_url(url, artifacts)
            return True
        print(
            f"[cyberrt] Dreamview AEM bootstrap finished but {host}:{port} is not reachable; "
            f"see {log_path}; falling back to direct startup"
        )
        return False

    def _dreamview_start_cmd(self) -> str:
        cfg = self._dreamview_cfg()
        raw = str(cfg.get("start_cmd") or "").strip()
        if raw:
            return " ".join(raw.split())
        return (
            "set -e; "
            "mkdir -p /tmp/apollo_workspace/log; "
            "if [ -x /apollo/scripts/dreamview_plus.sh ]; then "
            "nohup bash /apollo/scripts/dreamview_plus.sh start >/tmp/apollo_workspace/log/dreamview_plus.log 2>&1 & "
            "elif [ -x /apollo/scripts/bootstrap.sh ]; then "
            "nohup bash /apollo/scripts/bootstrap.sh start_plus >/tmp/apollo_workspace/log/dreamview_plus.log 2>&1 & "
            "elif [ -x /apollo/scripts/dreamview.sh ]; then "
            "nohup bash /apollo/scripts/dreamview.sh start >/tmp/apollo_workspace/log/dreamview_plus.log 2>&1 & "
            "else echo 'no dreamview launcher found'; exit 2; fi; "
            "sleep 2"
        )

    def _wait_for_tcp(self, host: str, port: int, timeout_sec: float) -> bool:
        deadline = time.time() + max(timeout_sec, 0.0)
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=1.0):
                    return True
            except OSError:
                time.sleep(0.5)
        return False

    def _docker_ensure_dreamview_runtime_deps(self, artifacts: Path) -> None:
        if not self._docker_enabled():
            return
        log_path = artifacts / "dreamview_runtime_deps.log"
        packages = [
            "libusb-1.0-0",
            "libopenni0",
            "libglew2.2",
            "libdouble-conversion3",
            "libqhull8.0",
        ]
        check_cmd = (
            "missing=(); "
            + " ".join(
                f'dpkg -s {shlex.quote(pkg)} >/dev/null 2>&1 || missing+=({shlex.quote(pkg)});'
                for pkg in packages
            )
            + ' printf "%s\n" "${missing[@]}"'
        )
        checked = self._docker_exec(check_cmd, check=False, capture_output=True)
        missing = [line.strip() for line in checked.stdout.splitlines() if line.strip()]
        lines = [
            "packages=" + ",".join(packages),
            f"check_returncode={checked.returncode}",
            "missing=" + (",".join(missing) if missing else ""),
        ]
        if checked.stderr:
            lines.extend(["[check_stderr]", checked.stderr])
        if not missing:
            lines.append("status=already_present")
            log_path.write_text("\n".join(lines) + "\n")
            return
        install_cmd = (
            "set -o pipefail; "
            "export DEBIAN_FRONTEND=noninteractive; "
            "apt-get update -y && "
            "apt-get install -y " + " ".join(shlex.quote(pkg) for pkg in missing)
        )
        install = self._docker_exec(install_cmd, check=False, capture_output=True)
        lines.append(f"install_returncode={install.returncode}")
        if install.stdout:
            lines.extend(["[install_stdout]", install.stdout])
        if install.stderr:
            lines.extend(["[install_stderr]", install.stderr])
        log_path.write_text("\n".join(lines) + "\n")
        if install.returncode != 0:
            print(
                "[cyberrt] warning: failed to install Dreamview runtime deps; "
                f"see {log_path}"
            )

    def _open_url(
        self,
        url: str,
        artifacts: Path,
        *,
        url_file_name: str = "dreamview_url.txt",
        log_name: str = "dreamview_open.log",
        label: str = "Dreamview Plus URL",
    ) -> None:
        url_file = artifacts / url_file_name
        url_file.write_text(url + "\n")
        candidates = []
        browser_cmd = str(self._dreamview_cfg().get("browser_cmd") or "").strip()
        if browser_cmd:
            if "{url}" in browser_cmd:
                candidates.append(shlex.split(browser_cmd.replace("{url}", url)))
            else:
                candidates.append(shlex.split(browser_cmd) + [url])
        if shutil.which("xdg-open"):
            candidates.append(["xdg-open", url])
        if shutil.which("gio"):
            candidates.append(["gio", "open", url])
        for cmd in candidates:
            try:
                subprocess.Popen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                (artifacts / log_name).write_text(
                    f"opened via: {' '.join(shlex.quote(x) for x in cmd)}\nurl={url}\n"
                )
                print(f"[cyberrt] {label} opened: {url}")
                return
            except Exception as exc:
                (artifacts / log_name).write_text(
                    f"open failed via {' '.join(shlex.quote(x) for x in cmd)}: {exc}\nurl={url}\n"
                )
        print(f"[cyberrt] {label}: {url}")

    def _open_dreamview_wait_page(self, url: str, artifacts: Path, *, reason: str) -> None:
        """Open a local auto-refresh page when Dreamview is still booting.

        This keeps demo recording closer to unattended operation: the browser can
        be opened once, before Dreamview is reachable, and the page keeps trying
        to load the real Dreamview URL.
        """
        safe_url = html.escape(url, quote=True)
        safe_reason = html.escape(reason, quote=True)
        page = artifacts / "dreamview_wait_page.html"
        page.write_text(
            "\n".join(
                [
                    "<!doctype html>",
                    "<html>",
                    "<head>",
                    "  <meta charset=\"utf-8\">",
                    "  <meta http-equiv=\"refresh\" content=\"3\">",
                    "  <title>Waiting for Apollo Dreamview</title>",
                    "  <style>",
                    "    body { margin: 0; font-family: sans-serif; background: #111; color: #eee; }",
                    "    header { padding: 12px 16px; background: #1e2a32; }",
                    "    iframe { width: 100vw; height: calc(100vh - 72px); border: 0; background: #222; }",
                    "    a { color: #8bd3ff; }",
                    "  </style>",
                    "</head>",
                    "<body>",
                    "  <header>",
                    "    <strong>Waiting for Apollo Dreamview</strong><br>",
                    f"    <span>{safe_reason}</span><br>",
                    f"    <a href=\"{safe_url}\">{safe_url}</a>",
                    "  </header>",
                    f"  <iframe src=\"{safe_url}\" title=\"Apollo Dreamview\"></iframe>",
                    "</body>",
                    "</html>",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (artifacts / "dreamview_url.txt").write_text(url + "\n")
        (artifacts / "dreamview_wait_page_target.txt").write_text(url + "\n")
        self._open_url(
            page.resolve().as_uri(),
            artifacts,
            url_file_name="dreamview_wait_page_url.txt",
            log_name="dreamview_wait_page_open.log",
            label="Dreamview wait page",
        )

    def _maybe_start_dreamview(self, artifacts: Path) -> None:
        if not self._dreamview_enabled():
            return
        cfg = self._dreamview_cfg()
        url = self._dreamview_url()
        host, port = self._dreamview_host_port()
        was_ready = self._wait_for_tcp(host, port, 0.5)
        if was_ready:
            log_path = artifacts / "dreamview_launch.log"
            log_path.write_text(f"url={url}\nstart_mode=already_running\nready=true\n")
            (artifacts / "dreamview_url.txt").write_text(url + "\n")
            print(f"[cyberrt] Dreamview already reachable at {url}; skip restart")
            if bool(cfg.get("auto_open", True)):
                self._open_url(url, artifacts)
            self._start_dreamview_recording(artifacts)
            return
        if bool(cfg.get("auto_start", True)) and self._dreamview_use_aem_bootstrap():
            if self._start_dreamview_via_aem(artifacts, url, host, port):
                return
            # AEM bootstrap is not reliable on this install; try the direct startup path next.
        ready_timeout = float(cfg.get("ready_timeout_sec") or 15.0)
        log_path = artifacts / "dreamview_launch.log"
        lines = []
        if log_path.exists():
            try:
                lines.extend(log_path.read_text().splitlines())
            except Exception:
                lines = []
        if not lines:
            lines = [f"url={url}"]
        lines.append("fallback_mode=docker_direct")
        ready = False
        if bool(cfg.get("auto_start", True)) and self._docker_enabled():
            try:
                if not self._docker_container_name:
                    self._docker_container_name = self._docker_container()
                self._docker_ensure_dreamview_runtime_deps(artifacts)
                raw_cmd = self._dreamview_start_cmd()
                cmd = f"{self._docker_modules_prefix()}; {raw_cmd}"
                result = self._docker_exec(
                    cmd,
                    check=False,
                    capture_output=True,
                )
                lines.append(f"start_cmd={raw_cmd}")
                lines.append(f"start_cmd_with_env={cmd}")
                lines.append(f"returncode={result.returncode}")
                if result.stdout:
                    lines.append("[stdout]")
                    lines.append(result.stdout)
                if result.stderr:
                    lines.append("[stderr]")
                    lines.append(result.stderr)
                ready = self._wait_for_tcp(host, port, ready_timeout)
                lines.append(f"ready={ready}")
                lines.append(f"ready_timeout_sec={ready_timeout}")
                if ready:
                    self._dreamview_started_by_backend = True
                    self._dreamview_started_mode = "docker"
                if not ready:
                    runtime = self._docker_runtime_modes(self._docker_container_name)
                    lines.append(f"docker_ipc_mode={runtime[0]}")
                    lines.append(f"docker_network_mode={runtime[1]}")
                    proc = self._docker_exec(
                        "pgrep -af 'dreamview|dreamview_plus|cyber_launch' || true",
                        check=False,
                        capture_output=True,
                    )
                    if proc.stdout:
                        lines.append("[processes]")
                        lines.append(proc.stdout)
                    tail = self._docker_exec(
                        "tail -80 /tmp/apollo_workspace/log/dreamview_plus.log 2>/dev/null || true",
                        check=False,
                        capture_output=True,
                    )
                    if tail.stdout:
                        lines.append("[dreamview_log_tail]")
                        lines.append(tail.stdout)
            except Exception as exc:
                lines.append(f"start_failed={exc}")
        log_path.write_text("\n".join(lines) + "\n")
        if bool(cfg.get("auto_open", True)):
            if ready or not bool(cfg.get("auto_start", True)):
                self._open_url(url, artifacts)
            else:
                (artifacts / "dreamview_url.txt").write_text(url + "\n")
                reason = f"Dreamview not reachable on {host}:{port} after {ready_timeout:.1f}s"
                if bool(cfg.get("open_wait_page_on_unreachable", False)):
                    self._open_dreamview_wait_page(url, artifacts, reason=reason)
                else:
                    (artifacts / "dreamview_open.log").write_text(
                        f"skip auto-open: {reason}\n"
                        f"url={url}\n"
                    )
                print(
                    f"[cyberrt] Dreamview did not become reachable on {host}:{port}; "
                    f"see {log_path}"
                )
        if ready:
            self._start_dreamview_recording(artifacts)

    def _stop_dreamview(self, artifacts: Path) -> None:
        self._stop_dreamview_recording(artifacts)
        self._sync_dreamview_recording_summary(artifacts)
        if not self._dreamview_started_by_backend or not self._dreamview_stop_on_exit():
            return
        log_path = artifacts / "dreamview_stop.log"
        lines = [f"mode={self._dreamview_started_mode or ''}"]
        try:
            if self._dreamview_started_mode == "aem":
                app_root = self._apollo_application_core_root()
                if app_root and shutil.which("aem"):
                    cmd = ["bash", "-lc", f"cd {shlex.quote(str(app_root))} && aem bootstrap stop"]
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    lines.append(f"cmd=cd {app_root} && aem bootstrap stop")
                    lines.append(f"returncode={result.returncode}")
                    if result.stdout:
                        lines.extend(["[stdout]", result.stdout])
                    if result.stderr:
                        lines.extend(["[stderr]", result.stderr])
            elif self._dreamview_started_mode == "docker":
                stop_cmd = (
                    "if [ -x /apollo/scripts/dreamview_plus.sh ]; then "
                    "bash /apollo/scripts/dreamview_plus.sh stop; "
                    "elif [ -x /apollo/scripts/bootstrap.sh ]; then "
                    "bash /apollo/scripts/bootstrap.sh stop; "
                    "elif [ -x /apollo/scripts/dreamview.sh ]; then "
                    "bash /apollo/scripts/dreamview.sh stop; "
                    "fi"
                )
                result = self._docker_exec(stop_cmd, check=False, capture_output=True)
                lines.append(f"cmd={stop_cmd}")
                lines.append(f"returncode={result.returncode}")
                if result.stdout:
                    lines.extend(["[stdout]", result.stdout])
                if result.stderr:
                    lines.extend(["[stderr]", result.stderr])
        except Exception as exc:
            lines.append(f"stop_failed={exc}")
        finally:
            log_path.write_text("\n".join(lines) + "\n")
            self._dreamview_started_by_backend = False
            self._dreamview_started_mode = None

    @staticmethod
    def _carla_scale_to_m(values: list[float], *, threshold: float = 20.0) -> float:
        finite = [abs(float(v)) for v in values if v is not None]
        if not finite:
            return 1.0
        return 0.01 if max(finite) > threshold else 1.0

    def _load_recent_carla_vehicle_param_override(self, artifacts: Path) -> tuple[Dict[str, Any], str]:
        current = (artifacts / "carla_vehicle_characteristics.json").resolve()
        candidates = sorted(
            self.repo_root.glob("runs/*/artifacts/carla_vehicle_characteristics.json"),
            key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
            reverse=True,
        )
        for path in candidates:
            try:
                resolved = path.resolve()
            except Exception:
                continue
            if resolved == current:
                continue
            try:
                payload = json.loads(path.read_text())
            except Exception:
                continue
            overrides: Dict[str, Any] = {}
            for key in (
                "length",
                "width",
                "height",
                "front_edge_to_center",
                "back_edge_to_center",
                "left_edge_to_center",
                "right_edge_to_center",
                "wheel_base",
                "min_turn_radius",
                "wheel_rolling_radius",
            ):
                value = payload.get(key)
                if value is not None:
                    overrides[key] = float(value)
            max_steer_deg = payload.get("max_steer_angle_deg")
            if max_steer_deg is not None:
                overrides["max_steer_angle"] = float(max_steer_deg)
            if overrides:
                return overrides, str(path)
        return {}, ""

    def _probe_carla_vehicle_param_override(self, artifacts: Path) -> Dict[str, Any]:
        cfg = (self._apollo_cfg().get("vehicle_param", {}) or {})
        if not bool(cfg.get("enabled", True)):
            return {}
        if not bool(cfg.get("auto_from_carla", False)):
            return {}
        timeout_sec = float(cfg.get("carla_probe_timeout_sec", 3.0) or 0.0)
        poll_sec = max(0.1, float(cfg.get("carla_probe_poll_sec", 0.25) or 0.25))
        allow_fallback_vehicle = bool(cfg.get("carla_probe_allow_fallback_vehicle", True))
        allow_artifact_fallback = bool(cfg.get("carla_probe_use_artifact_fallback", True))
        payload: Dict[str, Any] = {
            "enabled": True,
            "timeout_sec": timeout_sec,
            "poll_sec": poll_sec,
            "allow_fallback_vehicle": allow_fallback_vehicle,
            "allow_artifact_fallback": allow_artifact_fallback,
        }
        out_path = artifacts / "carla_vehicle_param_probe.json"
        if timeout_sec <= 0.0:
            payload["ok"] = False
            payload["reason"] = "timeout_disabled"
            out_path.write_text(json.dumps(payload, indent=2))
            return {}
        try:
            import carla  # type: ignore
        except Exception as exc:
            payload["ok"] = False
            payload["reason"] = f"carla_import_failed:{exc}"
            out_path.write_text(json.dumps(payload, indent=2))
            return {}

        carla_cfg = (self.profile.get("runtime", {}) or {}).get("carla", {}) or {}
        run_cfg = self.profile.get("run", {}) or {}
        host = str(carla_cfg.get("host", "127.0.0.1"))
        port = int(carla_cfg.get("port", 2000))
        ego_role = str(run_cfg.get("ego_id", "hero"))
        payload.update({"host": host, "port": port, "ego_role": ego_role})

        try:
            client = carla.Client(host, port)
            client.set_timeout(min(max(timeout_sec, 2.0), 5.0))
        except Exception as exc:
            payload["ok"] = False
            payload["reason"] = f"connect_failed:{exc}"
            out_path.write_text(json.dumps(payload, indent=2))
            return {}

        actor = None
        fallback_actor = None
        fallback_reason = ""
        deadline = time.monotonic() + timeout_sec
        last_error = ""
        while time.monotonic() < deadline and actor is None:
            try:
                world = client.get_world()
                candidates = list(world.get_actors().filter("vehicle.*"))
                payload["last_vehicle_count"] = len(candidates)
                same_type = len({str(getattr(candidate, "type_id", "") or "") for candidate in candidates}) == 1
                if allow_fallback_vehicle and candidates:
                    if len(candidates) == 1:
                        fallback_actor = candidates[0]
                        fallback_reason = "single_vehicle"
                    elif same_type:
                        fallback_actor = candidates[0]
                        fallback_reason = "uniform_vehicle_type"
                for candidate in candidates:
                    role = ((getattr(candidate, "attributes", {}) or {}).get("role_name", "") or "").strip()
                    if role == ego_role or role in {"hero", "ego", "tb_ego"}:
                        actor = candidate
                        break
            except Exception as exc:
                last_error = str(exc)
            if actor is None:
                time.sleep(poll_sec)
        if actor is None:
            if fallback_actor is not None:
                actor = fallback_actor
                payload["used_fallback_vehicle"] = True
                payload["fallback_reason"] = fallback_reason
            else:
                payload["used_fallback_vehicle"] = False
        if actor is None:
            if allow_artifact_fallback:
                overrides, source_path = self._load_recent_carla_vehicle_param_override(artifacts)
                if overrides:
                    payload["ok"] = True
                    payload["used_artifact_fallback"] = True
                    payload["artifact_fallback_path"] = source_path
                    payload["overrides"] = overrides
                    out_path.write_text(json.dumps(payload, indent=2))
                    return overrides
            payload["ok"] = False
            payload["reason"] = f"ego_not_found:{last_error}" if last_error else "ego_not_found"
            out_path.write_text(json.dumps(payload, indent=2))
            return {}

        try:
            bbox = actor.bounding_box
            extent = getattr(bbox, "extent", None)
            loc = getattr(bbox, "location", None)
            if extent is None or loc is None:
                raise RuntimeError("bounding_box_missing")
            length = max(2.0 * float(getattr(extent, "x", 0.0)), 0.0)
            width = max(2.0 * float(getattr(extent, "y", 0.0)), 0.0)
            height = max(2.0 * float(getattr(extent, "z", 0.0)), 0.0)
            overrides: Dict[str, Any] = {
                "length": length,
                "width": width,
                "height": height,
                "front_edge_to_center": max(float(getattr(extent, "x", 0.0)) + float(getattr(loc, "x", 0.0)), 0.0),
                "back_edge_to_center": max(float(getattr(extent, "x", 0.0)) - float(getattr(loc, "x", 0.0)), 0.0),
                "left_edge_to_center": max(float(getattr(extent, "y", 0.0)) + float(getattr(loc, "y", 0.0)), 0.0),
                "right_edge_to_center": max(float(getattr(extent, "y", 0.0)) - float(getattr(loc, "y", 0.0)), 0.0),
            }
            physics = actor.get_physics_control()
            wheels = list(getattr(physics, "wheels", []) or [])
            raw_positions: list[float] = []
            for wheel in wheels:
                pos = getattr(wheel, "position", None)
                if pos is None:
                    continue
                raw_positions.extend(
                    [
                        float(getattr(pos, "x", 0.0)),
                        float(getattr(pos, "y", 0.0)),
                        float(getattr(pos, "z", 0.0)),
                    ]
                )
            scale = self._carla_scale_to_m(raw_positions) if raw_positions else 1.0
            wheel_x_positions: list[float] = []
            wheel_radius_candidates: list[float] = []
            max_steer_candidates: list[float] = []
            for wheel in wheels:
                pos = getattr(wheel, "position", None)
                if pos is not None:
                    wheel_x_positions.append(float(getattr(pos, "x", 0.0)) * scale)
                radius = float(getattr(wheel, "radius", 0.0) or 0.0)
                if radius > 0.0:
                    wheel_radius_candidates.append(radius * (0.01 if radius > 5.0 else 1.0))
                max_steer = float(getattr(wheel, "max_steer_angle", 0.0) or 0.0)
                if abs(max_steer) > 1e-6:
                    max_steer_candidates.append(abs(max_steer))
            if len(wheel_x_positions) >= 2:
                xs = sorted(wheel_x_positions)
                front_x = sum(xs[-2:]) / min(2, len(xs[-2:]))
                rear_x = sum(xs[:2]) / min(2, len(xs[:2]))
                if front_x > rear_x:
                    overrides["wheel_base"] = front_x - rear_x
            if max_steer_candidates:
                max_steer_angle = max(max_steer_candidates)
                overrides["max_steer_angle"] = max_steer_angle
                wheel_base = float(overrides.get("wheel_base", 0.0) or 0.0)
                if wheel_base > 0.0 and max_steer_angle > 1e-3:
                    overrides["min_turn_radius"] = wheel_base / max(math.tan(math.radians(max_steer_angle)), 1e-6)
            if wheel_radius_candidates:
                overrides["wheel_rolling_radius"] = sum(wheel_radius_candidates) / len(wheel_radius_candidates)
            payload["ok"] = True
            payload["actor_id"] = int(getattr(actor, "id", 0) or 0)
            payload["role_name"] = ((getattr(actor, "attributes", {}) or {}).get("role_name", "") or "").strip()
            payload["type_id"] = str(getattr(actor, "type_id", "") or "")
            payload["overrides"] = overrides
            out_path.write_text(json.dumps(payload, indent=2))
            return overrides
        except Exception as exc:
            payload["ok"] = False
            payload["reason"] = f"probe_failed:{exc}"
            out_path.write_text(json.dumps(payload, indent=2))
            return {}

    def _sim_vehicle_param_cfg(self) -> Dict[str, Any]:
        cfg = (self._apollo_cfg().get("vehicle_param", {}) or {})
        defaults: Dict[str, Any] = {
            "enabled": True,
            # Keep default vehicle model aligned with Apollo MKZ calibration.
            "brand": "LINCOLN_MKZ",
            "front_edge_to_center": 3.89,
            "back_edge_to_center": 1.043,
            "left_edge_to_center": 1.055,
            "right_edge_to_center": 1.055,
            "length": 4.933,
            "width": 2.11,
            "height": 1.48,
            "min_turn_radius": 5.05386147161,
            "max_acceleration": 2.0,
            "max_deceleration": -6.0,
            "max_steer_angle": 8.2030,
            "max_steer_angle_rate": 6.9813,
            "steer_ratio": 16.0,
            "wheel_base": 2.8448,
            "wheel_rolling_radius": 0.3350,
            "max_abs_speed_when_stopped": 0.2,
            "brake_deadzone": 14.5,
            "throttle_deadzone": 15.7,
        }
        merged = dict(defaults)
        if self._carla_vehicle_param_override:
            merged.update(self._carla_vehicle_param_override)
        merged.update(cfg)
        return merged

    def _docker_sim_vehicle_param_shell(self) -> str:
        cfg = self._sim_vehicle_param_cfg()
        if not bool(cfg.get("enabled", True)):
            return ""
        vp_path = "/apollo_workspace/conf_overlay/modules/common/data/vehicle_param.pb.txt"
        lines = [
            "vehicle_param {",
            f"  brand: {cfg['brand']}",
            '  vehicle_id {',
            '      other_unique_id: "carla_mkz_sim"',
            '  }',
            f"  front_edge_to_center: {float(cfg['front_edge_to_center']):.4f}",
            f"  back_edge_to_center: {float(cfg['back_edge_to_center']):.4f}",
            f"  left_edge_to_center: {float(cfg['left_edge_to_center']):.4f}",
            f"  right_edge_to_center: {float(cfg['right_edge_to_center']):.4f}",
            "",
            f"  length: {float(cfg['length']):.4f}",
            f"  width: {float(cfg['width']):.4f}",
            f"  height: {float(cfg['height']):.4f}",
            f"  min_turn_radius: {float(cfg['min_turn_radius']):.4f}",
            f"  max_acceleration: {float(cfg['max_acceleration']):.4f}",
            f"  max_deceleration: {float(cfg['max_deceleration']):.4f}",
            f"  max_steer_angle: {float(cfg['max_steer_angle']):.4f}",
            f"  max_steer_angle_rate: {float(cfg['max_steer_angle_rate']):.4f}",
            f"  steer_ratio: {float(cfg['steer_ratio']):.4f}",
            f"  wheel_base: {float(cfg['wheel_base']):.4f}",
            f"  wheel_rolling_radius: {float(cfg['wheel_rolling_radius']):.4f}",
            f"  max_abs_speed_when_stopped: {float(cfg['max_abs_speed_when_stopped']):.4f}",
            f"  brake_deadzone: {float(cfg['brake_deadzone']):.4f}",
            f"  throttle_deadzone: {float(cfg['throttle_deadzone']):.4f}",
            "}",
            "",
        ]
        content = "\n".join(lines)
        py = (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({vp_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
        )
        link = (
            f"if [ -f {shlex.quote(vp_path)} ]; then "
            "mkdir -p /apollo/modules/common/data >/dev/null 2>&1 || true; "
            f"ln -sf {shlex.quote(vp_path)} /apollo/modules/common/data/vehicle_param.pb.txt >/dev/null 2>&1 "
            f"|| cp -f {shlex.quote(vp_path)} /apollo/modules/common/data/vehicle_param.pb.txt >/dev/null 2>&1 || true; "
            "fi; "
        )
        return py + "; " + link

    def _docker_start_modules_enabled(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("start_modules", False))

    def _apollo_planning_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("planning", {}) or {})

    def _apollo_control_pipeline_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("control_pipeline", {}) or {})

    def _apollo_control_runtime_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("control_runtime", {}) or {})

    def _global_flagfile_map_overlay_shell(self) -> str:
        """Point Apollo modules at the CARLA run map before launch.

        Apollo modules read `--map_dir` from `modules/common/data/global_flagfile.txt`.
        The bridge can use an explicit map file, but routing/planning/control still
        follow the module flagfile. Keep this as a runtime overlay so custom maps
        can be selected without editing the Apollo install permanently.
        """
        map_name = str(self._run_cfg().get("map") or "").strip()
        if not map_name:
            return ""
        overlay_path = "/apollo_workspace/conf_overlay/modules/common/data/global_flagfile.txt"
        target_path = "/apollo/modules/common/data/global_flagfile.txt"
        # Keep this shell-only: _docker_start_modules_cmd() deliberately collapses
        # whitespace, which breaks multi-line `python -c` scripts with blocks.
        quoted_map = shlex.quote(map_name)
        script = (
            f"map_name={quoted_map}; "
            "lower=$(printf '%s' \"$map_name\" | tr '[:upper:]' '[:lower:]'); "
            "selected_token=''; "
            "for token in \"$map_name\" \"$lower\" \"carla_$lower\"; do "
            "[ -n \"$token\" ] && [ -d \"/apollo/modules/map/data/$token\" ] && "
            "{ selected_token=\"$token\"; break; }; "
            "done; "
            f"overlay={shlex.quote(overlay_path)}; "
            f"target={shlex.quote(target_path)}; "
            "if [ -n \"$selected_token\" ]; then "
            "mkdir -p \"$(dirname \"$overlay\")\" >/dev/null 2>&1 || true; "
            "src=''; "
            "for cand in "
            "\"$target.carla_testbed.bak\" "
            "\"$target\" "
            "\"/opt/apollo/neo/share/modules/common/data/global_flagfile.txt\" "
            "\"/opt/apollo/neo/src/modules/common/data/global_flagfile.txt\"; do "
            "[ -f \"$cand\" ] && [ ! -L \"$cand\" ] && { src=\"$cand\"; break; }; "
            "done; "
            "tmp=\"$overlay.tmp.$$\"; "
            "if [ -n \"$src\" ]; then "
            "grep -vE '^\\s*--map_dir=' \"$src\" > \"$tmp\" || true; "
            "else "
            ": > \"$tmp\"; "
            "fi; "
            "printf '%s\\n' \"--map_dir=modules/map/data/$selected_token\" >> \"$tmp\"; "
            "mv -f \"$tmp\" \"$overlay\"; "
            "else "
            "rm -f \"$overlay\" >/dev/null 2>&1 || true; "
            "fi; "
        )
        return script + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)

    @staticmethod
    def _managed_overlay_link_shell(overlay_path: str, target_path: str, *, enabled: bool) -> str:
        backup_path = f"{target_path}.carla_testbed.bak"
        if not enabled:
            return (
                f"rm -f {shlex.quote(overlay_path)} >/dev/null 2>&1 || true; "
                f"if [ -f {shlex.quote(backup_path)} ]; then "
                f"rm -f {shlex.quote(target_path)} >/dev/null 2>&1 || true; "
                f"cp -f {shlex.quote(backup_path)} {shlex.quote(target_path)} >/dev/null 2>&1 || true; "
                "fi; "
            )
        return (
            f"if [ -f {shlex.quote(overlay_path)} ]; then "
            f"if [ ! -f {shlex.quote(backup_path)} ] && [ -e {shlex.quote(target_path)} ]; then "
            f"cp -f {shlex.quote(target_path)} {shlex.quote(backup_path)} >/dev/null 2>&1 || true; "
            "fi; "
            f"ln -sf {shlex.quote(overlay_path)} {shlex.quote(target_path)} >/dev/null 2>&1 "
            f"|| (rm -f {shlex.quote(target_path)} >/dev/null 2>&1 || true; "
            f"cp -f {shlex.quote(overlay_path)} {shlex.quote(target_path)} >/dev/null 2>&1 || true); "
            "fi; "
        )

    def _lane_follow_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        longitudinal_only = bool(planning_cfg.get("longitudinal_only_pipeline", False))
        longitudinal_only_keep_lane_follow_path = bool(
            planning_cfg.get("longitudinal_only_keep_lane_follow_path", True)
        )
        disable_lane_change = bool(planning_cfg.get("disable_lane_change_path", False))
        disable_lane_borrow = bool(planning_cfg.get("disable_lane_borrow_path", False))
        disable_rule_stop = bool(planning_cfg.get("disable_rule_based_stop_decider", False))
        if longitudinal_only:
            # Keep lane-follow path generation enabled in longitudinal-only mode.
            # Dropping all primary path tasks can intermittently trigger
            # "Path is empty / reference line is nullptr" near lane-segment joins.
            tasks = []
            if longitudinal_only_keep_lane_follow_path:
                tasks.append(("LANE_FOLLOW_PATH", "LaneFollowPath", True))
            tasks.extend(
                [
                    ("FALLBACK_PATH", "FallbackPath", True),
                    ("PATH_DECIDER", "PathDecider", True),
                    ("SPEED_BOUNDS_PRIORI_DECIDER", "SpeedBoundsDecider", True),
                    ("SPEED_HEURISTIC_OPTIMIZER", "PathTimeHeuristicOptimizer", True),
                    ("SPEED_DECIDER", "SpeedDecider", True),
                    ("SPEED_BOUNDS_FINAL_DECIDER", "SpeedBoundsDecider", True),
                    ("PIECEWISE_JERK_SPEED", "PiecewiseJerkSpeedOptimizer", True),
                ]
            )
        else:
            tasks = [
                ("LANE_CHANGE_PATH", "LaneChangePath", not disable_lane_change),
                ("LANE_FOLLOW_PATH", "LaneFollowPath", True),
                ("LANE_BORROW_PATH", "LaneBorrowPath", not disable_lane_borrow),
                ("FALLBACK_PATH", "FallbackPath", True),
                ("PATH_DECIDER", "PathDecider", True),
                ("RULE_BASED_STOP_DECIDER", "RuleBasedStopDecider", not disable_rule_stop),
                ("SPEED_BOUNDS_PRIORI_DECIDER", "SpeedBoundsDecider", True),
                ("SPEED_HEURISTIC_OPTIMIZER", "PathTimeHeuristicOptimizer", True),
                ("SPEED_DECIDER", "SpeedDecider", True),
                ("SPEED_BOUNDS_FINAL_DECIDER", "SpeedBoundsDecider", True),
                ("PIECEWISE_JERK_SPEED", "PiecewiseJerkSpeedOptimizer", True),
            ]
        enabled = [(name, kind) for name, kind, keep in tasks if keep]
        if len(enabled) == len(tasks):
            return (
                "for p in "
                "/apollo/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt "
                "/opt/apollo/neo/share/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt "
                "/opt/apollo/neo/src/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt; do "
                "[ -f \"$p\" ] && { ln -sf \"$p\" \"$lf_conf_dir/pipeline.pb.txt\" >/dev/null 2>&1 || true; break; }; "
                "done; "
            )
        lines = ['stage: {', '  name: "LANE_FOLLOW_STAGE"', '  type: "LaneFollowStage"', '  enabled: true']
        for name, kind in enabled:
            lines.extend(
                [
                    "  task {",
                    f'    name: "{name}"',
                    f'    type: "{kind}"',
                    "  }",
                ]
            )
        lines.append("}")
        content = "\n".join(lines) + "\n"
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                "p=Path('/apollo_workspace/conf_overlay/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt'); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
        )

    def _traffic_light_rule_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        disable_traffic_light_rule = bool(planning_cfg.get("disable_traffic_light_rule", False))
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/traffic_rules/traffic_light/conf/default_conf.pb.txt"
        if not disable_traffic_light_rule:
            return f"rm -f {shlex.quote(overlay_path)} >/dev/null 2>&1 || true; "
        content = "enabled: false\nstop_distance: 1.0\nmax_stop_deceleration: 4.0\n"
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({overlay_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
        )

    def _traffic_rules_pipeline_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        acc_only_mode = bool(planning_cfg.get("acc_only_mode", False))
        disable_destination_rule = bool(planning_cfg.get("disable_destination_rule", False))
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/traffic_rule_config.pb.txt"
        target_path = "/apollo/modules/planning/planning_component/conf/traffic_rule_config.pb.txt"
        if not acc_only_mode and not disable_destination_rule:
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        if acc_only_mode:
            content = (
                "rule {\n"
                '  name: "BACKSIDE_VEHICLE"\n'
                '  type: "BacksideVehicle"\n'
                "}\n"
                "rule {\n"
                '  name: "REROUTING"\n'
                '  type: "Rerouting"\n'
                "}\n"
            )
            return (
                "python3 -c "
                + shlex.quote(
                    "from pathlib import Path; "
                    f"p=Path({overlay_path!r}); "
                    "p.parent.mkdir(parents=True, exist_ok=True); "
                    f"p.write_text({content!r})"
                )
                + "; "
                + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
            )
        script = (
            "from pathlib import Path; "
            "import re; "
            "cands=["
            "'/apollo/modules/planning/planning_component/conf/traffic_rule_config.pb.txt',"
            "'/opt/apollo/neo/share/modules/planning/planning_component/conf/traffic_rule_config.pb.txt',"
            "'/opt/apollo/neo/src/modules/planning/planning_component/conf/traffic_rule_config.pb.txt'"
            "]; "
            "src=next((cand for cand in cands if Path(cand).exists()), ''); "
            "text=Path(src).read_text() if src else ''; "
            "pattern=r'rule\\s*\\{\\s*name:\\s*\"DESTINATION\"\\s*type:\\s*\"Destination\"\\s*\\}\\s*'; "
            "filtered=re.sub(pattern, '', text, flags=re.MULTILINE); "
            f"out=Path({overlay_path!r}); "
            "out.parent.mkdir(parents=True, exist_ok=True); "
            "payload=filtered or text; "
            "out.write_text(payload if payload.endswith('\\n') else payload + '\\n')"
        )
        return (
            "python3 -c "
            + shlex.quote(script)
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _reference_line_end_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        min_remain = planning_cfg.get("reference_line_end_min_remain_length_m")
        stop_distance = planning_cfg.get("reference_line_end_stop_distance_m", 0.5)
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/traffic_rules/reference_line_end/conf/default_conf.pb.txt"
        if min_remain is None:
            return f"rm -f {shlex.quote(overlay_path)} >/dev/null 2>&1 || true; "
        content = (
            f"stop_distance: {float(stop_distance):.3f}\n"
            f"min_reference_line_remain_length: {float(min_remain):.3f}\n"
        )
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({overlay_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
        )

    def _planning_flags_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        stitch = planning_cfg.get("enable_reference_line_stitching")
        trajectory_stitcher = planning_cfg.get("enable_trajectory_stitcher")
        control_interactive_replan = planning_cfg.get("enable_control_interactive_replan")
        default_cruise_speed = planning_cfg.get("default_cruise_speed_mps")
        planning_upper_speed_limit = planning_cfg.get("planning_upper_speed_limit_mps")
        smoother_config_filename = self._planning_smoother_config_filename(planning_cfg)
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/planning.conf"
        target_path = "/apollo/modules/planning/planning_component/conf/planning.conf"
        if (
            stitch is None
            and trajectory_stitcher is None
            and control_interactive_replan is None
            and default_cruise_speed is None
            and planning_upper_speed_limit is None
            and smoother_config_filename is None
        ):
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        script_parts = [
            "from pathlib import Path; ",
            "cands=[",
            "'/apollo/modules/planning/planning_component/conf/planning.conf',",
            "'/opt/apollo/neo/share/modules/planning/planning_component/conf/planning.conf',",
            "'/opt/apollo/neo/src/modules/planning/planning_component/conf/planning.conf'",
            "]; ",
            "from pathlib import Path as _P; ",
            "src=next((cand for cand in cands if _P(cand).exists()), ''); ",
            "lines=((_P(src).read_text().splitlines()) if src else ['--flagfile=modules/common/data/global_flagfile.txt']); ",
            "prefixes=[]; ",
        ]
        desired_stitch = None
        if stitch is not None:
            desired_stitch = "true" if bool(stitch) else "false"
            script_parts.append("prefixes.append('--enable_reference_line_stitching='); ")
        desired_trajectory_stitcher = None
        if trajectory_stitcher is not None:
            desired_trajectory_stitcher = "true" if bool(trajectory_stitcher) else "false"
            script_parts.append("prefixes.append('--enable_trajectory_stitcher='); ")
        desired_control_interactive_replan = None
        if control_interactive_replan is not None:
            desired_control_interactive_replan = "true" if bool(control_interactive_replan) else "false"
            script_parts.append("prefixes.append('--enable_control_interactive_replan='); ")
        if default_cruise_speed is not None:
            script_parts.append("prefixes.append('--default_cruise_speed='); ")
        if planning_upper_speed_limit is not None:
            script_parts.append("prefixes.append('--planning_upper_speed_limit='); ")
        if smoother_config_filename is not None:
            script_parts.append("prefixes.append('--smoother_config_filename='); ")
        script_parts.append("lines=[ln for ln in lines if not any(ln.startswith(prefix) for prefix in prefixes)]; ")
        if desired_stitch is not None:
            script_parts.append(f"lines.append('--enable_reference_line_stitching={desired_stitch}'); ")
        if desired_trajectory_stitcher is not None:
            script_parts.append(
                f"lines.append('--enable_trajectory_stitcher={desired_trajectory_stitcher}'); "
            )
        if desired_control_interactive_replan is not None:
            script_parts.append(
                f"lines.append('--enable_control_interactive_replan={desired_control_interactive_replan}'); "
            )
        if default_cruise_speed is not None:
            script_parts.append(f"lines.append('--default_cruise_speed={float(default_cruise_speed):.3f}'); ")
        if planning_upper_speed_limit is not None:
            script_parts.append(
                f"lines.append('--planning_upper_speed_limit={float(planning_upper_speed_limit):.3f}'); "
            )
        if smoother_config_filename is not None:
            script_parts.append(f"lines.append('--smoother_config_filename={smoother_config_filename}'); ")
        script_parts.extend(
            [
                f"out=Path({overlay_path!r}); ",
                "out.parent.mkdir(parents=True, exist_ok=True); ",
                "out.write_text('\\n'.join(lines)+'\\n')",
            ]
        )
        script = "".join(script_parts)
        return (
            "python3 -c "
            + shlex.quote(script)
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    @staticmethod
    def _planning_smoother_config_filename(planning_cfg: Dict[str, Any]) -> Optional[str]:
        raw_filename = planning_cfg.get("smoother_config_filename")
        if raw_filename not in {None, ""}:
            return str(raw_filename)
        raw_mode = str(planning_cfg.get("smoother") or "").strip().lower()
        if not raw_mode:
            return None
        aliases = {
            "discrete": "discrete_points",
            "discrete_points": "discrete_points",
            "discrete_points_smoother": "discrete_points",
            "qp": "qp_spline",
            "qp_spline": "qp_spline",
            "qp_spline_smoother": "qp_spline",
            "spiral": "spiral",
            "spiral_smoother": "spiral",
        }
        mode = aliases.get(raw_mode)
        if mode == "discrete_points":
            return "modules/planning/planning_component/conf/discrete_points_smoother_config.pb.txt"
        if mode == "qp_spline":
            return "modules/planning/planning_component/conf/qp_spline_smoother_config.pb.txt"
        if mode == "spiral":
            return "modules/planning/planning_component/conf/spiral_smoother_config.pb.txt"
        return None

    def _control_pipeline_overlay_shell(self) -> str:
        cfg = self._apollo_control_pipeline_cfg()
        raw_mode = str(cfg.get("mode") or cfg.get("pipeline_mode") or "").strip().lower()
        overlay_path = "/apollo_workspace/conf_overlay/modules/control/control_component/conf/pipeline.pb.txt"
        target_path = "/apollo/modules/control/control_component/conf/pipeline.pb.txt"
        if not raw_mode:
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        aliases = {
            "lat_lon": "lat_lon",
            "latlon": "lat_lon",
            "lateral_longitudinal": "lat_lon",
            "lqr_pid": "lat_lon",
            "mpc": "mpc",
            "mpc_controller": "mpc",
        }
        mode = aliases.get(raw_mode)
        if mode == "lat_lon":
            content = (
                "controller {\n"
                '  name: "LAT_CONTROLLER"\n'
                '  type: "LatController"\n'
                "}\n"
                "controller {\n"
                '  name: "LON_CONTROLLER"\n'
                '  type: "LonController"\n'
                "}\n"
            )
        elif mode == "mpc":
            content = (
                "controller {\n"
                '  name: "MPC_CONTROLLER"\n'
                '  type: "MPCController"\n'
                "}\n"
            )
        else:
            return f"echo unsupported_control_pipeline_mode={shlex.quote(raw_mode)}; exit 2; "
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({overlay_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _control_runtime_interval_ms(self) -> Optional[int]:
        cfg = self._apollo_control_runtime_cfg()
        raw_ms = cfg.get("control_interval_ms")
        if raw_ms is None:
            raw_period = cfg.get("control_period_s")
            if raw_period is None:
                return None
            try:
                raw_ms = float(raw_period) * 1000.0
            except Exception:
                return None
        try:
            interval_ms = int(round(float(raw_ms)))
        except Exception:
            return None
        return interval_ms if interval_ms > 0 else None

    def _control_runtime_period_s(self) -> Optional[float]:
        cfg = self._apollo_control_runtime_cfg()
        raw_period = cfg.get("control_period_s")
        if raw_period is not None:
            try:
                period = float(raw_period)
            except Exception:
                return None
            return period if period > 0.0 else None
        interval_ms = self._control_runtime_interval_ms()
        if interval_ms is None:
            return None
        return interval_ms / 1000.0

    def _control_dag_overlay_shell(self) -> str:
        interval_ms = self._control_runtime_interval_ms()
        overlay_path = "/apollo_workspace/conf_overlay/modules/control/control_component/dag/control.dag"
        target_path = "/apollo/modules/control/control_component/dag/control.dag"
        if interval_ms is None:
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        script = (
            "from pathlib import Path; import re; "
            "cands=["
            "'/apollo/modules/control/control_component/dag/control.dag',"
            "'/opt/apollo/neo/share/modules/control/control_component/dag/control.dag',"
            "'/opt/apollo/neo/src/modules/control/control_component/dag/control.dag'"
            "]; "
            "src=next((cand for cand in cands if Path(cand).exists()), ''); "
            "text=(Path(src).read_text() if src else "
            "'module_config {\\n    module_library : \"modules/control/control_component/libcontrol_component.so\"\\n"
            "    timer_components {\\n        class_name : \"ControlComponent\"\\n"
            "        config {\\n            name: \"control\"\\n"
            "            flag_file_path: \"modules/control/control_component/conf/control.conf\"\\n"
            "            interval: 10\\n        }\\n    }\\n}\\n'); "
            f"text=re.sub(r'interval:\\s*\\d+', 'interval: {interval_ms}', text, count=1); "
            f"out=Path({overlay_path!r}); "
            "out.parent.mkdir(parents=True, exist_ok=True); "
            "out.write_text(text)"
        )
        return (
            "python3 -c "
            + shlex.quote(script)
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _control_flags_overlay_shell(self) -> str:
        control_period_s = self._control_runtime_period_s()
        overlay_path = "/apollo_workspace/conf_overlay/modules/control/control_component/conf/control.conf"
        target_path = "/apollo/modules/control/control_component/conf/control.conf"
        if control_period_s is None:
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        script_parts = [
            "from pathlib import Path; ",
            "cands=[",
            "'/apollo/modules/control/control_component/conf/control.conf',",
            "'/opt/apollo/neo/share/modules/control/control_component/conf/control.conf',",
            "'/opt/apollo/neo/src/modules/control/control_component/conf/control.conf'",
            "]; ",
            "src=next((cand for cand in cands if Path(cand).exists()), ''); ",
            "lines=((Path(src).read_text().splitlines()) if src else ['--pipeline_file=modules/control/control_component/conf/pipeline.pb.txt']); ",
            "prefixes=['--control_period=']; ",
            "lines=[ln for ln in lines if not any(ln.startswith(prefix) for prefix in prefixes)]; ",
            f"lines.append('--control_period={float(control_period_s):.3f}'); ",
            f"out=Path({overlay_path!r}); ",
            "out.parent.mkdir(parents=True, exist_ok=True); ",
            "out.write_text('\\n'.join(lines)+'\\n')",
        ]
        return (
            "python3 -c "
            + shlex.quote("".join(script_parts))
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _speed_decider_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        cfg_keys = {
            "speed_decider_follow_min_obs_lateral_distance_m",
            "speed_decider_follow_min_time_sec",
            "speed_decider_stop_follow_distance_m",
            "speed_decider_follow_distance_scale",
        }
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/tasks/speed_decider/conf/default_conf.pb.txt"
        target_path = "/apollo/modules/planning/tasks/speed_decider/conf/default_conf.pb.txt"
        if not any(key in planning_cfg for key in cfg_keys):
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        follow_min_obs_lateral_distance = float(
            planning_cfg.get("speed_decider_follow_min_obs_lateral_distance_m", 2.5)
        )
        follow_min_time_sec = float(planning_cfg.get("speed_decider_follow_min_time_sec", 2.0))
        stop_follow_distance = float(planning_cfg.get("speed_decider_stop_follow_distance_m", 2.0))
        scale = planning_cfg.get("speed_decider_follow_distance_scale")
        follow_distance_pairs = [(0.0, 2.0), (2.7, 1.0), (5.4, 0.75), (8.1, 0.5)]
        if scale is not None:
            slope_scale = max(0.05, float(scale))
            follow_distance_pairs = [(speed, max(0.05, slope * slope_scale)) for speed, slope in follow_distance_pairs]
        lines = [
            f"follow_min_obs_lateral_distance: {follow_min_obs_lateral_distance:.3f}",
            f"follow_min_time_sec: {follow_min_time_sec:.3f}",
            f"stop_follow_distance: {stop_follow_distance:.3f}",
            "follow_distance_scheduler {",
        ]
        for speed, slope in follow_distance_pairs:
            lines.extend(
                [
                    "  follow_distance {",
                    f"    speed: {speed:.3f}",
                    f"    slope: {slope:.3f}",
                    "  }",
                ]
            )
        lines.append("}")
        content = "\n".join(lines) + "\n"
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({overlay_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _speed_bounds_decider_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        cfg_keys = {
            "speed_bounds_decider_lowest_speed_mps",
            "speed_bounds_decider_enable_nudge_slowdown",
        }
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt"
        target_path = "/apollo/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt"
        if not any(key in planning_cfg for key in cfg_keys):
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        lines: list[str] = []
        if "speed_bounds_decider_lowest_speed_mps" in planning_cfg:
            lines.append(
                f"lowest_speed: {float(planning_cfg.get('speed_bounds_decider_lowest_speed_mps', 0.1)):.3f}"
            )
        if "speed_bounds_decider_enable_nudge_slowdown" in planning_cfg:
            enabled = "true" if bool(planning_cfg.get("speed_bounds_decider_enable_nudge_slowdown")) else "false"
            lines.append(f"enable_nudge_slowdown: {enabled}")
        content = "\n".join(lines) + "\n"
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({overlay_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _public_road_planner_overlay_shell(self) -> str:
        planning_cfg = self._apollo_planning_cfg()
        lane_follow_only = bool(planning_cfg.get("lane_follow_only_scenario", False))
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/public_road_planner_config.pb.txt"
        target_path = "/apollo/modules/planning/planning_component/conf/public_road_planner_config.pb.txt"
        if not lane_follow_only:
            return self._managed_overlay_link_shell(overlay_path, target_path, enabled=False)
        content = (
            "scenario {\n"
            '  name: "LANE_FOLLOW"\n'
            '  type: "LaneFollowScenario"\n'
            "}\n"
        )
        return (
            "python3 -c "
            + shlex.quote(
                "from pathlib import Path; "
                f"p=Path({overlay_path!r}); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                f"p.write_text({content!r})"
            )
            + "; "
            + self._managed_overlay_link_shell(overlay_path, target_path, enabled=True)
        )

    def _docker_start_modules_cmd(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("start_modules_cmd") or "").strip()
        if not raw:
            try:
                control_launch_delay_sec = float(cfg.get("control_launch_delay_sec", 0.0) or 0.0)
            except Exception:
                control_launch_delay_sec = 0.0
            if control_launch_delay_sec < 0.0:
                control_launch_delay_sec = 0.0
            control_launch_delay_sec_int = int(math.ceil(control_launch_delay_sec))
            control_start_gate = self._docker_control_start_gate()
            defer_control_start = control_start_gate != "none"
            internal_debug_exports = self._apollo_internal_debug_shell_exports(docker=True)
            stage6_enabled = bool(self._apollo_stage6_cfg().get("enabled", False))
            routing_start_snippet = (
                "[ -f /apollo/modules/routing/dag/routing.dag ] && "
                "setsid -f mainboard -d modules/routing/dag/routing.dag "
                "-p routing -s CYBER_DEFAULT "
                ">/apollo_workspace/log/routing.stage6.tb.log 2>&1; "
                if stage6_enabled
                else
                "[ -f /apollo/modules/routing/launch/routing.launch ] && "
                "launches+=(/apollo/modules/routing/launch/routing.launch); "
            )
            planning_start_snippet = (
                "if [ -f /apollo/modules/planning/planning_component/dag/planning.dag ]; then "
                "if [ -f /apollo/modules/external_command/process_component/dag/external_command_process.dag ]; then "
                "setsid -f mainboard "
                "-d modules/external_command/process_component/dag/external_command_process.dag "
                "-d modules/planning/planning_component/dag/planning.dag "
                "-p planning -s CYBER_DEFAULT "
                ">/apollo_workspace/log/planning.stage6.tb.log 2>&1; "
                "else "
                "setsid -f mainboard -d modules/planning/planning_component/dag/planning.dag "
                "-p planning -s CYBER_DEFAULT "
                ">/apollo_workspace/log/planning.stage6.tb.log 2>&1; "
                "fi; fi; "
                if stage6_enabled
                else
                "[ -f /apollo/modules/planning/planning_component/launch/planning.launch ] && "
                "launches+=(/apollo/modules/planning/planning_component/launch/planning.launch); "
            )
            parts = [
                "set -eo pipefail; ",
                (internal_debug_exports + "; ") if internal_debug_exports else "",
                "for _pat in "
                "'modules/routing/dag/routing.dag' "
                "'modules/prediction/dag/prediction.dag' "
                "'modules/external_command/process_component/dag/external_command_process.dag' "
                "'modules/planning/planning_component/dag/planning.dag' "
                "'modules/control/control_component/dag/control.dag'; do "
                "pgrep -f \"$_pat\" | grep -vw $$ | xargs -r kill -9 >/dev/null 2>&1 || true; "
                "done; ",
                "sleep 1; ",
                "mkdir -p /usr/lib/x86_64-linux-gnu >/dev/null 2>&1 || true; ",
                "[ -f /opt/apollo/neo/lib/third_party/rtklib/librtklib.so ] && "
                "ln -sf /opt/apollo/neo/lib/third_party/rtklib/librtklib.so /usr/lib/x86_64-linux-gnu/librtklib.so >/dev/null 2>&1 || true; ",
                "ldconfig >/dev/null 2>&1 || true; ",
                "overlay_conf_root=/apollo_workspace/conf_overlay; ",
                "lf_conf_dir=${overlay_conf_root}/modules/planning/scenarios/lane_follow/conf; ",
                "lf_stage_dir=${lf_conf_dir}/lane_follow_stage; ",
                "mkdir -p \"$lf_stage_dir\" /apollo_workspace/log /apollo_workspace/dumps >/dev/null 2>&1 || true; ",
                "rm -f /apollo_workspace/dumps/control.data >/dev/null 2>&1 || true; ",
                "chown -R ubuntu:ubuntu /apollo_workspace/dumps >/dev/null 2>&1 || true; ",
                "chmod 0777 /apollo_workspace/dumps >/dev/null 2>&1 || true; ",
                self._global_flagfile_map_overlay_shell(),
                " ",
                self._docker_sim_vehicle_param_shell(),
                " ",
                self._lane_follow_overlay_shell(),
                self._traffic_rules_pipeline_overlay_shell(),
                self._traffic_light_rule_overlay_shell(),
                self._reference_line_end_overlay_shell(),
                self._planning_flags_overlay_shell(),
                self._speed_bounds_decider_overlay_shell(),
                self._speed_decider_overlay_shell(),
                self._public_road_planner_overlay_shell(),
                self._control_dag_overlay_shell(),
                self._control_flags_overlay_shell(),
                self._control_pipeline_overlay_shell(),
                "declare -A lf_map=( "
                "[lane_change_path.pb.txt]=lane_change_path "
                "[lane_follow_path.pb.txt]=lane_follow_path "
                "[lane_borrow_path.pb.txt]=lane_borrow_path "
                "[fallback_path.pb.txt]=fallback_path "
                "[path_decider.pb.txt]=path_decider "
                "[rule_based_stop_decider.pb.txt]=rule_based_stop_decider "
                "[speed_bounds_priori_decider.pb.txt]=speed_bounds_decider "
                "[speed_heuristic_optimizer.pb.txt]=path_time_heuristic "
                "[speed_decider.pb.txt]=speed_decider "
                "[speed_bounds_final_decider.pb.txt]=speed_bounds_decider "
                "[piecewise_jerk_speed.pb.txt]=piecewise_jerk_speed "
                "); ",
                "for dst in \"${!lf_map[@]}\"; do "
                "task=\"${lf_map[$dst]}\"; src=''; "
                "for cand in "
                "\"/apollo_workspace/conf_overlay/modules/planning/tasks/${task}/conf/default_conf.pb.txt\" "
                "\"/apollo/modules/planning/tasks/${task}/conf/default_conf.pb.txt\" "
                "\"/opt/apollo/neo/share/modules/planning/tasks/${task}/conf/default_conf.pb.txt\" "
                "\"/opt/apollo/neo/src/modules/planning/tasks/${task}/conf/default_conf.pb.txt\"; do "
                "[ -f \"$cand\" ] && { src=\"$cand\"; break; }; "
                "done; "
                "[ -f \"$src\" ] && ln -sf \"$src\" \"$lf_stage_dir/$dst\" >/dev/null 2>&1 || true; "
                "done; ",
                "launches=(); ",
                routing_start_snippet,
                "[ -f /apollo/modules/prediction/launch/prediction.launch ] && launches+=(/apollo/modules/prediction/launch/prediction.launch); ",
                planning_start_snippet,
                "control_launch_file=''; ",
                "[ -f /apollo/modules/control/control_component/launch/control.launch ] && control_launch_file=/apollo/modules/control/control_component/launch/control.launch; ",
                "if [ -n \"$control_launch_file\" ] && [ "
                + str(control_launch_delay_sec_int)
                + " -le 0 ]"
                + (" && [ 0 -eq 1 ] " if defer_control_start else " ")
                + "; then launches+=(\"$control_launch_file\"); fi; ",
                "if [ ${#launches[@]} -eq 0 ]",
                " && [ ! -f /apollo/modules/planning/planning_component/dag/planning.dag ]" if stage6_enabled else "",
                "; then echo 'no apollo launch files found under /apollo/modules'; exit 2; fi; ",
                "mkdir -p /apollo_workspace/log >/dev/null 2>&1 || true; ",
                "for lf in \"${launches[@]}\"; do "
                "setsid -f cyber_launch start \"$lf\" >/apollo_workspace/log/$(basename \"$lf\").tb.log 2>&1; "
                "sleep 1; "
                "done; ",
                (
                    ""
                    if defer_control_start
                    else (
                        "if [ -n \"$control_launch_file\" ] && [ "
                        + str(control_launch_delay_sec_int)
                        + " -gt 0 ]; then "
                        + "sleep "
                        + str(control_launch_delay_sec_int)
                        + "; "
                        + "setsid -f cyber_launch start \"$control_launch_file\" >/apollo_workspace/log/$(basename \"$control_launch_file\").tb.log 2>&1; "
                        + "sleep 1; "
                        + "fi; "
                    )
                ),
                "sleep 1; " if stage6_enabled else "",
                "sleep 1; ",
                "pgrep -af 'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|external_command/process_component/dag/external_command_process.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' || true",
            ]
            raw = "".join(parts)
        return " ".join(raw.split())

    def _docker_control_start_gate(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("control_start_gate") or "").strip().lower()
        if raw in {"none", "route_seen", "route_established", "planning_ready"}:
            return raw
        if raw in {"route_ready", "routing_ready", "routing_established"}:
            return "route_established"
        if raw in {"eager", "immediate"}:
            return "none"
        if bool(cfg.get("defer_control_until_planning_ready", False)):
            return "planning_ready"
        return "none"

    def _docker_defer_control_until_planning_ready(self) -> bool:
        return self._docker_control_start_gate() == "planning_ready"

    def _docker_deferred_control_start_mode(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("deferred_control_start_mode") or "").strip().lower()
        if raw in {"dag", "direct", "mainboard", "tb"}:
            return "dag"
        if raw in {"launch", "cyber_launch"}:
            return "launch"
        return "launch"

    def _docker_deferred_control_start_async(self) -> bool:
        cfg = self._docker_cfg()
        if "deferred_control_start_async" in cfg:
            return bool(cfg.get("deferred_control_start_async"))
        return True

    def _docker_disable_deferred_control_bvar_dump(self) -> bool:
        cfg = self._docker_cfg()
        if "deferred_control_disable_bvar_dump" in cfg:
            return bool(cfg.get("deferred_control_disable_bvar_dump"))
        return False

    def _docker_control_runtime_overlay_source_dirs(self) -> list[str]:
        cfg = self._docker_cfg()
        raw = cfg.get("control_runtime_overlay_source_dirs")
        if isinstance(raw, str):
            text = raw.strip()
            return [text] if text else []
        if isinstance(raw, (list, tuple)):
            out: list[str] = []
            for item in raw:
                text = str(item).strip()
                if text:
                    out.append(text)
            return out
        return []

    def _docker_control_runtime_overlay_names(self) -> list[str]:
        cfg = self._docker_cfg()
        raw = cfg.get("control_runtime_overlay_names")
        if isinstance(raw, str):
            text = raw.strip()
            return [text] if text else []
        if isinstance(raw, (list, tuple)):
            out: list[str] = []
            for item in raw:
                text = str(item).strip()
                if text:
                    out.append(text)
            return out
        return []

    @staticmethod
    def _docker_control_runtime_overlay_targets() -> Dict[str, str]:
        return {
            "libcontrol_component.so": "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so",
            "libDO_NOT_IMPORT_control_component.so": "/opt/apollo/neo/lib/modules/control/control_component/libDO_NOT_IMPORT_control_component.so",
            "libapollo_control_lib.so": "/opt/apollo/neo/lib/modules/control/control_component/libapollo_control_lib.so",
            "liblat_controller.so": "/opt/apollo/neo/lib/modules/control/controllers/lat_based_lqr_controller/liblat_controller.so",
            "liblon_controller.so": "/opt/apollo/neo/lib/modules/control/controllers/lon_based_pid_controller/liblon_controller.so",
            "libmpc_controller.so": "/opt/apollo/neo/lib/modules/control/controllers/mpc_controller/libmpc_controller.so",
            "control_module.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/control_module.so",
            "lat_lon_controller_submodule.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/lat_lon_controller_submodule.so",
            "libcontrol_module_lib.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/libcontrol_module_lib.so",
            "liblat_lon_controller_submodule_lib.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/liblat_lon_controller_submodule_lib.so",
            "libmpc_controller_submodule_lib.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/libmpc_controller_submodule_lib.so",
            "libpostprocessor_submodule_lib.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/libpostprocessor_submodule_lib.so",
            "libpreprocessor_submodule_lib.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/libpreprocessor_submodule_lib.so",
            "mpc_controller_submodule.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/mpc_controller_submodule.so",
            "postprocessor_submodule.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/postprocessor_submodule.so",
            "preprocessor_submodule.so": "/opt/apollo/neo/lib/modules/control/control_component/submodules/preprocessor_submodule.so",
            "lib_control_cmd_proto_mcc_bin.so": "/opt/apollo/neo/lib/modules/common_msgs/control_msgs/lib_control_cmd_proto_mcc_bin.so",
        }

    def _docker_restore_control_runtime_overlay(self, artifacts: Optional[Path] = None) -> None:
        if not (self._docker_enabled() and self._docker_container_name):
            return
        state_path = "/tmp/tb_control_runtime_overlay_active.json"
        restore_artifact = artifacts / "apollo_control_runtime_overlay_restore.json" if artifacts else None
        script = f"""python3 - <<'PY'
from pathlib import Path
import json, os, shutil

state_path = Path({state_path!r})
payload = {{}}
if not state_path.exists():
    payload = {{"restored": False, "reason": "no_active_state"}}
    print(json.dumps(payload, ensure_ascii=False))
    raise SystemExit(0)

state = json.loads(state_path.read_text())
records = list(state.get("records") or [])
restored = []
for record in records:
    backup = Path(str(record.get("backup") or ""))
    target = Path(str(record.get("target") or ""))
    if not backup.exists():
        continue
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.parent / (target.name + ".codex_restore_tmp")
    shutil.copy2(backup, tmp_target)
    os.replace(tmp_target, target)
    restored.append({{"target": str(target), "backup": str(backup)}})
backup_root = Path(str(state.get("backup_root") or ""))
if backup_root.exists():
    shutil.rmtree(backup_root, ignore_errors=True)
state_path.unlink(missing_ok=True)
payload = {{
    "restored": bool(restored),
    "reason": "restored" if restored else "empty_records",
    "restored_count": len(restored),
    "restored_targets": restored,
}}
print(json.dumps(payload, ensure_ascii=False))
PY"""
        try:
            result = self._docker_exec(
                script,
                capture_output=True,
                check=False,
                user=self._docker_module_exec_user(),
            )
            if restore_artifact is not None:
                restore_artifact.write_text(result.stdout or result.stderr or "")
            self._write_backend_startup_trace(
                "control_runtime_overlay_restore_done",
                output_path=str(restore_artifact) if restore_artifact else "",
            )
        except Exception as exc:
            if restore_artifact is not None:
                restore_artifact.write_text(str(exc))
            self._write_backend_startup_trace(
                "control_runtime_overlay_restore_failed",
                error=str(exc),
            )
        finally:
            self._control_runtime_overlay_active = False

    def _docker_stage_control_runtime_overlay(self, artifacts: Path) -> None:
        source_dirs = self._docker_control_runtime_overlay_source_dirs()
        if not source_dirs:
            return
        self._docker_restore_control_runtime_overlay()
        state_path = "/tmp/tb_control_runtime_overlay_active.json"
        overlay_artifact = artifacts / "apollo_control_runtime_overlay_manifest.json"
        target_map = self._docker_control_runtime_overlay_targets()
        overlay_names = self._docker_control_runtime_overlay_names()
        self._write_backend_startup_trace(
            "control_runtime_overlay_stage_begin",
            source_dirs=source_dirs,
            overlay_names=overlay_names,
        )
        script = f"""python3 - <<'PY'
from pathlib import Path
import json, os, shutil, time

source_dirs = {json.dumps(source_dirs, ensure_ascii=False)}
target_map = {json.dumps(target_map, ensure_ascii=False)}
overlay_names = set({json.dumps(overlay_names, ensure_ascii=False)})
state_path = Path({state_path!r})
selected = {{}}
missing_source_dirs = []
for raw_dir in source_dirs:
    src_dir = Path(raw_dir)
    if not src_dir.is_dir():
        missing_source_dirs.append(str(src_dir))
        continue
    for item in src_dir.rglob('*'):
        if item.is_file() and item.name in target_map and (not overlay_names or item.name in overlay_names):
            selected[item.name] = str(item)
missing_configured_names = sorted(name for name in overlay_names if name not in selected)
backup_root = Path(f"/tmp/tb_control_runtime_overlay_{{int(time.time())}}")
backup_root.mkdir(parents=True, exist_ok=True)
records = []
for name, source in sorted(selected.items()):
    target = Path(target_map[name])
    if not target.exists():
        continue
    backup = backup_root / name
    shutil.copy2(target, backup)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.parent / (target.name + ".codex_overlay_tmp")
    shutil.copy2(source, tmp_target)
    os.replace(tmp_target, target)
    records.append({{
        "name": name,
        "source": str(source),
        "target": str(target),
        "backup": str(backup),
    }})
payload = {{
    "overlay_active": bool(records),
    "backup_root": str(backup_root),
    "source_dirs": source_dirs,
    "configured_names": sorted(overlay_names),
    "missing_configured_names": missing_configured_names,
    "missing_source_dirs": missing_source_dirs,
    "selected_count": len(records),
    "records": records,
}}
state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
print(json.dumps(payload, ensure_ascii=False))
PY"""
        result = self._docker_exec(
            script,
            capture_output=True,
            check=False,
            user=self._docker_module_exec_user(),
        )
        overlay_artifact.write_text(result.stdout or result.stderr or "")
        try:
            payload = json.loads((result.stdout or "{}").strip() or "{}")
        except Exception:
            payload = {}
        selected_count = int(payload.get("selected_count") or 0)
        self._control_runtime_overlay_active = bool(payload.get("overlay_active"))
        self._write_backend_startup_trace(
            "control_runtime_overlay_stage_done",
            output_path=str(overlay_artifact),
            selected_count=selected_count,
            overlay_active=bool(payload.get("overlay_active")),
        )
        if selected_count <= 0:
            raise RuntimeError(
                "control runtime overlay configured but no matching runtime files were staged; "
                f"see {overlay_artifact}"
            )

    def _docker_deferred_control_bvar_env_prefix(self) -> str:
        if self._docker_disable_deferred_control_bvar_dump():
            return "export APOLLO_DISABLE_BVAR_DUMP=1; "
        return "unset APOLLO_DISABLE_BVAR_DUMP; "

    def _docker_control_planning_ready_min_nonempty_count(self) -> int:
        cfg = self._docker_cfg()
        try:
            value = int(cfg.get("control_planning_ready_min_nonempty_count", 1) or 1)
        except Exception:
            value = 1
        return max(1, value)

    def _docker_control_planning_ready_min_sequence_num(self) -> int:
        cfg = self._docker_cfg()
        try:
            value = int(cfg.get("control_planning_ready_min_sequence_num", 0) or 0)
        except Exception:
            value = 0
        return max(0, value)

    def _docker_control_planning_ready_require_routing_success(self) -> bool:
        cfg = self._docker_cfg()
        if "control_planning_ready_require_routing_success" in cfg:
            value = cfg.get("control_planning_ready_require_routing_success")
            if isinstance(value, str):
                return value.strip().lower() not in {"0", "false", "no", "off"}
            return bool(value)
        return True

    def _docker_control_planning_ready_timeout_sec(self) -> float:
        cfg = self._docker_cfg()
        try:
            value = float(cfg.get("control_planning_ready_timeout_sec", 20.0) or 20.0)
        except Exception:
            value = 20.0
        return max(1.0, value)

    def _docker_control_planning_ready_poll_sec(self) -> float:
        cfg = self._docker_cfg()
        try:
            value = float(cfg.get("control_planning_ready_poll_sec", 0.5) or 0.5)
        except Exception:
            value = 0.5
        return max(0.1, value)

    def _docker_control_require_chassis_ready(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("control_require_chassis_ready", False))

    def _docker_control_chassis_ready_min_count(self) -> int:
        cfg = self._docker_cfg()
        try:
            value = int(cfg.get("control_chassis_ready_min_count", 1) or 1)
        except Exception:
            value = 1
        return max(1, value)

    def _docker_required_modules(self) -> list[str]:
        cfg = self._docker_cfg()
        raw = cfg.get("required_modules")
        if isinstance(raw, (list, tuple)):
            out = [str(x).strip().lower() for x in raw if str(x).strip()]
            if out:
                return out
        return ["routing", "prediction", "planning", "control"]

    def _docker_modules_status_cmd(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("modules_status_cmd") or "").strip()
        if not raw:
            raw = (
                "pgrep -af "
                "'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|external_command/process_component/dag/external_command_process.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' "
                "|| true"
            )
        return " ".join(raw.split())

    @staticmethod
    def _module_status_stdout_lines(raw: str) -> list[str]:
        return [line.strip() for line in str(raw or "").splitlines() if line.strip()]

    @staticmethod
    def _is_runtime_module_status_line(line: str) -> bool:
        stripped = str(line or "").strip()
        if not stripped:
            return False
        parts = stripped.split(None, 1)
        command = parts[1] if len(parts) == 2 else parts[0]
        lowered = command.lower()
        launcher_prefixes = (
            "bash -lc ",
            "/bin/bash -lc ",
            "sh -lc ",
            "/bin/sh -lc ",
        )
        if any(lowered.startswith(prefix) for prefix in launcher_prefixes):
            return False
        launcher_markers = (
            "docker exec",
            "pgrep -af",
            "grep -e",
            "grep -f",
            "xargs -r",
        )
        if any(marker in lowered for marker in launcher_markers):
            return False
        return True

    def _runtime_module_status_lines(self, raw: str) -> list[str]:
        return [
            line
            for line in self._module_status_stdout_lines(raw)
            if self._is_runtime_module_status_line(line)
        ]

    @staticmethod
    def _safe_run_tag(text: str) -> str:
        return "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in text)

    def _docker_stage_root(self) -> str:
        cfg = self._docker_cfg()
        default = f"/tmp/carla_testbed_apollo_bridge/{self._safe_run_tag(self._run_dir().name)}"
        return str(cfg.get("stage_dir") or default)

    def _docker_exec(
        self,
        shell_cmd: str,
        *,
        user: Optional[str] = None,
        check: bool = True,
        capture_output: bool = False,
        text: bool = True,
        timeout: Optional[float] = None,
    ) -> subprocess.CompletedProcess:
        container = self._docker_container_name or self._docker_container()
        cmd = ["docker", "exec", "-i"]
        if user:
            cmd += ["-u", user]
        cmd += [container, "bash", "-lc", shell_cmd]
        return subprocess.run(
            cmd,
            check=check,
            capture_output=capture_output,
            text=text,
            timeout=timeout,
        )

    def _docker_check_running(self, container: str) -> None:
        try:
            out = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", container], text=True
            ).strip()
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"failed to inspect Apollo container {container}: {exc}") from exc
        if out.lower() != "true" and self._docker_auto_start_container():
            print(f"[cyberrt] starting Apollo container: {container}")
            started = subprocess.run(["docker", "start", container], text=True, capture_output=True)
            if started.returncode != 0:
                detail = (started.stderr or started.stdout or "").strip()
                raise RuntimeError(f"failed to start Apollo container {container}: {detail}")
            out = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", container], text=True
            ).strip()
        if out.lower() != "true":
            raise RuntimeError(f"Apollo container is not running: {container}")

    def _docker_runtime_modes(self, container: str) -> tuple[str, str]:
        ipc_mode = ""
        net_mode = ""
        try:
            ipc_mode = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.HostConfig.IpcMode}}", container],
                text=True,
            ).strip()
            net_mode = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.HostConfig.NetworkMode}}", container],
                text=True,
            ).strip()
        except subprocess.CalledProcessError:
            pass
        return ipc_mode, net_mode

    def _docker_check_runtime_requirements(self, container: str, artifacts: Path) -> None:
        ipc_mode, net_mode = self._docker_runtime_modes(container)
        report = artifacts / "apollo_container_runtime_check.txt"
        report.write_text(f"container={container}\nipc_mode={ipc_mode}\nnetwork_mode={net_mode}\n")
        issues: list[str] = []
        # Host bridge <-> Apollo container CyberRT needs shared IPC/network domains.
        if not self._docker_bridge_in_container() and self._docker_require_ipc_host() and ipc_mode != "host":
            issues.append(f"ipc_mode={ipc_mode} (expected host)")
        if not self._docker_bridge_in_container() and self._docker_require_network_host() and net_mode != "host":
            issues.append(f"network_mode={net_mode} (expected host)")
        if issues:
            raise RuntimeError(
                "Apollo container runtime is incompatible with host bridge: "
                + ", ".join(issues)
                + ". Recreate container with --network host --ipc host."
            )

    def _docker_prepare_user(self, artifacts: Path) -> None:
        cmd = (
            "set -o pipefail; "
            "if [ -x /opt/apollo/aem/docker_start_user.sh ]; then "
            "/opt/apollo/aem/docker_start_user.sh; "
            "fi"
        )
        self._write_backend_startup_trace("docker_prepare_user_begin")
        out = self._docker_exec(cmd, capture_output=True, check=False)
        (artifacts / "apollo_prepare_user.log").write_text(
            f"cmd: {cmd}\nreturncode: {out.returncode}\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )
        self._write_backend_startup_trace(
            "docker_prepare_user_done",
            returncode=int(out.returncode),
            log_path=str(artifacts / "apollo_prepare_user.log"),
        )

    def _docker_ensure_runtime_deps(self, artifacts: Path) -> None:
        check_log = artifacts / "apollo_mainboard_runtime_check.log"
        module_user = self._docker_module_exec_user()
        check_cmd = f"{self._docker_modules_prefix()}; mainboard --help"
        check_timeout_sec = float(self._apollo_cfg().get("runtime_check_timeout_sec", 15.0) or 15.0)
        fallback_timeout_sec = float(self._apollo_cfg().get("runtime_check_fallback_timeout_sec", 5.0) or 5.0)

        def _stringify_timeout(data: object) -> str:
            if data is None:
                return ""
            if isinstance(data, bytes):
                return data.decode("utf-8", errors="replace")
            return str(data)

        fallback_ok = False
        runtime_check_dual_timeout = False
        log_sections: list[str] = [f"check_cmd: {check_cmd}", f"timeout_sec: {check_timeout_sec}"]
        try:
            first = self._docker_exec(
                check_cmd,
                capture_output=True,
                check=False,
                user=module_user,
                timeout=check_timeout_sec,
            )
            log_sections.extend(
                [
                    f"returncode: {first.returncode}",
                    f"--- stdout ---\n{first.stdout}",
                    f"--- stderr ---\n{first.stderr}",
                ]
            )
            if first.returncode == 0:
                check_log.write_text("\n".join(log_sections) + "\n")
                return
        except subprocess.TimeoutExpired as exc:
            log_sections.extend(
                [
                    "returncode: timeout",
                    f"--- stdout ---\n{_stringify_timeout(exc.stdout)}",
                    f"--- stderr ---\n{_stringify_timeout(exc.stderr)}",
                ]
            )
            fallback_cmd = (
                f"{self._docker_modules_prefix()}; "
                "command -v mainboard >/dev/null 2>&1 && "
                "ldd $(command -v mainboard) 2>/dev/null | grep -F 'not found' || true"
            )
            try:
                fallback = self._docker_exec(
                    fallback_cmd,
                    capture_output=True,
                    check=False,
                    user=module_user,
                    timeout=fallback_timeout_sec,
                )
                log_sections.extend(
                    [
                        "",
                        "=== fallback ldd probe ===",
                        f"fallback_cmd: {fallback_cmd}",
                        f"fallback_timeout_sec: {fallback_timeout_sec}",
                        f"fallback_returncode: {fallback.returncode}",
                        f"--- stdout ---\n{fallback.stdout}",
                        f"--- stderr ---\n{fallback.stderr}",
                    ]
                )
                fallback_ok = fallback.returncode == 0 and not (fallback.stdout or "").strip()
            except subprocess.TimeoutExpired as fallback_exc:
                log_sections.extend(
                    [
                        "",
                        "=== fallback ldd probe ===",
                        f"fallback_cmd: {fallback_cmd}",
                        f"fallback_timeout_sec: {fallback_timeout_sec}",
                        "fallback_returncode: timeout",
                        f"--- stdout ---\n{_stringify_timeout(fallback_exc.stdout)}",
                        f"--- stderr ---\n{_stringify_timeout(fallback_exc.stderr)}",
                    ]
                )
                runtime_check_dual_timeout = True

        check_log.write_text("\n".join(log_sections) + "\n")
        if runtime_check_dual_timeout:
            raise RuntimeError(
                "Apollo runtime check timed out and fallback dependency probe also timed out; "
                f"see {check_log}"
            )
        if fallback_ok:
            return
        if not self._docker_auto_install_runtime_deps():
            raise RuntimeError(
                "Apollo runtime libraries are missing in container and auto_install_runtime_deps=false; "
                f"see {check_log}"
            )

        install_cmd = (
            "set -o pipefail; "
            "mkdir -p /root/.apollo /home/ubuntu /home/ubuntu/.apollo; "
            "touch /home/ubuntu/.bashrc; "
            "DEBIAN_FRONTEND=noninteractive apt-get install -y bvar libgoogle-perftools4"
        )
        install = self._docker_exec(install_cmd, capture_output=True, check=False)
        (artifacts / "apollo_runtime_deps_install.log").write_text(
            f"cmd: {install_cmd}\nreturncode: {install.returncode}\n"
            f"--- stdout ---\n{install.stdout}\n--- stderr ---\n{install.stderr}\n"
        )
        if install.returncode != 0:
            raise RuntimeError(
                "failed to auto-install Apollo runtime deps (bvar/libgoogle-perftools4); "
                f"see {artifacts / 'apollo_runtime_deps_install.log'}"
            )

        second = self._docker_exec(check_cmd, capture_output=True, check=False, user=module_user)
        check_log.write_text(
            check_log.read_text()
            + "\n=== after auto install ===\n"
            + f"returncode: {second.returncode}\n"
            + f"--- stdout ---\n{second.stdout}\n"
            + f"--- stderr ---\n{second.stderr}\n"
        )
        if second.returncode != 0:
            raise RuntimeError(
                "Apollo runtime check still failed after auto-install; "
                f"see {check_log}"
            )

    def _docker_start_modules(self, artifacts: Path) -> None:
        if not self._docker_start_modules_enabled():
            return
        module_user = self._docker_module_exec_user()
        log_path = artifacts / "apollo_modules_start.log"
        status_path = artifacts / "apollo_modules_status.log"
        prefix = self._docker_modules_prefix()
        cmd = self._docker_start_modules_cmd()
        full_cmd = f"{prefix}; {cmd}"
        container = self._docker_container_name or self._docker_container()
        script_host = artifacts / "apollo_modules_start.sh"
        script_container = "/tmp/carla_testbed_apollo_modules_start.sh"
        try:
            timeout_sec = float(self._docker_cfg().get("module_start_timeout_sec", 90.0) or 90.0)
        except Exception:
            timeout_sec = 90.0
        timeout_sec = max(5.0, timeout_sec)
        script_host.write_text("#!/usr/bin/env bash\n" + full_cmd + "\n")
        self._write_backend_startup_trace(
            "docker_start_modules_begin",
            timeout_sec=timeout_sec,
            script_host=str(script_host),
            script_container=script_container,
        )
        subprocess.run(["docker", "cp", str(script_host), f"{container}:{script_container}"], check=True)
        self._write_backend_startup_trace("docker_start_modules_script_copied", container=container)
        try:
            out = subprocess.run(
                ["docker", "exec", "-i", "-u", module_user, container, "bash", script_container],
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else str(exc.stdout or "")
            stderr = exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else str(exc.stderr or "")
            log_path.write_text(
                f"prefix: {prefix}\ncmd: {cmd}\nfull_cmd: {full_cmd}\nscript_host: {script_host}\n"
                f"script_container: {script_container}\ntimeout_sec: {timeout_sec}\nreturncode: timeout\n"
                f"--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}\n"
            )
            self._write_backend_startup_trace(
                "docker_start_modules_timeout",
                timeout_sec=timeout_sec,
                log_path=str(log_path),
            )
            raise RuntimeError(
                "Apollo modules start timed out; "
                f"see {log_path}"
            ) from exc
        log_path.write_text(
            f"prefix: {prefix}\ncmd: {cmd}\nfull_cmd: {full_cmd}\nscript_host: {script_host}\nscript_container: {script_container}\n"
            f"timeout_sec: {timeout_sec}\n"
            f"returncode: {out.returncode}\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )
        self._write_backend_startup_trace(
            "docker_start_modules_done",
            returncode=int(out.returncode),
            log_path=str(log_path),
        )
        if out.returncode != 0:
            raise RuntimeError(f"Apollo modules start failed, see {log_path}")
        status_cmd = self._docker_modules_status_cmd()
        status = self._docker_exec(f"{prefix}; {status_cmd}", capture_output=True, check=False, user=module_user)
        raw_lines = self._module_status_stdout_lines(status.stdout)
        lines = self._runtime_module_status_lines(status.stdout)
        status_path.write_text(
            f"cmd: {status_cmd}\nreturncode: {status.returncode}\n"
            f"runtime_filtered_lines: {json.dumps(lines, ensure_ascii=False)}\n"
            f"ignored_launcher_lines: {json.dumps([line for line in raw_lines if line not in lines], ensure_ascii=False)}\n"
            f"--- stdout ---\n{status.stdout}\n--- stderr ---\n{status.stderr}\n"
        )
        self._write_backend_startup_trace(
            "docker_start_modules_status_checked",
            returncode=int(status.returncode),
            status_path=str(status_path),
        )
        if not raw_lines:
            raise RuntimeError(
                "Apollo modules start command finished but no target module process was found; "
                f"see {status_path}"
            )
        if not lines:
            raise RuntimeError(
                "Apollo modules start command only found launcher shell processes, not live module processes; "
                f"see {status_path}"
            )
        pattern_map = {
            "routing": "modules/routing/dag/routing.dag",
            "prediction": "modules/prediction/dag/prediction.dag",
            "planning": "modules/planning/planning_component/dag/planning.dag",
            "control": "modules/control/control_component/dag/control.dag",
        }
        required_modules = list(self._docker_required_modules())
        if self._docker_control_start_gate() != "none":
            required_modules = [name for name in required_modules if name != "control"]
        missing = []
        for name in required_modules:
            patt = pattern_map.get(name, name)
            if not any(patt in line for line in lines):
                missing.append(name)
        if missing:
            raise RuntimeError(
                "Apollo modules are missing after startup: "
                + ",".join(missing)
                + f" (see {status_path})"
            )

    def _docker_start_control_module(self, artifacts: Path) -> None:
        container = self._docker_container_name or self._docker_container()
        module_user = self._docker_module_exec_user()
        prefix = self._docker_modules_prefix()
        launch_file = "/apollo/modules/control/control_component/launch/control.launch"
        dag_file = "/apollo/modules/control/control_component/dag/control.dag"
        log_path = artifacts / "apollo_control_deferred_start.log"
        status_path = artifacts / "apollo_control_deferred_status.log"
        direct_log_artifact = artifacts / "apollo_control_deferred_mainboard.log"
        launch_log_artifact = artifacts / "apollo_control_deferred_launch.log"
        control_pattern = "modules/control/control_component/dag/control.dag"
        control_pgrep_pattern = "modules/control/control_component/dag/control[.]dag"
        deferred_stamp = int(time.time())
        deferred_log_dir = "/apollo_workspace/log"
        direct_log = f"{deferred_log_dir}/control.deferred.{deferred_stamp}.tb.log"
        launch_log = f"{deferred_log_dir}/control.deferred.{deferred_stamp}.launch.log"
        preferred_start_mode = self._docker_deferred_control_start_mode()
        disable_bvar_dump = self._docker_disable_deferred_control_bvar_dump()
        bvar_env_prefix = self._docker_deferred_control_bvar_env_prefix()
        self._write_backend_startup_trace(
            "deferred_control_start_begin",
            control_start_gate=self._docker_control_start_gate(),
            preferred_deferred_control_start_mode=preferred_start_mode,
            deferred_control_disable_bvar_dump=disable_bvar_dump,
            deferred_control_bvar_env_disabled=disable_bvar_dump,
            container=container,
        )
        if preferred_start_mode == "launch":
            start_body = (
                f"if [ -f {shlex.quote(launch_file)} ]; then "
                "selected_start_mode=launch; "
                f"setsid -f cyber_launch start {shlex.quote(launch_file)} "
                f">{shlex.quote(launch_log)} 2>&1; "
                "elif [ -f "
                f"{shlex.quote(dag_file)} ]; then "
                "selected_start_mode=dag_fallback; "
                f"setsid -f mainboard -d {shlex.quote('modules/control/control_component/dag/control.dag')} "
                f"-p control -s CYBER_DEFAULT "
                f">{shlex.quote(direct_log)} 2>&1; "
                "else echo missing_control_launch_or_dag; exit 2; fi; "
            )
        else:
            start_body = (
                f"if [ -f {shlex.quote(dag_file)} ]; then "
                "selected_start_mode=dag; "
                f"setsid -f mainboard -d {shlex.quote('modules/control/control_component/dag/control.dag')} "
                f"-p control -s CYBER_DEFAULT "
                f">{shlex.quote(direct_log)} 2>&1; "
                "elif [ -f "
                f"{shlex.quote(launch_file)} ]; then "
                "selected_start_mode=launch_fallback; "
                f"setsid -f cyber_launch start {shlex.quote(launch_file)} "
                f">{shlex.quote(launch_log)} 2>&1; "
                "else echo missing_control_launch_or_dag; exit 2; fi; "
            )
        cmd = (
            f"{prefix}; "
            f"{bvar_env_prefix}"
            f"mkdir -p {shlex.quote(deferred_log_dir)} /apollo_workspace/dumps >/dev/null 2>&1 || true; "
            "rm -f /apollo_workspace/dumps/control.data >/dev/null 2>&1 || true; "
            "chown -R ubuntu:ubuntu /apollo_workspace/dumps >/dev/null 2>&1 || true; "
            "chmod 0777 /apollo_workspace/dumps >/dev/null 2>&1 || true; "
            f"pgrep -f '{control_pgrep_pattern}' | grep -vw $$ | "
            "xargs -r kill -9 >/dev/null 2>&1 || true; "
            "selected_start_mode=missing; "
            + start_body
            + "echo selected_start_mode=${selected_start_mode}; "
            "sleep 2; "
            f"pgrep -af '{control_pgrep_pattern}' || true"
        )
        out = subprocess.run(
            ["docker", "exec", "-i", "-u", module_user, container, "bash", "-lc", cmd],
            check=False,
            capture_output=True,
            text=True,
        )
        status_cmd = self._docker_modules_status_cmd()
        last_status = None
        last_lines: list[str] = []
        last_raw_lines: list[str] = []
        last_check_error = ""
        for attempt in range(10):
            time.sleep(0.5 if attempt > 0 else 0.0)
            try:
                last_status = self._docker_exec(
                    f"{prefix}; {status_cmd}",
                    capture_output=True,
                    check=False,
                    user=module_user,
                )
                last_raw_lines = self._module_status_stdout_lines(last_status.stdout)
                last_lines = self._runtime_module_status_lines(last_status.stdout)
                if any(control_pattern in line for line in last_lines):
                    break
            except Exception as exc:
                last_check_error = str(exc)
        status_parts = [f"cmd: {status_cmd}"]
        if last_status is not None:
            status_parts.append(f"returncode: {last_status.returncode}")
            status_parts.append(
                f"runtime_filtered_lines: {json.dumps(last_lines, ensure_ascii=False)}"
            )
            status_parts.append(
                "ignored_launcher_lines: "
                + json.dumps(
                    [line for line in last_raw_lines if line not in last_lines],
                    ensure_ascii=False,
                )
            )
            status_parts.append(f"--- stdout ---\n{last_status.stdout}")
            status_parts.append(f"--- stderr ---\n{last_status.stderr}")
        elif last_check_error:
            status_parts.append(f"status_check_error: {last_check_error}")
        status_path.write_text("\n".join(status_parts) + "\n")

        control_running = any(control_pattern in line for line in last_lines)
        actual_start_mode = preferred_start_mode
        for line in (out.stdout or "").splitlines():
            line = line.strip()
            if line.startswith("selected_start_mode="):
                actual_start_mode = line.split("=", 1)[1].strip() or preferred_start_mode
                break
        log_path.write_text(
            f"prefix: {prefix}\ncmd: {cmd}\nreturncode: {out.returncode}\n"
            f"preferred_start_mode: {preferred_start_mode}\n"
            f"actual_start_mode: {actual_start_mode}\n"
            f"deferred_control_disable_bvar_dump: {disable_bvar_dump}\n"
            f"deferred_control_bvar_env_prefix: {bvar_env_prefix}\n"
            f"deferred_log_dir: {deferred_log_dir}\n"
            f"direct_log: {direct_log}\n"
            f"launch_log: {launch_log}\n"
            f"control_running_after_probe: {control_running}\n"
            f"--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )
        def _dump_runtime_log(source: str, target: Path) -> None:
            for read_user in (module_user, None):
                try:
                    log_dump = self._docker_exec(
                        f"if [ -f {shlex.quote(source)} ]; then cat {shlex.quote(source)}; fi",
                        capture_output=True,
                        check=False,
                        user=read_user,
                    )
                except Exception:
                    continue
                if log_dump.stdout:
                    target.write_text(log_dump.stdout)
                    return

        for source, target in (
            (direct_log, direct_log_artifact),
            (launch_log, launch_log_artifact),
        ):
            _dump_runtime_log(source, target)

        if control_running:
            survival = self._docker_probe_deferred_control_survival(
                artifacts,
                initial_control_running=True,
            )
            for source, target in (
                (direct_log, direct_log_artifact),
                (launch_log, launch_log_artifact),
            ):
                _dump_runtime_log(source, target)
            self._write_backend_startup_trace(
                "deferred_control_start_done",
                docker_exec_returncode=int(out.returncode),
                control_running_after_probe=True,
                preferred_deferred_control_start_mode=preferred_start_mode,
                actual_deferred_control_start_mode=actual_start_mode,
                deferred_control_disable_bvar_dump=disable_bvar_dump,
                deferred_control_bvar_env_disabled=disable_bvar_dump,
                status_path=str(status_path),
                start_log_path=str(log_path),
                survival_path=str(artifacts / "apollo_control_deferred_survival.json"),
                control_survived_5s=survival.get("control_survived_5s"),
                control_survived_10s=survival.get("control_survived_10s"),
                control_present_after_first_nonzero_planning=survival.get(
                    "control_present_after_first_nonzero_planning"
                ),
            )
            return
        if out.returncode != 0:
            for source, target in (
                (direct_log, direct_log_artifact),
                (launch_log, launch_log_artifact),
            ):
                _dump_runtime_log(source, target)
            self._write_backend_startup_trace(
                "deferred_control_start_failed",
                docker_exec_returncode=int(out.returncode),
                control_running_after_probe=False,
                preferred_deferred_control_start_mode=preferred_start_mode,
                actual_deferred_control_start_mode=actual_start_mode,
                deferred_control_disable_bvar_dump=disable_bvar_dump,
                deferred_control_bvar_env_disabled=disable_bvar_dump,
                status_path=str(status_path),
                start_log_path=str(log_path),
            )
            raise RuntimeError(
                "Deferred Apollo control start returned non-zero and control process was not found; "
                f"see {log_path} and {status_path}"
            )
        self._write_backend_startup_trace(
            "deferred_control_start_failed",
            docker_exec_returncode=int(out.returncode),
            control_running_after_probe=False,
            preferred_deferred_control_start_mode=preferred_start_mode,
            actual_deferred_control_start_mode=actual_start_mode,
            deferred_control_disable_bvar_dump=disable_bvar_dump,
            deferred_control_bvar_env_disabled=disable_bvar_dump,
            status_path=str(status_path),
            start_log_path=str(log_path),
        )
        raise RuntimeError(
            "Deferred Apollo control start finished but control process was not found; "
            f"see {status_path}"
        )

    def _start_deferred_control_module(self, artifacts: Path) -> None:
        if self._deferred_control_started or self._deferred_control_start_in_progress:
            return
        if not self._docker_deferred_control_start_async():
            self._docker_start_control_module(artifacts)
            self._deferred_control_started = True
            self._deferred_control_pending = False
            return

        self._deferred_control_start_in_progress = True
        self._deferred_control_start_async_failure = None
        self._write_backend_startup_trace(
            "deferred_control_start_async_scheduled",
            control_start_gate=self._docker_control_start_gate(),
            preferred_deferred_control_start_mode=self._docker_deferred_control_start_mode(),
        )

        def _run() -> None:
            try:
                self._docker_start_control_module(artifacts)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                self._deferred_control_failure = error
                self._deferred_control_start_async_failure = error
                self._deferred_control_pending = False
                self._write_backend_startup_trace(
                    "deferred_control_start_async_failed",
                    error=error,
                    error_type=type(exc).__name__,
                )
                print(f"[cyberrt][warn] deferred control async start failed: {error}")
            else:
                self._deferred_control_started = True
                self._deferred_control_pending = False
                self._write_backend_startup_trace(
                    "deferred_control_start_async_done",
                    control_start_gate=self._docker_control_start_gate(),
                    preferred_deferred_control_start_mode=self._docker_deferred_control_start_mode(),
                )
            finally:
                self._deferred_control_start_in_progress = False

        thread = threading.Thread(
            target=_run,
            name="apollo-deferred-control-start",
            daemon=True,
        )
        self._deferred_control_start_thread = thread
        thread.start()

    def _docker_probe_deferred_control_survival(
        self,
        artifacts: Path,
        *,
        initial_control_running: bool,
    ) -> Dict[str, Any]:
        prefix = self._docker_modules_prefix()
        status_cmd = self._docker_modules_status_cmd()
        module_user = self._docker_module_exec_user()
        control_pattern = "modules/control/control_component/dag/control.dag"
        survival_path = artifacts / "apollo_control_deferred_survival.json"
        samples: list[Dict[str, Any]] = []
        start_sec = time.time()
        first_nonzero_planning_seen_at: Optional[float] = None
        control_present_after_first_nonzero_planning: Optional[bool] = None
        control_survived_5s = bool(initial_control_running)
        control_survived_10s = bool(initial_control_running)
        for target_offset_sec in (0.0, 5.0, 10.0):
            target_ts = start_sec + float(target_offset_sec)
            sleep_sec = target_ts - time.time()
            if sleep_sec > 0.0:
                time.sleep(sleep_sec)
            sample_ts = time.time()
            status_returncode: Optional[int] = None
            runtime_filtered_lines: list[str] = []
            status_error = ""
            control_present = False
            try:
                status = self._docker_exec(
                    f"{prefix}; {status_cmd}",
                    capture_output=True,
                    check=False,
                    user=module_user,
                )
                status_returncode = int(status.returncode)
                runtime_filtered_lines = self._runtime_module_status_lines(status.stdout)
                control_present = any(control_pattern in line for line in runtime_filtered_lines)
            except Exception as exc:
                status_error = str(exc)
            stats_exists, stats_age_sec, stats = self._read_stats()
            planning_nonempty_count = 0
            planning_messages_received = 0
            routing_success_count = 0
            stats_parse_error = ""
            if isinstance(stats, dict):
                planning_counters = self._bridge_planning_runtime_counters(stats)
                planning_nonempty_count = int(planning_counters.get("planning_nonempty_trajectory_count", 0) or 0)
                planning_messages_received = int(planning_counters.get("planning_messages_received", 0) or 0)
                routing_success_count = int(stats.get("routing_success_count", 0) or 0)
                stats_parse_error = str(stats.get("_parse_error") or "")
            if planning_nonempty_count > 0 and first_nonzero_planning_seen_at is None:
                first_nonzero_planning_seen_at = sample_ts
            if planning_nonempty_count > 0 and control_present_after_first_nonzero_planning is None:
                control_present_after_first_nonzero_planning = control_present
            if math.isclose(target_offset_sec, 5.0, rel_tol=0.0, abs_tol=1e-6):
                control_survived_5s = control_present
            if math.isclose(target_offset_sec, 10.0, rel_tol=0.0, abs_tol=1e-6):
                control_survived_10s = control_present
            samples.append(
                {
                    "ts_sec": sample_ts,
                    "elapsed_sec": float(sample_ts - start_sec),
                    "target_offset_sec": float(target_offset_sec),
                    "control_present": control_present,
                    "status_returncode": status_returncode,
                    "status_error": status_error or None,
                    "runtime_filtered_lines": runtime_filtered_lines,
                    "stats_exists": bool(stats_exists),
                    "stats_age_sec": stats_age_sec,
                    "planning_nonempty_trajectory_count": planning_nonempty_count,
                    "planning_messages_received": planning_messages_received,
                    "routing_success_count": routing_success_count,
                    "stats_parse_error": stats_parse_error or None,
                }
            )
        payload: Dict[str, Any] = {
            "started_at_sec": start_sec,
            "probe_completed_at_sec": time.time(),
            "probe_window_sec": 10.0,
            "poll_offsets_sec": [0.0, 5.0, 10.0],
            "control_started_pid_seen": bool(initial_control_running),
            "control_survived_5s": bool(control_survived_5s),
            "control_survived_10s": bool(control_survived_10s),
            "first_nonzero_planning_seen_at": first_nonzero_planning_seen_at,
            "control_present_after_first_nonzero_planning": control_present_after_first_nonzero_planning,
            "control_present_at_end": bool(samples[-1]["control_present"]) if samples else False,
            "samples": samples,
        }
        try:
            survival_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        except Exception:
            pass
        return payload

    def _docker_probe_planning_ready_before_control(self, artifacts: Path) -> tuple[str, Optional[str]]:
        def _has_positive_numeric(value: Any) -> bool:
            try:
                return float(value) > 0.0
            except Exception:
                return False

        def _has_present_value(value: Any) -> bool:
            if value is None:
                return False
            if isinstance(value, str):
                return value.strip().lower() not in {"", "none", "null", "nan"}
            return True

        control_start_gate = self._docker_control_start_gate()
        timeout_sec = self._docker_control_planning_ready_timeout_sec()
        poll_sec = self._docker_control_planning_ready_poll_sec()
        min_nonempty = self._docker_control_planning_ready_min_nonempty_count()
        min_sequence_num = self._docker_control_planning_ready_min_sequence_num()
        require_routing_success = self._docker_control_planning_ready_require_routing_success()
        require_chassis_ready = self._docker_control_require_chassis_ready()
        min_chassis_count = self._docker_control_chassis_ready_min_count()
        if control_start_gate == "route_seen":
            report_path = artifacts / "apollo_control_route_seen_wait.log"
        elif control_start_gate == "route_established":
            report_path = artifacts / "apollo_control_route_established_wait.log"
        else:
            report_path = artifacts / "apollo_control_planning_ready_wait.log"
        now = time.time()
        if self._deferred_control_overall_start_sec is None:
            self._deferred_control_overall_start_sec = now
        hard_timeout_sec = max(timeout_sec * 3.0, 120.0)
        planning_summary_path = artifacts / "planning_topic_debug_summary.json"
        planning_debug_path = artifacts / "planning_topic_debug.jsonl"
        bridge_health_path = artifacts / "bridge_health_summary.json"
        routing_event_debug_path = artifacts / "routing_event_debug.jsonl"
        exists, age, stats = self._read_stats()
        count = 0
        total = 0
        last_sequence_num = None
        last_trajectory_point_count = None
        routing_request_count = 0
        routing_success_count = 0
        chassis_count = 0
        parse_error = None
        if isinstance(stats, dict):
            parse_error = stats.get("_parse_error")
            planning_counters = self._bridge_planning_runtime_counters(stats)
            count = int(planning_counters.get("planning_nonempty_trajectory_count", 0) or 0)
            total = int(planning_counters.get("planning_messages_received", 0) or 0)
            routing_request_count = int(stats.get("routing_request_count", 0) or 0)
            routing_success_count = int(stats.get("routing_success_count", 0) or 0)
            chassis_count = int(stats.get("chassis_count", 0) or stats.get("chassis_msg_count", 0) or 0)
            last_sequence_num = planning_counters.get("last_planning_header_sequence_num")
            last_trajectory_point_count = planning_counters.get("last_trajectory_point_count")
        planning_summary_exists = planning_summary_path.exists()
        planning_debug_exists = planning_debug_path.exists()
        bridge_health_exists = bridge_health_path.exists()
        routing_event_debug_exists = routing_event_debug_path.exists()
        if planning_summary_exists:
            try:
                planning_summary = json.loads(planning_summary_path.read_text())
                count = max(
                    count,
                    int(planning_summary.get("messages_with_nonzero_trajectory_points", 0) or 0),
                )
                total = max(
                    total,
                    int(planning_summary.get("total_messages_received", 0) or 0),
                )
                summary_seq = planning_summary.get("last_planning_header_sequence_num")
                if summary_seq is not None:
                    last_sequence_num = int(summary_seq)
                summary_last_points = planning_summary.get("last_trajectory_point_count")
                if summary_last_points is not None:
                    last_trajectory_point_count = int(summary_last_points)
                if _has_present_value(planning_summary.get("routing_first_success_response_ts_sec")):
                    routing_success_count = max(routing_success_count, 1)
            except Exception as exc:
                parse_error = parse_error or f"planning_summary_parse_error:{exc}"
        if planning_debug_exists and last_sequence_num is None:
            try:
                last_line = ""
                with planning_debug_path.open("rb") as fh:
                    fh.seek(0, os.SEEK_END)
                    pos = fh.tell()
                    while pos > 0:
                        pos -= 1
                        fh.seek(pos)
                        if fh.read(1) == b"\n" and pos != fh.tell() - 1:
                            break
                    last_line = fh.readline().decode("utf-8", errors="ignore").strip()
                if last_line:
                    planning_row = json.loads(last_line)
                    seq = planning_row.get("planning_header_sequence_num")
                    if seq is not None:
                        last_sequence_num = int(seq)
                    traj_points = planning_row.get("trajectory_point_count")
                    if traj_points is not None:
                        last_trajectory_point_count = int(traj_points)
            except Exception as exc:
                parse_error = parse_error or f"planning_debug_parse_error:{exc}"
        if bridge_health_exists:
            try:
                bridge_health = json.loads(bridge_health_path.read_text())
                count = max(
                    count,
                    int(bridge_health.get("planning_nonempty_trajectory_count", 0) or 0),
                )
                routing_request_count = max(
                    routing_request_count,
                    int(bridge_health.get("routing_request_count", 0) or 0),
                )
                routing_success_count = max(
                    routing_success_count,
                    int(bridge_health.get("routing_success_count", 0) or 0),
                )
                if _has_present_value(bridge_health.get("routing_first_success_response_ts_sec")):
                    routing_success_count = max(routing_success_count, 1)
                if _has_positive_numeric(bridge_health.get("routing_response_count")):
                    routing_success_count = max(routing_success_count, 1)
                chassis_count = max(
                    chassis_count,
                    int(bridge_health.get("chassis_count", 0) or bridge_health.get("chassis_msg_count", 0) or 0),
                )
            except Exception as exc:
                parse_error = parse_error or f"bridge_health_parse_error:{exc}"
        if routing_event_debug_exists:
            try:
                with routing_event_debug_path.open("rb") as fh:
                    routing_request_count = max(
                        routing_request_count,
                        sum(1 for _ in fh),
                    )
            except Exception as exc:
                parse_error = parse_error or f"routing_event_debug_parse_error:{exc}"
        if routing_request_count > 0 and self._deferred_control_route_seen_sec is None:
            self._deferred_control_route_seen_sec = now
        if routing_success_count > 0 and self._deferred_control_route_established_sec is None:
            self._deferred_control_route_established_sec = now
        self._deferred_control_snapshots.append(
            json.dumps(
                {
                    "ts": now,
                    "stats_exists": exists,
                    "stats_age_sec": age,
                    "planning_summary_exists": planning_summary_exists,
                    "planning_debug_exists": planning_debug_exists,
                    "bridge_health_exists": bridge_health_exists,
                    "routing_event_debug_exists": routing_event_debug_exists,
                    "routing_request_count": routing_request_count,
                    "routing_success_count": routing_success_count,
                    "chassis_count": chassis_count,
                    "route_seen_at": self._deferred_control_route_seen_sec,
                    "route_established_at": self._deferred_control_route_established_sec,
                    "require_routing_success": require_routing_success,
                    "planning_messages_received": total,
                    "planning_nonempty_trajectory_count": count,
                    "last_planning_header_sequence_num": last_sequence_num,
                    "last_trajectory_point_count": last_trajectory_point_count,
                    "require_chassis_ready": require_chassis_ready,
                    "control_chassis_ready_min_count": min_chassis_count,
                    "parse_error": parse_error,
                },
                ensure_ascii=False,
            )
        )
        route_seen = self._deferred_control_route_seen_sec is not None
        route_established = self._deferred_control_route_established_sec is not None
        route_ready_for_planning_control = route_established if require_routing_success else route_seen
        latest_nonzero = (last_trajectory_point_count or 0) > 0
        enough_nonempty_history = count >= min_nonempty
        sequence_ready = (
            min_sequence_num <= 0
            or (last_sequence_num is not None and last_sequence_num >= min_sequence_num)
        )
        chassis_ready = (not require_chassis_ready) or chassis_count >= min_chassis_count
        if control_start_gate == "route_seen":
            if route_seen:
                report_path.write_text(
                    "status=ready\n"
                    "control_start_gate=route_seen\n"
                    f"timeout_sec={timeout_sec}\n"
                    f"hard_timeout_sec={hard_timeout_sec}\n"
                    f"poll_sec={poll_sec}\n"
                    + "\n".join(self._deferred_control_snapshots)
                    + "\n"
                )
                return "ready", None
            if now >= self._deferred_control_overall_start_sec + hard_timeout_sec:
                report_path.write_text(
                    "status=timeout_before_route_ready\n"
                    "control_start_gate=route_seen\n"
                    f"timeout_sec={timeout_sec}\n"
                    f"hard_timeout_sec={hard_timeout_sec}\n"
                    f"poll_sec={poll_sec}\n"
                    + "\n".join(self._deferred_control_snapshots)
                    + "\n"
                )
                return "error", (
                    "Route-seen-gated Apollo control start timed out before routing request became ready; "
                    f"see {report_path}"
                )
            report_path.write_text(
                "status=waiting\n"
                "control_start_gate=route_seen\n"
                f"timeout_sec={timeout_sec}\n"
                f"hard_timeout_sec={hard_timeout_sec}\n"
                f"poll_sec={poll_sec}\n"
                + "\n".join(self._deferred_control_snapshots[-20:])
                + "\n"
            )
            return "waiting", None
        if control_start_gate == "route_established":
            if route_established:
                report_path.write_text(
                    "status=ready\n"
                    "control_start_gate=route_established\n"
                    f"timeout_sec={timeout_sec}\n"
                    f"hard_timeout_sec={hard_timeout_sec}\n"
                    f"poll_sec={poll_sec}\n"
                    + "\n".join(self._deferred_control_snapshots)
                    + "\n"
                )
                return "ready", None
            if now >= self._deferred_control_overall_start_sec + hard_timeout_sec:
                report_path.write_text(
                    "status=timeout_before_route_established\n"
                    "control_start_gate=route_established\n"
                    f"timeout_sec={timeout_sec}\n"
                    f"hard_timeout_sec={hard_timeout_sec}\n"
                    f"poll_sec={poll_sec}\n"
                    + "\n".join(self._deferred_control_snapshots)
                    + "\n"
                )
                return "error", (
                    "Route-established-gated Apollo control start timed out before routing success became ready; "
                    f"see {report_path}"
                )
            report_path.write_text(
                "status=waiting\n"
                "control_start_gate=route_established\n"
                f"timeout_sec={timeout_sec}\n"
                f"hard_timeout_sec={hard_timeout_sec}\n"
                f"poll_sec={poll_sec}\n"
                + "\n".join(self._deferred_control_snapshots[-20:])
                + "\n"
            )
            return "waiting", None
        if route_ready_for_planning_control and latest_nonzero and enough_nonempty_history and sequence_ready:
            if require_chassis_ready and not chassis_ready:
                report_path.write_text(
                    "status=waiting_for_chassis_ready\n"
                    "control_start_gate=planning_ready\n"
                    f"min_nonempty={min_nonempty}\n"
                    f"min_sequence_num={min_sequence_num}\n"
                    f"require_routing_success={require_routing_success}\n"
                    f"route_established={route_established}\n"
                    f"require_chassis_ready={require_chassis_ready}\n"
                    f"control_chassis_ready_min_count={min_chassis_count}\n"
                    f"current_chassis_count={chassis_count}\n"
                    f"timeout_sec={timeout_sec}\n"
                    f"hard_timeout_sec={hard_timeout_sec}\n"
                    f"poll_sec={poll_sec}\n"
                    + "\n".join(self._deferred_control_snapshots[-20:])
                    + "\n"
                )
                return "waiting", None
            report_path.write_text(
                "status=ready\n"
                "control_start_gate=planning_ready\n"
                f"min_nonempty={min_nonempty}\n"
                f"min_sequence_num={min_sequence_num}\n"
                f"require_routing_success={require_routing_success}\n"
                f"route_established={route_established}\n"
                f"require_chassis_ready={require_chassis_ready}\n"
                f"control_chassis_ready_min_count={min_chassis_count}\n"
                f"current_chassis_count={chassis_count}\n"
                f"timeout_sec={timeout_sec}\n"
                f"poll_sec={poll_sec}\n"
                f"route_seen={route_seen}\n"
                f"latest_nonzero={latest_nonzero}\n"
                f"chassis_ready={chassis_ready}\n"
                + "\n".join(self._deferred_control_snapshots)
                + "\n"
            )
            return "ready", None
        if self._deferred_control_route_seen_sec is not None and now >= self._deferred_control_route_seen_sec + timeout_sec:
            report_path.write_text(
                "status=timeout_after_route_ready\n"
                "control_start_gate=planning_ready\n"
                f"min_nonempty={min_nonempty}\n"
                f"min_sequence_num={min_sequence_num}\n"
                f"require_routing_success={require_routing_success}\n"
                f"route_established={route_established}\n"
                f"require_chassis_ready={require_chassis_ready}\n"
                f"control_chassis_ready_min_count={min_chassis_count}\n"
                f"current_chassis_count={chassis_count}\n"
                f"timeout_sec={timeout_sec}\n"
                f"hard_timeout_sec={hard_timeout_sec}\n"
                f"poll_sec={poll_sec}\n"
                + "\n".join(self._deferred_control_snapshots)
                + "\n"
            )
            return "error", (
                "Deferred Apollo control start timed out waiting for planning readiness "
                f"after routing request was sent; see {report_path}"
            )
        if now >= self._deferred_control_overall_start_sec + hard_timeout_sec:
            report_path.write_text(
                "status=timeout_before_route_ready\n"
                "control_start_gate=planning_ready\n"
                f"min_nonempty={min_nonempty}\n"
                f"min_sequence_num={min_sequence_num}\n"
                f"require_routing_success={require_routing_success}\n"
                f"route_established={route_established}\n"
                f"timeout_sec={timeout_sec}\n"
                f"hard_timeout_sec={hard_timeout_sec}\n"
                f"poll_sec={poll_sec}\n"
                + "\n".join(self._deferred_control_snapshots)
                + "\n"
            )
            return "error", (
                "Deferred Apollo control start timed out before routing request became ready; "
                f"see {report_path}"
            )
        report_path.write_text(
            "status=waiting\n"
            "control_start_gate=planning_ready\n"
            f"min_nonempty={min_nonempty}\n"
            f"min_sequence_num={min_sequence_num}\n"
            f"require_routing_success={require_routing_success}\n"
            f"route_established={route_established}\n"
            f"require_chassis_ready={require_chassis_ready}\n"
            f"control_chassis_ready_min_count={min_chassis_count}\n"
            f"current_chassis_count={chassis_count}\n"
            f"timeout_sec={timeout_sec}\n"
            f"hard_timeout_sec={hard_timeout_sec}\n"
            f"poll_sec={poll_sec}\n"
            + "\n".join(self._deferred_control_snapshots[-20:])
            + "\n"
        )
        return "waiting", None

    def _docker_modules_prefix(self) -> str:
        apollo_root = self._docker_apollo_root()
        apollo_dist = self._docker_apollo_dist()
        env_script = self._docker_env_script()
        parts = ["set -o pipefail"]
        parts.append(f"export APOLLO_ROOT={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(apollo_dist)}")
        parts.append('if [ -d /apollo/cyber ]; then export CYBER_PATH=/apollo/cyber; fi')
        parts.append(
            f'if [ -f {shlex.quote(f"{apollo_dist}/setup.sh")} ]; then source {shlex.quote(f"{apollo_dist}/setup.sh")}; fi'
        )
        parts.append(
            f'if [ -f {shlex.quote(env_script)} ]; then source {shlex.quote(env_script)}; fi'
        )
        parts.append(
            'if [ -n "${APOLLO_CONF_PATH:-}" ]; then '
            'export APOLLO_CONF_PATH="/apollo_workspace/conf_overlay:/apollo:/opt/apollo/neo/share:/opt/apollo/neo/src:${APOLLO_CONF_PATH}"; '
            "else export APOLLO_CONF_PATH=/apollo_workspace/conf_overlay:/apollo:/opt/apollo/neo/share:/opt/apollo/neo/src; fi"
        )
        parts.append(f"export APOLLO_PLUGIN_INDEX_PATH={shlex.quote(f'{apollo_dist}/share/cyber_plugin_index')}")
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(f"{apollo_dist}/lib"),
                    shlex.quote(f"{apollo_dist}/lib64"),
                    shlex.quote(f"{apollo_dist}/lib/third_party/rtklib"),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        # Apollo runtime dependencies are packaged under /opt/apollo/neo/packages/*/lib.
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        parts.append(
            'while IFS= read -r _d; do export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            f"done < <(find {shlex.quote(f'{apollo_dist}/lib')} -type d)"
        )
        parts.append("cd /apollo")
        return "; ".join(parts)

    def _docker_prepare_stage(self, bridge_cfg: Path, bridge_py: Path) -> tuple[str, str, str, str]:
        container = self._docker_container()
        self._docker_check_running(container)
        stage_dir = self._docker_stage_root()
        pb_root = self._pb_root()
        if not pb_root.exists():
            raise RuntimeError(f"pb root not found for docker staging: {pb_root}")

        stage_bridge_dir = f"{stage_dir}/tools/apollo10_cyber_bridge"
        stage_pb_dir = f"{stage_dir}/pb"
        stage_cfg = f"{stage_dir}/apollo_bridge_effective.yaml"
        stage_stats = f"{stage_dir}/cyber_bridge_stats.json"
        self._docker_exec(f"mkdir -p {shlex.quote(stage_bridge_dir)} {shlex.quote(stage_pb_dir)}")
        bridge_dir = bridge_py.parent
        subprocess.check_call(["docker", "cp", f"{bridge_dir}/.", f"{container}:{stage_bridge_dir}/"])
        bridge_payload = yaml.safe_load(bridge_cfg.read_text()) or {}
        ctrl_map = ((bridge_payload.get("bridge", {}) or {}).get("control_mapping", {}) or {})
        physical_cfg = (ctrl_map.get("physical", {}) or {})
        calibration_file = str(physical_cfg.get("calibration_file", "") or "").strip()
        if calibration_file:
            cal_path = Path(calibration_file).expanduser()
            if not cal_path.is_absolute():
                cal_path = (self.repo_root / cal_path).resolve()
            if cal_path.exists():
                staged_cal = f"{stage_dir}/carla_actuator_calibration.json"
                subprocess.check_call(["docker", "cp", str(cal_path), f"{container}:{staged_cal}"])
                physical_cfg["calibration_file"] = staged_cal
                ctrl_map["physical"] = physical_cfg
                bridge_payload.setdefault("bridge", {})["control_mapping"] = ctrl_map
        staged_cfg_host = bridge_cfg.parent / "apollo_bridge_effective.docker_stage.yaml"
        staged_cfg_host.write_text(yaml.safe_dump(bridge_payload, sort_keys=False))
        subprocess.check_call(["docker", "cp", str(staged_cfg_host), f"{container}:{stage_cfg}"])
        subprocess.check_call(
            ["docker", "cp", f"{pb_root}/.", f"{container}:{stage_pb_dir}/"]
        )
        self._docker_container_name = container
        self._docker_stage_dir = stage_dir
        self._docker_stats_path = stage_stats
        return stage_bridge_dir, stage_cfg, stage_pb_dir, stage_stats

    def _docker_sync_host_libs(self, artifacts: Path) -> Optional[Path]:
        if not self._docker_container_name:
            return None
        libs_dir = self._docker_host_lib_cache_dir()
        libs_dir.mkdir(parents=True, exist_ok=True)
        list_cmd = (
            "python3 - <<'PY'\n"
            "import glob\n"
            "import json\n"
            "import os\n"
            "\n"
            "patterns = [\n"
            "    '/usr/local/lib/libbvar.so*',\n"
            "    '/usr/local/lib/libbrpc.so*',\n"
            "    '/usr/local/lib/libbutil.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libnvinfer.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libnvinfer_plugin.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libnvparsers.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libnvonnxparser.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libcudnn.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libdmumps_seq-5.4.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libmumps_common_seq-5.4.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libmpiseq_seq-5.4.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libfdk-aac.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcublas.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcublasLt.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcufft.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcurand.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcusolver.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcusparse.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libnpp*.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libnvToolsExt.so*',\n"
            "    '/usr/local/cuda/targets/x86_64-linux/lib/libcudart.so*',\n"
            "    '/usr/local/cuda/lib64/libcudart.so*',\n"
            "    '/usr/lib/x86_64-linux-gnu/libcudart.so*',\n"
            "]\n"
            "entries = []\n"
            "seen = set()\n"
            "def add_entry(path, alias_name=None):\n"
            "    if not path:\n"
            "        return\n"
            "    if not (os.path.exists(path) or os.path.islink(path)):\n"
            "        return\n"
            "    resolved = os.path.realpath(path)\n"
            "    if not os.path.exists(resolved):\n"
            "        return\n"
            "    basename = alias_name or os.path.basename(path)\n"
            "    key = (basename, resolved)\n"
            "    if key in seen:\n"
            "        return\n"
            "    seen.add(key)\n"
            "    payload = {\n"
            "            'path': path,\n"
            "            'basename': basename,\n"
            "            'resolved_path': resolved,\n"
            "            'resolved_basename': os.path.basename(resolved),\n"
            "            'is_symlink': os.path.islink(path),\n"
            "        }\n"
            "    if os.path.islink(path):\n"
            "        try:\n"
            "            payload['symlink_target'] = os.readlink(path)\n"
            "        except OSError:\n"
            "            payload['symlink_target'] = None\n"
            "    entries.append(payload)\n"
            "for pattern in patterns:\n"
            "    for path in sorted(glob.glob(pattern)):\n"
            "        add_entry(path)\n"
            "print(json.dumps(entries, ensure_ascii=False))\n"
            "PY"
        )
        out = self._docker_exec(list_cmd, capture_output=True, check=False)
        try:
            entries = json.loads((out.stdout or "").strip() or "[]")
        except Exception:
            entries = []
        if not entries:
            return None
        cache_manifest_path = libs_dir / "apollo_docker_libs_manifest.cache.json"
        manifest = self._load_docker_host_lib_manifest(cache_manifest_path)
        manifest, used_paths, newly_copied_count = self._materialize_docker_host_lib_entries(
            entries,
            libs_dir,
            manifest,
            sync_mode="base_sync",
        )
        if not any(libs_dir.iterdir()):
            return None
        cache_manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        self._write_docker_host_lib_artifacts(
            artifacts=artifacts,
            libs_dir=libs_dir,
            manifest=manifest,
            used_paths=used_paths,
            newly_copied_count=newly_copied_count,
        )
        return libs_dir

    def _docker_sync_missing_host_libs(
        self,
        artifacts: Path,
        missing_libs: list[str],
    ) -> Optional[Path]:
        if not self._docker_container_name:
            return None
        names = sorted({str(item).strip() for item in (missing_libs or []) if str(item).strip()})
        if not names:
            return None
        libs_dir = self._docker_host_lib_cache_dir()
        libs_dir.mkdir(parents=True, exist_ok=True)
        names_json = json.dumps(names, ensure_ascii=False)
        list_cmd = (
            "python3 - <<'PY'\n"
            "import json\n"
            "import os\n"
            "\n"
            f"names = set({names_json})\n"
            "entries = []\n"
            "seen = set()\n"
            "def add_entry(path):\n"
            "    if not path:\n"
            "        return\n"
            "    if not (os.path.exists(path) or os.path.islink(path)):\n"
            "        return\n"
            "    resolved = os.path.realpath(path)\n"
            "    if not os.path.exists(resolved):\n"
            "        return\n"
            "    basename = os.path.basename(path)\n"
            "    key = (basename, resolved)\n"
            "    if key in seen:\n"
            "        return\n"
            "    seen.add(key)\n"
            "    payload = {\n"
            "        'path': path,\n"
            "        'basename': basename,\n"
            "        'resolved_path': resolved,\n"
            "        'resolved_basename': os.path.basename(resolved),\n"
            "        'is_symlink': os.path.islink(path),\n"
            "    }\n"
            "    if os.path.islink(path):\n"
            "        try:\n"
            "            payload['symlink_target'] = os.readlink(path)\n"
            "        except OSError:\n"
            "            payload['symlink_target'] = None\n"
            "    entries.append(payload)\n"
            "for root in ('/usr', '/usr/local', '/opt'):\n"
            "    if not os.path.isdir(root):\n"
            "        continue\n"
            "    for dirpath, _dirnames, filenames in os.walk(root):\n"
            "        for filename in filenames:\n"
            "            if filename not in names:\n"
            "                continue\n"
            "            add_entry(os.path.join(dirpath, filename))\n"
            "print(json.dumps(entries, ensure_ascii=False))\n"
            "PY"
        )
        out = self._docker_exec(list_cmd, capture_output=True, check=False)
        try:
            entries = json.loads((out.stdout or "").strip() or "[]")
        except Exception:
            entries = []
        if not entries:
            return None
        cache_manifest_path = libs_dir / "apollo_docker_libs_manifest.cache.json"
        manifest = self._load_docker_host_lib_manifest(cache_manifest_path)
        manifest, used_paths, newly_copied_count = self._materialize_docker_host_lib_entries(
            entries,
            libs_dir,
            manifest,
            sync_mode="missing_shared_lib_retry",
        )
        if not any(libs_dir.iterdir()):
            return None
        cache_manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        existing_used_paths: set[str] = set()
        copied_txt_path = artifacts / "apollo_docker_libs.txt"
        if copied_txt_path.exists():
            existing_used_paths = {
                line.strip()
                for line in copied_txt_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
        self._write_docker_host_lib_artifacts(
            artifacts=artifacts,
            libs_dir=libs_dir,
            manifest=manifest,
            used_paths=existing_used_paths | used_paths,
            newly_copied_count=newly_copied_count,
        )
        return libs_dir

    def _docker_host_lib_cache_dir(self) -> Path:
        raw_name = str(self._docker_container_name or "apollo_unknown").strip() or "apollo_unknown"
        safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw_name)
        return (self.repo_root / ".runtime_cache" / "apollo_docker_libs" / safe_name).resolve()

    @staticmethod
    def _load_docker_host_lib_manifest(path: Path) -> list[Dict[str, Any]]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        except Exception:
            payload = []
        if not isinstance(payload, list):
            return []
        return [item for item in payload if isinstance(item, dict)]

    def _materialize_docker_host_lib_entries(
        self,
        entries: list[Dict[str, Any]],
        libs_dir: Path,
        manifest: list[Dict[str, Any]],
        *,
        sync_mode: str,
    ) -> tuple[list[Dict[str, Any]], set[str], int]:
        manifest_keys = {
            (
                str(item.get("alias_name") or ""),
                str(item.get("resolved_path") or ""),
            )
            for item in manifest
            if isinstance(item, dict)
        }
        used_paths: set[str] = set()
        copied_real: Dict[str, Path] = {}
        newly_copied_count = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            path = str(entry.get("path") or "").strip()
            resolved_path = str(entry.get("resolved_path") or "").strip()
            alias_name = str(entry.get("basename") or "").strip()
            real_name = str(entry.get("resolved_basename") or "").strip()
            if not path or not resolved_path or not alias_name or not real_name:
                continue
            try:
                local_real = copied_real.get(resolved_path)
                if local_real is None:
                    local_real = libs_dir / real_name
                    if not local_real.exists():
                        subprocess.check_call(
                            ["docker", "cp", f"{self._docker_container_name}:{resolved_path}", str(libs_dir)]
                        )
                        newly_copied_count += 1
                    copied_real[resolved_path] = local_real
                used_paths.add(resolved_path)
                if alias_name != real_name:
                    alias_path = libs_dir / alias_name
                    if alias_path.exists() or alias_path.is_symlink():
                        alias_path.unlink()
                    alias_path.symlink_to(real_name)
                manifest_key = (alias_name, resolved_path)
                if manifest_key not in manifest_keys:
                    manifest.append(
                        {
                            "path": path,
                            "resolved_path": resolved_path,
                            "alias_name": alias_name,
                            "resolved_name": real_name,
                            "is_symlink": bool(entry.get("is_symlink")),
                            "symlink_target": entry.get("symlink_target"),
                            "sync_mode": sync_mode,
                        }
                    )
                    manifest_keys.add(manifest_key)
            except Exception:
                continue
        # Some CUDA installs only expose the real file inside the container.
        # Synthesize the expected soname alias locally so the host loader can resolve it.
        for real_path in libs_dir.glob("libcudart.so.*"):
            if real_path.is_symlink():
                continue
            parts = real_path.name.split(".")
            if len(parts) < 4:
                continue
            major = parts[2]
            soname = libs_dir / f"libcudart.so.{major}.0"
            manifest_key = (soname.name, str(real_path))
            if not soname.exists() and not soname.is_symlink():
                soname.symlink_to(real_path.name)
            if manifest_key not in manifest_keys and soname.exists():
                manifest.append(
                    {
                        "path": str(real_path),
                        "resolved_path": str(real_path),
                        "alias_name": soname.name,
                        "resolved_name": real_path.name,
                        "is_symlink": True,
                        "symlink_target": real_path.name,
                        "synthesized": True,
                        "sync_mode": sync_mode,
                    }
                )
                manifest_keys.add(manifest_key)
        return manifest, used_paths, newly_copied_count

    def _write_docker_host_lib_artifacts(
        self,
        *,
        artifacts: Path,
        libs_dir: Path,
        manifest: list[Dict[str, Any]],
        used_paths: set[str],
        newly_copied_count: int,
    ) -> None:
        copied_txt_path = artifacts / "apollo_docker_libs.txt"
        manifest_path = artifacts / "apollo_docker_libs_manifest.json"
        status_path = artifacts / "apollo_docker_libs_cache_status.json"
        used_rows = sorted(str(item).strip() for item in used_paths if str(item).strip())
        copied_txt_path.write_text(
            ("\n".join(used_rows) + "\n") if used_rows else "",
            encoding="utf-8",
        )
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        status_payload = {
            "cache_dir": str(libs_dir),
            "cache_reused": bool(used_rows) and newly_copied_count == 0,
            "newly_copied_count": int(newly_copied_count),
            "used_resolved_paths_count": len(used_rows),
            "manifest_entry_count": len(manifest),
        }
        status_path.write_text(
            json.dumps(status_payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        self._last_docker_host_lib_cache_status = status_payload

    def _bridge_runtime_preflight_path(self, artifacts: Path) -> Path:
        return artifacts / "bridge_runtime_preflight.json"

    def _bridge_runtime_preflight_command(self, python_exec: str) -> str:
        return (
            f"{python_exec} - <<'PY'\n"
            "import json\n"
            "import os\n"
            "import subprocess\n"
            "import sys\n"
            "import traceback\n"
            "from pathlib import Path\n"
            "\n"
            "def _head_env(name, limit=12):\n"
            "    value = str(os.environ.get(name, '') or '').strip()\n"
            "    if not value:\n"
            "        return []\n"
            "    return [part for part in value.split(':') if part][:limit]\n"
            "\n"
            "def _dependency_probe(apollo_root, apollo_dist):\n"
            "    candidates = [\n"
            "        apollo_dist / 'lib' / 'cyber' / 'python' / 'internal' / '_cyber_wrapper.so',\n"
            "        apollo_root / 'cyber' / 'python' / 'internal' / '_cyber_wrapper.so',\n"
            "    ]\n"
            "    target = next((path for path in candidates if path.exists()), None)\n"
            "    if target is None:\n"
            "        return {\n"
            "            'bridge_runtime_dependency_probe_status': 'bridge_runtime_python_dependency_failed',\n"
            "            'bridge_runtime_dependency_probe_target': None,\n"
            "            'bridge_runtime_missing_shared_libs': [],\n"
            "        }\n"
            "    proc = subprocess.run(['ldd', str(target)], capture_output=True, text=True, check=False)\n"
            "    missing = []\n"
            "    for raw_line in (proc.stdout or '').splitlines():\n"
            "        line = raw_line.strip()\n"
            "        if '=> not found' not in line:\n"
            "            continue\n"
            "        missing.append(line.split('=>', 1)[0].strip())\n"
            "    status = 'bridge_runtime_ready'\n"
            "    if missing:\n"
            "        status = 'bridge_runtime_shared_library_failed'\n"
            "        if any('libcudart.so' in item for item in missing):\n"
            "            status = 'bridge_runtime_cuda_runtime_failed'\n"
            "    return {\n"
            "        'bridge_runtime_dependency_probe_status': status,\n"
            "        'bridge_runtime_dependency_probe_target': str(target),\n"
            "        'bridge_runtime_missing_shared_libs': missing,\n"
            "        'bridge_runtime_ldd_stdout_tail': '\\n'.join((proc.stdout or '').splitlines()[-40:]),\n"
            "        'bridge_runtime_ldd_stderr_tail': '\\n'.join((proc.stderr or '').splitlines()[-40:]),\n"
            "    }\n"
            "\n"
            "payload = {\n"
            "    'bridge_runtime_import_ok': False,\n"
            "    'bridge_runtime_preflight_status': 'bridge_runtime_import_failed',\n"
            "    'bridge_runtime_import_error': None,\n"
            "    'bridge_runtime_python_executable': sys.executable,\n"
            "    'bridge_runtime_pythonpath_head': _head_env('PYTHONPATH'),\n"
            "    'bridge_runtime_ld_library_path_head': _head_env('LD_LIBRARY_PATH'),\n"
            "    'bridge_runtime_missing_shared_libs': [],\n"
            "    'bridge_runtime_missing_python_modules': [],\n"
            "    'bridge_runtime_dependency_probe_status': 'bridge_runtime_import_failed',\n"
            "}\n"
            "apollo_root = Path(os.environ.get('APOLLO_ROOT', '')).expanduser()\n"
            "apollo_dist = Path(os.environ.get('APOLLO_DISTRIBUTION_HOME', '')).expanduser()\n"
            "payload.update(_dependency_probe(apollo_root, apollo_dist))\n"
            "for candidate in (apollo_root, apollo_root / 'cyber' / 'python'):\n"
            "    text = str(candidate)\n"
            "    if text and text not in sys.path:\n"
            "        sys.path.insert(0, text)\n"
            "try:\n"
            "    try:\n"
            "        from cyber.python.cyber_py3 import cyber  # noqa: F401\n"
            "        from cyber.python.cyber_py3 import cyber_time  # noqa: F401\n"
            "    except Exception:\n"
            "        from cyber_py3 import cyber  # noqa: F401\n"
            "        from cyber_py3 import cyber_time  # noqa: F401\n"
            "    payload.update({\n"
            "        'bridge_runtime_import_ok': True,\n"
            "        'bridge_runtime_preflight_status': 'bridge_runtime_ready',\n"
            "        'bridge_runtime_import_error': None,\n"
            "        'bridge_runtime_dependency_probe_status': 'bridge_runtime_ready',\n"
            "    })\n"
            "except Exception as exc:\n"
            "    exc_text = f'{type(exc).__name__}: {exc}'\n"
            "    missing_modules = []\n"
            "    if isinstance(exc, ModuleNotFoundError):\n"
            "        name = getattr(exc, 'name', None)\n"
            "        if name:\n"
            "            missing_modules.append(str(name))\n"
            "    elif 'No module named ' in exc_text:\n"
            "        missing_modules.append(exc_text.split('No module named ', 1)[1].strip().strip(\"'\\\"\"))\n"
            "    dep_status = str(payload.get('bridge_runtime_dependency_probe_status') or 'bridge_runtime_import_failed')\n"
            "    if missing_modules:\n"
            "        dep_status = 'bridge_runtime_python_dependency_failed'\n"
            "    elif 'cannot open shared object file' in exc_text and 'libcudart.so' in exc_text:\n"
            "        dep_status = 'bridge_runtime_cuda_runtime_failed'\n"
            "        payload['bridge_runtime_missing_shared_libs'] = sorted(\n"
            "            set(list(payload.get('bridge_runtime_missing_shared_libs') or []) + [str(exc).split(':', 1)[0]])\n"
            "        )\n"
            "    elif 'cannot open shared object file' in exc_text:\n"
            "        dep_status = 'bridge_runtime_shared_library_failed'\n"
            "        payload['bridge_runtime_missing_shared_libs'] = sorted(\n"
            "            set(list(payload.get('bridge_runtime_missing_shared_libs') or []) + [str(exc).split(':', 1)[0]])\n"
            "        )\n"
            "    payload.update({\n"
            "        'bridge_runtime_import_ok': False,\n"
            "        'bridge_runtime_preflight_status': 'bridge_runtime_import_failed',\n"
            "        'bridge_runtime_import_error': exc_text,\n"
            "        'bridge_runtime_missing_python_modules': missing_modules,\n"
            "        'bridge_runtime_dependency_probe_status': dep_status,\n"
            "        'bridge_runtime_import_traceback': traceback.format_exc(),\n"
            "    })\n"
            "print(json.dumps(payload, ensure_ascii=False))\n"
            "PY"
        )

    @staticmethod
    def _infer_bridge_runtime_failure_details(message: str) -> Dict[str, Any]:
        text = str(message or "").strip()
        payload: Dict[str, Any] = {
            "bridge_runtime_missing_shared_libs": [],
            "bridge_runtime_missing_python_modules": [],
            "bridge_runtime_dependency_probe_status": "bridge_runtime_import_failed",
        }
        if not text:
            return payload
        if "cannot open shared object file" in text:
            missing = ""
            for token in text.replace("\n", " ").split():
                candidate = token.strip(" '\"():,")
                if ".so" in candidate:
                    missing = candidate
                    if "libcudart.so" in candidate:
                        break
            if not missing:
                missing = text.split(":", 1)[0].strip()
            payload["bridge_runtime_missing_shared_libs"] = [missing] if missing else []
            payload["bridge_runtime_dependency_probe_status"] = "bridge_runtime_shared_library_failed"
            if "libcudart.so" in missing:
                payload["bridge_runtime_dependency_probe_status"] = "bridge_runtime_cuda_runtime_failed"
            return payload
        needle = "No module named "
        if needle in text:
            missing = text.split(needle, 1)[1].strip().strip("'\"")
            payload["bridge_runtime_missing_python_modules"] = [missing] if missing else []
            payload["bridge_runtime_dependency_probe_status"] = "bridge_runtime_python_dependency_failed"
        return payload

    @staticmethod
    def _parse_bridge_runtime_preflight_output(
        *,
        stdout: str,
        stderr: str,
        returncode: int,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "bridge_runtime_import_ok": False,
            "bridge_runtime_preflight_status": "bridge_runtime_import_failed",
            "bridge_runtime_import_error": f"preflight_command_failed(returncode={returncode})",
            "bridge_runtime_missing_shared_libs": [],
            "bridge_runtime_missing_python_modules": [],
            "bridge_runtime_dependency_probe_status": "bridge_runtime_import_failed",
        }
        for raw_line in reversed((stdout or "").splitlines()):
            line = raw_line.strip()
            if not line.startswith("{"):
                continue
            try:
                candidate = json.loads(line)
            except Exception:
                continue
            if isinstance(candidate, dict) and "bridge_runtime_preflight_status" in candidate:
                payload.update(candidate)
                break
        if payload.get("bridge_runtime_import_error") in {None, ""} and returncode != 0:
            payload["bridge_runtime_import_error"] = f"preflight_command_failed(returncode={returncode})"
        stderr_lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
        if (
            returncode != 0
            and payload.get("bridge_runtime_import_error", "").startswith("preflight_command_failed")
            and stderr_lines
        ):
            payload["bridge_runtime_import_error"] = stderr_lines[-1]
        inferred = CyberRTBackend._infer_bridge_runtime_failure_details(
            "\n".join(
                [
                    str(payload.get("bridge_runtime_import_error") or ""),
                    stderr or "",
                    stdout or "",
                ]
            )
        )
        for key, value in inferred.items():
            payload.setdefault(key, value)
            if key in {"bridge_runtime_missing_shared_libs", "bridge_runtime_missing_python_modules"}:
                if not payload.get(key):
                    payload[key] = value
            elif key == "bridge_runtime_dependency_probe_status" and payload.get(key) in {
                None,
                "",
                "bridge_runtime_import_failed",
            }:
                payload[key] = value
        payload["bridge_runtime_command_returncode"] = int(returncode)
        payload["bridge_runtime_stdout_tail"] = "\n".join((stdout or "").splitlines()[-20:])
        payload["bridge_runtime_stderr_tail"] = "\n".join((stderr or "").splitlines()[-20:])
        return payload

    def _run_bridge_runtime_preflight(
        self,
        *,
        artifacts: Path,
        source_prefix: str,
        python_exec: str,
        docker_container: Optional[str] = None,
    ) -> Dict[str, Any]:
        cmd = self._bridge_runtime_preflight_command(python_exec)
        shell_cmd = f"{source_prefix}; {cmd}" if source_prefix else cmd
        if docker_container:
            proc = self._docker_exec(shell_cmd, capture_output=True, check=False)
            stdout = proc.stdout or ""
            stderr = proc.stderr or ""
            returncode = int(proc.returncode)
        else:
            proc = subprocess.run(
                ["bash", "-lc", shell_cmd],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=False,
            )
            stdout = proc.stdout or ""
            stderr = proc.stderr or ""
            returncode = int(proc.returncode)
        payload = self._parse_bridge_runtime_preflight_output(
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )
        payload["checked_at_sec"] = time.time()
        payload["docker_container"] = docker_container or ""
        self._bridge_runtime_preflight_path(artifacts).write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return payload

    def _run_dir(self) -> Path:
        value = self.profile.get("_apollo_run_dir")
        if value:
            return Path(value).resolve()
        return Path(self.profile.get("artifacts", {}).get("dir", "runs/latest/artifacts")).resolve().parent

    def _artifacts_dir(self) -> Path:
        artifacts = self.profile.get("artifacts", {}) or {}
        value = artifacts.get("dir")
        if value:
            return Path(value).resolve()
        path = self._run_dir() / "artifacts"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _backend_startup_trace_jsonl_path(self) -> Path:
        return self._artifacts_dir() / "apollo_backend_startup_trace.jsonl"

    def _backend_startup_trace_json_path(self) -> Path:
        return self._artifacts_dir() / "apollo_backend_startup_trace.json"

    def _write_backend_startup_trace(self, step: str, **payload: Any) -> None:
        try:
            artifacts = self._artifacts_dir()
            artifacts.mkdir(parents=True, exist_ok=True)
            data: Dict[str, Any] = {"step": str(step), "ts_sec": time.time()}
            data.update(payload)
            with self._backend_startup_trace_jsonl_path().open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(data, ensure_ascii=False) + "\n")
            self._backend_startup_trace_json_path().write_text(
                json.dumps(data, indent=2, ensure_ascii=False)
            )
        except Exception:
            pass

    def _apollo_root(self) -> Path:
        cfg = self._apollo_cfg()
        root = cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT")
        if not root:
            home = Path.home()
            candidates = [
                home / "Apollo10.0" / "application-core" / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "src",
                home / "Apollo10.0" / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "src",
                home / "Apollo10.0" / "neo" / "src",
                home / "Apollo10.0" / "src",
                Path("/opt/apollo/neo/src"),
            ]
            for cand in candidates:
                if cand.exists():
                    root = str(cand)
                    break
        if not root:
            raise RuntimeError("APOLLO_ROOT is required (set env APOLLO_ROOT or algo.apollo.apollo_root)")
        raw = Path(root).expanduser().resolve()
        if not raw.exists():
            raise RuntimeError(f"APOLLO_ROOT not found: {raw}")

        def _looks_like_src(p: Path) -> bool:
            return (p / "modules" / "common_msgs").exists() and (p / "cyber").exists()

        candidates = [
            raw,
            raw / "application-core" / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "src",
            raw / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "src",
            raw / "neo" / "src",
            raw / "src",
        ]
        for cand in candidates:
            if cand.exists() and _looks_like_src(cand):
                return cand.resolve()
        raise RuntimeError(
            "APOLLO_ROOT does not look like Apollo source root (missing modules/common_msgs + cyber). "
            f"provided={raw}"
        )

    def _bridge_cfg(self) -> Path:
        cfg = self._apollo_cfg()
        path = cfg.get("bridge_config_path")
        if not path:
            raise RuntimeError("apollo bridge_config_path missing; run adapter.prepare first")
        out = Path(path).expanduser()
        if not out.is_absolute():
            out = (self.repo_root / out).resolve()
        if not out.exists():
            raise RuntimeError(f"apollo bridge config not found: {out}")
        return out

    def _bridge_stats_path(self) -> Path:
        cfg = self._apollo_cfg()
        path = cfg.get("stats_path")
        if path:
            out = Path(path).expanduser()
            if not out.is_absolute():
                out = (self.repo_root / out).resolve()
            return out
        return self._artifacts_dir() / "cyber_bridge_stats.json"

    def _pb_root(self) -> Path:
        cfg = self._apollo_cfg()
        path = cfg.get("pb_root")
        if path:
            out = Path(path).expanduser()
            if not out.is_absolute():
                out = (self.repo_root / out).resolve()
            return out
        return self.repo_root / "tools" / "apollo10_cyber_bridge" / "pb"

    def _required_pb_files(self) -> list[Path]:
        root = self._pb_root()
        return [
            root / "modules/common_msgs/localization_msgs/localization_pb2.py",
            root / "modules/common_msgs/chassis_msgs/chassis_pb2.py",
            root / "modules/common_msgs/perception_msgs/perception_obstacle_pb2.py",
            root / "modules/common_msgs/control_msgs/control_cmd_pb2.py",
            root / "modules/common_msgs/external_command_msgs/lane_follow_command_pb2.py",
        ]

    def _ensure_pb_ready(self) -> None:
        if all(p.exists() for p in self._required_pb_files()):
            return
        script = self.repo_root / "tools" / "apollo10_cyber_bridge" / "gen_pb2.sh"
        if not script.exists():
            raise RuntimeError(f"pb generation script missing: {script}")
        env = os.environ.copy()
        env["APOLLO_ROOT"] = str(self._apollo_root())
        print(f"[cyberrt] generating pb2 via {script}")
        subprocess.check_call(["bash", str(script)], cwd=self.repo_root, env=env)
        missing = [p for p in self._required_pb_files() if not p.exists()]
        if missing:
            raise RuntimeError(f"pb2 generation incomplete, missing: {missing}")

    def _source_prefix(self) -> str:
        apollo_cfg = self._apollo_cfg()
        apollo_root = self._apollo_root()
        apollo_dist = apollo_root.parent if apollo_root.name == "src" else apollo_root
        env_script = apollo_cfg.get("apollo_env_script")
        parts = ["set -eo pipefail"]
        # Apollo setup scripts assume APOLLO_ROOT_DIR may already exist.
        parts.append(f"export APOLLO_ROOT={shlex.quote(str(apollo_root))}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(str(apollo_root))}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(str(apollo_dist))}")
        parts.append(f"export APOLLO_PLUGIN_INDEX_PATH={shlex.quote(str(apollo_dist / 'share' / 'cyber_plugin_index'))}")
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        ros2_setup = self._host_ros2_setup_script()
        if ros2_setup and not (
            self._transport_mode() == "carla_direct" and self._direct_require_no_ros2_runtime()
        ):
            parts.append(f'source {shlex.quote(ros2_setup)}')
        if env_script:
            script = Path(env_script).expanduser()
            if not script.is_absolute():
                script = (self.repo_root / script).resolve()
            if not script.exists():
                raise RuntimeError(f"apollo_env_script not found: {script}")
            parts.append(f"source {shlex.quote(str(script))}")
        else:
            setup = apollo_root / "cyber" / "setup.bash"
            if setup.exists():
                parts.append(f"source {shlex.quote(str(setup))}")
        pb_root = self._pb_root()
        bridge_root = default_apollo_cyber_gt_bridge_entrypoint(self.repo_root).bridge_package_dir
        py_parts = [
            str(self.repo_root),
            str(bridge_root),
            str(pb_root),
            str(apollo_root / "cyber" / "python"),
            str(apollo_root / "cyber" / "python" / "internal"),
            str(apollo_dist / "lib" / "cyber" / "python"),
            str(apollo_dist / "lib" / "cyber" / "python" / "internal"),
            str(apollo_dist / "lib" / "python3.10" / "site-packages"),
        ]
        py_expr = ":".join(shlex.quote(p) for p in py_parts)
        parts.append(f"export PYTHONPATH={py_expr}:${{PYTHONPATH:-}}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(str(apollo_dist / "lib")),
                    shlex.quote(str(apollo_dist / "lib64")),
                    shlex.quote(str(apollo_dist / "lib" / "third_party" / "rtklib")),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        # Apollo dependencies (FastDDS/FastCDR, etc.) are shipped under packages/*/lib.
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        parts.append(
            'while IFS= read -r _d; do export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            f"done < <(find {shlex.quote(str(apollo_dist / 'lib'))} -type d)"
        )
        internal_debug_exports = self._apollo_internal_debug_shell_exports(docker=False)
        if internal_debug_exports:
            parts.append(internal_debug_exports)
        return "; ".join(parts)

    def _docker_source_prefix(self, bridge_root: str, pb_root: str) -> str:
        apollo_root = self._docker_apollo_root()
        apollo_dist = self._docker_apollo_dist()
        env_script = self._docker_env_script()
        parts = ["set -o pipefail"]
        parts.append(f"export APOLLO_ROOT={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(apollo_dist)}")
        parts.append(f"export APOLLO_PLUGIN_INDEX_PATH={shlex.quote(f'{apollo_dist}/share/cyber_plugin_index')}")
        parts.append('if [ -d /apollo/cyber ]; then export CYBER_PATH=/apollo/cyber; fi')
        parts.append(
            f'if [ -f {shlex.quote(f"{apollo_dist}/setup.sh")} ]; then source {shlex.quote(f"{apollo_dist}/setup.sh")}; fi'
        )
        parts.append(
            f'if [ -f {shlex.quote(env_script)} ]; then source {shlex.quote(env_script)}; fi'
        )
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        py_parts = [
            bridge_root,
            pb_root,
            f"{apollo_root}/cyber/python",
            f"{apollo_root}/cyber/python/internal",
            f"{apollo_dist}/lib/cyber/python",
            f"{apollo_dist}/lib/cyber/python/internal",
        ]
        py_expr = ":".join(shlex.quote(p) for p in py_parts)
        parts.append(f"export PYTHONPATH={py_expr}:${{PYTHONPATH:-}}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(f"{apollo_dist}/lib"),
                    shlex.quote(f"{apollo_dist}/lib64"),
                    shlex.quote(f"{apollo_dist}/lib/third_party/rtklib"),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        parts.append(
            'while IFS= read -r _d; do export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            f"done < <(find {shlex.quote(f'{apollo_dist}/lib')} -type d)"
        )
        internal_debug_exports = self._apollo_internal_debug_shell_exports(docker=True)
        if internal_debug_exports:
            parts.append(internal_debug_exports)
        return "; ".join(parts)

    def _docker_cyber_cli_prefix(self) -> str:
        apollo_root = self._docker_apollo_root()
        apollo_dist = self._docker_apollo_dist()
        env_script = self._docker_env_script()
        parts = ["set -o pipefail"]
        parts.append(f"export APOLLO_ROOT={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(apollo_dist)}")
        parts.append(f"export APOLLO_PLUGIN_INDEX_PATH={shlex.quote(f'{apollo_dist}/share/cyber_plugin_index')}")
        parts.append('if [ -d /apollo/cyber ]; then export CYBER_PATH=/apollo/cyber; fi')
        parts.append(
            f'if [ -f {shlex.quote(f"{apollo_dist}/setup.sh")} ]; then source {shlex.quote(f"{apollo_dist}/setup.sh")}; fi'
        )
        parts.append(
            f'if [ -f {shlex.quote(env_script)} ]; then source {shlex.quote(env_script)}; fi'
        )
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(f"{apollo_dist}/lib"),
                    shlex.quote(f"{apollo_dist}/lib64"),
                    shlex.quote(f"{apollo_dist}/lib/third_party/rtklib"),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        parts.append(
            'while IFS= read -r _d; do export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            f"done < <(find {shlex.quote(f'{apollo_dist}/lib')} -type d)"
        )
        internal_debug_exports = self._apollo_internal_debug_shell_exports(docker=True)
        if internal_debug_exports:
            parts.append(internal_debug_exports)
        return "; ".join(parts)

    def _launch(
        self,
        name: str,
        core_cmd: str,
        out_path: Path,
        err_path: Path,
        *,
        source_prefix: Optional[str] = None,
        docker_container: Optional[str] = None,
    ) -> subprocess.Popen:
        prefix = source_prefix if source_prefix is not None else self._source_prefix()
        shell_cmd = f"{prefix}; {core_cmd}" if prefix else core_cmd
        out_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.parent.mkdir(parents=True, exist_ok=True)
        out_fp = out_path.open("w")
        err_fp = err_path.open("w")
        cmd = ["bash", "-lc", shell_cmd]
        cwd = self.repo_root
        if docker_container:
            cmd = ["docker", "exec", "-i", docker_container, "bash", "-lc", shell_cmd]
            cwd = None
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=out_fp,
            stderr=err_fp,
            start_new_session=True,
        )
        if name == "bridge":
            self._bridge_out = out_fp
            self._bridge_err = err_fp
        else:
            self._control_out = out_fp
            self._control_err = err_fp
        return proc

    def start(self) -> bool:
        artifacts = self._artifacts_dir()
        artifacts.mkdir(parents=True, exist_ok=True)
        self._write_backend_startup_trace("start_enter")
        try:
            self._write_backend_startup_trace("cleanup_stale_bridge_begin")
            self._cleanup_stale_apollo_bridge()
            self._write_backend_startup_trace("cleanup_stale_bridge_done")
            self._carla_vehicle_param_override = self._probe_carla_vehicle_param_override(artifacts)
            self._write_backend_startup_trace("probe_vehicle_param_done")
            self._ensure_pb_ready()
            self._write_backend_startup_trace("ensure_pb_ready_done")
            self._capture_apollo_log_offsets(artifacts)
            self._write_backend_startup_trace("capture_log_offsets_done")

            apollo_cfg = self._apollo_cfg()
            transport_mode = self._transport_mode()
            if transport_mode not in {"ros2_gt", "carla_direct"}:
                raise RuntimeError(
                    f"unsupported algo.apollo.transport_mode={transport_mode}; expected ros2_gt|carla_direct"
                )
            bridge_py = default_apollo_cyber_gt_bridge_entrypoint(self.repo_root).bridge_script
            if not bridge_py.exists():
                raise RuntimeError(f"bridge.py not found: {bridge_py}")
            self._write_backend_startup_trace(
                "transport_mode_resolved",
                transport_mode=transport_mode,
                route_command_mode=self._direct_route_command_mode(),
                require_no_ros2_runtime=bool(self._direct_require_no_ros2_runtime()),
            )
            self._write_backend_startup_trace(
                "runtime_topology_resolved",
                transport_mode=transport_mode,
                gt_source="ros2_gt" if self._uses_ros2_gt() else "carla_world_snapshot_direct",
                control_apply_path=(
                    "ros2_control_bridge" if self._uses_ros2_control_bridge() else "bridge_direct_actor_apply"
                ),
                tick_owner="runner_harness_world_tick",
                uses_ros2_gt=bool(self._uses_ros2_gt()),
                uses_ros2_control_bridge=bool(self._uses_ros2_control_bridge()),
                requires_ros2_reexec=bool(self._requires_ros2_reexec()),
                route_command_mode=self._direct_route_command_mode(),
            )

            run_cfg = self.profile.get("run", {}) or {}
            io_ros = ((self.profile.get("io", {}) or {}).get("ros", {}) or {})
            ego_id = str(run_cfg.get("ego_id", "hero"))
            namespace = str(io_ros.get("namespace", "/carla"))

            bridge_cfg = self._bridge_cfg()
            stats_path = self._bridge_stats_path()
            bridge_yaml = yaml.safe_load(bridge_cfg.read_text()) if bridge_cfg.exists() else {}
            bridge_ros_cfg = (bridge_yaml.get("ros2", {}) if isinstance(bridge_yaml, dict) else {}) or {}
            docker_container: Optional[str] = None
            bridge_in_container = False
            if self._docker_enabled():
                self._write_backend_startup_trace("docker_start_begin")
                self._docker_container_name = self._docker_container()
                self._write_backend_startup_trace(
                    "docker_container_resolved",
                    container=self._docker_container_name,
                )
                self._docker_check_running(self._docker_container_name)
                self._write_backend_startup_trace("docker_check_running_done")
                self._prepare_apollo_internal_debug_dir(artifacts)
                self._write_backend_startup_trace("docker_internal_debug_dir_done")
                self._docker_check_runtime_requirements(self._docker_container_name, artifacts)
                self._write_backend_startup_trace("docker_runtime_requirements_done")
                self._docker_prepare_user(artifacts)
                self._docker_ensure_runtime_deps(artifacts)
                self._write_backend_startup_trace("docker_runtime_deps_done")
                self._docker_restore_control_runtime_overlay()
                self._docker_stage_control_runtime_overlay(artifacts)
                self._docker_start_modules(artifacts)
                self._write_backend_startup_trace("docker_start_modules_complete")
                self._write_backend_startup_trace("dreamview_start_begin")
                self._maybe_start_dreamview(artifacts)
                self._write_backend_startup_trace("dreamview_start_done")
                bridge_in_container = self._docker_bridge_in_container()
                print(
                    f"[cyberrt] docker mode enabled, container={self._docker_container_name}, "
                    f"bridge_in_container={bridge_in_container}"
                )
            else:
                self._prepare_apollo_internal_debug_dir(artifacts)
                self._write_backend_startup_trace("host_internal_debug_dir_done")
                self._write_backend_startup_trace("dreamview_start_begin")
                self._maybe_start_dreamview(artifacts)
                self._write_backend_startup_trace("dreamview_start_done")

            if bridge_in_container:
                self._write_backend_startup_trace("docker_prepare_stage_begin")
                stage_bridge_dir, stage_cfg, stage_pb_root, stage_stats = self._docker_prepare_stage(bridge_cfg, bridge_py)
                self._write_backend_startup_trace("docker_prepare_stage_done")
                docker_container = self._docker_container_name
                python_exec = shlex.quote(self._docker_python_exec())
                self._runtime_source_prefix = self._docker_source_prefix(stage_bridge_dir, stage_pb_root)
                bridge_cmd = " ".join(
                    [
                        python_exec,
                        shlex.quote(f"{stage_bridge_dir}/bridge.py"),
                        "--config",
                        shlex.quote(stage_cfg),
                        "--stats-path",
                        shlex.quote(stage_stats),
                        "--apollo-root",
                        shlex.quote(self._docker_apollo_root()),
                        "--pb-root",
                        shlex.quote(stage_pb_root),
                        "--ego-id",
                        shlex.quote(ego_id),
                        "--namespace",
                        shlex.quote(namespace),
                    ]
                )
            else:
                python_exec = shlex.quote(self._host_bridge_python_exec())
                self._runtime_source_prefix = self._source_prefix()
                if self._docker_enabled():
                    self._write_backend_startup_trace("docker_sync_host_libs_begin")
                    copied_libs = self._docker_sync_host_libs(artifacts)
                    cache_status = dict(self._last_docker_host_lib_cache_status or {})
                    self._write_backend_startup_trace(
                        "docker_sync_host_libs_done",
                        copied=bool(copied_libs),
                        cache_reused=bool(cache_status.get("cache_reused")),
                        newly_copied_count=int(cache_status.get("newly_copied_count") or 0),
                        cache_dir=str(cache_status.get("cache_dir") or ""),
                    )
                    if copied_libs:
                        self._runtime_source_prefix += (
                            f"; export LD_LIBRARY_PATH={shlex.quote(str(copied_libs))}:${{LD_LIBRARY_PATH:-}}"
                        )
                self._docker_stage_dir = None
                self._docker_stats_path = None
                bridge_cmd = " ".join(
                    [
                        python_exec,
                        shlex.quote(str(bridge_py)),
                        "--config",
                        shlex.quote(str(bridge_cfg)),
                        "--stats-path",
                        shlex.quote(str(stats_path)),
                        "--apollo-root",
                        shlex.quote(str(self._apollo_root())),
                        "--pb-root",
                        shlex.quote(str(self._pb_root())),
                        "--ego-id",
                        shlex.quote(ego_id),
                        "--namespace",
                        shlex.quote(namespace),
                    ]
                )
            self._write_backend_startup_trace("bridge_runtime_preflight_begin")
            bridge_runtime_preflight = self._run_bridge_runtime_preflight(
                artifacts=artifacts,
                source_prefix=self._runtime_source_prefix,
                python_exec=python_exec,
                docker_container=docker_container,
            )
            self._write_backend_startup_trace(
                "bridge_runtime_preflight_done",
                bridge_runtime_import_ok=bool(bridge_runtime_preflight.get("bridge_runtime_import_ok")),
                bridge_runtime_preflight_status=str(
                    bridge_runtime_preflight.get("bridge_runtime_preflight_status") or "unknown"
                ),
            )
            if not bool(bridge_runtime_preflight.get("bridge_runtime_import_ok")) and self._docker_enabled() and not bridge_in_container:
                recovery_round = 0
                seen_missing: set[str] = set()
                while recovery_round < 4:
                    missing_shared = [
                        str(item).strip()
                        for item in (bridge_runtime_preflight.get("bridge_runtime_missing_shared_libs") or [])
                        if str(item).strip()
                    ]
                    pending_missing = [item for item in missing_shared if item not in seen_missing]
                    if not pending_missing:
                        break
                    recovery_round += 1
                    seen_missing.update(pending_missing)
                    self._write_backend_startup_trace(
                        "bridge_runtime_missing_lib_sync_begin",
                        recovery_round=recovery_round,
                        missing_shared_libs=pending_missing,
                    )
                    copied_missing = self._docker_sync_missing_host_libs(artifacts, pending_missing)
                    self._write_backend_startup_trace(
                        "bridge_runtime_missing_lib_sync_done",
                        recovery_round=recovery_round,
                        copied=bool(copied_missing),
                    )
                    if not copied_missing:
                        break
                    self._write_backend_startup_trace(
                        "bridge_runtime_preflight_retry_begin",
                        recovery_round=recovery_round,
                    )
                    bridge_runtime_preflight = self._run_bridge_runtime_preflight(
                        artifacts=artifacts,
                        source_prefix=self._runtime_source_prefix,
                        python_exec=python_exec,
                        docker_container=docker_container,
                    )
                    self._write_backend_startup_trace(
                        "bridge_runtime_preflight_retry_done",
                        recovery_round=recovery_round,
                        bridge_runtime_import_ok=bool(bridge_runtime_preflight.get("bridge_runtime_import_ok")),
                        bridge_runtime_preflight_status=str(
                            bridge_runtime_preflight.get("bridge_runtime_preflight_status") or "unknown"
                        ),
                    )
                    if bool(bridge_runtime_preflight.get("bridge_runtime_import_ok")):
                        break
            if not bool(bridge_runtime_preflight.get("bridge_runtime_import_ok")):
                error = str(
                    bridge_runtime_preflight.get("bridge_runtime_import_error")
                    or "unknown bridge runtime import failure"
                )
                self._write_backend_startup_trace(
                    "bridge_runtime_preflight_failed",
                    bridge_runtime_import_error=error,
                )
                raise RuntimeError(f"bridge runtime import preflight failed: {error}")
            self._write_backend_startup_trace("bridge_launch_begin")
            self.bridge_proc = self._launch(
                "bridge",
                bridge_cmd,
                artifacts / "cyber_bridge.out.log",
                artifacts / "cyber_bridge.err.log",
                source_prefix=self._runtime_source_prefix,
                docker_container=docker_container,
            )
            self._write_backend_startup_trace(
                "bridge_launch_done",
                pid=int(self.bridge_proc.pid),
            )
            print(f"[cyberrt] bridge started pid={self.bridge_proc.pid}")

            control_start_gate = self._docker_control_start_gate() if self._docker_enabled() else "none"
            if self._docker_enabled() and control_start_gate != "none":
                self._deferred_control_pending = True
                self._deferred_control_started = False
                self._deferred_control_overall_start_sec = time.time()
                self._deferred_control_route_seen_sec = None
                self._deferred_control_route_established_sec = None
                self._deferred_control_last_poll_sec = 0.0
                self._deferred_control_snapshots = []
                self._deferred_control_failure = None
                self._write_backend_startup_trace(
                    "deferred_control_enabled",
                    control_start_gate=control_start_gate,
                )

            control_bridge_cfg = apollo_cfg.get("carla_control_bridge", {}) or {}
            if transport_mode != "ros2_gt":
                self._write_backend_startup_trace(
                    "control_bridge_skipped_for_transport_mode",
                    transport_mode=transport_mode,
                )
            elif control_bridge_cfg.get("enabled", True):
                control_py = self.repo_root / "algo" / "nodes" / "control" / "carla_control_bridge" / "ros2_autoware_to_carla.py"
                if control_py.exists():
                    control_out_type = str(bridge_ros_cfg.get("control_out_type", "ackermann")).lower()
                    if control_out_type not in {"ackermann", "twist", "direct"}:
                        print(f"[cyberrt][warn] unsupported control_out_type={control_out_type}; skip built-in control bridge")
                        self._write_backend_startup_trace(
                            "control_bridge_skipped_invalid_type",
                            control_out_type=control_out_type,
                        )
                        return self.bridge_proc is not None and self.bridge_proc.poll() is None
                    carla_cfg = (self.profile.get("runtime", {}) or {}).get("carla", {}) or {}
                    topic = str(control_bridge_cfg.get("control_topic", "/tb/ego/control_cmd"))
                    default_apply_hz = self._default_control_apply_hz()
                    self._cleanup_stale_control_bridge(topic)
                    self._write_backend_startup_trace("cleanup_stale_control_bridge_done", topic=topic)
                    control_cmd = " ".join(
                        [
                            python_exec,
                            shlex.quote(str(control_py)),
                            "--carla-host",
                            shlex.quote(str(carla_cfg.get("host", "127.0.0.1"))),
                            "--carla-port",
                            shlex.quote(str(carla_cfg.get("port", 2000))),
                            "--connect-timeout-sec",
                            shlex.quote(str(control_bridge_cfg.get("connect_timeout_sec", 6.0))),
                            "--control-topic",
                            shlex.quote(topic),
                            "--control-type",
                            shlex.quote(control_out_type),
                            "--ego-role-name",
                            shlex.quote(ego_id),
                            "--max-steer-angle",
                            shlex.quote(str(control_bridge_cfg.get("max_steer_angle", 0.6))),
                            "--timeout-sec",
                            shlex.quote(str(control_bridge_cfg.get("timeout_sec", 0.8))),
                            "--speed-gain",
                            shlex.quote(str(control_bridge_cfg.get("speed_gain", 10.0))),
                            "--brake-gain",
                            shlex.quote(str(control_bridge_cfg.get("brake_gain", 5.0))),
                            "--apply-hz",
                            shlex.quote(str(control_bridge_cfg.get("apply_hz", default_apply_hz))),
                            "--watchdog-arm-delay-sec",
                            shlex.quote(str(control_bridge_cfg.get("watchdog_arm_delay_sec", 1.5))),
                            "--startup-brake-suppression-speed-mps",
                            shlex.quote(
                                str(control_bridge_cfg.get("startup_brake_suppression_speed_mps", 1.0))
                            ),
                            "--startup-brake-suppression-max-brake",
                            shlex.quote(
                                str(control_bridge_cfg.get("startup_brake_suppression_max_brake", 0.2))
                            ),
                            "--startup-brake-suppression-min-throttle",
                            shlex.quote(
                                str(control_bridge_cfg.get("startup_brake_suppression_min_throttle", 0.3))
                            ),
                            "--startup-brake-suppression-hold-sec",
                            shlex.quote(
                                str(control_bridge_cfg.get("startup_brake_suppression_hold_sec", 3.0))
                            ),
                            "--startup-brake-recent-throttle-window-sec",
                            shlex.quote(
                                str(
                                    control_bridge_cfg.get(
                                        "startup_brake_recent_throttle_window_sec", 1.0
                                    )
                                )
                            ),
                        ]
                    )
                    if bool(control_bridge_cfg.get("watchdog_wait_for_first_msg", True)):
                        control_cmd += " --watchdog-wait-for-first-msg"
                    else:
                        control_cmd += " --no-watchdog-wait-for-first-msg"
                    if bool(control_bridge_cfg.get("startup_brake_suppression_enabled", True)):
                        control_cmd += " --startup-brake-suppression-enabled"
                    else:
                        control_cmd += " --startup-brake-suppression-disabled"
                    if bool(control_bridge_cfg.get("sync_to_world_tick", True)):
                        control_cmd += " --sync-to-world-tick"
                    else:
                        control_cmd += " --no-sync-to-world-tick"
                    if bool(control_bridge_cfg.get("dryrun", False)):
                        control_cmd += " --dryrun"
                    self._write_backend_startup_trace("control_bridge_launch_begin")
                    self.control_proc = self._launch(
                        "control",
                        control_cmd,
                        artifacts / "cyber_control_bridge.out.log",
                        artifacts / "cyber_control_bridge.err.log",
                        source_prefix=self._runtime_source_prefix if not docker_container else "",
                    )
                    self._write_backend_startup_trace(
                        "control_bridge_launch_done",
                        pid=int(self.control_proc.pid),
                    )
                    print(f"[cyberrt] control bridge started pid={self.control_proc.pid}")
                else:
                    print(f"[cyberrt][warn] control bridge script not found: {control_py}")
                    self._write_backend_startup_trace(
                        "control_bridge_script_missing",
                        path=str(control_py),
                    )

            time.sleep(1.0)
            ok = self.bridge_proc is not None and self.bridge_proc.poll() is None
            self._write_backend_startup_trace("start_return", ok=bool(ok))
            return ok
        except Exception as exc:
            self._write_backend_startup_trace(
                "start_exception",
                error=str(exc),
                exc_type=type(exc).__name__,
            )
            raise

    def _read_stats(self) -> tuple[bool, Optional[float], Dict[str, Any]]:
        stats_path = self._bridge_stats_path()
        if self._docker_container_name and self._docker_stats_path:
            cmd = f"cat {shlex.quote(self._docker_stats_path)}"
            try:
                out = self._docker_exec(cmd, capture_output=True).stdout
                stats_path.parent.mkdir(parents=True, exist_ok=True)
                stats_path.write_text(out)
                try:
                    return True, 0.0, json.loads(out)
                except Exception as exc:
                    return True, 0.0, {"_parse_error": str(exc)}
            except Exception:
                return False, None, {}
        if not stats_path.exists():
            return False, None, {}
        age = time.time() - stats_path.stat().st_mtime
        try:
            return True, age, json.loads(stats_path.read_text())
        except Exception as exc:
            return True, age, {"_parse_error": str(exc)}

    @staticmethod
    def _bridge_planning_runtime_counters(stats: Dict[str, Any]) -> Dict[str, Any]:
        def _safe_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        def _safe_float(value: Any) -> Optional[float]:
            try:
                return float(value)
            except Exception:
                return None

        planning_stats = stats.get("planning")
        if not isinstance(planning_stats, dict):
            planning_stats = {}
        nonempty_count = max(
            _safe_int(stats.get("planning_nonempty_trajectory_count", 0) or 0),
            _safe_int(planning_stats.get("nonempty_trajectory_count", 0) or 0),
        )
        message_count = max(
            _safe_int(stats.get("planning_messages_received", 0) or 0),
            _safe_int(planning_stats.get("msg_count", 0) or 0),
        )
        last_sequence_num = stats.get("last_planning_header_sequence_num")
        if last_sequence_num is None:
            last_sequence_num = planning_stats.get("last_planning_header_sequence_num")
        last_trajectory_point_count = stats.get("last_trajectory_point_count")
        if last_trajectory_point_count is None:
            last_trajectory_point_count = planning_stats.get("last_trajectory_point_count")
        return {
            "planning_nonempty_trajectory_count": nonempty_count,
            "planning_messages_received": message_count,
            "last_planning_header_sequence_num": _safe_int(last_sequence_num) if last_sequence_num is not None else None,
            "last_trajectory_point_count": (
                _safe_int(last_trajectory_point_count) if last_trajectory_point_count is not None else None
            ),
            "planning_first_nonempty_ts_sec": _safe_float(
                stats.get("planning_first_nonempty_ts_sec") or planning_stats.get("first_nonempty_ts_sec")
            ),
        }

    def on_sim_tick(self, frame_id: int, timestamp: Optional[float] = None, step: Optional[int] = None) -> None:
        if (
            self._docker_enabled()
            and self._deferred_control_pending
            and not self._deferred_control_started
            and not self._deferred_control_start_in_progress
        ):
            now = time.time()
            poll_sec = self._docker_control_planning_ready_poll_sec()
            if (now - self._deferred_control_last_poll_sec) >= max(0.1, poll_sec):
                self._deferred_control_last_poll_sec = now
                artifacts = self._artifacts_dir()
                status, error = self._docker_probe_planning_ready_before_control(artifacts)
                if status == "ready":
                    self._start_deferred_control_module(artifacts)
                elif status == "error":
                    self._deferred_control_failure = error
                    self._deferred_control_pending = False
                    if error:
                        print(f"[cyberrt][warn] {error}")
        if self._dreamview_record_mode != "tick_snapshot":
            return
        self._capture_dreamview_tick_snapshot(int(frame_id), timestamp)

    def _maybe_capture_channels(self, artifacts: Path) -> Dict[str, Any]:
        cfg = self._apollo_cfg()
        hc_cfg = cfg.get("healthcheck", {}) or {}
        if not hc_cfg.get("probe_channels", False):
            return {"enabled": False}
        out_path = artifacts / "cyber_channel_list.txt"
        try:
            if self._docker_container_name:
                prefix = self._docker_cyber_cli_prefix()
                cmd = f"{prefix}; timeout 5 cyber_channel list" if prefix else "timeout 5 cyber_channel list"
                out = self._docker_exec(cmd, capture_output=True).stdout
            else:
                prefix = self._runtime_source_prefix or self._source_prefix()
                cmd = f"{prefix}; timeout 5 cyber_channel list" if prefix else "timeout 5 cyber_channel list"
                out = subprocess.check_output(["bash", "-lc", cmd], cwd=self.repo_root, text=True)
            out_path.write_text(out)
            return {"enabled": True, "ok": True, "channels_count": len([x for x in out.splitlines() if x.strip()])}
        except Exception as exc:
            out_path.write_text(f"probe failed: {exc}\n")
            return {"enabled": True, "ok": False, "error": str(exc)}

    def health_check(self) -> bool:
        artifacts = self._artifacts_dir()
        stats_path = self._bridge_stats_path()
        stats_exists, stats_age, stats = self._read_stats()
        preflight_path = self._bridge_runtime_preflight_path(artifacts)
        try:
            bridge_runtime_preflight = (
                json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else {}
            )
        except Exception:
            bridge_runtime_preflight = {}
        status: Dict[str, Any] = {
            "bridge_running": self.bridge_proc is not None and self.bridge_proc.poll() is None,
            "control_running": self.control_proc is not None and self.control_proc.poll() is None,
            "stats_path": str(stats_path),
            "stats_exists": stats_exists,
            "checked_at_sec": time.time(),
            "transport_mode": self._transport_mode(),
            "uses_ros2_gt": self._uses_ros2_gt(),
            "uses_ros2_control_bridge": self._uses_ros2_control_bridge(),
            "requires_ros2_reexec": self._requires_ros2_reexec(),
            "route_command_mode": self._direct_route_command_mode(),
            "require_no_ros2_runtime": self._direct_require_no_ros2_runtime(),
            "deferred_control_pending": self._deferred_control_pending,
            "deferred_control_started": self._deferred_control_started,
            "deferred_control_failure": self._deferred_control_failure,
            "deferred_control_start_in_progress": self._deferred_control_start_in_progress,
            "deferred_control_start_async": self._docker_deferred_control_start_async(),
            "deferred_control_start_async_failure": self._deferred_control_start_async_failure,
            "bridge_runtime_preflight_status": bridge_runtime_preflight.get("bridge_runtime_preflight_status") or "unknown",
            "bridge_runtime_import_ok": bridge_runtime_preflight.get("bridge_runtime_import_ok"),
            "bridge_runtime_import_error": bridge_runtime_preflight.get("bridge_runtime_import_error"),
            "bridge_runtime_dependency_probe_status": bridge_runtime_preflight.get(
                "bridge_runtime_dependency_probe_status"
            ),
            "bridge_runtime_missing_shared_libs": bridge_runtime_preflight.get(
                "bridge_runtime_missing_shared_libs"
            )
            or [],
            "bridge_runtime_missing_python_modules": bridge_runtime_preflight.get(
                "bridge_runtime_missing_python_modules"
            )
            or [],
            "bridge_runtime_preflight": bridge_runtime_preflight,
        }
        status["stats_age_sec"] = stats_age
        status["stats"] = stats
        status["channels_probe"] = self._maybe_capture_channels(artifacts)
        stat_obj = status.get("stats", {}) or {}
        loc_count = int(stat_obj.get("loc_count", 0) or 0)
        chassis_count = int(stat_obj.get("chassis_count", 0) or 0)
        obstacles_count = int(stat_obj.get("obstacles_count", 0) or 0)
        recent = bool(status["stats_exists"] and (status["stats_age_sec"] is not None) and status["stats_age_sec"] <= 2.5)
        status["recent_updates"] = recent
        status["has_publish"] = (loc_count + chassis_count + obstacles_count) > 0
        ok = bool(status["bridge_running"] and status["recent_updates"])
        (artifacts / "cyber_bridge_healthcheck.json").write_text(json.dumps(status, indent=2))
        return ok

    @staticmethod
    def _terminate(proc: Optional[subprocess.Popen], timeout_s: float = 8.0) -> None:
        if proc is None or proc.poll() is not None:
            return
        try:
            pgid = os.getpgid(proc.pid)
        except Exception:
            pgid = None
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGINT)
            else:
                proc.send_signal(signal.SIGINT)
            proc.wait(timeout=timeout_s)
            return
        except Exception:
            pass
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                proc.terminate()
            proc.wait(timeout=3.0)
            return
        except Exception:
            pass
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                proc.kill()
        except Exception:
            pass

    def stop(self) -> None:
        apollo_cfg = self._apollo_cfg()
        artifacts = self._artifacts_dir()
        control_bridge_cfg = apollo_cfg.get("carla_control_bridge", {}) or {}
        topic = str(control_bridge_cfg.get("control_topic", "/tb/ego/control_cmd"))
        self._snapshot_docker_modules_status(artifacts, "apollo_modules_status_final.log")
        if self._deferred_control_start_thread is not None and self._deferred_control_start_thread.is_alive():
            try:
                self._deferred_control_start_thread.join(timeout=0.2)
            except Exception:
                pass
        self._terminate(self.control_proc)
        self._terminate(self.bridge_proc)
        self._cleanup_stale_control_bridge(topic)
        self._cleanup_stale_apollo_bridge()
        self._stop_dreamview(artifacts)
        if self._docker_container_name and self._docker_start_modules_enabled():
            try:
                patt = (
                    "modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|"
                    "planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)"
                )
                self._docker_exec(
                    f"pkill -f '{patt}' >/dev/null 2>&1 || true; "
                    f"sleep 1; pkill -9 -f '{patt}' >/dev/null 2>&1 || true",
                    check=False,
                )
            except Exception:
                pass
        self._docker_restore_control_runtime_overlay(artifacts)
        if self._docker_container_name and self._docker_stage_dir:
            try:
                pattern = shlex.quote(f"{self._docker_stage_dir}/tools/apollo10_cyber_bridge/bridge.py")
                self._docker_exec(f"pkill -f {pattern} >/dev/null 2>&1 || true", check=False)
            except Exception:
                pass
        self.control_proc = None
        self.bridge_proc = None
        self._snapshot_apollo_logs(artifacts)
        self._snapshot_apollo_bvar_dumps(artifacts)
        self._snapshot_apollo_internal_debug(artifacts)
        self._docker_stage_dir = None
        self._docker_stats_path = None
        self._docker_container_name = None
        self._runtime_source_prefix = None
        for fp_attr in ("_bridge_out", "_bridge_err", "_control_out", "_control_err"):
            fp = getattr(self, fp_attr, None)
            if fp is not None:
                try:
                    fp.close()
                except Exception:
                    pass
                setattr(self, fp_attr, None)

    def _snapshot_docker_modules_status(self, artifacts: Path, artifact_name: str) -> None:
        if not (self._docker_container_name and self._docker_start_modules_enabled()):
            return
        status_path = artifacts / artifact_name
        prefix = self._docker_modules_prefix()
        status_cmd = self._docker_modules_status_cmd()
        try:
            status = self._docker_exec(
                f"{prefix}; {status_cmd}",
                capture_output=True,
                check=False,
                user=self._docker_module_exec_user(),
            )
            raw_lines = self._module_status_stdout_lines(status.stdout)
            lines = self._runtime_module_status_lines(status.stdout)
            status_parts = [
                f"cmd: {status_cmd}",
                f"returncode: {status.returncode}",
                f"runtime_filtered_lines: {json.dumps(lines, ensure_ascii=False)}",
                "ignored_launcher_lines: "
                + json.dumps([line for line in raw_lines if line not in lines], ensure_ascii=False),
                f"--- stdout ---\n{status.stdout}",
                f"--- stderr ---\n{status.stderr}",
            ]
            status_path.write_text("\n".join(status_parts) + "\n")
        except Exception as exc:
            status_path.write_text(
                f"cmd: {status_cmd}\nstatus_check_error: {exc}\n",
                encoding="utf-8",
            )

    def _cleanup_stale_control_bridge(self, topic: str) -> None:
        pattern = (
            "algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py "
            f"--control-topic {topic} --control-type "
        )
        try:
            subprocess.run(["pkill", "-f", pattern], check=False)
        except Exception:
            pass

    def _cleanup_stale_apollo_bridge(self) -> None:
        patterns = [
            "tools/apollo10_cyber_bridge/bridge.py --config ",
            "tools/apollo10_cyber_bridge/bridge.py",
        ]
        if self._docker_stage_dir:
            patterns.append(f"{self._docker_stage_dir}/tools/apollo10_cyber_bridge/bridge.py")
        for pattern in patterns:
            try:
                subprocess.run(["pkill", "-f", pattern], check=False)
            except Exception:
                continue
