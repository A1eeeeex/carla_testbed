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
import time
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

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
        self._apollo_log_offsets: Dict[str, int] = {}
        self._carla_vehicle_param_override: Dict[str, Any] = {}

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

    def _docker_enabled(self) -> bool:
        cfg = self._docker_cfg()
        if "enabled" in cfg:
            return bool(cfg.get("enabled"))
        return bool(cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER"))

    def _docker_container(self) -> str:
        cfg = self._docker_cfg()
        name = str(cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER", "")).strip()
        if not name:
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

    @staticmethod
    def _apollo_core_log_files() -> tuple[str, ...]:
        return (
            "planning.INFO",
            "control.INFO",
            "routing.INFO",
            "external_command.INFO",
        )

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
            except Exception as exc:
                item["status"] = "error"
                item["error"] = str(exc)
            summary["files"][name] = item
        (artifacts / "apollo_log_snapshot_meta.json").write_text(json.dumps(summary, indent=2))
        self._apollo_log_offsets = {}

    def _resolve_dreamview_record_region(self, artifacts: Path) -> Optional[tuple[int, int, int, int]]:
        cfg = self._dreamview_record_cfg()
        region_mode = str(cfg.get("region_mode") or cfg.get("mode") or "window").strip().lower()
        if region_mode != "window":
            width = int(cfg.get("width") or 1920)
            height = int(cfg.get("height") or 1080)
            offset_x = int(cfg.get("offset_x") or 0)
            offset_y = int(cfg.get("offset_y") or 0)
            return width, height, offset_x, offset_y

        log_path = artifacts / "dreamview_record.log"
        title = str(cfg.get("window_title") or "Dreamview").strip() or "Dreamview"
        timeout_sec = float(cfg.get("window_search_timeout_sec") or 8.0)
        xdotool = shutil.which("xdotool")
        xwininfo = shutil.which("xwininfo")
        wmctrl = shutil.which("wmctrl")
        if not xwininfo:
            fallback = bool(cfg.get("fallback_to_screen", True))
            if fallback:
                width = int(cfg.get("width") or 1920)
                height = int(cfg.get("height") or 1080)
                offset_x = int(cfg.get("offset_x") or 0)
                offset_y = int(cfg.get("offset_y") or 0)
                log_path.write_text(
                    f"mode=window\nwindow_title={title}\nmissing_tools=xwininfo\n"
                    f"fallback_to_screen=true\nregion={width}x{height}+{offset_x},{offset_y}\n"
                )
                return width, height, offset_x, offset_y
            log_path.write_text(
                f"mode=window\nwindow_title={title}\nerror=missing xwininfo and fallback disabled\n"
            )
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
                        return int(width), int(height), int(offset_x), int(offset_y)
                    last_error = "failed_to_parse_window_geometry"
                else:
                    last_error = "window_not_found"
            except Exception as exc:
                last_error = str(exc)
            time.sleep(0.5)

        fallback = bool(cfg.get("fallback_to_screen", True))
        if fallback:
            width = int(cfg.get("width") or 1920)
            height = int(cfg.get("height") or 1080)
            offset_x = int(cfg.get("offset_x") or 0)
            offset_y = int(cfg.get("offset_y") or 0)
            log_path.write_text(
                f"mode=window\nwindow_title={title}\nerror={last_error}\n"
                f"fallback_to_screen=true\nregion={width}x{height}+{offset_x},{offset_y}\n"
            )
            return width, height, offset_x, offset_y
        log_path.write_text(
            f"mode=window\nwindow_title={title}\nerror={last_error}\nfallback_to_screen=false\n"
        )
        return None

    def _prepare_dreamview_tick_capture(self, artifacts: Path) -> None:
        cfg = self._dreamview_record_cfg()
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
            return

        video_root = artifacts.parent / "video" / "dreamview"
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
        with log_path.open("a") as fp:
            w, h, x, y = region
            fp.write(
                "capture_mode=tick_snapshot\n"
                f"tick_every_n={self._dreamview_tick_capture_every_n}\n"
                f"frames_dir={out_dir}\n"
                f"region={w}x{h}+{x},{y}\n"
            )
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
        log_path = artifacts / "dreamview_record.log"
        video_root = artifacts.parent / "video" / "dreamview"
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
        output = str(cfg.get("output") or "").strip()
        out_mp4 = Path(output).expanduser() if output else (video_root / "dreamview_capture.mp4")
        if not out_mp4.is_absolute():
            out_mp4 = (self.repo_root / out_mp4).resolve()
        out_mp4.parent.mkdir(parents=True, exist_ok=True)
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
                with log_path.open("a") as fp:
                    fp.write("finalize_mode=tick_snapshot\nencode_skipped=ffmpeg_not_found\n")
        else:
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
        self._dreamview_tick_capture_enabled = False
        self._dreamview_tick_capture_region = None
        self._dreamview_tick_capture_dir = None
        self._dreamview_tick_capture_index_path = None

    def _start_dreamview_recording(self, artifacts: Path) -> None:
        if not self._dreamview_record_enabled():
            return
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
            return
        cfg = self._dreamview_record_cfg()
        fps = int(cfg.get("fps") or 20)
        region = self._resolve_dreamview_record_region(artifacts)
        if region is None:
            print("[cyberrt] Dreamview recording skipped: browser window not found")
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
        output = str(cfg.get("output") or "").strip()
        out_path = Path(output).expanduser() if output else (artifacts / "dreamview_capture.mp4")
        if not out_path.is_absolute():
            out_path = (self.repo_root / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
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
            print(f"[cyberrt] Dreamview recording started: {out_path}")
        except Exception as exc:
            log_path.write_text(f"enabled=true\nstart_failed={exc}\n")
            print(f"[cyberrt] Dreamview recording failed: {exc}")
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
        if self._dreamview_record_mode == "tick_snapshot":
            self._finalize_dreamview_tick_capture(artifacts)
            self._dreamview_record_mode = "ffmpeg_realtime"
            return
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
        for fp_attr in ("_dreamview_record_out", "_dreamview_record_err"):
            fp = getattr(self, fp_attr, None)
            if fp is not None:
                try:
                    fp.close()
                except Exception:
                    pass
                setattr(self, fp_attr, None)
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

    def _open_url(self, url: str, artifacts: Path) -> None:
        url_file = artifacts / "dreamview_url.txt"
        url_file.write_text(url + "\n")
        candidates = []
        browser_cmd = str(self._dreamview_cfg().get("browser_cmd") or "").strip()
        if browser_cmd:
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
                (artifacts / "dreamview_open.log").write_text(
                    f"opened via: {' '.join(shlex.quote(x) for x in cmd)}\nurl={url}\n"
                )
                print(f"[cyberrt] Dreamview Plus URL opened: {url}")
                return
            except Exception as exc:
                (artifacts / "dreamview_open.log").write_text(
                    f"open failed via {' '.join(shlex.quote(x) for x in cmd)}: {exc}\nurl={url}\n"
                )
        print(f"[cyberrt] Dreamview Plus URL: {url}")

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
                (artifacts / "dreamview_open.log").write_text(
                    f"skip auto-open: Dreamview not reachable on {host}:{port} after {ready_timeout:.1f}s\n"
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
        default_cruise_speed = planning_cfg.get("default_cruise_speed_mps")
        planning_upper_speed_limit = planning_cfg.get("planning_upper_speed_limit_mps")
        overlay_path = "/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/planning.conf"
        target_path = "/apollo/modules/planning/planning_component/conf/planning.conf"
        if stitch is None and default_cruise_speed is None and planning_upper_speed_limit is None:
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
        if default_cruise_speed is not None:
            script_parts.append("prefixes.append('--default_cruise_speed='); ")
        if planning_upper_speed_limit is not None:
            script_parts.append("prefixes.append('--planning_upper_speed_limit='); ")
        script_parts.append("lines=[ln for ln in lines if not any(ln.startswith(prefix) for prefix in prefixes)]; ")
        if desired_stitch is not None:
            script_parts.append(f"lines.append('--enable_reference_line_stitching={desired_stitch}'); ")
        if default_cruise_speed is not None:
            script_parts.append(f"lines.append('--default_cruise_speed={float(default_cruise_speed):.3f}'); ")
        if planning_upper_speed_limit is not None:
            script_parts.append(
                f"lines.append('--planning_upper_speed_limit={float(planning_upper_speed_limit):.3f}'); "
            )
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
            raw = (
                "set -eo pipefail; "
                "pkill -9 -f 'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|external_command/process_component/dag/external_command_process.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' >/dev/null 2>&1 || true; "
                "sleep 1; "
                "overlay_conf_root=/apollo_workspace/conf_overlay; "
                "lf_conf_dir=${overlay_conf_root}/modules/planning/scenarios/lane_follow/conf; "
                "lf_stage_dir=${lf_conf_dir}/lane_follow_stage; "
                "mkdir -p \"$lf_stage_dir\" /apollo_workspace/log /apollo_workspace/dumps >/dev/null 2>&1 || true; "
                + self._docker_sim_vehicle_param_shell()
                + " "
                + self._lane_follow_overlay_shell()
                + self._traffic_rules_pipeline_overlay_shell()
                + self._traffic_light_rule_overlay_shell()
                + self._reference_line_end_overlay_shell()
                + self._planning_flags_overlay_shell()
                + self._speed_bounds_decider_overlay_shell()
                + self._speed_decider_overlay_shell()
                + self._public_road_planner_overlay_shell()
                + "declare -A lf_map=( "
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
                "); "
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
                "done; "
                "launches=(); "
                "[ -f /apollo/modules/routing/launch/routing.launch ] && launches+=(/apollo/modules/routing/launch/routing.launch); "
                "[ -f /apollo/modules/prediction/launch/prediction.launch ] && launches+=(/apollo/modules/prediction/launch/prediction.launch); "
                "[ -f /apollo/modules/planning/planning_component/launch/planning.launch ] && launches+=(/apollo/modules/planning/planning_component/launch/planning.launch); "
                "[ -f /apollo/modules/control/control_component/launch/control.launch ] && launches+=(/apollo/modules/control/control_component/launch/control.launch); "
                "if [ ${#launches[@]} -eq 0 ]; then echo 'no apollo launch files found under /apollo/modules'; exit 2; fi; "
                "mkdir -p /apollo_workspace/log >/dev/null 2>&1 || true; "
                "for lf in \"${launches[@]}\"; do "
                "setsid -f cyber_launch start \"$lf\" >/apollo_workspace/log/$(basename \"$lf\").tb.log 2>&1; "
                "sleep 1; "
                "done; "
                "sleep 1; "
                "pgrep -af 'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|external_command/process_component/dag/external_command_process.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' || true"
            )
        return " ".join(raw.split())

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
    ) -> subprocess.CompletedProcess:
        container = self._docker_container_name or self._docker_container()
        cmd = ["docker", "exec", "-i"]
        if user:
            cmd += ["-u", user]
        cmd += [container, "bash", "-lc", shell_cmd]
        return subprocess.run(cmd, check=check, capture_output=capture_output, text=text)

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
        out = self._docker_exec(cmd, capture_output=True, check=False)
        (artifacts / "apollo_prepare_user.log").write_text(
            f"cmd: {cmd}\nreturncode: {out.returncode}\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )

    def _docker_ensure_runtime_deps(self, artifacts: Path) -> None:
        check_log = artifacts / "apollo_mainboard_runtime_check.log"
        module_user = self._docker_module_exec_user()
        check_cmd = f"{self._docker_modules_prefix()}; mainboard --help"
        first = self._docker_exec(check_cmd, capture_output=True, check=False, user=module_user)
        check_log.write_text(
            f"check_cmd: {check_cmd}\n"
            f"returncode: {first.returncode}\n"
            f"--- stdout ---\n{first.stdout}\n"
            f"--- stderr ---\n{first.stderr}\n"
        )
        if first.returncode == 0:
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
        out = self._docker_exec(f"{prefix}; {cmd}", capture_output=True, check=False, user=module_user)
        log_path.write_text(
            f"cmd: {cmd}\nreturncode: {out.returncode}\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )
        if out.returncode != 0:
            raise RuntimeError(f"Apollo modules start failed, see {log_path}")
        status_cmd = self._docker_modules_status_cmd()
        status = self._docker_exec(f"{prefix}; {status_cmd}", capture_output=True, check=False, user=module_user)
        status_path.write_text(
            f"cmd: {status_cmd}\nreturncode: {status.returncode}\n--- stdout ---\n{status.stdout}\n--- stderr ---\n{status.stderr}\n"
        )
        if not status.stdout.strip():
            raise RuntimeError(
                "Apollo modules start command finished but no target module process was found; "
                f"see {status_path}"
            )
        lines = [line.strip() for line in status.stdout.splitlines() if line.strip()]
        pattern_map = {
            "routing": "modules/routing/dag/routing.dag",
            "prediction": "modules/prediction/dag/prediction.dag",
            "planning": "modules/planning/planning_component/dag/planning.dag",
            "control": "modules/control/control_component/dag/control.dag",
        }
        missing = []
        for name in self._docker_required_modules():
            patt = pattern_map.get(name, name)
            if not any(patt in line for line in lines):
                missing.append(name)
        if missing:
            raise RuntimeError(
                "Apollo modules are missing after startup: "
                + ",".join(missing)
                + f" (see {status_path})"
            )

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
        subprocess.check_call(
            ["docker", "cp", str(bridge_py), f"{container}:{stage_bridge_dir}/bridge.py"]
        )
        subprocess.check_call(
            ["docker", "cp", str(bridge_cfg), f"{container}:{stage_cfg}"]
        )
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
        libs_dir = artifacts / "apollo_docker_libs"
        libs_dir.mkdir(parents=True, exist_ok=True)
        list_cmd = (
            "for p in /usr/local/lib/libbvar.so* /usr/local/lib/libbrpc.so* /usr/local/lib/libbutil.so*; "
            'do [ -e "$p" ] && echo "$p"; done'
        )
        out = self._docker_exec(list_cmd, capture_output=True, check=False)
        paths = [line.strip() for line in out.stdout.splitlines() if line.strip()]
        if not paths:
            return None
        copied: list[str] = []
        for path in paths:
            try:
                subprocess.check_call(["docker", "cp", f"{self._docker_container_name}:{path}", str(libs_dir)])
                copied.append(path)
            except Exception:
                continue
        if not copied:
            return None
        (artifacts / "apollo_docker_libs.txt").write_text("\n".join(copied) + "\n")
        return libs_dir

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

    def _apollo_root(self) -> Path:
        cfg = self._apollo_cfg()
        root = cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT")
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
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        ros2_setup = self._host_ros2_setup_script()
        if ros2_setup:
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
        bridge_root = self.repo_root / "tools" / "apollo10_cyber_bridge"
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
        return "; ".join(parts)

    def _docker_source_prefix(self, bridge_root: str, pb_root: str) -> str:
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
        self._carla_vehicle_param_override = self._probe_carla_vehicle_param_override(artifacts)
        self._ensure_pb_ready()
        self._capture_apollo_log_offsets(artifacts)

        apollo_cfg = self._apollo_cfg()
        bridge_py = self.repo_root / "tools" / "apollo10_cyber_bridge" / "bridge.py"
        if not bridge_py.exists():
            raise RuntimeError(f"bridge.py not found: {bridge_py}")

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
            self._docker_container_name = self._docker_container()
            self._docker_check_running(self._docker_container_name)
            self._docker_check_runtime_requirements(self._docker_container_name, artifacts)
            self._docker_prepare_user(artifacts)
            self._docker_ensure_runtime_deps(artifacts)
            self._docker_start_modules(artifacts)
            self._maybe_start_dreamview(artifacts)
            bridge_in_container = self._docker_bridge_in_container()
            print(
                f"[cyberrt] docker mode enabled, container={self._docker_container_name}, "
                f"bridge_in_container={bridge_in_container}"
            )
        else:
            self._maybe_start_dreamview(artifacts)

        if bridge_in_container:
            stage_bridge_dir, stage_cfg, stage_pb_root, stage_stats = self._docker_prepare_stage(bridge_cfg, bridge_py)
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
            python_exec = shlex.quote(sys.executable)
            self._runtime_source_prefix = self._source_prefix()
            if self._docker_enabled():
                copied_libs = self._docker_sync_host_libs(artifacts)
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
        self.bridge_proc = self._launch(
            "bridge",
            bridge_cmd,
            artifacts / "cyber_bridge.out.log",
            artifacts / "cyber_bridge.err.log",
            source_prefix=self._runtime_source_prefix,
            docker_container=docker_container,
        )
        print(f"[cyberrt] bridge started pid={self.bridge_proc.pid}")

        control_bridge_cfg = apollo_cfg.get("carla_control_bridge", {}) or {}
        if control_bridge_cfg.get("enabled", True):
            control_py = self.repo_root / "algo" / "nodes" / "control" / "carla_control_bridge" / "ros2_autoware_to_carla.py"
            if control_py.exists():
                control_out_type = str(bridge_ros_cfg.get("control_out_type", "ackermann")).lower()
                if control_out_type not in {"ackermann", "twist", "direct"}:
                    print(f"[cyberrt][warn] unsupported control_out_type={control_out_type}; skip built-in control bridge")
                    return self.bridge_proc is not None and self.bridge_proc.poll() is None
                carla_cfg = (self.profile.get("runtime", {}) or {}).get("carla", {}) or {}
                topic = str(control_bridge_cfg.get("control_topic", "/tb/ego/control_cmd"))
                default_apply_hz = self._default_control_apply_hz()
                self._cleanup_stale_control_bridge(topic)
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
                self.control_proc = self._launch(
                    "control",
                    control_cmd,
                    artifacts / "cyber_control_bridge.out.log",
                    artifacts / "cyber_control_bridge.err.log",
                    source_prefix=self._runtime_source_prefix if not docker_container else "",
                )
                print(f"[cyberrt] control bridge started pid={self.control_proc.pid}")
            else:
                print(f"[cyberrt][warn] control bridge script not found: {control_py}")

        time.sleep(1.0)
        return self.bridge_proc is not None and self.bridge_proc.poll() is None

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

    def on_sim_tick(self, frame_id: int, timestamp: Optional[float] = None, step: Optional[int] = None) -> None:
        if self._dreamview_record_mode != "tick_snapshot":
            return
        self._capture_dreamview_tick_snapshot(int(frame_id), timestamp)

    def _maybe_capture_channels(self, artifacts: Path) -> Dict[str, Any]:
        cfg = self._apollo_cfg()
        hc_cfg = cfg.get("healthcheck", {}) or {}
        if not hc_cfg.get("probe_channels", False):
            return {"enabled": False}
        prefix = self._runtime_source_prefix or self._source_prefix()
        cmd = f"{prefix}; timeout 5 cyber_channel list" if prefix else "timeout 5 cyber_channel list"
        out_path = artifacts / "cyber_channel_list.txt"
        try:
            if self._docker_container_name:
                out = self._docker_exec(cmd, capture_output=True).stdout
            else:
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
        status: Dict[str, Any] = {
            "bridge_running": self.bridge_proc is not None and self.bridge_proc.poll() is None,
            "control_running": self.control_proc is not None and self.control_proc.poll() is None,
            "stats_path": str(stats_path),
            "stats_exists": stats_exists,
            "checked_at_sec": time.time(),
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
        self._terminate(self.control_proc)
        self._terminate(self.bridge_proc)
        self._cleanup_stale_control_bridge(topic)
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
        if self._docker_container_name and self._docker_stage_dir:
            try:
                pattern = shlex.quote(f"{self._docker_stage_dir}/tools/apollo10_cyber_bridge/bridge.py")
                self._docker_exec(f"pkill -f {pattern} >/dev/null 2>&1 || true", check=False)
            except Exception:
                pass
        self.control_proc = None
        self.bridge_proc = None
        self._snapshot_apollo_logs(artifacts)
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
        ]
        if self._docker_stage_dir:
            patterns.append(f"{self._docker_stage_dir}/tools/apollo10_cyber_bridge/bridge.py")
        for pattern in patterns:
            try:
                subprocess.run(["pkill", "-f", pattern], check=False)
            except Exception:
                continue
