from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence


DEFAULT_AUTOWARE_RVIZ_IMAGE = "ghcr.io/autowarefoundation/autoware:universe-devel-cuda"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_capture_region(raw: str | None) -> tuple[int, int, int, int]:
    text = str(raw or "1280x720+0,0").strip()
    match = re.fullmatch(r"(\d+)x(\d+)\+(-?\d+),(-?\d+)", text)
    if not match:
        raise ValueError(f"capture_region must be WIDTHxHEIGHT+X,Y, got {text!r}")
    width, height, x, y = [int(item) for item in match.groups()]
    if width <= 0 or height <= 0:
        raise ValueError(f"capture_region width/height must be positive, got {text!r}")
    return width, height, x, y


def write_autoware_rviz_config(path: Path, *, fixed_frame: str = "map") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""Panels:
  - Class: rviz_common/Displays
    Name: Displays
  - Class: rviz_common/Views
    Name: Views
Visualization Manager:
  Class: ""
  Displays:
    - Class: rviz_default_plugins/Grid
      Enabled: true
      Name: Grid
    - Class: rviz_default_plugins/TF
      Enabled: true
      Name: TF
    - Class: rviz_default_plugins/Odometry
      Enabled: true
      Name: Localization
      Topic: /localization/kinematic_state
      Keep: 200
    - Class: rviz_default_plugins/MarkerArray
      Enabled: true
      Name: Planning Debug
      Topic: /planning/scenario_planning/trajectory_marker
      Queue Size: 10
    - Class: rviz_default_plugins/MarkerArray
      Enabled: true
      Name: Perception Objects
      Topic: /perception/object_recognition/objects/marker
      Queue Size: 10
  Global Options:
    Background Color: 38; 38; 38
    Fixed Frame: {fixed_frame}
    Frame Rate: 30
  Enabled: true
  Name: root
  Tools:
    - Class: rviz_default_plugins/Interact
    - Class: rviz_default_plugins/MoveCamera
    - Class: rviz_default_plugins/Select
    - Class: rviz_default_plugins/FocusCamera
  Views:
    Current:
      Class: rviz_default_plugins/Orbit
      Distance: 80
      Name: Orbit
      Pitch: 0.85
      Target Frame: {fixed_frame}
      Yaw: 0.0
Window Geometry:
  Width: 1280
  Height: 720
  X: 0
  Y: 0
""",
        encoding="utf-8",
    )


@dataclass
class AutowareOperatorViewRecorder:
    run_dir: Path
    artifacts_dir: Path
    config: Mapping[str, Any]
    ros_domain_id: int = 0
    rmw_implementation: str = "rmw_cyclonedds_cpp"
    env: Optional[Mapping[str, str]] = None
    which: Callable[[str], Optional[str]] = shutil.which
    popen: Callable[..., subprocess.Popen] = subprocess.Popen

    def __post_init__(self) -> None:
        self.run_dir = Path(self.run_dir)
        self.artifacts_dir = Path(self.artifacts_dir)
        self.video_dir = self.run_dir / "video" / "rviz"
        self.rviz_dir = self.run_dir / "config" / "rviz"
        self.rviz_config_path = self.rviz_dir / "autoware_operator_view.rviz"
        self.video_path = self.video_dir / "autoware_rviz.mp4"
        self.status_path = self.artifacts_dir / "autoware_operator_view_status.json"
        self.status_md_path = self.artifacts_dir / "autoware_operator_view_status.md"
        self.rviz_log_path = self.artifacts_dir / "autoware_rviz.log"
        self.ffmpeg_log_path = self.artifacts_dir / "autoware_rviz_ffmpeg.log"
        self.rviz_process: Optional[subprocess.Popen] = None
        self.ffmpeg_process: Optional[subprocess.Popen] = None
        self.status: dict[str, Any] = {
            "schema_version": "autoware_operator_view_recording.v1",
            "enabled": bool(self.config.get("enabled", False)),
            "status": "initialized",
            "recording_success": False,
            "created_at": _utc_now(),
            "failure_messages": [],
            "warnings": [],
            "video_path": str(self.video_path),
            "rviz_config_path": str(self.rviz_config_path),
        }

    def _env(self) -> dict[str, str]:
        merged = dict(os.environ)
        if self.env:
            merged.update({str(k): str(v) for k, v in self.env.items()})
        return merged

    def build_rviz_command(self) -> list[str]:
        image = str(self.config.get("docker_image") or DEFAULT_AUTOWARE_RVIZ_IMAGE)
        display = str(self.config.get("display") or self._env().get("DISPLAY") or "")
        container_name = str(self.config.get("container_name") or "autoware_operator_rviz")
        cmd = [
            "docker",
            "run",
            "--rm",
            "--name",
            container_name,
            "--net=host",
            "-e",
            f"DISPLAY={display}",
            "-e",
            f"ROS_DOMAIN_ID={self.ros_domain_id}",
            "-e",
            f"RMW_IMPLEMENTATION={self.rmw_implementation}",
            "-e",
            "QT_X11_NO_MITSHM=1",
            "-v",
            "/tmp/.X11-unix:/tmp/.X11-unix:rw",
            "-v",
            f"{self.rviz_dir.resolve()}:/rviz:ro",
        ]
        xauthority = self._env().get("XAUTHORITY")
        if xauthority and Path(xauthority).exists():
            cmd.extend(["-e", f"XAUTHORITY={xauthority}", "-v", f"{xauthority}:{xauthority}:ro"])
        cmd.extend(
            [
                image,
                "bash",
                "-lc",
                "source /opt/ros/humble/setup.bash && rviz2 -d /rviz/autoware_operator_view.rviz",
            ]
        )
        return cmd

    def build_ffmpeg_command(self) -> list[str]:
        width, height, x, y = parse_capture_region(str(self.config.get("capture_region") or "1280x720+0,0"))
        display = str(self.config.get("display") or self._env().get("DISPLAY") or ":0")
        fps = float(self.config.get("fps") or 10.0)
        return [
            "ffmpeg",
            "-y",
            "-video_size",
            f"{width}x{height}",
            "-framerate",
            str(fps),
            "-f",
            "x11grab",
            "-i",
            f"{display}+{x},{y}",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            str(self.video_path),
        ]

    def start(self) -> None:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.video_dir.mkdir(parents=True, exist_ok=True)
        if not self.status["enabled"]:
            self._write_status("disabled")
            return
        env = self._env()
        if not str(self.config.get("display") or env.get("DISPLAY") or "").strip():
            self.status["failure_messages"].append("DISPLAY is not set")
            self._write_status("failed")
            return
        if self.which("docker") is None:
            self.status["failure_messages"].append("docker command not found")
            self._write_status("failed")
            return
        if self.which("ffmpeg") is None:
            self.status["failure_messages"].append("ffmpeg command not found")
            self._write_status("failed")
            return
        try:
            write_autoware_rviz_config(
                self.rviz_config_path,
                fixed_frame=str(self.config.get("fixed_frame") or "map"),
            )
            rviz_cmd = self.build_rviz_command()
            ffmpeg_cmd = self.build_ffmpeg_command()
            self.status["rviz_command"] = rviz_cmd
            self.status["ffmpeg_command"] = ffmpeg_cmd
            rviz_log = self.rviz_log_path.open("w", encoding="utf-8")
            self.rviz_process = self.popen(rviz_cmd, stdout=rviz_log, stderr=subprocess.STDOUT, env=env)
            time.sleep(float(self.config.get("startup_delay_s") or 2.0))
            ffmpeg_log = self.ffmpeg_log_path.open("w", encoding="utf-8")
            self.ffmpeg_process = self.popen(ffmpeg_cmd, stdout=ffmpeg_log, stderr=subprocess.STDOUT, env=env)
            self._write_status("started")
        except Exception as exc:
            self.status["failure_messages"].append(str(exc))
            self._write_status("failed")

    def stop(self) -> None:
        self._stop_process(self.ffmpeg_process, "ffmpeg")
        self._stop_process(self.rviz_process, "rviz")
        self.ffmpeg_process = None
        self.rviz_process = None
        if not self.status.get("enabled"):
            self._write_status("disabled")
            return
        if self.video_path.exists() and self.video_path.stat().st_size > 0:
            self.status["recording_success"] = True
            self.status["video_size_bytes"] = self.video_path.stat().st_size
            self._write_status("success")
        elif self.status.get("status") == "started":
            self.status["failure_messages"].append("RViz recording video was not generated")
            self._write_status("failed")
        else:
            self._write_status(str(self.status.get("status") or "failed"))

    def _stop_process(self, process: Optional[subprocess.Popen], label: str) -> None:
        if process is None:
            return
        try:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=float(self.config.get("stop_timeout_s") or 5.0))
        except Exception as exc:
            self.status["warnings"].append(f"{label} stop failed: {exc}")
            try:
                process.kill()
            except Exception:
                pass

    def _write_status(self, status: str) -> None:
        self.status["status"] = status
        self.status["updated_at"] = _utc_now()
        self.status_path.write_text(json.dumps(self.status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.status_md_path.write_text(self._render_markdown(), encoding="utf-8")

    def _render_markdown(self) -> str:
        lines = [
            "# Autoware Operator View Recording",
            "",
            f"- status: `{self.status.get('status')}`",
            f"- recording_success: `{self.status.get('recording_success')}`",
            f"- video_path: `{self.video_path}`",
            f"- rviz_config_path: `{self.rviz_config_path}`",
        ]
        failures: Sequence[str] = self.status.get("failure_messages") or []
        if failures:
            lines.append("- failure_messages:")
            lines.extend(f"  - {item}" for item in failures)
        warnings: Sequence[str] = self.status.get("warnings") or []
        if warnings:
            lines.append("- warnings:")
            lines.extend(f"  - {item}" for item in warnings)
        return "\n".join(lines) + "\n"
