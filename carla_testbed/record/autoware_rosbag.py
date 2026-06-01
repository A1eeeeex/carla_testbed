from __future__ import annotations

import json
import os
import signal
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dedupe_topics(topics: Sequence[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in topics:
        topic = str(raw or "").strip()
        if not topic or topic in seen:
            continue
        seen.add(topic)
        result.append(topic)
    return result


def build_autoware_demo_rosbag_topics(profile: Mapping[str, Any]) -> list[str]:
    record_cfg = profile.get("record", {}) if isinstance(profile.get("record", {}), Mapping) else {}
    rosbag_cfg = record_cfg.get("rosbag", {}) if isinstance(record_cfg.get("rosbag", {}), Mapping) else {}
    control_cfg = record_cfg.get("control_log", {}) if isinstance(record_cfg.get("control_log", {}), Mapping) else {}
    probe_cfg = record_cfg.get("probe") or record_cfg.get("sensor_probe") or {}
    if not isinstance(probe_cfg, Mapping):
        probe_cfg = {}

    topics: list[Any] = []
    if rosbag_cfg.get("include_tf", True):
        topics.extend(["/tf", "/tf_static"])
    if rosbag_cfg.get("include_clock", True):
        topics.append("/clock")

    explicit = rosbag_cfg.get("topics") or []
    extra = rosbag_cfg.get("extra_topics") or []
    if explicit:
        topics.extend(explicit)
    elif rosbag_cfg.get("auto_topics", True):
        topics.append(control_cfg.get("topic", "/control/command/control_cmd"))
        topics.extend(control_cfg.get("extra_topics") or [])
        topics.extend(probe_cfg.get("topics") or [])
    topics.extend(extra)
    return _dedupe_topics(topics)


def _container_path_for_repo_file(path: Path, repo_root: Path) -> str:
    resolved = path.resolve()
    try:
        rel = resolved.relative_to(repo_root.resolve())
        return str(Path("/work") / rel)
    except ValueError:
        return str(resolved)


@dataclass
class AutowareRosbagRecorder:
    compose_path: Path
    run_dir: Path
    artifacts_dir: Path
    repo_root: Path
    profile: Mapping[str, Any]
    env: Optional[Mapping[str, str]] = None
    container_name: str = "autoware"
    which: Callable[[str], Optional[str]] = shutil.which
    popen: Callable[..., subprocess.Popen] = subprocess.Popen
    run_cmd: Callable[..., subprocess.CompletedProcess] = subprocess.run

    def __post_init__(self) -> None:
        self.compose_path = Path(self.compose_path)
        self.run_dir = Path(self.run_dir)
        self.artifacts_dir = Path(self.artifacts_dir)
        self.repo_root = Path(self.repo_root)
        record_cfg = self.profile.get("record", {}) if isinstance(self.profile.get("record", {}), Mapping) else {}
        self.rosbag_cfg = record_cfg.get("rosbag", {}) if isinstance(record_cfg.get("rosbag", {}), Mapping) else {}
        out = self.rosbag_cfg.get("out") or self.run_dir / "rosbag2" / "autoware_demo"
        out_path = Path(str(out))
        if not out_path.is_absolute():
            out_path = self.run_dir / out_path
        self.out_path = out_path
        self.status_path = self.artifacts_dir / "autoware_rosbag_status.json"
        self.status_md_path = self.artifacts_dir / "autoware_rosbag_status.md"
        self.log_path = self.artifacts_dir / "autoware_rosbag.log"
        self.process: Optional[subprocess.Popen] = None
        self.topics = build_autoware_demo_rosbag_topics(self.profile)
        self.status: dict[str, Any] = {
            "schema_version": "autoware_rosbag_recording.v1",
            "enabled": bool(self.rosbag_cfg.get("enable", False)),
            "status": "initialized",
            "recording_success": False,
            "created_at": _utc_now(),
            "out_path": str(self.out_path),
            "topics": self.topics,
            "failure_messages": [],
            "warnings": [],
        }

    def _env(self) -> dict[str, str]:
        merged = dict(os.environ)
        if self.env:
            merged.update({str(k): str(v) for k, v in self.env.items()})
        return merged

    def build_command(self) -> list[str]:
        container_out = _container_path_for_repo_file(self.out_path, self.repo_root)
        cmd = ["ros2", "bag", "record"]
        storage = str(self.rosbag_cfg.get("storage") or "sqlite3")
        if storage:
            cmd.extend(["--storage", storage])
        compress = str(self.rosbag_cfg.get("compress") or "none")
        if compress and compress != "none":
            cmd.extend(["--compression-mode", "file", "--compression-format", compress])
        if self.rosbag_cfg.get("max_size_mb"):
            cmd.extend(["--max-bag-size", str(int(self.rosbag_cfg["max_size_mb"]) * 1024 * 1024)])
        if self.rosbag_cfg.get("max_duration_s"):
            cmd.extend(["--max-bag-duration", str(int(self.rosbag_cfg["max_duration_s"]))])
        cmd.extend(self.topics)
        cmd.extend(["-o", container_out])
        ros_cmd = " ".join(shlex.quote(part) for part in cmd)
        script = (
            "source /opt/ros/humble/setup.bash; "
            "if [ -f /opt/autoware/install/setup.bash ]; then source /opt/autoware/install/setup.bash; fi; "
            f"mkdir -p {shlex.quote(str(Path(container_out).parent))}; "
            f"{ros_cmd}"
        )
        return [
            "docker",
            "compose",
            "-f",
            str(self.compose_path),
            "exec",
            "-T",
            self.container_name,
            "bash",
            "-lc",
            script,
        ]

    def start(self) -> None:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        if not self.status["enabled"]:
            self._write_status("disabled")
            return
        if self.which("docker") is None:
            self.status["failure_messages"].append("docker command not found")
            self._write_status("failed")
            return
        if not self.topics:
            self.status["failure_messages"].append("no topics configured for Autoware rosbag")
            self._write_status("failed")
            return
        try:
            self.out_path.parent.mkdir(parents=True, exist_ok=True)
            command = self.build_command()
            self.status["command"] = command
            log = self.log_path.open("w", encoding="utf-8")
            self.process = self.popen(command, stdout=log, stderr=subprocess.STDOUT, env=self._env())
            self._write_status("started")
        except Exception as exc:
            self.status["failure_messages"].append(str(exc))
            self._write_status("failed")

    def stop(self) -> None:
        if self.process is not None:
            try:
                if self.process.poll() is None:
                    self.process.send_signal(signal.SIGINT)
                    self.process.wait(timeout=8)
            except Exception as exc:
                self.status["warnings"].append(f"rosbag SIGINT stop failed: {exc}")
                try:
                    self.process.terminate()
                    self.process.wait(timeout=3)
                except Exception:
                    try:
                        self.process.kill()
                    except Exception:
                        pass
            except BaseException:
                try:
                    self.process.kill()
                except Exception:
                    pass
                raise
        self._interrupt_container_rosbag()
        self.process = None
        if not self.status.get("enabled"):
            self._write_status("disabled")
            return
        if self._has_bag_files():
            self.status["recording_success"] = True
            self._write_status("success")
        elif self.status.get("status") == "started":
            self.status["failure_messages"].append("rosbag output was not generated")
            self._write_status("failed")
        else:
            self._write_status(str(self.status.get("status") or "failed"))

    def _interrupt_container_rosbag(self) -> None:
        if not self.status.get("enabled") or self.which("docker") is None:
            return
        cmd = [
            "docker",
            "compose",
            "-f",
            str(self.compose_path),
            "exec",
            "-T",
            self.container_name,
            "bash",
            "-lc",
            "pkill -INT -f 'ros2 bag record' || true",
        ]
        try:
            self.run_cmd(cmd, env=self._env(), capture_output=True, text=True, timeout=5, check=False)
        except Exception as exc:
            self.status["warnings"].append(f"container rosbag interrupt failed: {exc}")

    def _has_bag_files(self) -> bool:
        if not self.out_path.exists():
            return False
        if (self.out_path / "metadata.yaml").exists():
            return True
        return any(self.out_path.rglob("*.db3")) or any(self.out_path.rglob("*.mcap"))

    def _write_status(self, status: str) -> None:
        self.status["status"] = status
        self.status["updated_at"] = _utc_now()
        self.status_path.write_text(json.dumps(self.status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.status_md_path.write_text(self._render_markdown(), encoding="utf-8")

    def _render_markdown(self) -> str:
        lines = [
            "# Autoware ROS2 Bag Recording",
            "",
            f"- status: `{self.status.get('status')}`",
            f"- recording_success: `{self.status.get('recording_success')}`",
            f"- out_path: `{self.out_path}`",
            f"- topic_count: `{len(self.topics)}`",
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
