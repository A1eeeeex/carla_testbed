from __future__ import annotations

import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Mapping, Optional


def ros_env_setup_snippet() -> str:
    return (
        "source /opt/ros/humble/setup.bash; "
        "if [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
        "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
    )


class Ros2CommandRunner:
    def run_bash(self, bash_cmd: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        raise NotImplementedError

    def popen_bash(self, bash_cmd: str, **popen_kwargs) -> subprocess.Popen:
        raise NotImplementedError

    def run_ros2(self, ros_cmd: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        return self.run_bash(f"{ros_env_setup_snippet()} {ros_cmd}", timeout=timeout)

    def runtime_path(self, host_path: Path) -> str:
        return str(host_path)


class LocalRos2Runner(Ros2CommandRunner):
    def run_bash(self, bash_cmd: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "-lc", bash_cmd],
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    def popen_bash(self, bash_cmd: str, **popen_kwargs) -> subprocess.Popen:
        return subprocess.Popen(["bash", "-lc", bash_cmd], **popen_kwargs)


class ComposeRos2Runner(Ros2CommandRunner):
    def __init__(
        self,
        compose_file: Path,
        service: str = "autoware",
        repo_root: Optional[Path] = None,
        container_repo_root: Path = Path("/work"),
    ):
        self.compose_file = Path(compose_file).resolve()
        self.service = service
        self.repo_root = repo_root.resolve() if repo_root else None
        self.container_repo_root = Path(container_repo_root)

    def _exec_cmd(self, bash_cmd: str) -> List[str]:
        return [
            "docker",
            "compose",
            "-f",
            str(self.compose_file),
            "exec",
            "-T",
            self.service,
            "bash",
            "-lc",
            bash_cmd,
        ]

    def run_bash(self, bash_cmd: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self._exec_cmd(bash_cmd),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    def popen_bash(self, bash_cmd: str, **popen_kwargs) -> subprocess.Popen:
        return subprocess.Popen(self._exec_cmd(bash_cmd), **popen_kwargs)

    def wait_service_up(self, timeout_s: float = 30.0, poll_s: float = 1.0) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            proc = subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(self.compose_file),
                    "ps",
                    "--status",
                    "running",
                ],
                text=True,
                capture_output=True,
                timeout=10,
                check=False,
            )
            text = (proc.stdout or "") + (proc.stderr or "")
            if proc.returncode == 0 and self.service in text and "running" in text:
                return True
            time.sleep(poll_s)
        return False

    def runtime_path(self, host_path: Path) -> str:
        if self.repo_root is None:
            return str(host_path)
        try:
            rel = Path(host_path).resolve().relative_to(self.repo_root)
            return str(self.container_repo_root / rel)
        except Exception:
            return str(host_path)


@dataclass
class GoalEngageResult:
    goal_sent: bool
    goal_subscriber_ready: bool
    goal_subscriber_count: int
    engage_succeeded: bool


def _append_logs(log_path: Optional[Path], lines: List[str]) -> None:
    if not log_path:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n"))
            f.write("\n")


def _run_and_log(
    runner: Ros2CommandRunner,
    cmd: str,
    log: Callable[[str], None],
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    proc = runner.run_ros2(cmd, timeout=timeout)
    if proc.stdout:
        log(proc.stdout.rstrip("\n"))
    if proc.stderr:
        log(proc.stderr.rstrip("\n"))
    return proc


def _service_payloads(service_type: str, default_payload: str) -> List[str]:
    if service_type == "std_srvs/srv/Trigger":
        return ["{}"]
    if service_type == "std_srvs/srv/SetBool":
        return ["{data: true}"]
    return [default_payload, "{}"]


def _topic_payloads(topic_type: str) -> List[str]:
    if topic_type == "std_msgs/msg/Bool":
        return ["{data: true}"]
    if topic_type.endswith("/Engage"):
        return ["{engage: true}", "{data: true}"]
    return ["{data: true}", "{engage: true}"]


def send_goal_and_engage(
    runner: Ros2CommandRunner,
    pose: Mapping[str, float],
    *,
    frame_id: str = "map",
    goal_topic: str = "/planning/mission_planning/goal",
    wait_timeout_s: float = 30.0,
    log_path: Optional[Path] = None,
) -> GoalEngageResult:
    logs: List[str] = []

    def log(msg: str) -> None:
        print(msg)
        logs.append(msg)

    if isinstance(runner, ComposeRos2Runner):
        if not runner.wait_service_up(timeout_s=wait_timeout_s):
            log(f"[goal] service '{runner.service}' is not running within {wait_timeout_s:.0f}s")
            _append_logs(log_path, logs)
            return GoalEngageResult(False, False, 0, False)

    subscriber_count = 0
    deadline = time.time() + wait_timeout_s
    while time.time() < deadline:
        proc = _run_and_log(runner, f"ros2 topic info -v {shlex.quote(goal_topic)} || true", log, timeout=6)
        text = (proc.stdout or "") + "\n" + (proc.stderr or "")
        matched = re.search(r"Subscription count:\s*(\d+)", text)
        if matched:
            subscriber_count = int(matched.group(1))
            if subscriber_count >= 1:
                break
        time.sleep(1.0)
    if subscriber_count < 1:
        log(f"[goal] no subscriber for {goal_topic} within {wait_timeout_s:.0f}s")
        _append_logs(log_path, logs)
        return GoalEngageResult(False, False, subscriber_count, False)

    pose_yaml = (
        "{header:{frame_id: "
        + frame_id
        + "}, pose:{position:{x:"
        + f"{pose['x']:.3f},y:{pose['y']:.3f},z:{pose['z']:.3f}"
        + "}, orientation:{x:"
        + f"{pose['qx']:.6f},y:{pose['qy']:.6f},z:{pose['qz']:.6f},w:{pose['qw']:.6f}"
        + "}}}"
    )
    quoted_pose = shlex.quote(pose_yaml)
    pub_with_qos = (
        "ros2 topic pub --once --qos-reliability reliable "
        f"{shlex.quote(goal_topic)} geometry_msgs/msg/PoseStamped {quoted_pose}"
    )
    pub_without_qos = (
        "ros2 topic pub --once "
        f"{shlex.quote(goal_topic)} geometry_msgs/msg/PoseStamped {quoted_pose}"
    )

    goal_sent = False
    proc = _run_and_log(runner, pub_with_qos, log, timeout=20)
    if proc.returncode == 0:
        goal_sent = True
    else:
        text = ((proc.stdout or "") + "\n" + (proc.stderr or "")).lower()
        if "unrecognized arguments" in text or "unknown option" in text:
            log("[goal] retry publish without explicit qos flags")
            retry_proc = _run_and_log(runner, pub_without_qos, log, timeout=20)
            goal_sent = retry_proc.returncode == 0

    if not goal_sent:
        log("[goal] publish failed")
        _append_logs(log_path, logs)
        return GoalEngageResult(False, True, subscriber_count, False)

    services_proc = _run_and_log(runner, "ros2 service list || true", log, timeout=10)
    topics_proc = _run_and_log(runner, "ros2 topic list || true", log, timeout=10)
    services = set((services_proc.stdout or "").splitlines())
    topics = set((topics_proc.stdout or "").splitlines())
    log("[engage] detected services: " + ", ".join(sorted(services)) if services else "[engage] no services")
    log("[engage] detected topics: " + ", ".join(sorted(topics)) if topics else "[engage] no topics")

    engage_succeeded = False

    service_candidates = [
        ("/api/operation_mode/change_to_autonomous", "{}"),
        ("/api/autoware/set/engage", "{data: true}"),
    ]
    for service_name, default_payload in service_candidates:
        if engage_succeeded or service_name not in services:
            continue
        typ_proc = _run_and_log(
            runner,
            f"ros2 service type {shlex.quote(service_name)}",
            log,
            timeout=6,
        )
        service_type = (typ_proc.stdout or "").strip()
        if not service_type:
            log(f"[engage] skip {service_name}: service type unavailable")
            continue
        for payload in _service_payloads(service_type, default_payload):
            call_cmd = (
                f"ros2 service call {shlex.quote(service_name)} "
                f"{shlex.quote(service_type)} {shlex.quote(payload)}"
            )
            call_proc = _run_and_log(runner, call_cmd, log, timeout=12)
            if call_proc.returncode == 0:
                engage_succeeded = True
                break

    topic_candidates = ["/autoware/engage", "/vehicle/engage"]
    for topic_name in topic_candidates:
        if engage_succeeded or topic_name not in topics:
            continue
        typ_proc = _run_and_log(runner, f"ros2 topic type {shlex.quote(topic_name)}", log, timeout=6)
        topic_type = (typ_proc.stdout or "").strip()
        if not topic_type:
            log(f"[engage] skip {topic_name}: topic type unavailable")
            continue
        for payload in _topic_payloads(topic_type):
            pub_cmd = (
                "ros2 topic pub --once "
                f"{shlex.quote(topic_name)} {shlex.quote(topic_type)} {shlex.quote(payload)}"
            )
            pub_proc = _run_and_log(runner, pub_cmd, log, timeout=10)
            if pub_proc.returncode == 0:
                engage_succeeded = True
                break

    if not engage_succeeded:
        log("[engage] no engage endpoint succeeded")

    _append_logs(log_path, logs)
    return GoalEngageResult(goal_sent, True, subscriber_count, engage_succeeded)
