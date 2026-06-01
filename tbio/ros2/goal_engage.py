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
        "set +u; source /opt/ros/humble/setup.bash; "
        "if [ -f /opt/autoware/setup.bash ]; then source /opt/autoware/setup.bash; "
        "elif [ -f /opt/autoware/install/setup.bash ]; then source /opt/autoware/install/setup.bash; "
        "elif [ -f /opt/Autoware/setup.bash ]; then source /opt/Autoware/setup.bash; "
        "elif [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
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
            status_text = text.lower()
            if proc.returncode == 0 and self.service in text and (
                "running" in status_text or " up " in f" {status_text} " or "\tup" in status_text
            ):
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
    localization_initialized: bool = False
    route_set: bool = False
    pre_engage_ready: Optional[bool] = None
    missing_engage_ready_topics: Optional[List[str]] = None


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
    try:
        proc = runner.run_ros2(cmd, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode(errors="replace")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode(errors="replace")
        log(f"[ros2][timeout] command exceeded {timeout:.1f}s: {cmd}")
        proc = subprocess.CompletedProcess(cmd, 124, stdout, stderr)
    if proc.stdout:
        log(proc.stdout.rstrip("\n"))
    if proc.stderr:
        log(proc.stderr.rstrip("\n"))
    return proc


def _run_quiet(
    runner: Ros2CommandRunner,
    cmd: str,
    log: Callable[[str], None],
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    try:
        return runner.run_ros2(cmd, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode(errors="replace")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode(errors="replace")
        log(f"[ros2][timeout] command exceeded {timeout:.1f}s: {cmd}")
        return subprocess.CompletedProcess(cmd, 124, stdout, stderr)


def _service_payloads(service_type: str, default_payload: str) -> List[str]:
    if service_type == "std_srvs/srv/Trigger":
        return ["{}"]
    if service_type == "std_srvs/srv/SetBool":
        return ["{data: true}"]
    if service_type.endswith("/Engage"):
        return ["{engage: true}", default_payload, "{}"]
    return [default_payload, "{}"]


def _topic_payloads(topic_type: str) -> List[str]:
    if topic_type == "std_msgs/msg/Bool":
        return ["{data: true}"]
    if topic_type.endswith("/Engage"):
        return ["{engage: true}", "{data: true}"]
    return ["{data: true}", "{engage: true}"]


def _pose_yaml(pose: Mapping[str, float]) -> str:
    return (
        "{position: {x: "
        + f"{pose['x']:.3f}, y: {pose['y']:.3f}, z: {pose['z']:.3f}"
        + "}, orientation: {x: "
        + f"{pose['qx']:.6f}, y: {pose['qy']:.6f}, z: {pose['qz']:.6f}, w: {pose['qw']:.6f}"
        + "}}"
    )


def _pose_stamped_yaml(pose: Mapping[str, float], frame_id: str) -> str:
    return "{header: {frame_id: '" + frame_id + "'}, pose: " + _pose_yaml(pose) + "}"


def _pose_with_covariance_stamped_yaml(pose: Mapping[str, float], frame_id: str) -> str:
    covariance = [0.0] * 36
    # Small but non-zero pose covariance mirrors common initialpose usage and
    # avoids presenting an impossible certainty to localization components.
    covariance[0] = 0.25
    covariance[7] = 0.25
    covariance[35] = 0.06853892326654787  # (15 deg)^2
    cov = "[" + ", ".join(f"{value:.9f}" for value in covariance) + "]"
    return (
        "{header: {frame_id: '"
        + frame_id
        + "'}, pose: {pose: "
        + _pose_yaml(pose)
        + ", covariance: "
        + cov
        + "}}"
    )


def _status_success(text: str) -> bool:
    lowered = text.replace(" ", "").lower()
    if "success=false" in lowered or "success:false" in lowered:
        return False
    return (
        "success=true" in lowered
        or "success:true" in lowered
        or "code=0" in lowered
        or "code:0" in lowered
    )


def _log_interface_summary(
    services: set[str],
    topics: set[str],
    log: Callable[[str], None],
) -> None:
    key_services = [
        "/localization/initialize",
        "/api/localization/initialize",
        "/api/routing/clear_route",
        "/api/routing/set_route_points",
        "/planning/mission_planning/mission_planner/set_waypoint_route",
        "/planning/set_waypoint_route",
        "/api/operation_mode/change_to_autonomous",
        "/api/autoware/set/engage",
    ]
    key_topics = [
        "/initialpose",
        "/planning/mission_planning/goal",
        "/api/routing/state",
        "/planning/trajectory",
        "/control/command/control_cmd",
        "/system/operation_mode/state",
        "/localization/kinematic_state",
        "/localization/pose_twist_fusion_filter/pose",
    ]
    present_services = [name for name in key_services if name in services]
    present_topics = [name for name in key_topics if name in topics]
    log(
        "[engage] interface summary: "
        f"services={len(services)} key_services={present_services}; "
        f"topics={len(topics)} key_topics={present_topics}"
    )


def _wait_topic_once(
    runner: Ros2CommandRunner,
    topic: str,
    log: Callable[[str], None],
    timeout_s: float,
) -> bool:
    proc = _run_quiet(
        runner,
        f"timeout {timeout_s:.1f} ros2 topic echo --once {shlex.quote(topic)}",
        log,
        timeout=timeout_s + 3.0,
    )
    if proc.returncode == 0 and (proc.stdout or "").strip():
        log(f"[wait] observed one message on {topic}")
        return True
    text = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if text:
        log(f"[wait] no usable sample on {topic}: {text[:500]}")
    else:
        log(f"[wait] no usable sample on {topic} within {timeout_s:.1f}s")
    return False


def _wait_topics_until_ready(
    runner: Ros2CommandRunner,
    topics: list[str],
    log: Callable[[str], None],
    *,
    timeout_s: float,
    probe_timeout_s: float = 5.0,
    label: str = "route",
) -> dict[str, bool]:
    """Poll required topics as a group instead of doing one long serial wait.

    Autoware startup and the CARLA GT bridge can become ready in either order.
    A serial 60s odom wait followed by a 30s tf wait can still miss the exact
    window where the bridge binds ego. This loop keeps re-checking missing
    topics until all are observed or the shared deadline expires.
    """
    ready = {topic: False for topic in topics}
    deadline = time.time() + max(float(timeout_s), 0.0)
    while time.time() < deadline and not all(ready.values()):
        made_progress = False
        for topic in topics:
            if ready[topic]:
                continue
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            wait_s = max(1.0, min(float(probe_timeout_s), remaining))
            if _wait_topic_once(runner, topic, log, timeout_s=wait_s):
                ready[topic] = True
                made_progress = True
        if not made_progress and time.time() < deadline and not all(ready.values()):
            time.sleep(0.5)
    missing = [topic for topic, is_ready in ready.items() if not is_ready]
    if missing:
        log(f"[wait] required topics not ready before {label}: missing={missing}")
    else:
        log(f"[wait] required topics ready before {label}: topics={topics}")
    return ready


def _publish_initial_pose(
    runner: Ros2CommandRunner,
    pose: Mapping[str, float],
    frame_id: str,
    log: Callable[[str], None],
) -> bool:
    payload = _pose_with_covariance_stamped_yaml(pose, frame_id)
    quoted_payload = shlex.quote(payload)
    cmds = [
        "ros2 topic pub --once --qos-reliability reliable "
        f"/initialpose geometry_msgs/msg/PoseWithCovarianceStamped {quoted_payload}",
        f"ros2 topic pub --once /initialpose geometry_msgs/msg/PoseWithCovarianceStamped {quoted_payload}",
    ]
    for idx, cmd in enumerate(cmds):
        proc = _run_and_log(runner, cmd, log, timeout=12)
        if proc.returncode == 0:
            log(f"[localization] initialpose published ({'reliable' if idx == 0 else 'default qos'})")
            return True
        text = ((proc.stdout or "") + "\n" + (proc.stderr or "")).lower()
        if idx == 0 and ("unrecognized arguments" in text or "unknown option" in text):
            log("[localization] retry initialpose without explicit qos flags")
            continue
    log("[localization] initialpose publish failed")
    return False


def _initialize_localization(
    runner: Ros2CommandRunner,
    services: set[str],
    pose: Mapping[str, float],
    frame_id: str,
    log: Callable[[str], None],
) -> bool:
    pose_cov_stamped = _pose_with_covariance_stamped_yaml(pose, frame_id)
    attempts = [
        (
            "/localization/initialize",
            "autoware_localization_msgs/srv/InitializeLocalization",
            "{pose_with_covariance: [" + pose_cov_stamped + "], method: 1}",
        ),
        (
            "/api/localization/initialize",
            "autoware_adapi_v1_msgs/srv/InitializeLocalization",
            "{pose: [" + pose_cov_stamped + "]}",
        ),
    ]
    initialized = False
    for service_name, fallback_type, payload in attempts:
        if service_name not in services:
            log(f"[localization] skip {service_name}: service unavailable")
            continue
        typ_proc = _run_and_log(runner, f"ros2 service type {shlex.quote(service_name)}", log, timeout=6)
        service_type = (typ_proc.stdout or "").strip() or fallback_type
        call_cmd = (
            f"ros2 service call {shlex.quote(service_name)} "
            f"{shlex.quote(service_type)} {shlex.quote(payload)}"
        )
        call_proc = _run_and_log(runner, call_cmd, log, timeout=15)
        text = (call_proc.stdout or "") + "\n" + (call_proc.stderr or "")
        if call_proc.returncode == 0 and _status_success(text):
            log(f"[localization] {service_name} accepted initial pose")
            initialized = True
        else:
            log(f"[localization] {service_name} did not report success")
    return initialized


def _set_route_points(
    runner: Ros2CommandRunner,
    services: set[str],
    pose: Mapping[str, float],
    frame_id: str,
    log: Callable[[str], None],
) -> bool:
    pose_yaml = _pose_yaml(pose)
    attempts = [
        (
            "/api/routing/set_route_points",
            "autoware_adapi_v1_msgs/srv/SetRoutePoints",
            "{header: {frame_id: '"
            + frame_id
            + "'}, option: {allow_goal_modification: true}, goal: "
            + pose_yaml
            + ", waypoints: []}",
        ),
        (
            "/planning/mission_planning/mission_planner/set_waypoint_route",
            "autoware_planning_msgs/srv/SetWaypointRoute",
            "{header: {frame_id: '"
            + frame_id
            + "'}, goal_pose: "
            + pose_yaml
            + ", waypoints: [], uuid: {uuid: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}, allow_modification: true}",
        ),
        (
            "/planning/set_waypoint_route",
            "autoware_planning_msgs/srv/SetWaypointRoute",
            "{header: {frame_id: '"
            + frame_id
            + "'}, goal_pose: "
            + pose_yaml
            + ", waypoints: [], uuid: {uuid: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}, allow_modification: true}",
        ),
    ]
    if "/api/routing/clear_route" in services:
        typ_proc = _run_and_log(runner, "ros2 service type /api/routing/clear_route", log, timeout=6)
        service_type = (typ_proc.stdout or "").strip() or "autoware_adapi_v1_msgs/srv/ClearRoute"
        _run_and_log(
            runner,
            f"ros2 service call /api/routing/clear_route {shlex.quote(service_type)} {{}}",
            log,
            timeout=10,
        )
    for service_name, fallback_type, payload in attempts:
        if service_name not in services:
            log(f"[route] skip {service_name}: service unavailable")
            continue
        typ_proc = _run_and_log(runner, f"ros2 service type {shlex.quote(service_name)}", log, timeout=6)
        service_type = (typ_proc.stdout or "").strip() or fallback_type
        call_cmd = (
            f"ros2 service call {shlex.quote(service_name)} "
            f"{shlex.quote(service_type)} {shlex.quote(payload)}"
        )
        call_proc = _run_and_log(runner, call_cmd, log, timeout=20)
        text = (call_proc.stdout or "") + "\n" + (call_proc.stderr or "")
        if call_proc.returncode == 0 and _status_success(text):
            log(f"[route] {service_name} accepted route goal")
            return True
        if "route is already set" in text.lower():
            # In failed Town01 Autoware runs this response appeared while
            # /planning/mission_planning/route never materialized, so keep
            # trying lower-level fallbacks instead of treating it as proof.
            log(f"[route] {service_name} reports route already set; continue fallback attempts")
            continue
        log(f"[route] {service_name} did not report success")
    return False


def send_goal_and_engage(
    runner: Ros2CommandRunner,
    pose: Mapping[str, float],
    *,
    frame_id: str = "map",
    goal_topic: str = "/planning/mission_planning/goal",
    publish_goal_topic_after_route_service: bool = True,
    initial_pose: Optional[Mapping[str, float]] = None,
    wait_timeout_s: float = 30.0,
    localization_wait_s: float = 12.0,
    tf_wait_s: float = 8.0,
    localization_retry_wait_s: float = 15.0,
    tf_retry_wait_s: float = 15.0,
    route_ready_wait_s: Optional[float] = None,
    engage_retry_timeout_s: float = 0.0,
    engage_retry_period_s: float = 2.0,
    engage_ready_topics: Optional[list[str]] = None,
    engage_ready_wait_s: Optional[float] = None,
    engage_ready_probe_timeout_s: float = 5.0,
    require_engage_ready_topics: bool = False,
    require_localization_before_route: bool = False,
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
    localization_initialized = False
    route_set = False
    deadline = time.time() + wait_timeout_s
    while time.time() < deadline:
        proc = _run_quiet(
            runner,
            f"timeout 5 ros2 topic info -v {shlex.quote(goal_topic)} || true",
            log,
            timeout=8,
        )
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
        return GoalEngageResult(False, False, subscriber_count, False, localization_initialized, route_set)

    services_proc = _run_quiet(runner, "ros2 service list || true", log, timeout=10)
    topics_proc = _run_quiet(runner, "ros2 topic list || true", log, timeout=10)
    services = set((services_proc.stdout or "").splitlines())
    topics = set((topics_proc.stdout or "").splitlines())
    _log_interface_summary(services, topics, log)

    if initial_pose is not None:
        initial_pose_sent = _publish_initial_pose(runner, initial_pose, frame_id, log)
        localization_initialized = _initialize_localization(runner, services, initial_pose, frame_id, log)
        if not localization_initialized and initial_pose_sent:
            log("[localization] continue after initialpose publish despite service initialization failure")
        if require_localization_before_route:
            wait_budget = (
                float(route_ready_wait_s)
                if route_ready_wait_s is not None
                else (
                    float(localization_wait_s)
                    + float(tf_wait_s)
                    + float(localization_retry_wait_s)
                    + float(tf_retry_wait_s)
                )
            )
            ready = _wait_topics_until_ready(
                runner,
                ["/localization/kinematic_state", "/tf"],
                log,
                timeout_s=max(1.0, wait_budget),
                probe_timeout_s=5.0,
                label="route",
            )
            odom_ready = ready["/localization/kinematic_state"]
            tf_ready = ready["/tf"]
        else:
            odom_ready = _wait_topic_once(
                runner,
                "/localization/kinematic_state",
                log,
                timeout_s=max(1.0, float(localization_wait_s)),
            )
            tf_ready = _wait_topic_once(runner, "/tf", log, timeout_s=max(1.0, float(tf_wait_s)))
        if require_localization_before_route and not (odom_ready and tf_ready):
            log(
                "[route] skip route request: localization/tf sample not ready "
                f"(odom_ready={odom_ready}, tf_ready={tf_ready})"
            )
            _append_logs(log_path, logs)
            return GoalEngageResult(
                False,
                True,
                subscriber_count,
                False,
                localization_initialized,
                False,
            )

    route_set = _set_route_points(runner, services, pose, frame_id, log)

    pose_yaml = _pose_stamped_yaml(pose, frame_id)
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
    pre_engage_ready: Optional[bool] = None
    missing_ready_topics: List[str] = []
    if route_set and not publish_goal_topic_after_route_service:
        goal_sent = True
        log("[goal] skip legacy goal topic publish after successful route service call")
    else:
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
        return GoalEngageResult(False, True, subscriber_count, False, localization_initialized, route_set)

    engage_ready_topics = [topic for topic in (engage_ready_topics or []) if str(topic).strip()]
    if engage_ready_topics:
        ready_wait_s = (
            float(engage_ready_wait_s)
            if engage_ready_wait_s is not None
            else max(float(engage_retry_timeout_s or 0.0), 0.0)
        )
        ready = _wait_topics_until_ready(
            runner,
            engage_ready_topics,
            log,
            timeout_s=max(0.0, ready_wait_s),
            probe_timeout_s=max(1.0, float(engage_ready_probe_timeout_s or 5.0)),
            label="engage",
        )
        missing_ready_topics = [topic for topic, is_ready in ready.items() if not is_ready]
        pre_engage_ready = not missing_ready_topics
        if missing_ready_topics:
            log(
                "[engage] pre-engage readiness incomplete: "
                f"missing={missing_ready_topics} require={bool(require_engage_ready_topics)}"
            )
            if require_engage_ready_topics:
                _append_logs(log_path, logs)
                return GoalEngageResult(
                    goal_sent,
                    True,
                    subscriber_count,
                    False,
                    localization_initialized,
                    route_set,
                    pre_engage_ready,
                    missing_ready_topics,
                )
        else:
            log(f"[engage] pre-engage readiness satisfied: topics={engage_ready_topics}")

    def _attempt_engage(services_snapshot: set[str], topics_snapshot: set[str]) -> bool:
        service_candidates = [
            ("/api/operation_mode/change_to_autonomous", "{}"),
            ("/api/autoware/set/engage", "{data: true}"),
        ]
        for service_name, default_payload in service_candidates:
            if service_name not in services_snapshot:
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
                text = (call_proc.stdout or "") + "\n" + (call_proc.stderr or "")
                if call_proc.returncode == 0 and _status_success(text):
                    return True

        topic_candidates = ["/autoware/engage", "/vehicle/engage"]
        for topic_name in topic_candidates:
            if topic_name not in topics_snapshot:
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
                    return True
        return False

    engage_succeeded = False
    engage_deadline = time.time() + max(0.0, float(engage_retry_timeout_s or 0.0))
    engage_attempt = 0
    while True:
        engage_attempt += 1
        if engage_attempt > 1:
            services_proc = _run_quiet(runner, "ros2 service list || true", log, timeout=10)
            topics_proc = _run_quiet(runner, "ros2 topic list || true", log, timeout=10)
            services = set((services_proc.stdout or "").splitlines())
            topics = set((topics_proc.stdout or "").splitlines())
            log(f"[engage] retry attempt {engage_attempt}")
        engage_succeeded = _attempt_engage(services, topics)
        if engage_succeeded:
            break
        if time.time() >= engage_deadline:
            break
        time.sleep(max(0.2, float(engage_retry_period_s or 2.0)))

    if not engage_succeeded:
        log("[engage] no engage endpoint succeeded")

    _append_logs(log_path, logs)
    return GoalEngageResult(
        goal_sent,
        True,
        subscriber_count,
        engage_succeeded,
        localization_initialized,
        route_set,
        pre_engage_ready,
        missing_ready_topics,
    )
