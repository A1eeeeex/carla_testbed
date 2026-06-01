import subprocess

from tbio.ros2.goal_engage import Ros2CommandRunner, send_goal_and_engage


class FakeGoalRunner(Ros2CommandRunner):
    def __init__(self, *, planning_ready: bool = True):
        self.planning_ready = planning_ready
        self.commands: list[str] = []

    def run_bash(self, bash_cmd: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"unexpected run_bash call: {bash_cmd}")

    def run_ros2(self, ros_cmd: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        self.commands.append(ros_cmd)
        if "topic info" in ros_cmd:
            return subprocess.CompletedProcess(ros_cmd, 0, "Subscription count: 1\n", "")
        if "service list" in ros_cmd:
            return subprocess.CompletedProcess(
                ros_cmd,
                0,
                "/api/routing/set_route_points\n/api/operation_mode/change_to_autonomous\n",
                "",
            )
        if "topic list" in ros_cmd:
            return subprocess.CompletedProcess(
                ros_cmd,
                0,
                "/planning/mission_planning/goal\n/planning/trajectory\n",
                "",
            )
        if "service type /api/routing/set_route_points" in ros_cmd:
            return subprocess.CompletedProcess(
                ros_cmd,
                0,
                "autoware_adapi_v1_msgs/srv/SetRoutePoints\n",
                "",
            )
        if "service call /api/routing/set_route_points" in ros_cmd:
            return subprocess.CompletedProcess(ros_cmd, 0, "success: true\n", "")
        if "topic pub --once" in ros_cmd and "/planning/mission_planning/goal" in ros_cmd:
            return subprocess.CompletedProcess(ros_cmd, 0, "published\n", "")
        if "topic echo --once /planning/trajectory" in ros_cmd:
            if self.planning_ready:
                return subprocess.CompletedProcess(ros_cmd, 0, "trajectory: {}\n", "")
            return subprocess.CompletedProcess(ros_cmd, 124, "", "timeout\n")
        if "service type /api/operation_mode/change_to_autonomous" in ros_cmd:
            return subprocess.CompletedProcess(
                ros_cmd,
                0,
                "autoware_adapi_v1_msgs/srv/ChangeOperationMode\n",
                "",
            )
        if "service call /api/operation_mode/change_to_autonomous" in ros_cmd:
            return subprocess.CompletedProcess(ros_cmd, 0, "success: true\n", "")
        return subprocess.CompletedProcess(ros_cmd, 0, "", "")


def _pose():
    return {"x": 1.0, "y": 2.0, "z": 0.0, "qx": 0.0, "qy": 0.0, "qz": 0.0, "qw": 1.0}


def test_send_goal_waits_for_planning_before_engage():
    runner = FakeGoalRunner(planning_ready=True)

    result = send_goal_and_engage(
        runner,
        _pose(),
        wait_timeout_s=1.0,
        engage_ready_topics=["/planning/trajectory"],
        engage_ready_wait_s=1.0,
        engage_ready_probe_timeout_s=1.0,
        engage_retry_timeout_s=0.0,
    )

    assert result.goal_sent is True
    assert result.engage_succeeded is True
    assert result.pre_engage_ready is True
    assert result.missing_engage_ready_topics == []
    wait_index = next(i for i, cmd in enumerate(runner.commands) if "topic echo --once /planning/trajectory" in cmd)
    engage_index = next(
        i for i, cmd in enumerate(runner.commands) if "service call /api/operation_mode/change_to_autonomous" in cmd
    )
    assert wait_index < engage_index


def test_send_goal_can_require_planning_before_engage():
    runner = FakeGoalRunner(planning_ready=False)

    result = send_goal_and_engage(
        runner,
        _pose(),
        wait_timeout_s=1.0,
        engage_ready_topics=["/planning/trajectory"],
        engage_ready_wait_s=1.0,
        engage_ready_probe_timeout_s=1.0,
        require_engage_ready_topics=True,
        engage_retry_timeout_s=0.0,
    )

    assert result.goal_sent is True
    assert result.engage_succeeded is False
    assert result.pre_engage_ready is False
    assert result.missing_engage_ready_topics == ["/planning/trajectory"]
    assert any("topic echo --once /planning/trajectory" in cmd for cmd in runner.commands)
    assert not any("service call /api/operation_mode/change_to_autonomous" in cmd for cmd in runner.commands)


def test_send_goal_can_skip_legacy_goal_topic_after_route_service():
    runner = FakeGoalRunner(planning_ready=True)

    result = send_goal_and_engage(
        runner,
        _pose(),
        wait_timeout_s=1.0,
        publish_goal_topic_after_route_service=False,
        engage_ready_topics=["/planning/trajectory"],
        engage_ready_wait_s=1.0,
        engage_ready_probe_timeout_s=1.0,
        engage_retry_timeout_s=0.0,
    )

    assert result.goal_sent is True
    assert result.route_set is True
    assert result.engage_succeeded is True
    assert not any(
        "topic pub --once" in cmd and "/planning/mission_planning/goal" in cmd for cmd in runner.commands
    )
