from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from algo.adapters.base import Adapter


class AutowareAdapter(Adapter):
    def __init__(self):
        self.compose_proc: Optional[subprocess.Popen] = None
        self.repo_root = Path(__file__).resolve().parents[2]

    @staticmethod
    def _is_relative_to(path: Path, base: Path) -> bool:
        try:
            path.relative_to(base)
            return True
        except ValueError:
            return False

    def _refresh_artifacts(self, profile: Dict[str, Any], run_path: Path) -> None:
        artifacts_dir = run_path / "artifacts"
        artifacts = profile.setdefault("artifacts", {})
        artifacts.update(
            {
                "dir": str(artifacts_dir),
                "sensor_mapping": str(artifacts_dir / "sensor_mapping.yaml"),
                "sensor_kit_calibration": str(artifacts_dir / "sensor_kit_calibration.yaml"),
                "qos_overrides": str(artifacts_dir / "qos_overrides.yaml"),
                "frames": str(artifacts_dir / "frames.yaml"),
            }
        )

    def _ensure_run_dir(self, profile: Dict[str, Any], run_dir) -> Path:
        run_path = Path(run_dir).resolve()
        if not self._is_relative_to(run_path, self.repo_root):
            fallback = self.repo_root / "runs" / run_path.name
            fallback.mkdir(parents=True, exist_ok=True)
            if run_path.exists():
                shutil.copytree(run_path, fallback, dirs_exist_ok=True)
            print(f"[autoware] run_dir {run_path} is outside repo; using {fallback} for container IO")
            run_path = fallback.resolve()
            self._refresh_artifacts(profile, run_path)
        artifacts_dir = run_path / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        return run_path

    def prepare(self, profile: Dict[str, Any], run_dir):
        # assume artifacts already generated and paths recorded in profile["artifacts"]
        return

    def start(self, profile: Dict[str, Any], run_dir):
        stack_cfg = profile.get("algo", {}).get("autoware", {})
        compose = stack_cfg.get("compose")
        if not compose:
            print("[autoware] compose file missing in config")
            return
        compose_path = Path(compose).resolve()
        if not compose_path.exists():
            print(f"[autoware] compose not found: {compose_path}")
            return

        run_path = self._ensure_run_dir(profile, run_dir)
        control_topic = stack_cfg.get("control_topic") or "/control/command/control_cmd"
        if isinstance(control_topic, list):
            control_topic = control_topic[0]
        container_name = stack_cfg.get("container_name", "autoware")

        env = os.environ.copy()
        domain_id = profile.get("io", {}).get("ros", {}).get("domain_id", 0)
        env["ROS_DOMAIN_ID"] = str(domain_id)
        env.setdefault("USE_SIM_TIME", "true" if profile.get("io", {}).get("ros", {}).get("use_sim_time", True) else "false")
        artifacts_dir = profile.get("artifacts", {}).get("dir")
        if artifacts_dir:
            env["IO_ARTIFACTS_DIR"] = str(Path(artifacts_dir).resolve())
        compose_clean = profile.get("runtime", {}).get("compose_clean", False)
        if compose_clean:
            down_cmd = ["docker", "compose", "-f", str(compose_path), "down"]
            print(f"[autoware] {' '.join(down_cmd)} (compose_clean)")
            subprocess.call(down_cmd, env=env)

        up_cmd = ["docker", "compose", "-f", str(compose_path), "up", "-d"]
        print(f"[autoware] {' '.join(up_cmd)}")
        try:
            subprocess.check_call(up_cmd, env=env)
        except FileNotFoundError:
            print("[autoware] docker compose not available")
            return
        except subprocess.CalledProcessError as exc:
            print(f"[autoware] compose up failed: {exc}")
            return

        container_run_dir = Path("/work") / run_path.relative_to(self.repo_root)
        container_out = container_run_dir / "artifacts" / "autoware_control.jsonl"
        logger_path = "/work/io/ros2/tools/control_logger.py"

        check_cmd = ["docker", "exec", container_name, "bash", "-lc", f"test -f {logger_path}"]
        try:
            subprocess.check_call(check_cmd)
        except subprocess.CalledProcessError:
            print(f"[autoware] control_logger missing inside container at {logger_path}; ensure repo mounted to /work")
            raise SystemExit(1)

        ros_setup = (
            "source /opt/ros/humble/setup.bash; "
            "if [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
            "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
            "export PYTHONPATH=/work:${PYTHONPATH};"
        )
        log_cmd = (
            f"{ros_setup} "
            f"python {logger_path} --topic {control_topic} --out {container_out}"
        )
        logger_cmd = ["docker", "exec", "-d", container_name, "bash", "-lc", log_cmd]
        print(f"[autoware] {' '.join(logger_cmd)}")
        try:
            subprocess.check_call(logger_cmd)
        except subprocess.CalledProcessError as exc:
            print(f"[autoware] failed to start control_logger: {exc}")

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        stack_cfg = profile.get("algo", {}).get("autoware", {})
        compose = stack_cfg.get("compose")
        control_topic = stack_cfg.get("control_topic") or "/control/command/control_cmd"
        if isinstance(control_topic, list):
            control_topic = control_topic[0]
        container_name = stack_cfg.get("container_name", "autoware")
        if not compose:
            print("[autoware][healthcheck] compose not configured")
            return False

        compose_path = Path(compose).resolve()
        cmd = ["docker", "compose", "-f", str(compose_path), "ps"]
        try:
            ps_out = subprocess.check_output(cmd, text=True, timeout=15)
            print(ps_out)
            if "Up" not in ps_out:
                print("[autoware][healthcheck] container not Up; check compose logs")
                return False
        except Exception as exc:
            print(f"[autoware][healthcheck] compose ps failed: {exc}")
            return False

        ros_setup = (
            "source /opt/ros/humble/setup.bash; "
            "if [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
            "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
            "export PYTHONPATH=/work:${PYTHONPATH};"
        )

        def exec_check(cmd_str: str, desc: str, suggestion: str, timeout: int = 10):
            full_cmd = ["docker", "exec", container_name, "bash", "-lc", f"{ros_setup} {cmd_str}"]
            try:
                out = subprocess.check_output(full_cmd, text=True, timeout=timeout)
                print(out)
                return True, out
            except subprocess.TimeoutExpired:
                print(f"[autoware][healthcheck] {desc} timeout; {suggestion}")
                return False, ""
            except subprocess.CalledProcessError as exc:
                print(f"[autoware][healthcheck] {desc} failed: {exc}")
                print(f"  suggestion: {suggestion}")
                return False, ""
            except Exception as exc:
                print(f"[autoware][healthcheck] {desc} error: {exc}")
                print(f"  suggestion: {suggestion}")
                return False, ""

        ok, node_out = exec_check(
            "ros2 node list | grep -i carla",
            "autoware_carla_interface node presence",
            "ensure autoware_carla_interface launch is running in the container",
        )
        if not ok:
            return False

        ok, topics_out = exec_check(
            "ros2 topic list",
            "sensor topic discovery",
            "check bridge/ros2 launch; expected clock or sensor topics",
        )
        if not ok:
            return False

        sensor_topic = None
        for line in topics_out.splitlines():
            if any(key in line for key in ["/clock", "lidar", "points", "camera", "image", "imu", "gnss", "odom"]):
                sensor_topic = line.strip()
                break
        if not sensor_topic:
            print("[autoware][healthcheck] no sensor topics matched (/clock|lidar|points|camera|image|imu|gnss|odom)")
            print("  suggestion: verify CARLA bridge and sensors are publishing")
            return False
        else:
            print(f"[autoware][healthcheck] detected sensor topic: {sensor_topic}")

        ok, _ = exec_check(
            f"ros2 topic info -v {control_topic}",
            "control topic presence",
            f"algorithm should publish {control_topic}; ensure bridge config matches",
        )
        if not ok:
            return False

        # Optional: quick hz check to confirm traffic
        hz_topic = sensor_topic if sensor_topic != "/clock" else None
        if hz_topic:
            exec_check(
                f"timeout 3 ros2 topic hz {hz_topic}",
                "sensor topic rate",
                "check sensor bridge rate; optional info only",
                timeout=5,
            )
        return True

    def stop(self, profile: Dict[str, Any], run_dir):
        compose = profile.get("algo", {}).get("autoware", {}).get("compose")
        if not compose:
            return
        compose_path = Path(compose).resolve()
        cmd = ["docker", "compose", "-f", str(compose_path), "down"]
        try:
            subprocess.check_call(cmd)
        except Exception as exc:
            print(f"[autoware] stop failed: {exc}")

    def get_control_topics(self, profile: Dict[str, Any]):
        stack_cfg = profile.get("algo", {}).get("autoware", {})
        topic = stack_cfg.get("control_topic") or "/control/command/control_cmd"
        return topic if isinstance(topic, list) else [topic]
