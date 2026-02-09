from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from algo.adapters.base import Adapter


class AutowareAdapter(Adapter):
    def __init__(self):
        self.compose_proc: Optional[subprocess.Popen] = None
        self.repo_root = Path(__file__).resolve().parents[2]
        self.last_env: Dict[str, str] = {}

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

    def _infer_map_root(self, stack_cfg: Dict[str, Any]) -> Tuple[Path, str]:
        """Infer map_root and carla_map from config."""
        map_root_cfg = stack_cfg.get("map_root")
        map_path_cfg = stack_cfg.get("map_path")
        carla_map = stack_cfg.get("carla_map") or "Town01"
        if map_root_cfg:
            root = Path(map_root_cfg).expanduser().resolve()
            return root, carla_map
        if map_path_cfg:
            p = Path(map_path_cfg).expanduser().resolve()
            if (p / "point_cloud_maps").exists() or (p / "vector_maps").exists():
                return p, carla_map
            # maybe town dir
            lane = list(p.glob("*.osm"))
            pcd = list(p.glob("*.pcd"))
            if lane or pcd:
                return p.parent, p.name
        raise ValueError(
            f"map_root/map_path invalid; expected a maps根目录(含 point_cloud_maps,vector_maps)或具体Town目录。传入: {map_path_cfg or map_root_cfg}"
        )

    def _preflight(self, map_root: Path, carla_map: str, carla_wheel_dir: Path, perception_mode: str, autoware_data_path: Optional[Path]):
        if not map_root.exists():
            raise FileNotFoundError(f"map_root not found: {map_root}")
        if not (map_root / "point_cloud_maps").exists() or not (map_root / "vector_maps").exists():
            raise FileNotFoundError(f"map_root missing subdirs point_cloud_maps/vector_maps: {map_root}")
        pc = map_root / "point_cloud_maps" / f"{carla_map}.pcd"
        osm = map_root / "vector_maps" / "lanelet2" / f"{carla_map}.osm"
        if not pc.exists() or not osm.exists():
            raise FileNotFoundError(f"map files missing for {carla_map}: {pc} or {osm}")
        if not carla_wheel_dir.exists():
            raise FileNotFoundError(f"carla_wheel_dir not found: {carla_wheel_dir}")
        wheels = list(carla_wheel_dir.glob("carla-0.9.16-cp3*.whl"))
        if not wheels:
            raise FileNotFoundError(f"carla wheels not found under {carla_wheel_dir}")
        if perception_mode == "ml":
            if not autoware_data_path or not autoware_data_path.exists():
                raise FileNotFoundError("enable_perception=true but autoware_data_path missing")
    def start(self, profile: Dict[str, Any], run_dir) -> bool:
        stack_cfg = profile.get("algo", {}).get("autoware", {})
        compose = stack_cfg.get("compose")
        if not compose:
            print("[autoware] compose file missing in config")
            return False
        if stack_cfg.get("is_direct", True) is False:
            print("[autoware] is_direct is false; skipping compose bring-up")
            return False
        compose_path = Path(compose).resolve()
        if not compose_path.exists():
            print(f"[autoware] compose not found: {compose_path}")
            return False

        run_path = self._ensure_run_dir(profile, run_dir)
        self._refresh_artifacts(profile, run_path)

        env = os.environ.copy()
        domain_id = profile.get("io", {}).get("ros", {}).get("domain_id", 0)
        env["ROS_DOMAIN_ID"] = str(domain_id)
        env.setdefault("USE_SIM_TIME", "true" if profile.get("io", {}).get("ros", {}).get("use_sim_time", True) else "false")
        if stack_cfg.get("rmw_implementation"):
            env["RMW_IMPLEMENTATION"] = stack_cfg["rmw_implementation"]
        artifacts_dir = Path(profile.get("artifacts", {}).get("dir", run_path / "artifacts")).resolve()
        try:
            run_rel = run_path.relative_to(self.repo_root)
            env["RUN_ARTIFACT_DIR"] = str(Path("/work") / run_rel / "artifacts")
        except ValueError:
            env["RUN_ARTIFACT_DIR"] = str(Path("/work/runs") / run_path.name / "artifacts")
        map_root, carla_map = self._infer_map_root(stack_cfg)
        autoware_map_root = map_root
        carla_wheel_dir = Path(stack_cfg.get("carla_wheel_dir", os.environ.get("CARLA_ROOT", "")) / Path("PythonAPI/carla/dist")).expanduser().resolve() if stack_cfg.get("carla_wheel_dir") is None else Path(stack_cfg.get("carla_wheel_dir")).expanduser().resolve()
        autoware_data_path_cfg = stack_cfg.get("autoware_data_path")
        autoware_data_path = (
            Path(autoware_data_path_cfg).expanduser().resolve()
            if autoware_data_path_cfg
            else Path("/tmp/autoware_data_empty")
        )
        mode = stack_cfg.get("mode", "gt_planning")
        perception_mode = stack_cfg.get("perception_mode")
        if perception_mode is None:
            if "enable_perception" in stack_cfg:
                perception_mode = "ml" if stack_cfg.get("enable_perception") else "off"
            else:
                perception_mode = "gt"
        enable_perception = perception_mode == "ml"
        autoware_data_path.mkdir(parents=True, exist_ok=True)
        try:
            self._preflight(autoware_map_root, carla_map, carla_wheel_dir, perception_mode, autoware_data_path if autoware_data_path else None)
        except Exception as exc:
            print(f"[autoware][preflight] {exc}")
            return False

        # perception mode compatibility
        env["AUTOWARE_MODE"] = str(mode)
        env["AUTOWARE_PERCEPTION_MODE"] = str(perception_mode)
        env["AUTOWARE_DISABLE_CAMERAS"] = "1" if stack_cfg.get("disable_cameras", perception_mode in ("gt", "off")) else "0"
        env["AUTOWARE_TRUTH_OBJECTS_TOPIC"] = str(stack_cfg.get("truth_objects_topic", "/perception/object_recognition/objects"))
        env["AUTOWARE_LAUNCH_FILE"] = str(stack_cfg.get("launch_file", "autoware.launch.xml"))
        env["AUTOWARE_LAUNCH_EXTRA_ARGS"] = str(
            stack_cfg.get(
                "launch_extra_args",
                "launch_sensing:=false launch_perception:=false launch_localization:=false "
                "launch_planning:=true launch_control:=true launch_map:=true launch_vehicle:=true",
            )
        )
        env["EGO_ROLE_NAME"] = str(profile.get("run", {}).get("ego_id", "ego"))
        env["CARLA_MAP"] = str(carla_map)
        env["AUTOWARE_MAP_ROOT"] = str(autoware_map_root)
        env["CARLA_WHEEL_DIR"] = str(carla_wheel_dir)
        env["AUTOWARE_ENABLE_PERCEPTION"] = "1" if enable_perception else "0"
        env["AUTOWARE_DATA_PATH"] = str(autoware_data_path)
        env["CARLA_HOST"] = str(stack_cfg.get("carla_host", "127.0.0.1"))
        env["CARLA_PORT"] = str(stack_cfg.get("carla_port", 2000))
        env["CONTROL_TOPIC"] = str(profile.get("record", {}).get("control_log", {}).get("topic", "/control/command/control_cmd"))
        probe_cfg = profile.get("record", {}).get("probe") or profile.get("record", {}).get("sensor_probe", {})
        probe_topics = probe_cfg.get("topics", ["/clock", "/tf"])
        env["PROBE_TOPICS"] = ",".join(probe_topics) if isinstance(probe_topics, list) else str(probe_topics)
        env["PROBE_MAX_MSGS"] = str(probe_cfg.get("max_msgs", 5))
        env["TESTBED_ROOT"] = str(self.repo_root)
        compose_clean = profile.get("runtime", {}).get("compose_clean", False)
        if compose_clean:
            down_cmd = ["docker", "compose", "-f", str(compose_path), "down"]
            print(f"[autoware] {' '.join(down_cmd)} (compose_clean)")
            subprocess.call(down_cmd, env=env)

        # dump env + compose config for debugging
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        env_dump_path = artifacts_dir / "autoware_env_dump.txt"
        extra_args_str = " ".join(env["AUTOWARE_LAUNCH_EXTRA_ARGS"].split()) if env.get("AUTOWARE_LAUNCH_EXTRA_ARGS") else ""
        env_dump_lines = [
            f"AUTOWARE_LAUNCH_FILE={env.get('AUTOWARE_LAUNCH_FILE','')}",
            f"AUTOWARE_LAUNCH_EXTRA_ARGS={extra_args_str}",
            f"AUTOWARE_MODE={env.get('AUTOWARE_MODE','')}",
            f"AUTOWARE_PERCEPTION_MODE={env.get('AUTOWARE_PERCEPTION_MODE','')}",
            f"RUN_ARTIFACT_DIR={env.get('RUN_ARTIFACT_DIR','')}",
            f"CARLA_HOST={env.get('CARLA_HOST','')}",
            f"CARLA_PORT={env.get('CARLA_PORT','')}",
            f"CARLA_MAP={env.get('CARLA_MAP','')}",
            f"EGO_ROLE_NAME={env.get('EGO_ROLE_NAME','')}",
        ]
        try:
            env_dump_path.write_text("\n".join(env_dump_lines))
        except Exception as exc:
            print(f"[autoware][warn] failed to write env dump: {exc}")

        compose_cfg_out = artifacts_dir / "autoware_compose_config.txt"
        try:
            cfg_out = subprocess.check_output([
                "docker", "compose", "-f", str(compose_path), "config"
            ], text=True, env=env, timeout=20)
            compose_cfg_out.write_text(cfg_out)
        except Exception as exc:
            compose_cfg_out.write_text(f"failed to dump compose config: {exc}\n")

        up_cmd = ["docker", "compose", "-f", str(compose_path), "up", "-d"]
        print(f"[autoware] {' '.join(up_cmd)}")
        try:
            subprocess.check_call(up_cmd, env=env)
        except FileNotFoundError:
            print("[autoware] docker compose not available")
            return False
        except subprocess.CalledProcessError as exc:
            print(f"[autoware] compose up failed: {exc}")
            return False

        ps_cmd = ["docker", "compose", "-f", str(compose_path), "ps"]
        try:
            ps_out = subprocess.check_output(ps_cmd, text=True, env=env)
            (artifacts_dir / "autoware_compose_ps.txt").write_text(ps_out)
            logs_out = subprocess.check_output(
                ["docker", "compose", "-f", str(compose_path), "logs", "--no-color", "--timestamps", "--tail", "300"],
                text=True,
                env=env,
                timeout=30,
            )
            (artifacts_dir / "autoware_compose_tail.log").write_text(logs_out)
            print(ps_out)
            if "Up" not in ps_out:
                print("[autoware][health] container not running; see autoware_compose_tail.log")
                return False
        except Exception as exc:
            print(f"[autoware][health] compose ps failed: {exc}")
            return False
        self.last_env = env
        return True

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
        artifacts_dir = Path(profile.get("artifacts", {}).get("dir", Path(run_dir) / "artifacts"))
        env = self.last_env or os.environ.copy()
        cmd = ["docker", "compose", "-f", str(compose_path), "ps"]
        try:
            ps_out = subprocess.check_output(cmd, text=True, timeout=15, env=env)
            print(ps_out)
            if "Up" not in ps_out:
                print("[autoware][healthcheck] container not Up; check compose logs")
                return False
        except Exception as exc:
            print(f"[autoware][healthcheck] compose ps failed: {exc}")
            return False

        ros_setup = (
            "source /opt/ros/humble/setup.bash; "
            "if [ -f /opt/autoware/install/setup.bash ]; then source /opt/autoware/install/setup.bash; "
            "elif [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
            "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
        )

        def exec_check(cmd_str: str, desc: str, suggestion: str, timeout: int = 10):
            full_cmd = ["docker", "compose", "-f", str(compose_path), "exec", "-T", container_name, "bash", "-lc", f"{ros_setup} {cmd_str}"]
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

        results = {}
        ok_carla, node_out = exec_check(
            "python3 - <<'PY'\nimport carla\nprint('carla import OK')\nPY",
            "carla python import",
            "ensure CARLA wheel installed inside container",
        )
        results["carla_python"] = ok_carla

        ok_node, _ = exec_check(
            "ros2 node list | grep -i carla",
            "autoware_carla_interface node presence",
            "ensure autoware_carla_interface launch is running in the container",
        )
        results["carla_interface"] = ok_node

        ok_topics, topics_out = exec_check(
            "ros2 topic list",
            "sensor topic discovery",
            "check bridge/ros2 launch; expected clock or sensor topics",
        )
        results["topic_list"] = ok_topics

        sensor_topic = None
        for line in (topics_out.splitlines() if topics_out else []):
            if any(key in line for key in ["/clock", "lidar", "points", "camera", "image", "imu", "gnss", "odom"]):
                sensor_topic = line.strip()
                break
        results["sensor_topic_detected"] = sensor_topic

        ok_ctrl, _ = exec_check(
            f"ros2 topic info -v {control_topic}",
            "control topic presence",
            f"algorithm should publish {control_topic}; ensure bridge config matches",
        )
        results["control_topic"] = ok_ctrl

        # Optional: quick hz check to confirm traffic
        hz_topic = sensor_topic if sensor_topic != "/clock" else None
        if hz_topic:
            ok_hz, hz_out = exec_check(
                f"timeout 3 ros2 topic hz {hz_topic}",
                "sensor topic rate",
                "check sensor bridge rate; optional info only",
                timeout=5,
            )
            results["sensor_hz"] = ok_hz
            results["sensor_hz_output"] = hz_out

        (artifacts_dir).mkdir(parents=True, exist_ok=True)
        try:
            (artifacts_dir / "autoware_healthcheck.json").write_text(yaml.safe_dump(results, sort_keys=False))
        except Exception as exc:
            print(f"[autoware][healthcheck] failed to write healthcheck file: {exc}")
        return all(results.values() or [False])

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
