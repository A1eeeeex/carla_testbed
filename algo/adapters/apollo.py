from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

import yaml

from algo.adapters.base import Adapter
from tbio.backends.cyberrt import CyberRTBackend


class ApolloAdapter(Adapter):
    def __init__(self):
        self.repo_root = Path(__file__).resolve().parents[2]
        self.backend: CyberRTBackend | None = None

    def _ensure_bridge_config(self, profile: Dict[str, Any], run_dir: Path) -> Path:
        apollo_cfg = profile.setdefault("algo", {}).setdefault("apollo", {})
        template_path = apollo_cfg.get("bridge_template", "tools/apollo10_cyber_bridge/config_example.yaml")
        template_file = Path(template_path)
        if not template_file.is_absolute():
            template_file = (self.repo_root / template_file).resolve()
        if not template_file.exists():
            raise FileNotFoundError(f"apollo bridge template missing: {template_file}")

        cfg = yaml.safe_load(template_file.read_text()) or {}
        run_cfg = profile.get("run", {}) or {}
        io_ros = ((profile.get("io", {}) or {}).get("ros", {}) or {})
        bridge = (cfg.get("bridge", {}) or {})
        ros2_cfg = cfg.setdefault("ros2", {})
        cyber_cfg = cfg.setdefault("cyber", {})

        ego_id = str(run_cfg.get("ego_id", "hero"))
        namespace = str(io_ros.get("namespace", "/carla"))
        if not namespace.startswith("/"):
            namespace = "/" + namespace
        namespace = namespace.rstrip("/") or "/carla"
        prefix = f"{namespace}/{ego_id}"

        ros2_cfg["ego_id"] = ego_id
        ros2_cfg["namespace"] = namespace
        ros2_cfg["odom_topic"] = str(ros2_cfg.get("odom_topic") or f"{prefix}/odom")
        ros2_cfg["objects3d_topic"] = str(ros2_cfg.get("objects3d_topic") or f"{prefix}/objects3d")
        ros2_cfg["objects_markers_topic"] = str(
            ros2_cfg.get("objects_markers_topic") or f"{prefix}/objects_markers"
        )
        ros2_cfg["objects_json_topic"] = str(ros2_cfg.get("objects_json_topic") or f"{prefix}/objects_gt_json")
        ros2_cfg["control_out_topic"] = str(ros2_cfg.get("control_out_topic") or "/tb/ego/control_cmd")
        ros2_cfg["control_out_type"] = str(ros2_cfg.get("control_out_type") or "ackermann")

        cyber_cfg["localization_channel"] = str(
            cyber_cfg.get("localization_channel") or "/apollo/localization/pose"
        )
        cyber_cfg["chassis_channel"] = str(cyber_cfg.get("chassis_channel") or "/apollo/canbus/chassis")
        cyber_cfg["obstacles_channel"] = str(
            cyber_cfg.get("obstacles_channel") or "/apollo/perception/obstacles"
        )
        cyber_cfg["control_channel"] = str(cyber_cfg.get("control_channel") or "/apollo/control")
        cyber_cfg["routing_request_channel"] = str(
            cyber_cfg.get("routing_request_channel") or "/apollo/raw_routing_request"
        )
        cyber_cfg["action_channel"] = str(
            cyber_cfg.get("action_channel") or "/apollo/external_command/action"
        )
        cyber_cfg["lane_follow_channel"] = str(
            cyber_cfg.get("lane_follow_channel") or "/apollo/external_command/lane_follow"
        )
        cyber_cfg["routing_response_channel"] = str(
            cyber_cfg.get("routing_response_channel") or "/apollo/routing_response"
        )

        apollo_bridge_cfg = apollo_cfg.get("bridge", {}) or {}
        if "publish_rate_hz" in apollo_bridge_cfg:
            bridge["publish_rate_hz"] = float(apollo_bridge_cfg["publish_rate_hz"])
        if "max_obstacles" in apollo_bridge_cfg:
            bridge["max_obstacles"] = int(apollo_bridge_cfg["max_obstacles"])
        if "radius_m" in apollo_bridge_cfg:
            bridge["radius_m"] = float(apollo_bridge_cfg["radius_m"])

        auto_routing = bridge.setdefault("auto_routing", {})
        routing_cfg = apollo_cfg.get("routing", {}) or {}
        if "enable" in routing_cfg:
            auto_routing["enabled"] = bool(routing_cfg["enable"])
        if "end_ahead_m" in routing_cfg:
            auto_routing["end_ahead_m"] = float(routing_cfg["end_ahead_m"])
        if "resend_sec" in routing_cfg:
            auto_routing["resend_sec"] = float(routing_cfg["resend_sec"])
        if "max_attempts" in routing_cfg:
            auto_routing["max_attempts"] = int(routing_cfg["max_attempts"])
        if "freeze_after_success" in routing_cfg:
            auto_routing["freeze_after_success"] = bool(routing_cfg["freeze_after_success"])
        if "use_seed_heading" in routing_cfg:
            auto_routing["use_seed_heading"] = bool(routing_cfg["use_seed_heading"])
        if "clamp_to_map_bounds" in routing_cfg:
            auto_routing["clamp_to_map_bounds"] = bool(routing_cfg["clamp_to_map_bounds"])
        if "map_bounds_margin_m" in routing_cfg:
            auto_routing["map_bounds_margin_m"] = float(routing_cfg["map_bounds_margin_m"])

        tf = bridge.setdefault("carla_to_apollo", {})
        apollo_tf = apollo_cfg.get("carla_to_apollo", {}) or {}
        for key in ("tx", "ty", "tz", "yaw_deg"):
            if key in apollo_tf:
                tf[key] = float(apollo_tf[key])

        ctrl_map = bridge.setdefault("control_mapping", {})
        ctrl_cfg = apollo_cfg.get("control_mapping", {}) or {}
        for key in ("max_steer_angle", "speed_gain", "brake_gain", "throttle_scale", "brake_scale", "brake_deadzone"):
            if key in ctrl_cfg:
                ctrl_map[key] = float(ctrl_cfg[key])

        out_cfg = run_dir / "artifacts" / "apollo_bridge_effective.yaml"
        out_cfg.parent.mkdir(parents=True, exist_ok=True)
        out_cfg.write_text(yaml.safe_dump(cfg, sort_keys=False))
        return out_cfg

    def prepare(self, profile: Dict[str, Any], run_dir):
        run_path = Path(run_dir).resolve()
        run_path.mkdir(parents=True, exist_ok=True)
        artifacts = run_path / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)

        apollo_cfg = profile.setdefault("algo", {}).setdefault("apollo", {})
        apollo_cfg.setdefault("ros2_setup_script", "")
        apollo_cfg.setdefault("cyber_domain_id", 80)
        apollo_cfg.setdefault("cyber_ip", "")
        docker_cfg = apollo_cfg.setdefault("docker", {})
        docker_cfg.setdefault("container", os.environ.get("APOLLO_DOCKER_CONTAINER", ""))
        docker_cfg.setdefault("apollo_root_in_container", "/apollo")
        docker_cfg.setdefault("apollo_distribution_home", "/opt/apollo/neo")
        docker_cfg.setdefault("python_exec", "python3")
        docker_cfg.setdefault("bridge_in_container", False)
        docker_cfg.setdefault("auto_start_container", True)
        docker_cfg.setdefault("auto_install_runtime_deps", True)
        docker_cfg.setdefault("module_exec_user", "1000:1000")
        docker_cfg.setdefault("start_modules", False)
        docker_cfg.setdefault("start_modules_cmd", "")
        docker_cfg.setdefault("modules_status_cmd", "")
        docker_cfg.setdefault("required_modules", ["routing", "prediction", "planning", "control"])
        if "enabled" not in docker_cfg:
            docker_cfg["enabled"] = bool(docker_cfg.get("container"))

        bridge_cfg = self._ensure_bridge_config(profile, run_path)
        apollo_cfg["bridge_config_path"] = str(bridge_cfg)
        apollo_cfg["stats_path"] = str(artifacts / "cyber_bridge_stats.json")
        apollo_cfg.setdefault("pb_root", "tools/apollo10_cyber_bridge/pb")
        apollo_cfg.setdefault("carla_control_bridge", {})
        apollo_cfg["carla_control_bridge"].setdefault("enabled", True)

        profile.setdefault("artifacts", {})["dir"] = str(artifacts)
        profile["_apollo_run_dir"] = str(run_path)

        meta = {
            "bridge_config_path": str(bridge_cfg),
            "stats_path": apollo_cfg["stats_path"],
            "pb_root": str(apollo_cfg["pb_root"]),
            "apollo_root": apollo_cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT", ""),
            "docker": docker_cfg,
        }
        (artifacts / "apollo_adapter_meta.json").write_text(json.dumps(meta, indent=2))

    def start(self, profile: Dict[str, Any], run_dir):
        self.backend = CyberRTBackend(profile)
        return self.backend.start()

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        if self.backend is None:
            self.backend = CyberRTBackend(profile)
        return self.backend.health_check()

    def stop(self, profile: Dict[str, Any], run_dir):
        if self.backend is not None:
            self.backend.stop()
            self.backend = None

    def get_control_topics(self, profile: Dict[str, Any]):
        apollo_cfg = profile.get("algo", {}).get("apollo", {}) or {}
        bridge_cfg_path = apollo_cfg.get("bridge_config_path")
        if not bridge_cfg_path:
            return ["/tb/ego/control_cmd"]
        path = Path(bridge_cfg_path)
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        if not path.exists():
            return ["/tb/ego/control_cmd"]
        try:
            cfg = yaml.safe_load(path.read_text()) or {}
            topic = ((cfg.get("ros2", {}) or {}).get("control_out_topic")) or "/tb/ego/control_cmd"
            return [topic]
        except Exception:
            return ["/tb/ego/control_cmd"]
