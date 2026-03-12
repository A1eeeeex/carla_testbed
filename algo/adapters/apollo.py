from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict

import yaml

from algo.adapters.base import Adapter
from tbio.backends.cyberrt import CyberRTBackend


class ApolloAdapter(Adapter):
    def __init__(self):
        self.repo_root = Path(__file__).resolve().parents[2]
        self.backend: CyberRTBackend | None = None

    def _resolve_apollo_root(self, apollo_cfg: Dict[str, Any]) -> Path | None:
        raw = str(apollo_cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT", "")).strip()
        if not raw:
            return None
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        return path.resolve()

    def _apollo_map_dir(self, apollo_root: Path | None, map_name: str) -> Path | None:
        if apollo_root is None or not map_name:
            return None
        map_tokens = [map_name, map_name.lower(), f"carla_{map_name.lower()}"]
        candidates: list[Path] = []
        for token in map_tokens:
            candidates.append(apollo_root / "modules" / "map" / "data" / token)
            candidates.append((apollo_root / ".." / "share" / "modules" / "map" / "data" / token).resolve())
        app_core_root = None
        parents = list(apollo_root.parents)
        if len(parents) >= 6:
            app_core_root = parents[5]
        if app_core_root is not None:
            for token in map_tokens:
                candidates.append((app_core_root / "data" / "map_data" / token).resolve())
                candidates.append((app_core_root / "data" / "map-data" / token).resolve())
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0] if candidates else None

    def _ensure_map_bounds_file(self, map_dir: Path | None, map_name: str, artifacts: Path) -> Path:
        out = self.repo_root / "configs" / "io" / "maps" / map_name / "map_bounds.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            return out
        if map_dir is None or not map_dir.exists():
            log = {
                "map_dir": str(map_dir) if map_dir is not None else "",
                "output": str(out),
                "returncode": None,
                "stdout": "",
                "stderr": "map_dir_missing",
            }
            (artifacts / "map_bounds_generation.json").write_text(json.dumps(log, indent=2))
            return out
        gen_script = self.repo_root / "tools" / "gen_map_bounds.py"
        if not gen_script.exists():
            return out
        proc = subprocess.run(
            [
                "python3",
                str(gen_script),
                "--map_dir",
                str(map_dir),
                "--output",
                str(out),
            ],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        log = {
            "map_dir": str(map_dir),
            "output": str(out),
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
        (artifacts / "map_bounds_generation.json").write_text(json.dumps(log, indent=2))
        return out

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
        artifacts = run_dir / "artifacts"
        io_ros = ((profile.get("io", {}) or {}).get("ros", {}) or {})
        bridge = (cfg.get("bridge", {}) or {})
        ros2_cfg = cfg.setdefault("ros2", {})
        cyber_cfg = cfg.setdefault("cyber", {})
        apollo_root = self._resolve_apollo_root(apollo_cfg)
        map_name = str(run_cfg.get("map", "")).strip()
        map_dir = self._apollo_map_dir(apollo_root, map_name)

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
        ros2_cfg["control_out_topic"] = str(
            apollo_cfg.get("control_out_topic")
            or ros2_cfg.get("control_out_topic")
            or "/tb/ego/control_cmd"
        )
        ros2_cfg["control_out_type"] = str(
            apollo_cfg.get("control_out_type")
            or ros2_cfg.get("control_out_type")
            or "direct"
        )

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
        if "localization_back_offset_m" in apollo_bridge_cfg:
            bridge["localization_back_offset_m"] = float(apollo_bridge_cfg["localization_back_offset_m"])
        if "debug_pose_print" in apollo_bridge_cfg:
            bridge["debug_pose_print"] = bool(apollo_bridge_cfg["debug_pose_print"])
        if "debug_dump_control_raw" in apollo_bridge_cfg:
            bridge["debug_dump_control_raw"] = bool(apollo_bridge_cfg["debug_dump_control_raw"])
        if "map_file" in apollo_bridge_cfg:
            bridge["map_file"] = str(apollo_bridge_cfg["map_file"])
        if "map_bounds_file" in apollo_bridge_cfg:
            bridge["map_bounds_file"] = str(apollo_bridge_cfg["map_bounds_file"])
        if isinstance(apollo_bridge_cfg.get("front_obstacle_behavior"), dict):
            bridge["front_obstacle_behavior"] = dict(apollo_bridge_cfg["front_obstacle_behavior"])

        if map_dir is not None and not str(bridge.get("map_file", "")).strip():
            selected_map_file = ""
            for candidate in ("base_map.txt", "base_map.xml", "sim_map.txt", "sim_map.xml"):
                path = map_dir / candidate
                if path.exists():
                    selected_map_file = str(path)
                    break
            if selected_map_file:
                bridge["map_file"] = selected_map_file
            elif not str(bridge.get("map_file", "")).strip():
                bridge["map_file"] = str(map_dir / "base_map.txt")
        if not str(bridge.get("map_bounds_file", "")).strip():
            bounds_file = self._ensure_map_bounds_file(map_dir, map_name or "unknown", artifacts)
            bridge["map_bounds_file"] = str(bounds_file)

        auto_routing = bridge.setdefault("auto_routing", {})
        routing_cfg = apollo_cfg.get("routing", {}) or {}
        if "enable" in routing_cfg:
            auto_routing["enabled"] = bool(routing_cfg["enable"])
        if "goal_mode" in routing_cfg:
            auto_routing["goal_mode"] = str(routing_cfg["goal_mode"])
        if "end_ahead_m" in routing_cfg:
            auto_routing["end_ahead_m"] = float(routing_cfg["end_ahead_m"])
        if "min_end_ahead_m" in routing_cfg:
            auto_routing["min_end_ahead_m"] = float(routing_cfg["min_end_ahead_m"])
        if "startup_end_ahead_m" in routing_cfg:
            auto_routing["startup_end_ahead_m"] = float(routing_cfg["startup_end_ahead_m"])
        if "startup_speed_threshold_mps" in routing_cfg:
            auto_routing["startup_speed_threshold_mps"] = float(routing_cfg["startup_speed_threshold_mps"])
        if "startup_hold_sec" in routing_cfg:
            auto_routing["startup_hold_sec"] = float(routing_cfg["startup_hold_sec"])
        if "start_nudge_m" in routing_cfg:
            auto_routing["start_nudge_m"] = float(routing_cfg["start_nudge_m"])
        if "start_nudge_retry_step_m" in routing_cfg:
            auto_routing["start_nudge_retry_step_m"] = float(routing_cfg["start_nudge_retry_step_m"])
        if "start_nudge_min_safe_m" in routing_cfg:
            auto_routing["start_nudge_min_safe_m"] = float(routing_cfg["start_nudge_min_safe_m"])
        if "start_nudge_max_m" in routing_cfg:
            auto_routing["start_nudge_max_m"] = float(routing_cfg["start_nudge_max_m"])
        if "resend_sec" in routing_cfg:
            auto_routing["resend_sec"] = float(routing_cfg["resend_sec"])
        if "max_attempts" in routing_cfg:
            auto_routing["max_attempts"] = int(routing_cfg["max_attempts"])
        if "target_speed_mps" in routing_cfg:
            auto_routing["target_speed_mps"] = float(routing_cfg["target_speed_mps"])
        if "startup_delay_sec" in routing_cfg:
            auto_routing["startup_delay_sec"] = float(routing_cfg["startup_delay_sec"])
        if "lane_follow_refresh_sec" in routing_cfg:
            auto_routing["lane_follow_refresh_sec"] = float(routing_cfg["lane_follow_refresh_sec"])
        if "freeze_after_success" in routing_cfg:
            auto_routing["freeze_after_success"] = bool(routing_cfg["freeze_after_success"])
        if "use_seed_heading" in routing_cfg:
            auto_routing["use_seed_heading"] = bool(routing_cfg["use_seed_heading"])
        if "use_long_goal_after_move" in routing_cfg:
            auto_routing["use_long_goal_after_move"] = bool(routing_cfg["use_long_goal_after_move"])
        if "clamp_to_map_bounds" in routing_cfg:
            auto_routing["clamp_to_map_bounds"] = bool(routing_cfg["clamp_to_map_bounds"])
        if "map_bounds_margin_m" in routing_cfg:
            auto_routing["map_bounds_margin_m"] = float(routing_cfg["map_bounds_margin_m"])
        if isinstance(routing_cfg.get("fixed_goal_xy"), dict):
            auto_routing["fixed_goal_xy"] = dict(routing_cfg["fixed_goal_xy"])
        auto_routing["scenario_goal_path"] = str(
            routing_cfg.get("scenario_goal_path") or (artifacts / "scenario_goal.json")
        )
        auto_routing["send_lane_follow"] = bool(auto_routing.get("send_lane_follow", False))
        for key in (
            "send_action",
            "send_lane_follow",
            "send_routing_request",
            "auto_enable_lane_follow_fallback",
            "snap_start_to_lane",
            "snap_goal_to_lane",
            "start_nudge_use_lane_heading",
        ):
            if key in routing_cfg:
                auto_routing[key] = bool(routing_cfg[key])
        if "disable_lane_follow_on_no_response" in routing_cfg:
            auto_routing["disable_lane_follow_on_no_response"] = bool(
                routing_cfg["disable_lane_follow_on_no_response"]
            )

        traffic_light = bridge.setdefault("traffic_light", {})
        traffic_light_cfg = apollo_cfg.get("traffic_light", {}) or {}
        if "policy" in traffic_light_cfg:
            traffic_light["policy"] = str(traffic_light_cfg["policy"])
        if "publish_hz" in traffic_light_cfg:
            traffic_light["publish_hz"] = float(traffic_light_cfg["publish_hz"])
        if "channel" in traffic_light_cfg:
            traffic_light["channel"] = str(traffic_light_cfg["channel"])
        if "force_ids" in traffic_light_cfg:
            traffic_light["force_ids"] = [str(item) for item in (traffic_light_cfg.get("force_ids") or [])]
        if "ignore_roll_enabled" in traffic_light_cfg:
            traffic_light["ignore_roll_enabled"] = bool(traffic_light_cfg["ignore_roll_enabled"])
        for key in ("ignore_roll_distance_m", "ignore_roll_ahead_m"):
            if key in traffic_light_cfg:
                traffic_light[key] = float(traffic_light_cfg[key])
        if "ignore_roll_max_refresh" in traffic_light_cfg:
            traffic_light["ignore_roll_max_refresh"] = int(traffic_light_cfg["ignore_roll_max_refresh"])
        effective_tl_policy = str(traffic_light.get("policy", "") or "").strip().lower()
        if effective_tl_policy in {"ignore", "force_green"}:
            planning_cfg = apollo_cfg.setdefault("planning", {})
            # In sim debug profiles, when traffic light is ignored or force-green is
            # requested, default to disabling planning traffic-light rule unless user
            # explicitly set otherwise.
            planning_cfg.setdefault("disable_traffic_light_rule", True)

        tf = bridge.setdefault("carla_to_apollo", {})
        apollo_tf = apollo_cfg.get("carla_to_apollo", {}) or {}
        for key in ("tx", "ty", "tz", "yaw_deg"):
            if key in apollo_tf:
                tf[key] = float(apollo_tf[key])
        if "auto_calib" in apollo_tf:
            tf["auto_calib"] = bool(apollo_tf["auto_calib"])
        if "auto_calib_snap_right_angle" in apollo_tf:
            tf["auto_calib_snap_right_angle"] = bool(apollo_tf["auto_calib_snap_right_angle"])
        if "auto_calib_samples" in apollo_tf:
            tf["auto_calib_samples"] = int(apollo_tf["auto_calib_samples"])
        if "auto_calib_dump_file" in apollo_tf:
            tf["auto_calib_dump_file"] = str(apollo_tf["auto_calib_dump_file"])

        runtime_carla = (profile.get("runtime", {}) or {}).get("carla", {}) or {}
        carla_feedback = bridge.setdefault("carla_feedback", {})
        carla_feedback["enabled"] = bool(carla_feedback.get("enabled", True))
        carla_feedback["host"] = str(runtime_carla.get("host", "127.0.0.1"))
        carla_feedback["port"] = int(runtime_carla.get("port", 2000))
        carla_feedback["ego_role_name"] = ego_id

        ctrl_map = bridge.setdefault("control_mapping", {})
        ctrl_cfg = apollo_cfg.get("control_mapping", {}) or {}
        for key in (
            "max_steer_angle",
            "speed_gain",
            "brake_gain",
            "steer_sign",
            "throttle_scale",
            "brake_scale",
            "steer_scale",
            "brake_deadzone",
            "zero_hold_sec",
            "startup_throttle_boost_add",
            "startup_throttle_boost_cap",
            "straight_lane_zero_steer_max_speed_mps",
            "straight_lane_zero_steer_max_e_y_m",
            "straight_lane_zero_steer_max_e_psi_deg",
            "straight_lane_zero_steer_max_curvature",
            "straight_lane_zero_steer_release_max_e_y_m",
            "straight_lane_zero_steer_release_max_e_psi_deg",
            "straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps",
            "low_speed_steer_guard_speed_mps",
            "low_speed_steer_guard_max_abs_steer",
            "low_speed_steer_guard_max_e_y_m",
            "low_speed_steer_guard_max_e_psi_deg",
        ):
            if key in ctrl_cfg:
                ctrl_map[key] = float(ctrl_cfg[key])
        for key in (
            "auto_apply_steer_sign",
            "startup_throttle_boost_enabled",
            "straight_lane_zero_steer_enabled",
            "straight_lane_zero_steer_latch_enabled",
            "low_speed_steer_guard_enabled",
            "force_zero_steer_output",
        ):
            if key in ctrl_cfg:
                ctrl_map[key] = bool(ctrl_cfg[key])
        if "steer_sign_check_frames" in ctrl_cfg:
            ctrl_map["steer_sign_check_frames"] = int(ctrl_cfg["steer_sign_check_frames"])
        if isinstance(ctrl_cfg.get("straight_acc_override"), dict):
            ctrl_map["straight_acc_override"] = dict(ctrl_cfg["straight_acc_override"])

        out_cfg = run_dir / "artifacts" / "apollo_bridge_effective.yaml"
        out_cfg.parent.mkdir(parents=True, exist_ok=True)
        out_cfg.write_text(yaml.safe_dump(cfg, sort_keys=False))
        return out_cfg

    def _resolve_bridge_map_file(self, bridge_cfg: Path) -> Path | None:
        try:
            cfg = yaml.safe_load(bridge_cfg.read_text()) or {}
        except Exception:
            return None
        bridge = (cfg.get("bridge", {}) or {}) if isinstance(cfg, dict) else {}
        raw = str(bridge.get("map_file", "") or "").strip()
        if not raw:
            return None
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        return path.resolve()

    def _patch_apollo_map_speed_limit(self, profile: Dict[str, Any], bridge_cfg: Path, run_dir: Path) -> None:
        apollo_cfg = profile.setdefault("algo", {}).setdefault("apollo", {})
        speed_cfg = apollo_cfg.setdefault("map_speed_limit", {})
        enabled = bool(speed_cfg.get("enabled", False))
        restore_original = bool(speed_cfg.get("restore_original", False))
        target_mps = float(speed_cfg.get("override_mps", 23.61) or 23.61)
        artifacts = run_dir / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        report_path = artifacts / "apollo_map_speed_limit_patch.json"

        map_file_raw = str(speed_cfg.get("map_file", "") or "").strip()
        map_file = Path(map_file_raw).expanduser() if map_file_raw else self._resolve_bridge_map_file(bridge_cfg)
        # When speed patch is disabled and no restore is requested, skip map
        # file probing entirely so host-mode Apollo can still start even if the
        # legacy modules/map_data path is absent on this machine.
        if not enabled and not restore_original:
            report = {
                "enabled": enabled,
                "restore_original": restore_original,
                "target_speed_limit_mps": target_mps,
                "map_file": str(map_file) if map_file is not None else "",
                "backup_file": "",
                "ok": True,
                "patched": False,
                "reason": "disabled_skip",
            }
            report_path.write_text(json.dumps(report, indent=2))
            return

        if map_file is None:
            report_path.write_text(
                json.dumps(
                    {
                        "enabled": enabled,
                        "restore_original": restore_original,
                        "ok": False,
                        "reason": "map_file_missing",
                    },
                    indent=2,
                )
            )
            return
        if not map_file.is_absolute():
            map_file = (self.repo_root / map_file).resolve()
        map_file = map_file.resolve()
        backup_path = Path(f"{map_file}.carla_testbed.bak")

        report: Dict[str, Any] = {
            "enabled": enabled,
            "restore_original": restore_original,
            "target_speed_limit_mps": target_mps,
            "map_file": str(map_file),
            "backup_file": str(backup_path),
        }
        if not map_file.exists():
            report["ok"] = False
            report["reason"] = "map_file_not_found"
            report_path.write_text(json.dumps(report, indent=2))
            raise FileNotFoundError(f"Apollo map_file not found for speed patch: {map_file}")

        if restore_original and backup_path.exists():
            shutil.copyfile(backup_path, map_file)
            report["restored_from_backup"] = True
        else:
            report["restored_from_backup"] = False

        if not enabled:
            report["ok"] = True
            report["patched"] = False
            report_path.write_text(json.dumps(report, indent=2))
            speed_cfg["effective_map_file"] = str(map_file)
            return

        original_text = map_file.read_text()
        if not backup_path.exists():
            backup_path.write_text(original_text)
        speed_pattern = re.compile(r"(\bspeed_limit:\s*)([0-9]+(?:\.[0-9]+)?)")
        old_values = [float(v) for _, v in speed_pattern.findall(original_text)]
        patched_text, count = speed_pattern.subn(
            lambda m: f"{m.group(1)}{target_mps:.12f}",
            original_text,
        )
        if count <= 0:
            report["ok"] = False
            report["reason"] = "speed_limit_field_not_found"
            report_path.write_text(json.dumps(report, indent=2))
            raise RuntimeError(f"No speed_limit field found in Apollo map file: {map_file}")

        map_file.write_text(patched_text)
        report["ok"] = True
        report["patched"] = True
        report["patched_count"] = count
        report["old_unique_speed_limits_mps"] = sorted(set(old_values))
        report["new_speed_limit_mps"] = target_mps
        report_path.write_text(json.dumps(report, indent=2))
        speed_cfg["effective_map_file"] = str(map_file)

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
        map_speed_cfg = apollo_cfg.setdefault("map_speed_limit", {})
        map_speed_cfg.setdefault("enabled", False)
        map_speed_cfg.setdefault("override_mps", 23.61)
        map_speed_cfg.setdefault("restore_original", False)

        bridge_cfg = self._ensure_bridge_config(profile, run_path)
        self._patch_apollo_map_speed_limit(profile, bridge_cfg, run_path)
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
