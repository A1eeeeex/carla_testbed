from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import carla
import yaml

try:
    import rclpy
    from rclpy.node import Node
    from rosgraph_msgs.msg import Clock
except Exception:  # rclpy might be missing in pure sim environments
    rclpy = None
    Node = None
    Clock = None

from .base import Backend


class Ros2NativePublisher:
    """
    Lightweight helper that enables CARLA's native ROS2 publishing on already-spawned actors.
    No rclpy dependency is required; the CARLA server handles publishing once enable_for_ros() is called.
    """

    def __init__(
        self,
        world: carla.World,
        traffic_manager: Optional[carla.TrafficManager],
        ego_vehicle: Optional[carla.Vehicle],
        rig_spec: Union[Iterable, Dict, object],
        ego_id: str = "hero",
        invert_tf: bool = True,
    ):
        self.world = world
        self.traffic_manager = traffic_manager
        self.ego_vehicle = ego_vehicle
        self.rig_spec = rig_spec
        self.ego_id = ego_id or "hero"
        self.invert_tf = invert_tf
        self._tm_sync_original: Optional[bool] = None
        self._enabled_ids: List[str] = []

    def _collect_sensor_ids(self) -> List[str]:
        ids: List[str] = []
        if hasattr(self.rig_spec, "entries"):
            for entry in getattr(self.rig_spec, "entries", []):
                spec = entry.get("spec")
                if spec and getattr(spec, "enabled", True):
                    ids.append(getattr(spec, "sensor_id", None))
        elif isinstance(self.rig_spec, dict):
            for sensor in self.rig_spec.get("sensors", []):
                if sensor.get("enabled", True):
                    ids.append(sensor.get("id"))
        else:
            for spec in self.rig_spec or []:
                if getattr(spec, "enabled", True):
                    ids.append(getattr(spec, "sensor_id", None) or getattr(spec, "id", None))
        return [sid for sid in ids if sid]

    def _iter_sensor_actors(self) -> List[Tuple[str, carla.Actor]]:
        ids = set(self._collect_sensor_ids())
        if hasattr(self.rig_spec, "entries"):
            out = []
            for entry in getattr(self.rig_spec, "entries", []):
                spec = entry.get("spec")
                actor = entry.get("actor")
                sid = getattr(spec, "sensor_id", None) if spec else None
                if actor is not None:
                    if ids and sid not in ids:
                        continue
                    out.append((sid or str(actor.id), actor))
            return out

        actors = []
        try:
            candidates = self.world.get_actors().filter("sensor.*")
        except Exception:
            candidates = []
        for actor in candidates:
            attrs = getattr(actor, "attributes", {}) or {}
            role = attrs.get("role_name") or attrs.get("ros_name")
            if ids and role not in ids:
                continue
            parent = getattr(actor, "parent", None)
            if parent is not None and self.ego_vehicle is not None:
                try:
                    if parent.id != self.ego_vehicle.id:
                        continue
                except Exception:
                    pass
            actors.append((role or str(actor.id), actor))
        return actors

    def setup_publishers(self) -> int:
        print("[ROS2 native] ensure CARLA server started with --ros2; server will publish /carla/<ego>/<sensor>/...")
        if self.traffic_manager is not None:
            try:
                self._tm_sync_original = self.traffic_manager.get_synchronous_mode()
            except Exception:
                self._tm_sync_original = None
            try:
                self.traffic_manager.set_synchronous_mode(True)
            except Exception as exc:
                print(f"[WARN] failed to set traffic manager synchronous mode: {exc}")

        enabled = 0
        for sid, actor in self._iter_sensor_actors():
            try:
                actor.enable_for_ros()
                enabled += 1
            except Exception as exc:
                print(f"[WARN] enable_for_ros failed for {sid}: {exc}")
            try:
                tr = actor.get_transform()
                print(
                    f"[ROS2 native] {sid} -> "
                    f"loc({tr.location.x:.2f},{tr.location.y:.2f},{tr.location.z:.2f}) "
                    f"rpy({tr.rotation.roll:.2f},{tr.rotation.pitch:.2f},{tr.rotation.yaw:.2f})"
                )
            except Exception:
                pass
        try:
            ego_attrs = getattr(self.ego_vehicle, "attributes", {}) or {}
            ego_role = ego_attrs.get("role_name", self.ego_id)
            print(f"[ROS2 native] ego role_name={ego_role}, ros_name={ego_attrs.get('ros_name', ego_role)}")
        except Exception:
            pass

        self._enabled_ids = self._collect_sensor_ids()
        return enabled

    def teardown(self):
        if self.traffic_manager is not None and self._tm_sync_original is not None:
            try:
                self.traffic_manager.set_synchronous_mode(self._tm_sync_original)
            except Exception as exc:
                print(f"[WARN] failed to restore traffic manager sync mode: {exc}")


class Ros2NativeBackend(Backend):
    """Backend for Mode-1: CARLA native ROS2 topics + external algorithms."""

    def __init__(self, profile: Dict[str, Any]):
        super().__init__(profile)
        self._contract = self._load_contract()
        self.control_proc: Optional[subprocess.Popen] = None

    def _load_contract(self) -> Dict[str, Any]:
        contract_path = (
            Path(self.profile.get("contract", {}).get("canon_ros2", "io/contract/canon_ros2.yaml")).expanduser()
        )
        if not contract_path.is_absolute():
            contract_path = Path.cwd() / contract_path
        if contract_path.exists():
            with open(contract_path, "r") as f:
                return yaml.safe_load(f) or {}
        print(f"[WARN] contract file not found: {contract_path}")
        return {}

    def _start_control_bridge(self) -> None:
        cb_cfg = self.profile.get("control_bridge", {}) or {}
        if not cb_cfg.get("enabled", True):
            return
        node_path = Path("algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py").resolve()
        if not node_path.exists():
            print(f"[WARN] control bridge not found at {node_path}, skip start")
            return
        topic = cb_cfg.get("topic")
        if topic and topic.startswith("slot."):
            slot_key = topic.split("slot.", 1)[1]
            slot = (self._contract.get("slots", {}) or {}).get(slot_key, {})
            topic = slot.get("topic", "/tb/ego/control_cmd")
        cmd = [
            sys.executable,
            str(node_path),
            "--carla-host",
            str(self.profile.get("carla", {}).get("host", "127.0.0.1")),
            "--carla-port",
            str(self.profile.get("carla", {}).get("port", 2000)),
            "--control-topic",
            topic or "/tb/ego/control_cmd",
            "--max-steer-angle",
            str(cb_cfg.get("max_steer_angle", 0.6)),
            "--timeout-sec",
            str(cb_cfg.get("timeout_sec", 0.8)),
        ]
        env = os.environ.copy()
        repo_root = Path(__file__).resolve().parents[2]
        extra = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{repo_root}:{extra}" if extra else str(repo_root)
        print(f"[backend] start control bridge: {' '.join(cmd)}")
        self.control_proc = subprocess.Popen(cmd, env=env)

    def _enable_carla_ros2_native(self) -> None:
        carla_cfg = self.profile.get("carla", {}) or {}
        host = carla_cfg.get("host", "127.0.0.1")
        port = int(carla_cfg.get("port", 2000))
        try:
            client = carla.Client(host, port)
            client.set_timeout(2.0)
            world = client.get_world()
            ego = None
            tm = None
            try:
                tm = client.get_trafficmanager()
            except Exception:
                tm = None
            try:
                ego = next((v for v in world.get_actors().filter("vehicle.*") if v.attributes.get("role_name") in ["ego", "hero", "tb_ego"]), None)
            except Exception:
                ego = None
            publisher = Ros2NativePublisher(world, tm, ego, rig_spec=None, ego_id=carla_cfg.get("ego_id", "hero"))
            enabled = publisher.setup_publishers()
            print(f"[backend] enabled {enabled} ROS2-native sensors (based on current world actors)")
        except Exception as exc:
            print(f"[WARN] failed to enable ROS2 native publishers: {exc}")

    def start(self) -> None:
        if self.profile.get("carla", {}).get("start_server", False):
            print("[TODO] starting CARLA server is not automated; please start manually with --ros2 flag")
        self._enable_carla_ros2_native()
        self._start_control_bridge()

    def _check_clock(self, node: Node) -> bool:
        if Clock is None:
            return False
        stamps: List[float] = []

        def _cb(msg):
            ts = msg.clock.sec + msg.clock.nanosec * 1e-9
            stamps.append(ts)

        sub = node.create_subscription(Clock, "/clock", _cb, 10)
        end = time.time() + 5.0
        while time.time() < end and len(stamps) < 2:
            rclpy.spin_once(node, timeout_sec=0.2)
        node.destroy_subscription(sub)
        if len(stamps) < 2:
            print("[health] /clock missing or not updating")
            return False
        if stamps[-1] <= stamps[0]:
            print("[health] /clock not increasing")
            return False
        return True

    def _check_topics(self, node: Node) -> List[str]:
        missing = []
        contract_slots = self._contract.get("slots", {}) or {}
        topics = {name for name, _ in node.get_topic_names_and_types()}
        for slot, spec in contract_slots.items():
            topic = spec.get("topic")
            direction = spec.get("direction", "publish")
            if not topic or direction == "internal":
                continue
            if topic not in topics:
                missing.append(f"{slot} -> {topic}")
        return missing

    def health_check(self) -> bool:
        if rclpy is None:
            print("[health] rclpy not available, skipping ROS2 health check")
            return False
        rclpy.init(args=None)
        node = rclpy.create_node("io_ros2_native_health")
        ok = self._check_clock(node)
        missing = self._check_topics(node)
        if missing:
            print("[health] missing topics: " + ", ".join(missing))
            ok = False
        if ok:
            print("[health] ros2_native OK")
        rclpy.try_shutdown()
        return ok

    def stop(self) -> None:
        if self.control_proc and self.control_proc.poll() is None:
            print("[backend] stopping control bridge")
            self.control_proc.send_signal(signal.SIGINT)
            try:
                self.control_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.control_proc.kill()
        self.control_proc = None
