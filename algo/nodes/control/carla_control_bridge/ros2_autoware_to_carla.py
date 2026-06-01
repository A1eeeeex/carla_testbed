from __future__ import annotations

import argparse
import time
from typing import Optional

import carla
import rclpy
try:
    from ackermann_msgs.msg import AckermannDriveStamped
except Exception:
    AckermannDriveStamped = None
try:
    from autoware_control_msgs.msg import Control as AutowareControl
except Exception:
    AutowareControl = None
try:
    from autoware_auto_control_msgs.msg import AckermannControlCommand as AutowareAutoAckermannControl
except Exception:
    AutowareAutoAckermannControl = None
try:
    from geometry_msgs.msg import Twist
except Exception:
    Twist = None
try:
    from std_msgs.msg import Float32MultiArray
except Exception:
    Float32MultiArray = None
try:
    from autoware_vehicle_msgs.msg import VelocityReport
except Exception:
    VelocityReport = None
from rclpy.node import Node


def clamp(val: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


class CarlaControlBridge(Node):
    def __init__(
        self,
        carla_host: str,
        carla_port: int,
        control_topic: str,
        ego_role_name: str,
        control_type: str,
        max_steer_angle: float,
        timeout_sec: float,
        speed_gain: float,
        brake_gain: float,
        accel_throttle_gain: float,
        longitudinal_mode: str,
        speed_feedback_gain: float,
        speed_feedback_brake_gain: float,
        speed_feedback_deadband_mps: float,
        accel_feedforward_speed_margin_mps: float,
        accel_feedforward_overspeed_taper_mps: float,
        positive_accel_brake_inhibit_enabled: bool,
        positive_accel_brake_inhibit_min_accel_mps2: float,
        positive_accel_brake_inhibit_speed_margin_mps: float,
        positive_accel_overshoot_action: str,
        positive_accel_min_throttle: float,
        positive_accel_min_throttle_speed_mps: float,
        negative_accel_brake_speed_margin_mps: float,
        speed_feedback_transition_coast_sec: float,
        speed_feedback_max_throttle_step: float,
        speed_feedback_max_brake_step: float,
        speed_source: str,
        velocity_status_topic: str,
        velocity_status_stale_sec: float,
        apply_hz: float,
        sync_to_world_tick: bool,
        dryrun: bool,
        connect_timeout_sec: float,
        watchdog_wait_for_first_msg: bool,
        watchdog_arm_delay_sec: float,
        startup_brake_suppression_enabled: bool,
        startup_brake_suppression_speed_mps: float,
        startup_brake_suppression_max_brake: float,
        startup_brake_suppression_min_throttle: float,
        startup_brake_suppression_hold_sec: float,
        startup_brake_recent_throttle_window_sec: float,
        autoware_steer_sign: float,
    ) -> None:
        super().__init__("carla_control_bridge")
        self.carla_host = carla_host
        self.carla_port = carla_port
        self.control_topic = control_topic
        self.ego_role_name = ego_role_name
        self.control_type = str(control_type or "ackermann").lower()
        self.max_steer_angle = max_steer_angle
        self.timeout_sec = timeout_sec
        self.speed_gain = speed_gain
        self.brake_gain = brake_gain
        self.accel_throttle_gain = max(float(accel_throttle_gain), 0.0)
        self.longitudinal_mode = str(longitudinal_mode or "open_loop").strip().lower()
        if self.longitudinal_mode not in {"open_loop", "speed_feedback"}:
            self.get_logger().warning(
                f"unsupported longitudinal_mode={self.longitudinal_mode}; fallback to open_loop"
            )
            self.longitudinal_mode = "open_loop"
        self.speed_feedback_gain = max(float(speed_feedback_gain), 0.0)
        self.speed_feedback_brake_gain = max(float(speed_feedback_brake_gain), 0.0)
        self.speed_feedback_deadband_mps = max(float(speed_feedback_deadband_mps), 0.0)
        self.accel_feedforward_speed_margin_mps = max(float(accel_feedforward_speed_margin_mps), 0.0)
        self.accel_feedforward_overspeed_taper_mps = max(
            float(accel_feedforward_overspeed_taper_mps), 0.0
        )
        self.positive_accel_brake_inhibit_enabled = bool(positive_accel_brake_inhibit_enabled)
        self.positive_accel_brake_inhibit_min_accel_mps2 = max(
            float(positive_accel_brake_inhibit_min_accel_mps2), 0.0
        )
        self.positive_accel_brake_inhibit_speed_margin_mps = max(
            float(positive_accel_brake_inhibit_speed_margin_mps), 0.0
        )
        self.positive_accel_overshoot_action = str(
            positive_accel_overshoot_action or "throttle"
        ).strip().lower()
        if self.positive_accel_overshoot_action not in {"throttle", "coast", "coast_all"}:
            self.get_logger().warning(
                "unsupported positive_accel_overshoot_action="
                f"{self.positive_accel_overshoot_action}; fallback to throttle"
            )
            self.positive_accel_overshoot_action = "throttle"
        self.positive_accel_min_throttle = clamp(float(positive_accel_min_throttle), 0.0, 1.0)
        self.positive_accel_min_throttle_speed_mps = max(
            float(positive_accel_min_throttle_speed_mps), 0.0
        )
        # Disabled by default with -1.0 to preserve legacy behavior. When set
        # to a non-negative margin in speed_feedback mode, negative acceleration
        # only becomes CARLA brake after ego is above target speed by that much.
        self.negative_accel_brake_speed_margin_mps = float(negative_accel_brake_speed_margin_mps)
        self.speed_feedback_transition_coast_sec = max(float(speed_feedback_transition_coast_sec), 0.0)
        self.speed_feedback_max_throttle_step = max(float(speed_feedback_max_throttle_step), 0.0)
        self.speed_feedback_max_brake_step = max(float(speed_feedback_max_brake_step), 0.0)
        self.speed_source = str(speed_source or "carla").strip().lower()
        if self.speed_source not in {"carla", "ros_velocity_status", "auto"}:
            self.get_logger().warning(f"unsupported speed_source={self.speed_source}; fallback to carla")
            self.speed_source = "carla"
        self.velocity_status_topic = str(velocity_status_topic or "/vehicle/status/velocity_status")
        self.velocity_status_stale_sec = max(float(velocity_status_stale_sec), 0.0)
        self.apply_hz = max(float(apply_hz), 1.0)
        self.sync_to_world_tick = bool(sync_to_world_tick)
        self.dryrun = dryrun
        self.connect_timeout_sec = max(float(connect_timeout_sec), 0.5)
        self.watchdog_wait_for_first_msg = bool(watchdog_wait_for_first_msg)
        self.watchdog_arm_delay_sec = max(float(watchdog_arm_delay_sec), 0.0)
        self.startup_brake_suppression_enabled = bool(startup_brake_suppression_enabled)
        self.startup_brake_suppression_speed_mps = max(float(startup_brake_suppression_speed_mps), 0.0)
        self.startup_brake_suppression_max_brake = max(float(startup_brake_suppression_max_brake), 0.0)
        self.startup_brake_suppression_min_throttle = max(float(startup_brake_suppression_min_throttle), 0.0)
        self.startup_brake_suppression_hold_sec = max(float(startup_brake_suppression_hold_sec), 0.0)
        self.startup_brake_recent_throttle_window_sec = max(
            float(startup_brake_recent_throttle_window_sec), 0.0
        )
        self.autoware_steer_sign = -1.0 if float(autoware_steer_sign) < 0.0 else 1.0

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.ego: Optional[carla.Vehicle] = None
        self.last_msg_time = self.get_clock().now()
        self._warned_no_ego = False
        self._warned_fallback_role = False
        self._warned_duplicate_role = False
        self._last_discovery_sec = 0.0
        self._last_discovery_warn_sec = 0.0
        self._last_apply_log_sec = 0.0
        self._bound_actor_id: Optional[int] = None

        self._pending_ctrl: Optional[carla.VehicleControl] = None
        self._pending_source_steer: float = 0.0
        self._pending_steer_norm: float = 0.0
        self._pending_steer_clamped: bool = False
        self._pending_target_speed: Optional[float] = None
        self._pending_accel: Optional[float] = None
        self._pending_current_speed: Optional[float] = None
        self._pending_current_speed_source: Optional[str] = None
        self._pending_speed_error: Optional[float] = None
        self._last_applied_frame: Optional[int] = None
        self._received_count = 0
        self._applied_count = 0
        self._dropped_same_frame_count = 0
        self._first_msg_received = False
        self._ego_bound_monotonic: Optional[float] = None
        self._recent_positive_throttle_monotonic: Optional[float] = None
        self._startup_brake_suppressed_count = 0
        self._throttle_brake_conflict_resolved_count = 0
        self._last_ros_speed_mps: Optional[float] = None
        self._last_ros_speed_monotonic: Optional[float] = None
        self._last_speed_source = "carla"
        self._last_longitudinal_active_state: Optional[str] = None
        self._last_longitudinal_switch_monotonic = time.monotonic()
        self._last_output_throttle = 0.0
        self._last_output_brake = 0.0

        self._connect_carla()
        if self.control_type == "ackermann":
            if AckermannDriveStamped is None:
                raise RuntimeError("ackermann_msgs is unavailable, cannot subscribe ackermann control")
            self.create_subscription(AckermannDriveStamped, control_topic, self._on_ackermann, 10)
        elif self.control_type == "autoware_control":
            msg_cls = AutowareControl or AutowareAutoAckermannControl
            if msg_cls is None:
                raise RuntimeError(
                    "Autoware control message packages are unavailable; cannot subscribe autoware_control"
                )
            self.create_subscription(msg_cls, control_topic, self._on_autoware_control, 10)
        elif self.control_type == "twist":
            if Twist is None:
                raise RuntimeError("geometry_msgs/Twist is unavailable, cannot subscribe twist control")
            self.create_subscription(Twist, control_topic, self._on_twist, 10)
        elif self.control_type == "direct":
            if Float32MultiArray is None:
                raise RuntimeError("std_msgs/Float32MultiArray is unavailable, cannot subscribe direct control")
            self.create_subscription(Float32MultiArray, control_topic, self._on_direct, 10)
        else:
            raise RuntimeError(
                f"unsupported control_type={self.control_type}, expected ackermann|autoware_control|twist|direct"
            )
        if self.speed_source in {"ros_velocity_status", "auto"}:
            if VelocityReport is None:
                self.get_logger().warning(
                    "autoware_vehicle_msgs/VelocityReport unavailable; speed feedback will use CARLA actor velocity"
                )
            else:
                self.create_subscription(VelocityReport, self.velocity_status_topic, self._on_velocity_status, 10)
                self.get_logger().info(
                    f"speed feedback can use ROS velocity status topic {self.velocity_status_topic}"
                )
        self.create_timer(1.0 / self.apply_hz, self._flush_pending_control)
        self.get_logger().info(
            f"Bridge listening on {control_topic} ({self.control_type}), "
            f"carla={carla_host}:{carla_port}, ego_role={ego_role_name}, dryrun={self.dryrun}, "
            f"apply_hz={self.apply_hz:.1f}, sync_to_world_tick={self.sync_to_world_tick}, "
            f"longitudinal_mode={self.longitudinal_mode}, accel_throttle_gain={self.accel_throttle_gain:.3f}, "
            f"accel_feedforward_overspeed_taper_mps={self.accel_feedforward_overspeed_taper_mps:.3f}, "
            f"positive_accel_brake_inhibit={self.positive_accel_brake_inhibit_enabled}, "
            f"positive_accel_overshoot_action={self.positive_accel_overshoot_action}, "
            f"positive_accel_min_throttle={self.positive_accel_min_throttle:.3f}, "
            f"positive_accel_min_throttle_speed_mps={self.positive_accel_min_throttle_speed_mps:.3f}, "
            f"negative_accel_brake_speed_margin_mps={self.negative_accel_brake_speed_margin_mps:.3f}, "
            f"speed_feedback_transition_coast_sec={self.speed_feedback_transition_coast_sec:.3f}, "
            f"speed_source={self.speed_source}, "
            f"watchdog_wait_for_first_msg={self.watchdog_wait_for_first_msg}, "
            f"watchdog_arm_delay_sec={self.watchdog_arm_delay_sec:.2f}, "
            f"startup_brake_suppression={self.startup_brake_suppression_enabled}"
        )

    def _connect_carla(self) -> None:
        try:
            self.client = carla.Client(self.carla_host, self.carla_port)
            self.client.set_timeout(self.connect_timeout_sec)
            self.world = self.client.get_world()
            self.ego = self._discover_ego(force_world_refresh=False)
            if self.ego is None:
                self.get_logger().warn("ego vehicle not found; control commands will be buffered")
        except Exception as exc:
            self.get_logger().error(f"Failed to connect to CARLA: {exc}")

    def _discover_ego(self, *, force_world_refresh: bool) -> Optional[carla.Vehicle]:
        if self.client is None:
            return None
        try:
            if force_world_refresh or self.world is None:
                self.world = self.client.get_world()
            world = self.world
            if world is None:
                return None
            vehicles = list(world.get_actors().filter("vehicle.*"))
            if not vehicles:
                try:
                    # Synchronous CARLA can leave a newly connected client with
                    # an empty actor cache until it observes a tick. This bridge
                    # does not own ticking; the wait only follows runner ticks.
                    world.wait_for_tick(0.2)
                    vehicles = list(world.get_actors().filter("vehicle.*"))
                except Exception:
                    pass
            exact_matches = []
            alias_matches = []
            for v in vehicles:
                role = ((v.attributes or {}).get("role_name") or "").strip()
                if role == self.ego_role_name:
                    exact_matches.append(v)
                elif role in ["ego", "hero", "tb_ego"]:
                    alias_matches.append(v)
            matched = exact_matches or alias_matches
            if matched:
                candidate = max(matched, key=lambda v: int(v.id))
                if len(matched) > 1 and not self._warned_duplicate_role:
                    ids = [int(v.id) for v in matched]
                    roles = [str((v.attributes or {}).get("role_name", "")) for v in matched]
                    self.get_logger().warning(
                        f"multiple ego-role candidates found for '{self.ego_role_name}'; "
                        f"choose newest actor_id={candidate.id}, candidates={ids}, roles={roles}"
                    )
                    self._warned_duplicate_role = True
                if self._bound_actor_id != int(candidate.id):
                    self._bound_actor_id = int(candidate.id)
                    self._ego_bound_monotonic = time.monotonic()
                    role = (candidate.attributes or {}).get("role_name")
                    self.get_logger().info(
                        f"control target bound actor_id={candidate.id} role={role or '<empty>'}"
                    )
                return candidate
            if not vehicles:
                return None
            non_front = [v for v in vehicles if "front" not in ((v.attributes or {}).get("role_name", "").lower())]
            candidate_pool = non_front if non_front else vehicles
            candidate = max(candidate_pool, key=lambda v: int(v.id))
            if not self._warned_fallback_role:
                roles = [str((v.attributes or {}).get("role_name", "")) for v in vehicles]
                self.get_logger().warning(
                    f"ego role '{self.ego_role_name}' not found; fallback to actor_id={candidate.id}, roles={roles}"
                )
                self._warned_fallback_role = True
            if self._bound_actor_id != int(candidate.id):
                self._bound_actor_id = int(candidate.id)
                self._ego_bound_monotonic = time.monotonic()
                role = (candidate.attributes or {}).get("role_name", "")
                self.get_logger().info(
                    f"control target fallback actor_id={candidate.id} role={role or '<empty>'}"
                )
            return candidate
        except Exception as exc:
            now = time.monotonic()
            if now - self._last_discovery_warn_sec > 2.0:
                self._last_discovery_warn_sec = now
                self.get_logger().warning(f"discover ego failed: {exc}")
            return None

    def _warn_no_ego_once(self) -> None:
        now = time.monotonic()
        if self._warned_no_ego and (now - self._last_discovery_warn_sec) < 2.0:
            return
        self._last_discovery_warn_sec = now
        self.get_logger().warning("no ego found yet; skip control")
        self._warned_no_ego = True

    def _ensure_ego(self) -> bool:
        if self.world is None:
            self._connect_carla()
        now_sec = time.monotonic()
        force_refresh = (now_sec - self._last_discovery_sec) > 0.5
        if self.ego is None or force_refresh:
            discovered = self._discover_ego(force_world_refresh=force_refresh)
            if discovered is not None:
                self.ego = discovered
            if force_refresh:
                self._last_discovery_sec = now_sec
        if self.ego is None:
            self._warn_no_ego_once()
            return False
        self._warned_no_ego = False
        return True

    def _queue_control(
        self,
        ctrl: carla.VehicleControl,
        *,
        source_steer: float,
        steer_norm: float,
        steer_clamped: bool,
        target_speed: Optional[float] = None,
        accel: Optional[float] = None,
        current_speed: Optional[float] = None,
        speed_error: Optional[float] = None,
    ) -> None:
        self.last_msg_time = self.get_clock().now()
        self._resolve_throttle_brake_conflict(ctrl)
        self._pending_ctrl = ctrl
        self._pending_source_steer = float(source_steer)
        self._pending_steer_norm = float(steer_norm)
        self._pending_steer_clamped = bool(steer_clamped)
        self._pending_target_speed = None if target_speed is None else float(target_speed)
        self._pending_accel = None if accel is None else float(accel)
        self._pending_current_speed = None if current_speed is None else float(current_speed)
        self._pending_current_speed_source = self._last_speed_source
        self._pending_speed_error = None if speed_error is None else float(speed_error)
        self._received_count += 1
        self._first_msg_received = True
        if float(ctrl.throttle) >= self.startup_brake_suppression_min_throttle:
            self._recent_positive_throttle_monotonic = time.monotonic()

    def _resolve_throttle_brake_conflict(self, ctrl: carla.VehicleControl) -> carla.VehicleControl:
        """Guarantee CARLA never receives simultaneous throttle and brake."""

        throttle = float(getattr(ctrl, "throttle", 0.0) or 0.0)
        brake = float(getattr(ctrl, "brake", 0.0) or 0.0)
        if throttle <= 0.01 or brake <= 0.01:
            return ctrl
        self._throttle_brake_conflict_resolved_count += 1
        if brake > throttle:
            ctrl.throttle = 0.0
        else:
            ctrl.brake = 0.0
        if (
            self._throttle_brake_conflict_resolved_count <= 5
            or (self._throttle_brake_conflict_resolved_count % 20) == 0
        ):
            self.get_logger().warning(
                "resolved throttle/brake conflict "
                f"count={self._throttle_brake_conflict_resolved_count} "
                f"input_throttle={throttle:.3f} input_brake={brake:.3f} "
                f"output_throttle={float(ctrl.throttle):.3f} output_brake={float(ctrl.brake):.3f}"
            )
        return ctrl

    def _build_control_from_target(
        self,
        target_speed: float,
        steer_norm: float,
        accel: float = 0.0,
    ) -> tuple[carla.VehicleControl, bool, Optional[float], Optional[float]]:
        ctrl = carla.VehicleControl()
        steer_out = clamp(float(steer_norm), -1.0, 1.0)
        steer_clamped = abs(steer_out - float(steer_norm)) > 1e-6
        ctrl.steer = steer_out

        if target_speed >= 0.0:
            current_speed = self._current_speed_mps()
            speed_error = float(target_speed) - current_speed
            if self.longitudinal_mode == "speed_feedback":
                accel_feedforward = 0.0
                if current_speed <= float(target_speed) + self.accel_feedforward_speed_margin_mps:
                    accel_feedforward = max(float(accel), 0.0) * self.accel_throttle_gain
                    if (
                        self.accel_feedforward_overspeed_taper_mps > 0.0
                        and current_speed > float(target_speed)
                    ):
                        overspeed = current_speed - float(target_speed)
                        taper = 1.0 - (overspeed / self.accel_feedforward_overspeed_taper_mps)
                        accel_feedforward *= clamp(taper, 0.0, 1.0)
                positive_accel_active = (
                    float(accel) >= self.positive_accel_brake_inhibit_min_accel_mps2
                )
                inhibit_brake = (
                    self.positive_accel_brake_inhibit_enabled
                    and positive_accel_active
                    and current_speed
                    <= float(target_speed) + self.positive_accel_brake_inhibit_speed_margin_mps
                )
                coast_positive_overspeed = (
                    positive_accel_active
                    and speed_error < -self.speed_feedback_deadband_mps
                    and self.positive_accel_overshoot_action == "coast_all"
                )
                if speed_error < -self.speed_feedback_deadband_mps and not inhibit_brake:
                    if coast_positive_overspeed:
                        # Diagnostic mode for contradictory Autoware commands:
                        # positive acceleration with target velocity below ego
                        # speed. Do not create bridge-induced throttle/brake
                        # chatter; the run should be judged by target tracking
                        # and Autoware semantics diagnostics.
                        ctrl.throttle = 0.0
                        ctrl.brake = 0.0
                    else:
                        ctrl.throttle = 0.0
                        ctrl.brake = clamp(abs(speed_error) * self.speed_feedback_brake_gain, 0.0, 1.0)
                elif (
                    speed_error < -self.speed_feedback_deadband_mps
                    and inhibit_brake
                    and self.positive_accel_overshoot_action == "coast"
                ):
                    # Autoware can request positive acceleration while target
                    # velocity lags near ego. In this diagnostic mode we
                    # avoid turning that disagreement into throttle/brake
                    # chatter; larger overshoot still brakes once outside the
                    # inhibit margin.
                    ctrl.throttle = 0.0
                    ctrl.brake = 0.0
                else:
                    speed_feedback = max(speed_error, 0.0) * self.speed_feedback_gain
                    ctrl.throttle = clamp(speed_feedback + accel_feedforward, 0.0, 1.0)
                    ctrl.brake = 0.0
                if (
                    positive_accel_active
                    and self.positive_accel_min_throttle > 0.0
                    and current_speed <= self.positive_accel_min_throttle_speed_mps
                    and current_speed
                    <= float(target_speed) + self.positive_accel_brake_inhibit_speed_margin_mps
                    and float(ctrl.brake) <= 0.0
                ):
                    # Diagnostic calibration aid for CARLA low-speed
                    # actuation. Disabled by default; do not treat it as
                    # Autoware's raw control output.
                    ctrl.throttle = max(float(ctrl.throttle), self.positive_accel_min_throttle)
                ctrl = self._apply_speed_feedback_debounce(ctrl)
            else:
                base = float(target_speed) / max(self.speed_gain, 0.1)
                if accel:
                    base += max(float(accel), 0.0) * self.accel_throttle_gain
                ctrl.throttle = clamp(base, 0.0, 1.0)
                ctrl.brake = 0.0
        else:
            current_speed = self._current_speed_mps()
            speed_error = float(target_speed) - current_speed
            ctrl.throttle = 0.0
            ctrl.brake = clamp(abs(float(target_speed)) / max(self.brake_gain, 0.1), 0.0, 1.0)
        return ctrl, steer_clamped, current_speed, speed_error

    @staticmethod
    def _longitudinal_state(ctrl: carla.VehicleControl) -> str:
        throttle = float(getattr(ctrl, "throttle", 0.0) or 0.0)
        brake = float(getattr(ctrl, "brake", 0.0) or 0.0)
        if throttle > 0.01 and brake > 0.01:
            return "conflict"
        if throttle > 0.01:
            return "throttle"
        if brake > 0.01:
            return "brake"
        return "coast"

    def _apply_speed_feedback_debounce(self, ctrl: carla.VehicleControl) -> carla.VehicleControl:
        """Reduce throttle/brake chatter in experimental speed_feedback mode.

        Autoware's longitudinal command carries both target velocity and
        acceleration. During startup the nearest target velocity can lag while
        acceleration remains positive; a direct target-speed brake can then
        alternate with acceleration feedforward. The optional transition coast
        and per-command step limits are diagnostic/calibration aids, not a
        default behavior change.
        """

        desired_state = self._longitudinal_state(ctrl)
        now = time.monotonic()
        active_state = desired_state if desired_state in {"throttle", "brake"} else None
        if (
            self.speed_feedback_transition_coast_sec > 0.0
            and active_state is not None
            and self._last_longitudinal_active_state in {"throttle", "brake"}
            and active_state != self._last_longitudinal_active_state
            and (now - self._last_longitudinal_switch_monotonic) < self.speed_feedback_transition_coast_sec
        ):
            ctrl.throttle = 0.0
            ctrl.brake = 0.0
        elif active_state is not None and active_state != self._last_longitudinal_active_state:
            self._last_longitudinal_active_state = active_state
            self._last_longitudinal_switch_monotonic = now

        # Step limits are only for ramping the active actuator up/down. When
        # the desired state changes, release the opposite actuator immediately;
        # lingering throttle while braking (or vice versa) is not valid control
        # evidence for natural driving.
        desired_state = self._longitudinal_state(ctrl)
        if desired_state != "throttle":
            ctrl.throttle = 0.0
        if desired_state != "brake":
            ctrl.brake = 0.0

        if self.speed_feedback_max_throttle_step < 1.0:
            lo = self._last_output_throttle - self.speed_feedback_max_throttle_step
            hi = self._last_output_throttle + self.speed_feedback_max_throttle_step
            if self._longitudinal_state(ctrl) == "throttle":
                ctrl.throttle = clamp(float(ctrl.throttle), max(0.0, lo), min(1.0, hi))
        if self.speed_feedback_max_brake_step < 1.0:
            lo = self._last_output_brake - self.speed_feedback_max_brake_step
            hi = self._last_output_brake + self.speed_feedback_max_brake_step
            if self._longitudinal_state(ctrl) == "brake":
                ctrl.brake = clamp(float(ctrl.brake), max(0.0, lo), min(1.0, hi))
        if ctrl.throttle > 0.0 and ctrl.brake > 0.0:
            if ctrl.throttle >= ctrl.brake:
                ctrl.brake = 0.0
            else:
                ctrl.throttle = 0.0
        self._last_output_throttle = float(ctrl.throttle)
        self._last_output_brake = float(ctrl.brake)
        return ctrl

    def _negative_accel_brake_allowed(
        self,
        *,
        accel: float,
        target_speed: float,
        current_speed: Optional[float],
    ) -> bool:
        if float(accel) >= -0.05:
            return False
        if (
            self.longitudinal_mode != "speed_feedback"
            or self.negative_accel_brake_speed_margin_mps < 0.0
        ):
            return True
        if current_speed is None:
            # Preserve braking as the safer fallback if speed feedback is
            # configured but the current speed sample is unavailable.
            return True
        return float(current_speed) >= float(target_speed) + self.negative_accel_brake_speed_margin_mps

    def _current_world_frame(self) -> Optional[int]:
        if self.world is None:
            return None
        try:
            snap = self.world.get_snapshot()
            return int(getattr(snap, "frame", -1))
        except Exception:
            return None

    def _current_speed_mps(self) -> float:
        self._last_speed_source = "carla"
        if self.speed_source in {"ros_velocity_status", "auto"} and self._last_ros_speed_mps is not None:
            age = (
                time.monotonic() - self._last_ros_speed_monotonic
                if self._last_ros_speed_monotonic is not None
                else float("inf")
            )
            if age <= self.velocity_status_stale_sec:
                self._last_speed_source = "ros_velocity_status"
                return float(self._last_ros_speed_mps)
            if self.speed_source == "ros_velocity_status":
                self._last_speed_source = "ros_velocity_status_stale_fallback_carla"
        if self.ego is None:
            return 0.0
        try:
            return float(self.ego.get_velocity().length())
        except Exception:
            return 0.0

    def _on_velocity_status(self, msg: VelocityReport) -> None:
        longitudinal = float(getattr(msg, "longitudinal_velocity", 0.0) or 0.0)
        lateral = float(getattr(msg, "lateral_velocity", 0.0) or 0.0)
        self._last_ros_speed_mps = (longitudinal * longitudinal + lateral * lateral) ** 0.5
        self._last_ros_speed_monotonic = time.monotonic()

    def _maybe_suppress_startup_brake(
        self,
        ctrl: carla.VehicleControl,
        *,
        source: str,
    ) -> carla.VehicleControl:
        if not self.startup_brake_suppression_enabled:
            return ctrl
        if source != "pending":
            return ctrl
        if float(ctrl.brake) <= 0.0 or float(ctrl.brake) > self.startup_brake_suppression_max_brake:
            return ctrl

        now = time.monotonic()
        speed_mps = self._current_speed_mps()
        recent_throttle = (
            self._recent_positive_throttle_monotonic is not None
            and (now - self._recent_positive_throttle_monotonic) <= self.startup_brake_recent_throttle_window_sec
        )
        within_startup_hold = (
            self._ego_bound_monotonic is not None
            and (now - self._ego_bound_monotonic) <= self.startup_brake_suppression_hold_sec
        )
        if speed_mps > self.startup_brake_suppression_speed_mps:
            return ctrl
        if not recent_throttle and not within_startup_hold:
            return ctrl

        ctrl.brake = 0.0
        self._startup_brake_suppressed_count += 1
        if self._startup_brake_suppressed_count <= 5 or (self._startup_brake_suppressed_count % 20) == 0:
            self.get_logger().info(
                f"startup brake suppressed count={self._startup_brake_suppressed_count} "
                f"speed={speed_mps:.3f} recent_throttle={recent_throttle} within_hold={within_startup_hold}"
            )
        return ctrl

    @staticmethod
    def _fmt_optional(value: Optional[float]) -> str:
        return "null" if value is None else f"{float(value):.3f}"

    def _flush_pending_control(self) -> None:
        if not self._ensure_ego():
            return

        frame_id = self._current_world_frame() if self.sync_to_world_tick else None
        if (
            self.sync_to_world_tick
            and frame_id is not None
            and self._last_applied_frame is not None
            and frame_id == self._last_applied_frame
        ):
            self._dropped_same_frame_count += 1
            return

        elapsed = (self.get_clock().now() - self.last_msg_time).nanoseconds / 1e9
        source = "pending"
        if elapsed > self.timeout_sec:
            if self.watchdog_wait_for_first_msg and not self._first_msg_received:
                return
            if (
                self._ego_bound_monotonic is not None
                and (time.monotonic() - self._ego_bound_monotonic) < self.watchdog_arm_delay_sec
            ):
                return
            ctrl = carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0)
            source_steer = 0.0
            steer_norm = 0.0
            steer_clamped = False
            target_speed = None
            accel = None
            current_speed = None
            speed_error = None
            source = "watchdog"
        else:
            if self._pending_ctrl is None:
                return
            ctrl = self._pending_ctrl
            source_steer = self._pending_source_steer
            steer_norm = self._pending_steer_norm
            steer_clamped = self._pending_steer_clamped
            target_speed = self._pending_target_speed
            accel = self._pending_accel
            current_speed = self._pending_current_speed
            current_speed_source = self._pending_current_speed_source
            speed_error = self._pending_speed_error
            ctrl = self._maybe_suppress_startup_brake(ctrl, source=source)
            self._resolve_throttle_brake_conflict(ctrl)
        if source != "pending":
            current_speed_source = None

        now = time.monotonic()
        if now - self._last_apply_log_sec > 1.0:
            self._last_apply_log_sec = now
            role = (self.ego.attributes or {}).get("role_name", "") if self.ego else ""
            self.get_logger().info(
                f"apply frame={frame_id} source={source} actor_id={self.ego.id if self.ego else -1} "
                f"role={role or '<empty>'} src_steer={source_steer:.3f} norm_steer={steer_norm:.3f} "
                f"carla_steer={ctrl.steer:.3f} clamped={steer_clamped} "
                f"target_speed={self._fmt_optional(target_speed)} accel={self._fmt_optional(accel)} "
                f"current_speed={self._fmt_optional(current_speed)} speed_error={self._fmt_optional(speed_error)} "
                f"speed_source={current_speed_source or 'null'} "
                f"longitudinal_mode={self.longitudinal_mode} "
                f"positive_accel_overshoot_action={self.positive_accel_overshoot_action} "
                f"positive_accel_min_throttle={self.positive_accel_min_throttle:.3f} "
                f"throttle={ctrl.throttle:.3f} brake={ctrl.brake:.3f} "
                f"rx={self._received_count} applied={self._applied_count} drop_same_frame={self._dropped_same_frame_count}"
            )

        if not self.dryrun:
            try:
                self.ego.apply_control(ctrl)
            except Exception as exc:
                self.get_logger().error(f"apply_control failed: {exc}")
                return
        self._applied_count += 1
        if frame_id is not None:
            self._last_applied_frame = frame_id

    def _on_ackermann(self, msg: AckermannDriveStamped) -> None:
        steer_norm = float(msg.drive.steering_angle) / max(self.max_steer_angle, 1e-3)
        target_speed = float(msg.drive.speed)
        accel = float(msg.drive.acceleration)
        ctrl, steer_clamped, current_speed, speed_error = self._build_control_from_target(
            target_speed=target_speed,
            steer_norm=steer_norm,
            accel=accel,
        )
        self._queue_control(
            ctrl,
            source_steer=float(msg.drive.steering_angle),
            steer_norm=steer_norm,
            steer_clamped=steer_clamped,
            target_speed=target_speed,
            accel=accel,
            current_speed=current_speed,
            speed_error=speed_error,
        )

    def _on_autoware_control(self, msg) -> None:
        """Map Autoware native control messages to CARLA VehicleControl.

        Autoware Universe commonly publishes ``autoware_control_msgs/Control`` on
        ``/control/command/control_cmd``. Older stacks can publish
        ``autoware_auto_control_msgs/AckermannControlCommand``. Both use
        lateral/longitudinal sub-messages rather than ``ackermann_msgs/drive``.
        """

        lateral = getattr(msg, "lateral", None)
        longitudinal = getattr(msg, "longitudinal", None)
        if lateral is None or longitudinal is None:
            self.get_logger().warning("autoware_control message missing lateral/longitudinal fields")
            return
        steering_tire_angle = float(getattr(lateral, "steering_tire_angle", 0.0) or 0.0)
        target_speed = float(
            getattr(longitudinal, "velocity", getattr(longitudinal, "speed", 0.0)) or 0.0
        )
        accel = float(getattr(longitudinal, "acceleration", 0.0) or 0.0)
        steer_norm = (self.autoware_steer_sign * steering_tire_angle) / max(self.max_steer_angle, 1e-3)
        ctrl, steer_clamped, current_speed, speed_error = self._build_control_from_target(
            target_speed=target_speed,
            steer_norm=steer_norm,
            accel=accel,
        )
        if self._negative_accel_brake_allowed(
            accel=accel,
            target_speed=target_speed,
            current_speed=current_speed,
        ):
            ctrl.throttle = 0.0
            ctrl.brake = clamp(abs(accel) / max(self.brake_gain, 0.1), 0.0, 1.0)
        self._queue_control(
            ctrl,
            source_steer=steering_tire_angle,
            steer_norm=steer_norm,
            steer_clamped=steer_clamped,
            target_speed=target_speed,
            accel=accel,
            current_speed=current_speed,
            speed_error=speed_error,
        )

    def _on_twist(self, msg: Twist) -> None:
        steer_norm = float(msg.angular.z)
        target_speed = float(msg.linear.x)
        ctrl, steer_clamped, current_speed, speed_error = self._build_control_from_target(
            target_speed=target_speed,
            steer_norm=steer_norm,
            accel=0.0,
        )
        self._queue_control(
            ctrl,
            source_steer=steer_norm,
            steer_norm=steer_norm,
            steer_clamped=steer_clamped,
            target_speed=target_speed,
            accel=0.0,
            current_speed=current_speed,
            speed_error=speed_error,
        )

    def _on_direct(self, msg: Float32MultiArray) -> None:
        data = list(msg.data or [])
        if len(data) < 3:
            self.get_logger().warning("direct control expects [throttle, brake, steer]")
            return
        raw_steer = float(data[2])
        steer = clamp(raw_steer, -1.0, 1.0)
        ctrl = carla.VehicleControl()
        ctrl.throttle = clamp(float(data[0]), 0.0, 1.0)
        ctrl.brake = clamp(float(data[1]), 0.0, 1.0)
        ctrl.steer = steer
        self._queue_control(
            ctrl,
            source_steer=raw_steer,
            steer_norm=raw_steer,
            steer_clamped=abs(steer - raw_steer) > 1e-6,
        )


def parse_args():
    ap = argparse.ArgumentParser(description="ROS2 control command -> CARLA VehicleControl bridge")
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--connect-timeout-sec", type=float, default=6.0)
    ap.add_argument("--control-topic", default="/tb/ego/control_cmd")
    ap.add_argument("--control-type", default="ackermann", choices=["ackermann", "autoware_control", "twist", "direct"])
    ap.add_argument("--ego-role-name", default="ego")
    ap.add_argument("--max-steer-angle", type=float, default=0.6)
    ap.add_argument("--timeout-sec", type=float, default=0.8)
    ap.add_argument("--speed-gain", type=float, default=10.0, help="speed (m/s) to throttle mapping denominator")
    ap.add_argument("--brake-gain", type=float, default=5.0)
    ap.add_argument(
        "--accel-throttle-gain",
        type=float,
        default=0.05,
        help="Additional throttle per positive Autoware longitudinal acceleration (m/s^2).",
    )
    ap.add_argument(
        "--longitudinal-mode",
        choices=["open_loop", "speed_feedback"],
        default="open_loop",
        help="open_loop preserves legacy target-speed/acceleration mapping; speed_feedback uses CARLA ego speed.",
    )
    ap.add_argument(
        "--speed-feedback-gain",
        type=float,
        default=0.25,
        help="Throttle per positive target-current speed error in speed_feedback mode.",
    )
    ap.add_argument(
        "--speed-feedback-brake-gain",
        type=float,
        default=0.25,
        help="Brake per negative target-current speed error in speed_feedback mode.",
    )
    ap.add_argument("--speed-feedback-deadband-mps", type=float, default=0.2)
    ap.add_argument(
        "--accel-feedforward-speed-margin-mps",
        type=float,
        default=0.5,
        help="Positive acceleration feedforward is disabled once current speed exceeds target by this margin.",
    )
    ap.add_argument(
        "--accel-feedforward-overspeed-taper-mps",
        type=float,
        default=0.0,
        help=(
            "Optional linear taper for positive acceleration feedforward once current speed exceeds target; "
            "0 preserves the historical hard-margin behavior."
        ),
    )
    ap.add_argument(
        "--positive-accel-brake-inhibit-enabled",
        dest="positive_accel_brake_inhibit_enabled",
        action="store_true",
        help=(
            "In speed_feedback mode, do not brake solely on target-speed error while "
            "Autoware still commands positive acceleration within the configured speed margin."
        ),
    )
    ap.add_argument(
        "--positive-accel-brake-inhibit-disabled",
        dest="positive_accel_brake_inhibit_enabled",
        action="store_false",
        help="Disable positive-acceleration brake inhibition.",
    )
    ap.set_defaults(positive_accel_brake_inhibit_enabled=False)
    ap.add_argument("--positive-accel-brake-inhibit-min-accel-mps2", type=float, default=0.05)
    ap.add_argument("--positive-accel-brake-inhibit-speed-margin-mps", type=float, default=0.5)
    ap.add_argument(
        "--positive-accel-overshoot-action",
        choices=["throttle", "coast", "coast_all"],
        default="throttle",
        help=(
            "In speed_feedback mode, choose whether positive acceleration should still create "
            "throttle while current speed is already above target. coast only applies inside "
            "the brake-inhibit margin; coast_all applies to any positive-acceleration overspeed."
        ),
    )
    ap.add_argument(
        "--positive-accel-min-throttle",
        type=float,
        default=0.0,
        help=(
            "Optional minimum throttle while Autoware commands positive acceleration at low speed "
            "in speed_feedback mode. 0 disables this calibration aid."
        ),
    )
    ap.add_argument(
        "--positive-accel-min-throttle-speed-mps",
        type=float,
        default=5.0,
        help="Apply positive-accel minimum throttle only below this current speed.",
    )
    ap.add_argument(
        "--negative-accel-brake-speed-margin-mps",
        type=float,
        default=-1.0,
        help=(
            "In speed_feedback mode, require ego speed to exceed target speed by this margin "
            "before mapping negative Autoware acceleration to CARLA brake. Negative values "
            "preserve legacy immediate braking."
        ),
    )
    ap.add_argument(
        "--speed-feedback-transition-coast-sec",
        type=float,
        default=0.0,
        help="Optional coast window before switching directly between throttle and brake in speed_feedback mode.",
    )
    ap.add_argument(
        "--speed-feedback-max-throttle-step",
        type=float,
        default=1.0,
        help="Optional per-command throttle step limit in speed_feedback mode; 1.0 disables limiting.",
    )
    ap.add_argument(
        "--speed-feedback-max-brake-step",
        type=float,
        default=1.0,
        help="Optional per-command brake step limit in speed_feedback mode; 1.0 disables limiting.",
    )
    ap.add_argument(
        "--speed-source",
        choices=["carla", "ros_velocity_status", "auto"],
        default="carla",
        help="speed source for speed_feedback mode; default preserves CARLA actor velocity behavior",
    )
    ap.add_argument("--velocity-status-topic", default="/vehicle/status/velocity_status")
    ap.add_argument("--velocity-status-stale-sec", type=float, default=0.5)
    ap.add_argument("--apply-hz", type=float, default=20.0, help="control apply frequency, one command per apply tick")
    ap.add_argument(
        "--sync-to-world-tick",
        dest="sync_to_world_tick",
        action="store_true",
        help="gate apply to at most one command per CARLA world frame",
    )
    ap.add_argument(
        "--no-sync-to-world-tick",
        dest="sync_to_world_tick",
        action="store_false",
        help="disable world-frame gate and apply purely by timer frequency",
    )
    ap.set_defaults(sync_to_world_tick=True)
    ap.add_argument(
        "--watchdog-wait-for-first-msg",
        dest="watchdog_wait_for_first_msg",
        action="store_true",
        help="do not arm watchdog braking until at least one control msg has been received",
    )
    ap.add_argument(
        "--no-watchdog-wait-for-first-msg",
        dest="watchdog_wait_for_first_msg",
        action="store_false",
        help="arm watchdog immediately even before the first control msg",
    )
    ap.set_defaults(watchdog_wait_for_first_msg=True)
    ap.add_argument("--watchdog-arm-delay-sec", type=float, default=1.5)
    ap.add_argument(
        "--startup-brake-suppression-enabled",
        dest="startup_brake_suppression_enabled",
        action="store_true",
        help="suppress small startup brake pulses while ego is nearly stopped",
    )
    ap.add_argument(
        "--startup-brake-suppression-disabled",
        dest="startup_brake_suppression_enabled",
        action="store_false",
        help="disable startup brake suppression",
    )
    ap.set_defaults(startup_brake_suppression_enabled=True)
    ap.add_argument("--startup-brake-suppression-speed-mps", type=float, default=1.0)
    ap.add_argument("--startup-brake-suppression-max-brake", type=float, default=0.2)
    ap.add_argument("--startup-brake-suppression-min-throttle", type=float, default=0.3)
    ap.add_argument("--startup-brake-suppression-hold-sec", type=float, default=3.0)
    ap.add_argument("--startup-brake-recent-throttle-window-sec", type=float, default=1.0)
    ap.add_argument(
        "--autoware-steer-sign",
        type=float,
        default=-1.0,
        help="Sign applied to Autoware steering_tire_angle before CARLA steer normalization.",
    )
    ap.add_argument("--dryrun", action="store_true", help="log mapped controls without applying to CARLA")
    return ap.parse_args()


def main():
    args = parse_args()
    rclpy.init(args=None)
    node = CarlaControlBridge(
        carla_host=args.carla_host,
        carla_port=args.carla_port,
        connect_timeout_sec=args.connect_timeout_sec,
        control_topic=args.control_topic,
        ego_role_name=args.ego_role_name,
        control_type=args.control_type,
        max_steer_angle=args.max_steer_angle,
        timeout_sec=args.timeout_sec,
        speed_gain=args.speed_gain,
        brake_gain=args.brake_gain,
        accel_throttle_gain=args.accel_throttle_gain,
        longitudinal_mode=args.longitudinal_mode,
        speed_feedback_gain=args.speed_feedback_gain,
        speed_feedback_brake_gain=args.speed_feedback_brake_gain,
        speed_feedback_deadband_mps=args.speed_feedback_deadband_mps,
        accel_feedforward_speed_margin_mps=args.accel_feedforward_speed_margin_mps,
        accel_feedforward_overspeed_taper_mps=args.accel_feedforward_overspeed_taper_mps,
        positive_accel_brake_inhibit_enabled=args.positive_accel_brake_inhibit_enabled,
        positive_accel_brake_inhibit_min_accel_mps2=args.positive_accel_brake_inhibit_min_accel_mps2,
        positive_accel_brake_inhibit_speed_margin_mps=args.positive_accel_brake_inhibit_speed_margin_mps,
        positive_accel_overshoot_action=args.positive_accel_overshoot_action,
        positive_accel_min_throttle=args.positive_accel_min_throttle,
        positive_accel_min_throttle_speed_mps=args.positive_accel_min_throttle_speed_mps,
        negative_accel_brake_speed_margin_mps=args.negative_accel_brake_speed_margin_mps,
        speed_feedback_transition_coast_sec=args.speed_feedback_transition_coast_sec,
        speed_feedback_max_throttle_step=args.speed_feedback_max_throttle_step,
        speed_feedback_max_brake_step=args.speed_feedback_max_brake_step,
        speed_source=args.speed_source,
        velocity_status_topic=args.velocity_status_topic,
        velocity_status_stale_sec=args.velocity_status_stale_sec,
        apply_hz=args.apply_hz,
        sync_to_world_tick=args.sync_to_world_tick,
        dryrun=args.dryrun,
        watchdog_wait_for_first_msg=args.watchdog_wait_for_first_msg,
        watchdog_arm_delay_sec=args.watchdog_arm_delay_sec,
        startup_brake_suppression_enabled=args.startup_brake_suppression_enabled,
        startup_brake_suppression_speed_mps=args.startup_brake_suppression_speed_mps,
        startup_brake_suppression_max_brake=args.startup_brake_suppression_max_brake,
        startup_brake_suppression_min_throttle=args.startup_brake_suppression_min_throttle,
        startup_brake_suppression_hold_sec=args.startup_brake_suppression_hold_sec,
        startup_brake_recent_throttle_window_sec=args.startup_brake_recent_throttle_window_sec,
        autoware_steer_sign=args.autoware_steer_sign,
    )
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
