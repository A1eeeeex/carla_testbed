#!/usr/bin/env python3
"""
轻量版跟停控制器：
- TruthEstimator -> Lateral -> ACC -> SafetySupervisor -> Actuator -> CompositeController
- 去除 legacy/route graph/Dijkstra/多版本限速，保留最小可解释逻辑
"""
import math
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Tuple
import carla

# Ensure CARLA agents are importable (only agents.* modules)
BasicAgent = None
BehaviorAgent = None
_basic_import_error = None
_behavior_import_error = None
_shapely_import_error = None
_networkx_import_error = None

def ensure_agents_imported():
    global BasicAgent, BehaviorAgent
    global _basic_import_error, _behavior_import_error, _shapely_import_error, _networkx_import_error
    if BasicAgent is not None and BehaviorAgent is not None:
        return

    root = Path(__file__).resolve().parents[2]
    paths_to_add = []
    # 先添加 dist 下 wheel/egg 以提供 carla 模块
    dist_dir = root / "PythonAPI" / "carla" / "dist"
    if dist_dir.exists():
        for pkg in sorted(dist_dir.glob("carla-*"), reverse=True):
            paths_to_add.append(pkg)
    paths_to_add += [
        root / "PythonAPI",
        root / "PythonAPI" / "carla",
    ]
    for p in paths_to_add:
        if p.exists() and str(p) not in sys.path:
            sys.path.insert(0, str(p))

    # Pre-flight dependency checks
    try:
        import shapely  # type: ignore
        _shapely_import_error = None
    except Exception as e:
        _shapely_import_error = e
    try:
        import networkx  # type: ignore
        _networkx_import_error = None
    except Exception as e:
        _networkx_import_error = e

    if BasicAgent is None:
        try:
            from agents.navigation.basic_agent import BasicAgent as BA  # type: ignore
            BasicAgent = BA
            _basic_import_error = None
        except Exception as e:
            _basic_import_error = e
    if BehaviorAgent is None:
        try:
            from agents.navigation.behavior_agent import BehaviorAgent as BehA  # type: ignore
            BehaviorAgent = BehA
            _behavior_import_error = None
        except Exception as e:
            _behavior_import_error = e

# Try import once on module load
ensure_agents_imported()

# ---------- 基础工具 ----------
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def clamp01(x):
    return clamp(x, 0.0, 1.0)

def wrap_pi(a):
    while a > math.pi:
        a -= 2.0 * math.pi
    while a < -math.pi:
        a += 2.0 * math.pi
    return a

def yaw_rad(tr: carla.Transform):
    return math.radians(tr.rotation.yaw)

def angle_diff_deg(a, b):
    return abs(math.degrees(wrap_pi(a - b)))

def speed_mps(vehicle: carla.Vehicle) -> float:
    v = vehicle.get_velocity()
    return math.sqrt(v.x * v.x + v.y * v.y + v.z * v.z)


# ---------- 分支选择（局部连续性） ----------
def choose_candidate_with_continuity(cands, prev_wp: carla.Waypoint, prev_yaw: float, return_score: bool = False):
    if not cands:
        return (prev_wp, 0.0) if return_score else prev_wp
    best = cands[0]
    best_score = 1e9
    for w in cands:
        yaw0 = abs(wrap_pi(yaw_rad(w.transform) - prev_yaw))
        lane_pen = 0.0 if w.lane_id == prev_wp.lane_id else 0.4
        future_pen = yaw0
        nxts = w.next(3.0)
        if nxts:
            w2 = LaneKeepingPurePursuit.choose_straight_candidate(nxts, yaw_rad(w.transform))
            future_pen = abs(wrap_pi(yaw_rad(w2.transform) - prev_yaw))
        score = 1.0 * yaw0 + 1.5 * future_pen + lane_pen
        if score < best_score:
            best_score = score
            best = w
    if return_score:
        return best, best_score
    return best


# ---------- 状态结构 ----------
@dataclass
class ControlState:
    t: float
    dt: float
    ego: carla.Vehicle
    front: carla.Vehicle
    world: carla.World
    carla_map: carla.Map
    v: float
    d: float  # 保留占位，不再直接使用

@dataclass
class EgoLeadState:
    t: float
    dt: float
    ego: carla.Vehicle
    front: carla.Vehicle
    carla_map: carla.Map
    v: float
    v_front: float
    gap_euclid: float
    gap_s: float
    gap_s_valid: bool
    gap_reason: str
    gap_match_d_center: Optional[float]
    gap_match_yaw_diff_deg: Optional[float]
    closing_v_s: float
    closing_v_s_valid: bool
    e_y: float
    e_psi: float
    kappa: float
    lane_id: int
    road_id: int
    section_id: Optional[int]
    lane_type: str
    speed_limit: float


# ---------- 几何辅助 ----------
def compute_lateral_error(carla_map: carla.Map, ego: carla.Vehicle, ref_wp: Optional[carla.Waypoint] = None) -> float:
    wp = ref_wp or carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    return ego.get_location().distance(wp.transform.location)

def compute_signed_lateral_error(carla_map: carla.Map, ego: carla.Vehicle, ref_wp: Optional[carla.Waypoint] = None) -> float:
    wp = ref_wp or carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    loc = ego.get_location()
    center = wp.transform.location
    dx = loc.x - center.x
    dy = loc.y - center.y
    fwd = wp.transform.get_forward_vector()
    sign = 1.0 if (fwd.x * dy - fwd.y * dx) >= 0 else -1.0
    return sign * math.hypot(dx, dy)

def heading_error_to_lane(carla_map: carla.Map, ego: carla.Vehicle, ref_wp: Optional[carla.Waypoint] = None) -> float:
    wp = ref_wp or carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    return wrap_pi(yaw_rad(ego.get_transform()) - yaw_rad(wp.transform))

def estimate_lane_curvature(carla_map: carla.Map, ego: carla.Vehicle, ds: float = 8.0, ref_wp: Optional[carla.Waypoint] = None) -> float:
    wp0 = ref_wp or carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    yaw0 = yaw_rad(wp0.transform)
    nxts = wp0.next(ds)
    if not nxts:
        return 0.0
    chooser = choose_candidate_with_continuity if (wp0.is_junction or any(getattr(w, "is_junction", False) for w in nxts)) else LaneKeepingPurePursuit.choose_straight_candidate
    if chooser is choose_candidate_with_continuity:
        wp1 = chooser(nxts, prev_wp=wp0, prev_yaw=yaw0)
    else:
        wp1 = chooser(nxts, yaw0)
    yaw1 = yaw_rad(wp1.transform)
    return wrap_pi(yaw1 - yaw0) / max(ds, 1e-3)

def lane_info(carla_map: carla.Map, ego: carla.Vehicle, ref_wp: Optional[carla.Waypoint] = None):
    wp = ref_wp or carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    speed_limit = None
    for attr in ["speed_limit", "get_speed_limit"]:
        try:
            cand = getattr(wp, attr)
            speed_limit = cand() if callable(cand) else cand
            if speed_limit is not None:
                break
        except Exception:
            continue
    if speed_limit is None:
        try:
            speed_limit = ego.get_speed_limit()
        except Exception:
            speed_limit = 0.0
    return wp.lane_id, getattr(wp, "road_id", 0), getattr(wp, "section_id", None), str(wp.lane_type), speed_limit


# ---------- 同道 gap 估计 ----------
def gap_s_along_reachable_lane(
    carla_map: carla.Map,
    ego_wp0: carla.Waypoint,
    front_loc: carla.Location,
    front_wp: carla.Waypoint,
    max_dist: float = 150.0,
    step: float = 1.5,
    match_dist: float = 2.0,
    yaw_tol_deg: float = 40.0,
) -> Tuple[float, bool, Optional[Tuple[float, float]], dict]:
    info = {
        "reason": "",
        "matched": False,
        "best_d_center": None,
        "best_yaw_diff": None,
        "ref_lane_id": None,
        "ref_road_id": None,
        "ref_section_id": None,
    }
    s_acc = 0.0
    wp = ego_wp0
    best = None
    best_score = 1e9
    max_steps = max(1, int(max_dist / max(step, 1e-3)))

    front_yaw = yaw_rad(front_wp.transform)
    for _ in range(max_steps):
        loc_c = wp.transform.location
        d_center = loc_c.distance(front_loc)
        yaw_diff = angle_diff_deg(yaw_rad(wp.transform), front_yaw)
        lane_sign_ok = wp.lane_id * front_wp.lane_id > 0
        if d_center <= match_dist and yaw_diff <= yaw_tol_deg and lane_sign_ok:
            score = d_center + 0.02 * yaw_diff
            if score < best_score:
                best_score = score
                fwd = wp.transform.get_forward_vector()
                norm = math.hypot(fwd.x, fwd.y)
                hx = fwd.x / max(norm, 1e-3)
                hy = fwd.y / max(norm, 1e-3)
                best = {"s": s_acc, "d": d_center, "yaw": yaw_diff, "fwd": (hx, hy), "wp": wp}
        nxts = wp.next(step)
        if not nxts:
            break
        chooser = choose_candidate_with_continuity if (wp.is_junction or any(getattr(w, "is_junction", False) for w in nxts)) else LaneKeepingPurePursuit.choose_straight_candidate
        if chooser is choose_candidate_with_continuity:
            wp_next = chooser(nxts, prev_wp=wp, prev_yaw=yaw_rad(wp.transform))
        else:
            wp_next = chooser(nxts, yaw_rad(wp.transform))
        seg = wp.transform.location.distance(wp_next.transform.location)
        s_acc += max(seg, step)
        wp = wp_next

    if best is None:
        info["reason"] = "NOT_REACHABLE"
        return math.inf, False, None, info

    info["matched"] = True
    info["reason"] = "OK"
    info["best_d_center"] = best["d"]
    info["best_yaw_diff"] = best["yaw"]
    info["ref_lane_id"] = best["wp"].lane_id
    info["ref_road_id"] = best["wp"].road_id
    info["ref_section_id"] = getattr(best["wp"], "section_id", None)
    return best["s"], True, best["fwd"], info


def closing_speed_along_lane(carla_map: carla.Map, ego: carla.Vehicle, front: carla.Vehicle) -> float:
    ego_wp = carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    fwd = ego_wp.transform.get_forward_vector()
    hx, hy = fwd.x, fwd.y
    norm = math.hypot(hx, hy)
    hx /= max(norm, 1e-3)
    hy /= max(norm, 1e-3)
    ego_v = ego.get_velocity()
    front_v = front.get_velocity()
    ego_along = ego_v.x * hx + ego_v.y * hy
    front_along = front_v.x * hx + front_v.y * hy
    return ego_along - front_along


# ---------- Estimator ----------
class TruthEstimator:
    def __init__(self, curvature_ds: float = 8.0):
        self.curvature_ds = curvature_ds
        self.last_wp: Optional[carla.Waypoint] = None

    def _ref_wp(self, carla_map: carla.Map, ego: carla.Vehicle) -> carla.Waypoint:
        loc = ego.get_location()
        wp = carla_map.get_waypoint(loc, project_to_road=True, lane_type=carla.LaneType.Driving)
        if self.last_wp is not None:
            yaw_diff = angle_diff_deg(yaw_rad(wp.transform), yaw_rad(self.last_wp.transform))
            if yaw_diff > 90.0:
                nxts = self.last_wp.next(2.0)
                if nxts:
                    wp = LaneKeepingPurePursuit.choose_straight_candidate(nxts, yaw_rad(self.last_wp.transform))
        self.last_wp = wp
        return wp

    def compute(self, raw: ControlState) -> EgoLeadState:
        ego = raw.ego
        front = raw.front
        carla_map = raw.carla_map
        ref_wp = self._ref_wp(carla_map, ego)
        front_wp = carla_map.get_waypoint(front.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)

        v = raw.v
        v_front = speed_mps(front)
        gap_euclid = ego.get_location().distance(front.get_location())

        if front_wp is None or ref_wp is None:
            gap_s = math.inf
            gap_valid = False
            fwd_dir = None
            meta = {"reason": "NO_WP"}
        else:
            gap_s, gap_valid, fwd_dir, meta = gap_s_along_reachable_lane(
                carla_map, ref_wp, front.get_location(), front_wp
            )
        if gap_valid and gap_s < 0.3 * max(gap_euclid, 1e-3):
            gap_valid = False
            gap_s = math.inf
            meta["reason"] = "SANITY_REJECT"

        if gap_valid and fwd_dir is not None:
            ego_v = ego.get_velocity()
            front_v = front.get_velocity()
            hx, hy = fwd_dir
            closing_v_s = (ego_v.x - front_v.x) * hx + (ego_v.y - front_v.y) * hy
            closing_valid = True
        else:
            closing_v_s = 0.0
            closing_valid = False

        e_y = compute_signed_lateral_error(carla_map, ego, ref_wp)
        e_psi = heading_error_to_lane(carla_map, ego, ref_wp)
        kappa = estimate_lane_curvature(carla_map, ego, ds=self.curvature_ds, ref_wp=ref_wp)
        lane_id, road_id, section_id, lane_type, speed_limit = lane_info(carla_map, ego, ref_wp)

        return EgoLeadState(
            t=raw.t,
            dt=raw.dt,
            ego=ego,
            front=front,
            carla_map=carla_map,
            v=v,
            v_front=v_front,
            gap_euclid=gap_euclid,
            gap_s=gap_s,
            gap_s_valid=gap_valid,
            gap_reason=meta.get("reason", ""),
            gap_match_d_center=meta.get("best_d_center"),
            gap_match_yaw_diff_deg=meta.get("best_yaw_diff"),
            closing_v_s=closing_v_s,
            closing_v_s_valid=closing_valid,
            e_y=e_y,
            e_psi=e_psi,
            kappa=kappa,
        lane_id=lane_id,
        road_id=road_id,
        section_id=section_id,
        lane_type=lane_type,
        speed_limit=speed_limit,
        )


# ---------- 配置 ----------
@dataclass
class ACCConfig:
    s0: float = 9.0
    headway: float = 1.6
    kp: float = 0.35
    kd: float = 0.55
    a_max: float = 2.0
    b_max: float = 6.0
    k_v: float = 0.5

@dataclass
class LateralConfig:
    wheelbase: float = 2.8
    steer_limit: float = 0.7
    lookahead_min: float = 8.0
    lookahead_max: float = 20.0
    lookahead_k: float = 0.55
    steer_smooth: float = 0.75
    steer_rate_limit: float = 2.2
    kappa_short_th: float = 0.02
    lookahead_shrink: float = 0.55

@dataclass
class SafetyConfig:
    v_cruise_mps: float = 22.22
    v_cruise_tol_mps: float = 0.5
    v_cruise_brake_a: float = 1.8
    a_lat_max: float = 1.8
    kappa_curve_th: float = 0.02
    curve_v_cap_mps: float = 7.0
    d_brake_m: float = 8.0
    hard_gap_thr: float = 8.0
    hard_gap_euclid_thr: float = 20.0
    closing_thr: float = 0.4
    a_brake_hard: float = 9.0
    lat_guard_e_y: float = 1.0
    lat_guard_e_psi: float = 0.35
    lat_guard_target_v: float = 7.0
    lat_guard_hard_e_y: float = 2.5

@dataclass
class ActuatorConfig:
    a_max: float = 2.0
    b_max: float = 6.0
    j_max_acc: float = 2.5
    j_max_dec: float = 6.0
    v_stop_thr: float = 0.2
    hold_brake: float = 0.4
    stop_dist: float = 8.0


# ---------- 横向控制 ----------
class LaneKeepingPurePursuit:
    def __init__(self, cfg: LateralConfig):
        self.cfg = cfg
        self.prev_steer = 0.0
        self.ref_wp: Optional[carla.Waypoint] = None
        self.ref_yaw: float = 0.0
        self.last_target_wp: Optional[carla.Waypoint] = None
        self.last_branch_score: Optional[float] = None

    @staticmethod
    def choose_straight_candidate(cands, current_yaw):
        best = cands[0]
        best_err = 1e9
        for w in cands:
            err = abs(wrap_pi(yaw_rad(w.transform) - current_yaw))
            if err < best_err:
                best_err = err
                best = w
        return best

    def reset(self):
        self.prev_steer = 0.0
        self.ref_wp = None
        self.ref_yaw = 0.0
        self.last_target_wp = None
        self.last_branch_score = None

    def step(self, state: EgoLeadState) -> float:
        cfg = self.cfg
        ego = state.ego
        ego_tr = ego.get_transform()
        ego_loc = ego_tr.location
        ego_yaw = yaw_rad(ego_tr)

        if self.ref_wp is None:
            self.ref_wp = state.carla_map.get_waypoint(ego_loc, project_to_road=True, lane_type=carla.LaneType.Driving)
            self.ref_yaw = yaw_rad(self.ref_wp.transform)

        ds_step = clamp(state.v * state.dt, 0.5, 2.0)
        cands = self.ref_wp.next(ds_step)
        if cands:
            self.ref_wp, _ = choose_candidate_with_continuity(cands, prev_wp=self.ref_wp, prev_yaw=self.ref_yaw, return_score=True)
            self.ref_yaw = yaw_rad(self.ref_wp.transform)

        lookahead = clamp(cfg.lookahead_min + cfg.lookahead_k * state.v, cfg.lookahead_min, cfg.lookahead_max)
        if self.ref_wp.is_junction:
            lookahead = min(lookahead, 6.0)

        tgt_cands = self.ref_wp.next(lookahead)
        if tgt_cands:
            target_wp, score = choose_candidate_with_continuity(tgt_cands, prev_wp=self.ref_wp, prev_yaw=self.ref_yaw, return_score=True)
        else:
            target_wp, score = self.ref_wp, None
        self.last_target_wp = target_wp
        self.last_branch_score = score

        loc = ego_loc
        tgt = target_wp.transform.location
        dx, dy = tgt.x - loc.x, tgt.y - loc.y
        alpha = wrap_pi(math.atan2(dy, dx) - ego_yaw)

        kappa_est = abs(alpha) / max(lookahead, 1e-3)
        if kappa_est > cfg.kappa_short_th:
            lookahead = max(cfg.lookahead_min, lookahead * cfg.lookahead_shrink)
            tgt_cands = self.ref_wp.next(lookahead)
            if tgt_cands:
                target_wp, score = choose_candidate_with_continuity(tgt_cands, prev_wp=self.ref_wp, prev_yaw=self.ref_yaw, return_score=True)
                self.last_target_wp = target_wp
                self.last_branch_score = score
                tgt = target_wp.transform.location
                dx, dy = tgt.x - loc.x, tgt.y - loc.y
                alpha = wrap_pi(math.atan2(dy, dx) - ego_yaw)

        delta = math.atan2(2.0 * cfg.wheelbase * math.sin(alpha), max(lookahead, 1e-3))
        pc = state.ego.get_physics_control()
        max_steer_rad = math.radians(pc.wheels[0].max_steer_angle) if pc.wheels else 1.0
        steer_raw = clamp(delta / max_steer_rad, -cfg.steer_limit, cfg.steer_limit)
        max_delta = cfg.steer_rate_limit * state.dt
        steer_limited = clamp(steer_raw, self.prev_steer - max_delta, self.prev_steer + max_delta)
        steer = cfg.steer_smooth * self.prev_steer + (1.0 - cfg.steer_smooth) * steer_limited
        steer = clamp(steer, -cfg.steer_limit, cfg.steer_limit)
        self.prev_steer = steer
        return steer


class StanleyLateralController:
    def __init__(self, steer_limit: float = 0.7):
        self.k_y = 0.8
        self.k_psi = 1.2
        self.steer_limit = steer_limit

    def reset(self):
        pass

    def step(self, state: EgoLeadState) -> float:
        v = max(state.v, 0.1)
        steer = self.k_psi * state.e_psi + math.atan2(self.k_y * state.e_y, v)
        return clamp(steer, -self.steer_limit, self.steer_limit)


class DummyLateral:
    """占位横向控制：保持直行/零转向，用于仅测试纵向 ACC 或外部横向接管。"""
    name = "dummy_lateral"

    def reset(self):
        pass

    def step(self, state: EgoLeadState) -> float:  # noqa: ARG002
        return 0.0


# ---------- 纵向策略 ----------
class ACCPolicy:
    def __init__(self, cfg: ACCConfig, v_cruise_mps: float):
        self.cfg = cfg
        self.v_cruise = v_cruise_mps
        self.last_s_des = cfg.s0

    def reset(self):
        self.last_s_des = self.cfg.s0

    def step(self, state: EgoLeadState) -> float:
        if not state.gap_s_valid:
            a_ref = self.cfg.k_v * (self.v_cruise - state.v)
            return clamp(a_ref, -self.cfg.b_max, self.cfg.a_max)

        s_des = self.cfg.s0 + self.cfg.headway * max(state.v, 0.0)
        self.last_s_des = s_des
        e = state.gap_s - s_des
        closing = state.closing_v_s if state.closing_v_s_valid else 0.0
        a_ref = self.cfg.kp * e - self.cfg.kd * closing
        return clamp(a_ref, -self.cfg.b_max, self.cfg.a_max)


# ---------- 安全监督 ----------
class SafetySupervisor:
    def __init__(self, cfg: SafetyConfig):
        self.cfg = cfg

    def limit(self, state: EgoLeadState, steer: float, a_ref: float) -> Tuple[float, dict]:
        cfg = self.cfg
        info = {
            "v_cap": None,
            "curve_limited": False,
            "cruise_limited": False,
            "hard_brake": False,
            "lat_guard": False,
            "a_need": None,
            "reason": "",
        }
        a_safe = a_ref

        # 巡航上限
        if state.v > cfg.v_cruise_mps + cfg.v_cruise_tol_mps:
            info["cruise_limited"] = True
            a_safe = min(a_safe, 0.0)
            if state.v > cfg.v_cruise_mps + 2.0:
                a_safe = min(a_safe, -cfg.v_cruise_brake_a)

        # 曲率限速（本地小步）
        kappa_abs = abs(state.kappa)
        if kappa_abs > 1e-5:
            v_cap = math.sqrt(cfg.a_lat_max / kappa_abs)
            if kappa_abs >= cfg.kappa_curve_th:
                v_cap = min(v_cap, cfg.curve_v_cap_mps)
            info["v_cap"] = v_cap
            if state.v > v_cap:
                a_need = (v_cap * v_cap - state.v * state.v) / (2.0 * cfg.d_brake_m)
                a_need = clamp(a_need, -cfg.a_brake_hard, 0.0)
                info["a_need"] = a_need
                info["curve_limited"] = True
                a_safe = min(a_safe, a_need)

        # 硬刹（要求 gap/closing 有效且接近）
        if (
            state.gap_s_valid
            and state.closing_v_s_valid
            and state.gap_euclid < cfg.hard_gap_euclid_thr
            and state.gap_s < cfg.hard_gap_thr
            and state.closing_v_s > cfg.closing_thr
        ):
            a_safe = -cfg.a_brake_hard
            info["hard_brake"] = True
            info["reason"] = "HARD_BRAKE"

        # 横向保护：降速而非停车
        if abs(state.e_y) > cfg.lat_guard_e_y or abs(state.e_psi) > cfg.lat_guard_e_psi:
            target_v = cfg.lat_guard_target_v
            if state.v > target_v:
                a_need = (target_v * target_v - state.v * state.v) / (2.0 * cfg.d_brake_m)
                a_need = clamp(a_need, -cfg.a_brake_hard, 0.0)
                a_safe = min(a_safe, a_need)
                info["a_need"] = a_need
                info["lat_guard"] = True
                info["reason"] = "LAT_GUARD"
            if abs(state.e_y) > cfg.lat_guard_hard_e_y:
                a_safe = min(a_safe, -cfg.a_brake_hard)
                info["hard_brake"] = True
                info["reason"] = "LAT_HARD"

        return a_safe, info


# ---------- 执行器 ----------
class LongitudinalActuator:
    def __init__(self, cfg: ActuatorConfig):
        self.cfg = cfg
        self.prev_a = 0.0
        self.holding = False

    def reset(self):
        self.prev_a = 0.0
        self.holding = False

    def _limit_jerk(self, a_cmd: float, dt: float) -> float:
        if dt <= 0:
            return a_cmd
        if a_cmd > self.prev_a:
            j = self.cfg.j_max_acc
        else:
            j = self.cfg.j_max_dec
        max_delta = j * dt
        return clamp(a_cmd, self.prev_a - max_delta, self.prev_a + max_delta)

    def _should_hold(self, state: EgoLeadState) -> bool:
        return (
            state.gap_s_valid
            and state.gap_euclid < self.cfg.stop_dist + 1.0
            and state.v < self.cfg.v_stop_thr
            and (not state.closing_v_s_valid or state.closing_v_s <= 0.2)
        )

    def apply(self, state: EgoLeadState, a_cmd: float) -> Tuple[float, float, float, bool]:
        if not self._should_hold(state):
            self.holding = False

        a_cmd = clamp(a_cmd, -self.cfg.b_max, self.cfg.a_max)
        a_cmd = self._limit_jerk(a_cmd, state.dt)

        if self._should_hold(state):
            self.holding = True
            self.prev_a = -self.cfg.b_max
            return 0.0, self.cfg.hold_brake, self.prev_a, True

        self.prev_a = a_cmd
        if a_cmd >= 0:
            thr = clamp01(a_cmd / self.cfg.a_max)
            return thr, 0.0, a_cmd, False
        brk = clamp01(-a_cmd / self.cfg.b_max)
        return 0.0, brk, a_cmd, False


# ---------- 组合控制器 ----------
class CompositeController:
    name = "composite"

    def __init__(self, lateral, policy: ACCPolicy, actuator: LongitudinalActuator, supervisor: SafetySupervisor, estimator: TruthEstimator):
        self.lateral = lateral
        self.policy = policy
        self.actuator = actuator
        self.supervisor = supervisor
        self.estimator = estimator
        self.last_debug = {}

    def reset(self):
        if hasattr(self.lateral, "reset"):
            self.lateral.reset()
        if hasattr(self.policy, "reset"):
            self.policy.reset()
        if hasattr(self.actuator, "reset"):
            self.actuator.reset()
        self.last_debug = {}

    def step(self, raw: ControlState) -> carla.VehicleControl:
        state = self.estimator.compute(raw)
        steer = self.lateral.step(state)
        a_ref = self.policy.step(state)
        a_safe, sup_info = self.supervisor.limit(state, steer, a_ref)
        thr, brk, a_applied, hold_active = self.actuator.apply(state, a_safe)

        stop_reason = "NONE"
        if sup_info.get("hard_brake"):
            stop_reason = "HARD_BRAKE"
        elif sup_info.get("lat_guard"):
            stop_reason = "LAT_GUARD"
        elif sup_info.get("curve_limited"):
            stop_reason = "CURVE_LIMIT"
        elif sup_info.get("cruise_limited"):
            stop_reason = "CRUISE_LIMIT"
        if hold_active:
            stop_reason = "HOLD"

        self.last_debug = {
            "v": state.v,
            "gap_s": state.gap_s,
            "gap_euclid": state.gap_euclid,
            "gap_s_valid": state.gap_s_valid,
            "gap_reason": state.gap_reason,
            "match_d_center": state.gap_match_d_center,
            "match_yaw_diff_deg": state.gap_match_yaw_diff_deg,
            "closing_v_s": state.closing_v_s,
            "closing_v_s_valid": state.closing_v_s_valid,
            "e_y": state.e_y,
            "e_psi": state.e_psi,
            "kappa": state.kappa,
            "a_ref": a_ref,
            "a_safe": a_safe,
            "a_applied": a_applied,
            "thr": thr,
            "brk": brk,
            "steer": steer,
            "v_cap": sup_info.get("v_cap"),
            "curve_limited": sup_info.get("curve_limited"),
            "cruise_limited": sup_info.get("cruise_limited"),
            "hard_brake": sup_info.get("hard_brake"),
            "lat_guard": sup_info.get("lat_guard"),
            "hold_active": hold_active,
            "stop_reason": stop_reason,
            # 占位兼容字段
            "steer_limited": False,
            "v_cap_preview": None,
            "kappa_preview_max": None,
            "dist_to_curve_m": None,
            "a_need_curve_mps2": sup_info.get("a_need"),
            "v_cruise_mps": self.supervisor.cfg.v_cruise_mps,
            "v_over_mps": max(0.0, state.v - self.supervisor.cfg.v_cruise_mps),
            "d_need_m": None,
            "v_entry_mps": None,
            "curve_brake_active": sup_info.get("curve_limited", False),
            "e_psi_guard_triggered": False,
            "e_y_guard_triggered": False,
            "ref_lane_id": state.lane_id,
            "ref_road_id": state.road_id,
            "ref_section_id": state.section_id,
            "ref_wp_is_junction": getattr(getattr(self.lateral, "ref_wp", None), "is_junction", False),
            "target_wp_lane_id": getattr(getattr(self.lateral, "last_target_wp", None), "lane_id", None),
            "target_wp_road_id": getattr(getattr(self.lateral, "last_target_wp", None), "road_id", None),
            "target_wp_section_id": getattr(getattr(self.lateral, "last_target_wp", None), "section_id", None),
            "target_wp_is_junction": getattr(getattr(self.lateral, "last_target_wp", None), "is_junction", False),
            "branch_choice_score": getattr(self.lateral, "last_branch_score", None),
            "s_des": getattr(self.policy, "last_s_des", None),
        }

        c = carla.VehicleControl()
        c.throttle = thr
        c.brake = brk
        c.steer = steer
        c.hand_brake = False
        c.reverse = False
        c.manual_gear_shift = False
        return c

    def _desired_gap(self, v: float) -> float:
        return getattr(self.policy, "last_s_des", 0.0)

# ---------- 混合 Agent 纵向接管控制器 ----------
class HybridAgentLongitudinalController:
    name = "hybrid_agent_acc"

    def __init__(
        self,
        carla_world: carla.World,
        carla_map: carla.Map,
        ego_vehicle: carla.Vehicle,
        front_vehicle: carla.Vehicle,
        agent_type: str = "basic",
        takeover_dist_m: float = 200.0,
        blend_time_s: float = 1.5,
        acc_cfg: ACCConfig = None,
        safety_cfg: SafetyConfig = None,
        actuator_cfg: ActuatorConfig = None,
    ):
        ensure_agents_imported()
        missing = []
        errors = {}
        if agent_type == "basic" and BasicAgent is None:
            missing.append("BasicAgent")
            errors["BasicAgent_error"] = repr(_basic_import_error)
        if agent_type == "behavior" and BehaviorAgent is None:
            missing.append("BehaviorAgent")
            errors["BehaviorAgent_error"] = repr(_behavior_import_error)
        if missing:
            root = Path(__file__).resolve().parents[2]
            basic_path = root / "PythonAPI" / "carla" / "agents" / "navigation" / "basic_agent.py"
            behavior_path = root / "PythonAPI" / "carla" / "agents" / "navigation" / "behavior_agent.py"
            exists_info = {
                "basic_agent.py": basic_path.exists(),
                "behavior_agent.py": behavior_path.exists(),
                "shapely_ok": _shapely_import_error is None,
                "networkx_ok": _networkx_import_error is None,
            }
            path_head = sys.path[:8]
            raise RuntimeError(
                f"Missing agents: {missing}; import_errors={errors}; exists={exists_info}; "
                f"sys.path[:8]={path_head} "
                f"(attempted module agents.navigation.basic_agent; ensure shapely, networkx installed)"
            )
        self.world = carla_world
        self.map = carla_map
        self.ego = ego_vehicle
        self.front = front_vehicle
        self.takeover_dist = takeover_dist_m
        self.blend_time_s = max(0.0, blend_time_s)
        self._blend_alpha = 0.0

        acc_cfg = acc_cfg or ACCConfig()
        safety_cfg = safety_cfg or SafetyConfig()
        actuator_cfg = actuator_cfg or ActuatorConfig(a_max=acc_cfg.a_max, b_max=acc_cfg.b_max, stop_dist=acc_cfg.s0)

        if agent_type == "basic":
            self.agent = BasicAgent(self.ego, target_speed=80.0)
        elif agent_type == "behavior":
            self.agent = BehaviorAgent(self.ego, behavior="cautious")
            try:
                self.agent.set_target_speed(80.0)
            except Exception:
                pass
        else:
            raise ValueError("unknown agent_type")
        self.agent_type = agent_type
        self.estimator = TruthEstimator()
        self.policy = ACCPolicy(acc_cfg, safety_cfg.v_cruise_mps)
        self.supervisor = SafetySupervisor(safety_cfg)
        self.actuator = LongitudinalActuator(actuator_cfg)
        self.last_debug = {}
        self._set_destination_to_front()

    def _set_destination_to_front(self, gap_hint: Optional[float] = None):
        """动态更新目标点：远距离时沿 ego 车道向前引导，靠近时沿前车车道前方目标。"""
        try:
            ego_wp = self.map.get_waypoint(self.ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
            front_wp = self.map.get_waypoint(self.front.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
            if ego_wp is None or front_wp is None:
                return
            step = 3.0
            # 远距离：沿 ego 车道向前引导，避免掉头
            if gap_hint is None or not math.isfinite(gap_hint) or gap_hint > self.takeover_dist * 1.2:
                dest_wp = ego_wp
                ahead = 60.0
                moved = 0.0
                while dest_wp is not None and moved < ahead:
                    nxts = dest_wp.next(step)
                    if not nxts:
                        break
                    dest_wp = nxts[0]
                    moved += step
            else:
                dest_wp = front_wp
                stop_margin = (self.actuator.cfg.stop_dist if hasattr(self, "actuator") else 8.0) + 5.0
                forward_target = max(10.0, min(gap_hint * 0.6, self.takeover_dist * 0.9))
                moved = 0.0
                while dest_wp is not None and moved < forward_target:
                    nxts = dest_wp.next(step)
                    if not nxts:
                        break
                    dest_wp = nxts[0]
                    moved += step
                if dest_wp is None:
                    dest_wp = front_wp
                    moved = 0.0
                    while dest_wp is not None and moved < stop_margin:
                        prevs = dest_wp.previous(step)
                        if not prevs:
                            break
                        dest_wp = prevs[0]
                        moved += step
            dest_loc = dest_wp.transform.location if dest_wp is not None else self.front.get_location()
            self.agent.set_destination(dest_loc)
        except Exception:
            pass

    def reset(self):
        self._blend_alpha = 0.0
        if hasattr(self.policy, "reset"):
            self.policy.reset()
        if hasattr(self.actuator, "reset"):
            self.actuator.reset()
        if hasattr(self.estimator, "last_wp"):
            self.estimator.last_wp = None
        self._set_destination_to_front()
        self.last_debug = {}

    def step(self, raw: ControlState) -> carla.VehicleControl:
        state = self.estimator.compute(raw)
        self._set_destination_to_front(gap_hint=state.gap_euclid)
        d_front = state.gap_euclid
        agent_ctrl = self.agent.run_step()

        takeover = d_front <= self.takeover_dist
        my_thr = my_brk = 0.0
        a_ref = a_safe = a_applied = 0.0
        hold_active = False
        sup_info = {"curve_limited": False, "cruise_limited": False, "hard_brake": False, "lat_guard": False, "v_cap": None}

        if not takeover:
            try:
                self.agent.set_target_speed(40.0)
            except Exception:
                pass

        if takeover:
            # 动态调整 agent 目标速度：弯中/预弯时放缓，直线可恢复
            try:
                kappa_abs = abs(state.kappa)
                target_v_agent = 30.0 if kappa_abs > self.supervisor.cfg.kappa_curve_th else 80.0
                self.agent.set_target_speed(target_v_agent)
            except Exception:
                pass
            a_ref = self.policy.step(state)
            a_safe, sup_info = self.supervisor.limit(state, agent_ctrl.steer, a_ref)
            my_thr, my_brk, a_applied, hold_active = self.actuator.apply(state, a_safe)
            if self.blend_time_s > 0:
                self._blend_alpha = clamp(self._blend_alpha + state.dt / self.blend_time_s, 0.0, 1.0)
                thr = (1 - self._blend_alpha) * agent_ctrl.throttle + self._blend_alpha * my_thr
                brk = (1 - self._blend_alpha) * agent_ctrl.brake + self._blend_alpha * my_brk
            else:
                thr, brk = my_thr, my_brk
        else:
            self._blend_alpha = 0.0
            thr, brk = agent_ctrl.throttle, agent_ctrl.brake

        stop_reason = "NONE"
        if sup_info.get("hard_brake"):
            stop_reason = "HARD_BRAKE"
        elif sup_info.get("lat_guard"):
            stop_reason = "LAT_GUARD"
        elif sup_info.get("curve_limited"):
            stop_reason = "CURVE_LIMIT"
        elif sup_info.get("cruise_limited"):
            stop_reason = "CRUISE_LIMIT"
        if hold_active:
            stop_reason = "HOLD"

        self.last_debug = {
            "v": state.v,
            "gap_s": state.gap_s,
            "gap_euclid": state.gap_euclid,
            "gap_s_valid": state.gap_s_valid,
            "gap_reason": state.gap_reason,
            "match_d_center": state.gap_match_d_center,
            "match_yaw_diff_deg": state.gap_match_yaw_diff_deg,
            "closing_v_s": state.closing_v_s,
            "closing_v_s_valid": state.closing_v_s_valid,
            "e_y": state.e_y,
            "e_psi": state.e_psi,
            "kappa": state.kappa,
            "a_ref": a_ref,
            "a_safe": a_safe,
            "a_applied": a_applied,
            "thr": thr,
            "brk": brk,
            "steer": agent_ctrl.steer,
            "v_cap": sup_info.get("v_cap"),
            "curve_limited": sup_info.get("curve_limited"),
            "cruise_limited": sup_info.get("cruise_limited"),
            "hard_brake": sup_info.get("hard_brake"),
            "lat_guard": sup_info.get("lat_guard"),
            "hold_active": hold_active,
            "stop_reason": stop_reason,
            "steer_limited": False,
            "v_cap_preview": None,
            "kappa_preview_max": None,
            "dist_to_curve_m": None,
            "a_need_curve_mps2": sup_info.get("a_need"),
            "v_cruise_mps": self.supervisor.cfg.v_cruise_mps,
            "v_over_mps": max(0.0, state.v - self.supervisor.cfg.v_cruise_mps),
            "d_need_m": None,
            "v_entry_mps": None,
            "curve_brake_active": sup_info.get("curve_limited", False),
            "e_psi_guard_triggered": False,
            "e_y_guard_triggered": False,
            "ref_lane_id": state.lane_id,
            "ref_road_id": state.road_id,
            "ref_section_id": state.section_id,
            "target_wp_lane_id": getattr(getattr(self, "estimator", None).last_wp, "lane_id", None),
            "target_wp_road_id": getattr(getattr(self, "estimator", None).last_wp, "road_id", None),
            "target_wp_section_id": getattr(getattr(self, "estimator", None).last_wp, "section_id", None),
            "target_wp_is_junction": getattr(getattr(self, "estimator", None).last_wp, "is_junction", False),
            "takeover_active": takeover,
            "blend_alpha": self._blend_alpha,
            "d_front_euclid": d_front,
            "agent_thr": agent_ctrl.throttle,
            "agent_brk": agent_ctrl.brake,
            "agent_steer": agent_ctrl.steer,
            "my_thr": my_thr,
            "my_brk": my_brk,
            "s_des": getattr(self.policy, "last_s_des", None),
        }

        c = carla.VehicleControl()
        c.throttle = thr
        c.brake = brk
        c.steer = agent_ctrl.steer
        c.hand_brake = False
        c.reverse = False
        c.manual_gear_shift = False
        return c


# ---------- 构造器 ----------
def build_default_controller(
    lateral_mode: str = "pure_pursuit",
    policy_mode: str = "acc",
    acc_cfg: ACCConfig = None,
    lat_cfg: LateralConfig = None,
    safety_cfg: SafetyConfig = None,
    actuator_cfg: ActuatorConfig = None,
    controller_mode: str = "composite",
    agent_type: str = "basic",
    takeover_dist_m: float = 200.0,
    blend_time_s: float = 1.5,
    carla_world: Optional[carla.World] = None,
    carla_map: Optional[carla.Map] = None,
    ego_vehicle: Optional[carla.Vehicle] = None,
    front_vehicle: Optional[carla.Vehicle] = None,
):
    acc_cfg = acc_cfg or ACCConfig()
    lat_cfg = lat_cfg or LateralConfig()
    safety_cfg = safety_cfg or SafetyConfig()
    actuator_cfg = actuator_cfg or ActuatorConfig(a_max=acc_cfg.a_max, b_max=acc_cfg.b_max, stop_dist=acc_cfg.s0)

    if controller_mode == "hybrid_agent_acc":
        if carla_world is None or carla_map is None or ego_vehicle is None or front_vehicle is None:
            raise ValueError("Hybrid controller requires world/map/ego/front vehicles")
        ctrl = HybridAgentLongitudinalController(
            carla_world=carla_world,
            carla_map=carla_map,
            ego_vehicle=ego_vehicle,
            front_vehicle=front_vehicle,
            agent_type=agent_type,
            takeover_dist_m=takeover_dist_m,
            blend_time_s=blend_time_s,
            acc_cfg=acc_cfg,
            safety_cfg=safety_cfg,
            actuator_cfg=actuator_cfg,
        )
        return ctrl

    if lateral_mode == "pure_pursuit":
        lateral = LaneKeepingPurePursuit(lat_cfg)
    elif lateral_mode == "dummy":
        lateral = DummyLateral()
    elif lateral_mode == "stanley":
        lateral = StanleyLateralController(steer_limit=lat_cfg.steer_limit)
    else:
        raise ValueError(f"unknown lateral_mode {lateral_mode}")

    if policy_mode != "acc":
        raise ValueError("only acc policy is supported")

    policy = ACCPolicy(acc_cfg, safety_cfg.v_cruise_mps)
    actuator = LongitudinalActuator(actuator_cfg)
    supervisor = SafetySupervisor(safety_cfg)
    estimator = TruthEstimator()
    ctrl = CompositeController(lateral, policy, actuator, supervisor, estimator)
    ctrl.name = f"comp_{policy_mode}_{lateral_mode}"
    return ctrl


# ---------- 兼容占位 ----------
class BaselineController(CompositeController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class FollowStopPIDController(CompositeController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

def longitudinal_route_distance(carla_map: carla.Map, ego: carla.Vehicle, front: carla.Vehicle) -> float:
    ego_wp = carla_map.get_waypoint(ego.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    front_wp = carla_map.get_waypoint(front.get_location(), project_to_road=True, lane_type=carla.LaneType.Driving)
    if ego_wp is None or front_wp is None:
        return math.inf
    gap_s, valid, _, _ = gap_s_along_reachable_lane(carla_map, ego_wp, front.get_location(), front_wp)
    return gap_s if valid else math.inf
