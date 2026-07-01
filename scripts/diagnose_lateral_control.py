#!/usr/bin/env python3
"""
Lateral Control Quality Diagnostic Tool.

Analyses run artifacts to determine whether poor lateral control is caused by:
  (a) communication-link failures (latency, packet loss, bridge errors), or
  (b) Apollo's own lateral-control capability (excessive tracking error despite
      a healthy link).

Usage:
  python scripts/diagnose_lateral_control.py runs/<run_dir>
  python scripts/diagnose_lateral_control.py --latest
"""

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

@dataclass
class LinkThresholds:
    """Thresholds for communication-link quality."""
    max_control_latency_ms: float = 10.0       # bridge internal processing latency
    max_control_message_age_ms: float = 50.0   # Apollo -> bridge message age
    max_planning_age_ms: float = 300.0          # planning message staleness
    max_gt_state_age_wall_ms: float = 100.0    # GT odometry staleness
    max_publish_exception_rate: float = 0.01   # >1% exceptions = link fault
    max_control_rx_drop_rate: float = 0.05     # >5% rx/tx mismatch = link fault
    min_loc_to_control_ratio: float = 0.5      # loc_count should be >50% of control cycles


@dataclass
class LateralThresholds:
    """Thresholds for Apollo lateral-control quality."""
    max_acceptable_e_y_abs_m: float = 0.50     # |e_y| > 0.5m is a deviation
    max_severe_e_y_abs_m: float = 1.00          # |e_y| > 1.0m is severe
    max_acceptable_deviation_ratio: float = 0.05  # >5% of samples deviating = poor
    max_severe_deviation_ratio: float = 0.02     # >2% severe = capability fault
    min_required_planning_samples: int = 20     # need enough data to judge
    max_steady_state_e_y_m: float = 0.30        # steady-state CTE should be small
    planning_age_warning_ms: float = 300.0      # stale planning can explain deviation


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class LinkMetrics:
    """Aggregated communication-link quality metrics."""
    control_rx_count: int = 0
    control_tx_count: int = 0
    control_publish_exception_count: int = 0
    publish_errors: int = 0
    loc_count: int = 0
    chassis_count: int = 0
    control_latency_ms: Optional[float] = None
    control_message_age_ms: Optional[float] = None
    planning_message_age_ms: Optional[float] = None
    gt_state_age_wall_ms: Optional[float] = None
    gt_state_age_sim_ms: Optional[float] = None
    planning_lateral_contract_valid: Optional[bool] = None
    planning_lateral_contract_reason: str = ""
    control_anomaly_active: bool = False
    control_anomaly_count: int = 0
    # Derived
    rx_tx_ratio: float = 1.0
    exception_rate: float = 0.0
    loc_control_ratio: float = 0.0

    def derive(self) -> None:
        if self.control_rx_count > 0:
            self.rx_tx_ratio = self.control_tx_count / max(self.control_rx_count, 1)
            self.exception_rate = self.control_publish_exception_count / max(self.control_rx_count, 1)
            self.loc_control_ratio = self.loc_count / max(self.control_rx_count, 1)


@dataclass
class LateralMetrics:
    """Aggregated lateral-control quality metrics."""
    total_samples: int = 0
    samples_after_routing: int = 0
    samples_with_planning: int = 0
    e_y_mean_m: float = 0.0
    e_y_abs_mean_m: float = 0.0
    e_y_max_abs_m: float = 0.0
    e_y_min_m: float = 0.0
    e_y_max_m: float = 0.0
    e_y_start_m: float = 0.0
    e_y_end_m: float = 0.0
    e_y_deviation_count: int = 0       # |e_y| > 0.5m
    e_y_severe_count: int = 0          # |e_y| > 1.0m
    e_y_deviation_ratio: float = 0.0
    e_y_severe_ratio: float = 0.0
    e_psi_mean_deg: float = 0.0
    e_psi_max_abs_deg: float = 0.0
    e_y_first_half_abs_mean_m: float = 0.0
    e_y_second_half_abs_mean_m: float = 0.0
    control_duration_s: float = 0.0
    # Guard interference
    sustained_guard_active_ratio: float = 0.0
    sustained_guard_applied_ratio: float = 0.0
    max_steer_guard_reduction: float = 0.0
    guard_cycles_total: int = 0
    guard_cycles_modified: int = 0
    # Planning age during control
    planning_age_mean_ms: float = 0.0
    planning_age_max_ms: float = 0.0
    planning_age_stale_ratio: float = 0.0  # >300ms
    trajectory_valid_ratio: float = 0.0


@dataclass
class ApolloControlInternalMetrics:
    """Apollo Control module internal state metrics (from apollo_control_raw.jsonl)."""
    raw_control_count: int = 0
    # Speed
    apollo_speed_zero_ratio: float = 0.0         # fraction of cycles where speed==0
    apollo_speed_mean: float = 0.0
    # Engage / drive mode
    engage_state_values: str = ""                 # unique values seen
    auto_drive_mode_values: str = ""
    drive_mode_values: str = ""
    engage_ready_ratio: float = 0.0               # engage_state==2 (READY_TO_ENGAGE)
    engage_engaged_ratio: float = 0.0             # engage_state==3 (ENGAGED)
    # Steering saturation
    steer_target_saturated_ratio: float = 0.0     # steering_target <= -100 or >= 100
    steer_angle_saturated_ratio: float = 0.0      # debug lat_steer_angle saturated
    steer_angle_limited_mean: float = 0.0
    # Apollo internal errors
    apollo_lat_error_mean: float = 0.0
    apollo_lat_error_max: float = 0.0
    apollo_head_error_mean_abs: float = 0.0
    apollo_head_error_max_abs: float = 0.0
    # Planning stall
    latest_replan_stalled: bool = False           # no new replan in last 50% of cycles
    latest_replan_max_age_s: float = 0.0
    trajectory_fraction_zero_ratio: float = 0.0   # traj_fraction==0
    # Summary
    apollo_internal_healthy: bool = True


@dataclass
class DiagnosticResult:
    """Final diagnostic output."""
    run_dir: str = ""
    timestamp: str = ""
    # Classification
    root_cause: str = ""                # "link_fault" | "apollo_capability" | "both" | "inconclusive"
    root_cause_confidence: str = ""     # "high" | "medium" | "low"
    summary: str = ""
    # Detailed findings
    link_issues: List[str] = field(default_factory=list)
    lateral_issues: List[str] = field(default_factory=list)
    apollo_internal_issues: List[str] = field(default_factory=list)
    link_metrics: Optional[LinkMetrics] = None
    lateral_metrics: Optional[LateralMetrics] = None
    apollo_internal_metrics: Optional[ApolloControlInternalMetrics] = None
    # Evidence
    key_evidence: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    try:
        if val is None or val == "" or val == "nan":
            return None
        v = float(val)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError):
        return None


def load_link_metrics(artifacts_dir: Path) -> LinkMetrics:
    """Extract link-quality metrics from cyber_bridge_stats.json."""
    m = LinkMetrics()
    stats_path = artifacts_dir / "cyber_bridge_stats.json"
    if not stats_path.exists():
        return m
    with open(stats_path) as f:
        s = json.load(f)

    m.control_rx_count = int(s.get("control_rx_count", 0) or 0)
    m.control_tx_count = int(s.get("control_tx_count", 0) or 0)
    m.control_publish_exception_count = int(s.get("control_publish_exception_count", 0) or 0)
    m.publish_errors = int(s.get("publish_errors", 0) or 0)
    m.loc_count = int(s.get("loc_count", 0) or 0)
    m.chassis_count = int(s.get("chassis_count", 0) or 0)

    lo = s.get("last_control_out", {}) or {}
    m.control_latency_ms = _safe_float(lo.get("control_latency_ms"))
    m.control_message_age_ms = _safe_float(lo.get("control_message_age_ms"))
    m.planning_message_age_ms = _safe_float(lo.get("planning_message_age_ms"))
    m.gt_state_age_wall_ms = _safe_float(lo.get("gt_state_age_wall_ms"))
    m.gt_state_age_sim_ms = _safe_float(lo.get("gt_state_age_sim_ms"))
    m.planning_lateral_contract_valid = lo.get("planning_lateral_contract_valid")
    m.planning_lateral_contract_reason = str(lo.get("planning_lateral_contract_reason", "") or "")

    ca = s.get("control_anomaly", {}) or {}
    m.control_anomaly_active = bool(ca.get("active", False))
    m.control_anomaly_count = int(ca.get("count", 0) or 0)

    m.derive()
    return m


def load_lateral_metrics(artifacts_dir: Path) -> LateralMetrics:
    """Extract lateral-control quality from geometry CSV and guard JSONL."""
    m = LateralMetrics()

    # --- lateral_geometry_debug.csv ---
    geo_path = artifacts_dir / "lateral_geometry_debug.csv"
    if not geo_path.exists():
        return m

    e_y_all: List[float] = []
    e_y_after_routing: List[float] = []
    e_psi_after_routing: List[float] = []
    ts_after_routing: List[float] = []

    with open(geo_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            m.total_samples += 1
            ey = _safe_float(row.get("e_y_m"))
            if ey is None:
                continue
            e_y_all.append(ey)
            if row.get("routing_established") == "True":
                e_y_after_routing.append(ey)
                ts_after_routing.append(_safe_float(row.get("ts_sec")) or 0.0)
                if row.get("planning_has_trajectory") == "True":
                    m.samples_with_planning += 1
                ep = _safe_float(row.get("e_psi_deg"))
                if ep is not None:
                    e_psi_after_routing.append(ep)

    m.samples_after_routing = len(e_y_after_routing)

    if e_y_after_routing:
        m.e_y_mean_m = sum(e_y_after_routing) / len(e_y_after_routing)
        m.e_y_abs_mean_m = sum(abs(v) for v in e_y_after_routing) / len(e_y_after_routing)
        m.e_y_max_abs_m = max(abs(v) for v in e_y_after_routing)
        m.e_y_min_m = min(e_y_after_routing)
        m.e_y_max_m = max(e_y_after_routing)
        m.e_y_start_m = e_y_after_routing[0]
        m.e_y_end_m = e_y_after_routing[-1]
        m.e_y_deviation_count = sum(1 for v in e_y_after_routing if abs(v) > 0.5)
        m.e_y_severe_count = sum(1 for v in e_y_after_routing if abs(v) > 1.0)
        if len(e_y_after_routing) > 0:
            m.e_y_deviation_ratio = m.e_y_deviation_count / len(e_y_after_routing)
            m.e_y_severe_ratio = m.e_y_severe_count / len(e_y_after_routing)

        half = len(e_y_after_routing) // 2
        if half > 0:
            m.e_y_first_half_abs_mean_m = (
                sum(abs(v) for v in e_y_after_routing[:half]) / half
            )
            m.e_y_second_half_abs_mean_m = (
                sum(abs(v) for v in e_y_after_routing[half:]) / (len(e_y_after_routing) - half)
            )

    if ts_after_routing:
        m.control_duration_s = ts_after_routing[-1] - ts_after_routing[0]

    if e_psi_after_routing:
        m.e_psi_mean_deg = sum(e_psi_after_routing) / len(e_psi_after_routing)
        m.e_psi_max_abs_deg = max(abs(v) for v in e_psi_after_routing)

    # --- lateral_guard_debug.jsonl ---
    guard_path = artifacts_dir / "lateral_guard_debug.jsonl"
    if guard_path.exists():
        guards: List[dict] = []
        with open(guard_path) as f:
            for line in f:
                guards.append(json.loads(line))
        m.guard_cycles_total = len(guards)
        if guards:
            m.guard_cycles_modified = sum(
                1 for g in guards
                if abs(g.get("steer_before_lateral_guards", 0) - g.get("steer_after_lateral_guards", 0)) > 0.01
            )
            m.sustained_guard_active_ratio = (
                sum(1 for g in guards if g.get("sustained_lateral_guard_active"))
                / len(guards)
            )
            m.sustained_guard_applied_ratio = (
                sum(1 for g in guards if g.get("sustained_lateral_guard_applied"))
                / len(guards)
            )
            diffs = [
                abs(g.get("steer_before_lateral_guards", 0) - g.get("steer_after_lateral_guards", 0))
                for g in guards
            ]
            m.max_steer_guard_reduction = max(diffs) if diffs else 0.0

    # --- control_trajectory_consume_debug_live.jsonl ---
    consume_path = artifacts_dir / "control_trajectory_consume_debug_live.jsonl"
    if consume_path.exists():
        consumes: List[dict] = []
        with open(consume_path) as f:
            for line in f:
                consumes.append(json.loads(line))
        if consumes:
            ages = [
                c.get("control_now_relative_to_planning_header_sec", 0) or 0
                for c in consumes
            ]
            ages_ms = [a * 1000 for a in ages]
            m.planning_age_mean_ms = sum(ages_ms) / len(ages_ms)
            m.planning_age_max_ms = max(ages_ms)
            m.planning_age_stale_ratio = sum(1 for a in ages_ms if a > 300) / len(ages)
            m.trajectory_valid_ratio = (
                sum(1 for c in consumes if c.get("trajectory_time_window_valid"))
                / len(consumes)
            )

    return m


def load_apollo_internal_metrics(artifacts_dir: Path) -> ApolloControlInternalMetrics:
    """Extract Apollo Control module internal state from apollo_control_raw.jsonl."""
    m = ApolloControlInternalMetrics()
    path = artifacts_dir / "apollo_control_raw.jsonl"
    if not path.exists():
        return m

    raws: List[dict] = []
    with open(path) as f:
        for line in f:
            raws.append(json.loads(line))

    m.raw_control_count = len(raws)
    if not raws:
        return m

    # Speed
    speeds = [r.get("apollo_control_raw", {}).get("speed", 0) or 0 for r in raws]
    m.apollo_speed_mean = sum(speeds) / len(speeds)
    m.apollo_speed_zero_ratio = sum(1 for s in speeds if s == 0) / len(speeds)

    # Engage / drive mode
    engage_set: set = set()
    drive_set: set = set()
    auto_set: set = set()
    steer_sat = 0
    steer_angle_sat = 0
    lat_errors: List[float] = []
    head_errors: List[float] = []
    steer_limited_vals: List[float] = []
    traj_frac_zero = 0

    for r in raws:
        a = r.get("apollo_control_raw", {})
        engage_set.add(str(a.get("engage_advice", "?")))
        drive_set.add(str(a.get("driving_mode", "?")))
        auto_set.add(str(a.get("driving_mode", "?")))  # same field in raw
        st = a.get("steering_target", 0) or 0
        if abs(st) >= 99.9:
            steer_sat += 1
        sa = a.get("debug_simple_lat_steer_angle", 0) or 0
        if abs(sa) >= 99.9:
            steer_angle_sat += 1
        le = a.get("debug_simple_lat_lateral_error", 0) or 0
        lat_errors.append(le)
        he = a.get("debug_simple_lat_heading_error", 0) or 0
        head_errors.append(he)
        sl = a.get("debug_simple_lat_steer_angle_limited", 0) or 0
        steer_limited_vals.append(sl)
        tf = a.get("trajectory_fraction", 0) or 0
        if tf == 0:
            traj_frac_zero += 1

    m.engage_state_values = ",".join(sorted(engage_set))
    m.drive_mode_values = ",".join(sorted(drive_set))
    m.auto_drive_mode_values = ",".join(sorted(auto_set))
    m.engage_ready_ratio = sum(1 for r in raws
        if (r.get("apollo_control_raw", {}).get("engage_advice") or 0) == 2) / len(raws)
    m.engage_engaged_ratio = sum(1 for r in raws
        if (r.get("apollo_control_raw", {}).get("engage_advice") or 0) == 3) / len(raws)

    m.steer_target_saturated_ratio = steer_sat / len(raws)
    m.steer_angle_saturated_ratio = steer_angle_sat / len(raws)
    m.steer_angle_limited_mean = sum(steer_limited_vals) / len(steer_limited_vals)

    m.apollo_lat_error_mean = sum(lat_errors) / len(lat_errors)
    m.apollo_lat_error_max = max(abs(e) for e in lat_errors)
    m.apollo_head_error_mean_abs = sum(abs(e) for e in head_errors) / len(head_errors)
    m.apollo_head_error_max_abs = max(abs(e) for e in head_errors)

    # Latest replan stall detection: replan stalled if the last 40% of cycles
    # all use the same 1-2 replan trajectory sequence numbers (allowing one
    # transition), AND the replan age at the end is > 3.0s (indicating no
    # fresh replan arrived for an extended period).
    replan_seqs = [r.get("apollo_control_raw", {}).get(
        "debug_input_latest_replan_trajectory_header_sequence_num", 0) or 0
        for r in raws]
    replan_ages = [
        (r.get("apollo_control_raw", {}).get("control_header_timestamp_sec", 0) or 0)
        - (r.get("apollo_control_raw", {}).get(
            "debug_input_latest_replan_trajectory_header_timestamp_sec", 0) or 0)
        for r in raws
    ]
    m.latest_replan_max_age_s = max(replan_ages) if replan_ages else 0.0
    if len(replan_seqs) >= 10:
        tail_30pct = replan_seqs[-max(1, len(replan_seqs) * 3 // 10):]
        tail_unique = set(tail_30pct)
        tail_ages = replan_ages[-max(1, len(replan_seqs) * 3 // 10):]
        mean_tail_age = sum(tail_ages) / len(tail_ages) if tail_ages else 0.0
        m.latest_replan_stalled = (
            len(tail_unique) <= 2  # at most 1 transition in tail 30%
            and mean_tail_age > 3.0  # replan age > 3s confirms no fresh data
        )
    replan_ages = [
        (r.get("apollo_control_raw", {}).get("control_header_timestamp_sec", 0) or 0)
        - (r.get("apollo_control_raw", {}).get(
            "debug_input_latest_replan_trajectory_header_timestamp_sec", 0) or 0)
        for r in raws
    ]
    m.latest_replan_max_age_s = max(replan_ages) if replan_ages else 0.0

    m.trajectory_fraction_zero_ratio = traj_frac_zero / len(raws)

    # Health assessment
    m.apollo_internal_healthy = (
        m.apollo_speed_zero_ratio < 0.5
        and m.engage_engaged_ratio > 0.5
        and m.steer_target_saturated_ratio < 0.5
        and not m.latest_replan_stalled
    )

    return m


def load_command_materialization(artifacts_dir: Path) -> Dict[str, Any]:
    """Load command_materialization_summary.json for stage info."""
    path = artifacts_dir / "command_materialization_summary.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def load_summary(run_dir: Path) -> Dict[str, Any]:
    """Load summary.json."""
    path = run_dir / "summary.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Diagnosis engine
# ---------------------------------------------------------------------------

def diagnose_link(link: LinkMetrics, lat: LateralMetrics, thresh: LinkThresholds) -> List[str]:
    """
    Check link metrics against thresholds. Returns list of issues found.

    Uses control_trajectory_consume data for planning-age assessment during
    active driving (the last_control_out snapshot from cyber_bridge_stats.json
    reflects the final state after the vehicle stopped and is not representative).
    """
    issues: List[str] = []

    if link.control_rx_count == 0:
        issues.append("CRITICAL: zero control_rx_count — Apollo Control may not have started or bridge did not receive /apollo/control")
        return issues

    # Control publish exceptions
    if link.control_publish_exception_count > 0:
        issues.append(
            f"control_publish_exception_count={link.control_publish_exception_count} "
            f"(rate={link.exception_rate:.4f}) — bridge failed to publish control to CARLA"
        )
    if link.publish_errors > 0:
        issues.append(f"publish_errors={link.publish_errors} — general publish failures")

    # RX/TX mismatch
    if link.rx_tx_ratio < (1.0 - thresh.max_control_rx_drop_rate):
        issues.append(
            f"control_tx/rx ratio={link.rx_tx_ratio:.3f} — "
            f"{link.control_rx_count - link.control_tx_count} control commands were "
            f"received but not transmitted (drop rate={1-link.rx_tx_ratio:.1%})"
        )

    # Latency (bridge internal)
    if link.control_latency_ms is not None and link.control_latency_ms > thresh.max_control_latency_ms:
        issues.append(
            f"control_latency={link.control_latency_ms:.1f}ms > threshold={thresh.max_control_latency_ms}ms"
        )
    if link.control_message_age_ms is not None and link.control_message_age_ms > thresh.max_control_message_age_ms:
        issues.append(
            f"control_message_age={link.control_message_age_ms:.1f}ms > threshold={thresh.max_control_message_age_ms}ms — Apollo Control message is stale on arrival"
        )

    # Planning staleness — use control_trajectory_consume data (during active driving)
    # rather than last_control_out snapshot (end-of-scenario, vehicle stopped)
    if lat.guard_cycles_total > 0 and lat.planning_age_stale_ratio > 0.1:
        issues.append(
            f"planning staleness during active control: {lat.planning_age_stale_ratio:.0%} of cycles "
            f"used planning >{thresh.max_planning_age_ms}ms old (max={lat.planning_age_max_ms:.0f}ms)"
        )

    # GT staleness — same reasoning: check during active driving
    # (We don't have per-cycle GT age, but loc/control ratio is a proxy)
    if link.loc_control_ratio < thresh.min_loc_to_control_ratio:
        issues.append(
            f"loc/control ratio={link.loc_control_ratio:.2f} — localization count is low relative to control cycles, "
            f"GT odometry may not be arriving at expected rate"
        )

    # Contract validity — note: last_control_out contract validity is from the
    # final snapshot and may be stale_planning_message because scenario ended.
    # Only flag if also accompanied by planning-age issues during active driving.
    if link.planning_lateral_contract_valid is False:
        if lat.planning_age_stale_ratio > 0.1:
            issues.append(
                f"planning_lateral_contract_valid=False (reason: {link.planning_lateral_contract_reason}) — "
                f"confirmed by {lat.planning_age_stale_ratio:.0%} stale planning cycles during active control"
            )
        else:
            # Contract invalid at end-of-scenario only — not a real issue
            pass

    # Anomaly
    if link.control_anomaly_active:
        issues.append(f"control_anomaly active with count={link.control_anomaly_count}")

    return issues


def diagnose_lateral(lat: LateralMetrics, link: LinkMetrics, thresh: LateralThresholds) -> List[str]:
    """Check lateral-control quality against thresholds. Returns list of issues."""
    issues: List[str] = []

    if lat.samples_after_routing < thresh.min_required_planning_samples:
        issues.append(
            f"insufficient data: only {lat.samples_after_routing} samples after routing "
            f"(need >= {thresh.min_required_planning_samples})"
        )
        return issues

    # CTE magnitude
    if lat.e_y_max_abs_m > thresh.max_severe_e_y_abs_m:
        issues.append(
            f"severe CTE: max |e_y| = {lat.e_y_max_abs_m:.3f}m > threshold={thresh.max_severe_e_y_abs_m}m"
        )
    elif lat.e_y_max_abs_m > thresh.max_acceptable_e_y_abs_m:
        issues.append(
            f"excessive CTE: max |e_y| = {lat.e_y_max_abs_m:.3f}m > threshold={thresh.max_acceptable_e_y_abs_m}m"
        )

    # Deviation ratios
    if lat.e_y_deviation_ratio > thresh.max_acceptable_deviation_ratio:
        issues.append(
            f"CTE deviation ratio: {lat.e_y_deviation_ratio:.1%} of samples have |e_y|>0.5m "
            f"(threshold={thresh.max_acceptable_deviation_ratio:.1%})"
        )
    if lat.e_y_severe_ratio > thresh.max_severe_deviation_ratio:
        issues.append(
            f"CTE severe ratio: {lat.e_y_severe_ratio:.1%} of samples have |e_y|>1.0m "
            f"(threshold={thresh.max_severe_deviation_ratio:.1%})"
        )

    # Trend (worsening?)
    if lat.e_y_second_half_abs_mean_m > lat.e_y_first_half_abs_mean_m * 1.3:
        issues.append(
            f"CTE worsening: first-half |e_y| mean={lat.e_y_first_half_abs_mean_m:.3f}m, "
            f"second-half mean={lat.e_y_second_half_abs_mean_m:.3f}m — error is growing"
        )

    # Steady-state
    if lat.e_y_abs_mean_m > thresh.max_steady_state_e_y_m:
        issues.append(
            f"steady-state CTE: mean |e_y| = {lat.e_y_abs_mean_m:.3f}m > threshold={thresh.max_steady_state_e_y_m}m — "
            f"Apollo unable to converge to lane center"
        )

    # Guard interference
    if lat.sustained_guard_applied_ratio > 0.5:
        issues.append(
            f"guard interference: sustained_lateral_guard applied in {lat.sustained_guard_applied_ratio:.0%} "
            f"of control cycles, reducing steer by up to {lat.max_steer_guard_reduction:.2f} — "
            f"Apollo steer commands are being suppressed by bridge safety guards"
        )

    # Planning age during control
    if lat.planning_age_stale_ratio > 0.1:
        issues.append(
            f"planning staleness during control: {lat.planning_age_stale_ratio:.0%} of control cycles "
            f"used planning >300ms old (max={lat.planning_age_max_ms:.0f}ms)"
        )

    # Trajectory validity
    if lat.trajectory_valid_ratio < 0.9:
        issues.append(
            f"trajectory validity: only {lat.trajectory_valid_ratio:.0%} of control cycles "
            f"had valid trajectory time windows"
        )

    return issues


def diagnose_apollo_internal(apm: ApolloControlInternalMetrics) -> List[str]:
    """Check Apollo Control module internal state for anomalies."""
    issues: List[str] = []

    if apm.raw_control_count == 0:
        return issues

    # Speed = 0
    if apm.apollo_speed_zero_ratio > 0.8:
        issues.append(
            f"Apollo ControlCommand.speed is 0 in {apm.apollo_speed_zero_ratio:.0%} of cycles "
            f"— Apollo Control module thinks vehicle is stationary (actual speed was non-zero). "
            f"This causes incorrect lateral control gain scheduling and saturated steer output."
        )
    elif apm.apollo_speed_zero_ratio > 0.3:
        issues.append(
            f"Apollo ControlCommand.speed is 0 in {apm.apollo_speed_zero_ratio:.0%} of cycles "
            f"— intermittent speed reporting issue"
        )

    # Engage state
    if apm.engage_engaged_ratio < 0.5:
        if apm.engage_ready_ratio > 0.8:
            issues.append(
                f"engage_state stuck at READY_TO_ENGAGE (2) in {apm.engage_ready_ratio:.0%} of cycles "
                f"— Apollo Control not fully engaged. "
                f"drive_mode={apm.drive_mode_values}, auto_drive={apm.auto_drive_mode_values}"
            )
        else:
            issues.append(
                f"engage_state not ENGAGED in majority of cycles "
                f"(ready={apm.engage_ready_ratio:.0%}, engaged={apm.engage_engaged_ratio:.0%})"
            )

    # Steering saturation
    if apm.steer_target_saturated_ratio > 0.5:
        issues.append(
            f"steering_target saturated (|steer|>=100) in {apm.steer_target_saturated_ratio:.0%} of cycles "
            f"— Apollo lateral controller is hitting its output limit"
        )
    if apm.steer_angle_saturated_ratio > 0.5:
        issues.append(
            f"debug_simple_lat_steer_angle saturated in {apm.steer_angle_saturated_ratio:.0%} of cycles"
        )

    # Apollo internal errors
    if apm.apollo_lat_error_max > 1.0:
        issues.append(
            f"Apollo internal lateral_error max={apm.apollo_lat_error_max:.2f}m "
            f"(mean={apm.apollo_lat_error_mean:.2f}m) — Apollo sees large tracking error"
        )
    if apm.apollo_head_error_max_abs > 1.0:
        issues.append(
            f"Apollo internal heading_error max={apm.apollo_head_error_max_abs:.2f}rad "
            f"({apm.apollo_head_error_max_abs*180/3.14159:.0f}°) — Apollo thinks vehicle orientation is severely wrong"
        )

    # Planning stall
    if apm.latest_replan_stalled:
        issues.append(
            f"Planning replan stalled: no new replan trajectory in second half of control cycles "
            f"(max replan age={apm.latest_replan_max_age_s:.1f}s). "
            f"Control is using an expired trajectory."
        )

    if apm.trajectory_fraction_zero_ratio > 0.5:
        issues.append(
            f"trajectory_fraction=0 in {apm.trajectory_fraction_zero_ratio:.0%} of cycles "
            f"— Apollo Control has consumed its entire trajectory"
        )

    return issues


def classify_root_cause(
    link_issues: List[str],
    lateral_issues: List[str],
    apollo_internal_issues: List[str],
    link: LinkMetrics,
    lat: LateralMetrics,
    apm: ApolloControlInternalMetrics,
) -> Tuple[str, str, str]:
    """
    Classify root cause as link_fault, apollo_capability, both, or inconclusive.

    Decision logic:
      - If link has critical issues (zero rx, high exceptions, severe latency) → link_fault
      - If link is clean but lateral/internal metrics are poor → apollo_capability
      - If both have issues → both
      - If neither has enough data → inconclusive
    """
    link_critical = any(
        "CRITICAL" in issue or "zero control_rx" in issue
        for issue in link_issues
    )
    link_moderate = len(link_issues) > 0 and not link_critical
    lateral_significant = len(lateral_issues) > 0
    apollo_internal_significant = len(apollo_internal_issues) > 0

    # Count severity of lateral issues
    lateral_severe = any("severe" in issue.lower() for issue in lateral_issues)

    # Apollo internal issues that clearly point to apollo_capability
    apollo_internal_definitive = apollo_internal_significant and (
        not apm.apollo_internal_healthy
        or apm.apollo_speed_zero_ratio > 0.8
        or apm.latest_replan_stalled
    )

    if link_critical:
        if lateral_significant:
            return (
                "both",
                "high",
                "Link has critical failures AND lateral control is poor. "
                "Fix link issues first, then re-evaluate lateral quality.",
            )
        return (
            "link_fault",
            "high",
            "Critical communication-link failure detected. "
            "Apollo control quality cannot be evaluated until link is healthy.",
        )

    if link_moderate and (lateral_severe or apollo_internal_definitive):
        return (
            "both",
            "medium",
            "Link has moderate issues that may contribute to lateral errors. "
            "Fix link issues and re-test; if lateral issues persist, Apollo capability is suspect.",
        )

    if link_moderate and not lateral_significant and not apollo_internal_significant:
        return (
            "link_fault",
            "medium",
            "Link issues detected but lateral control is within bounds. "
            "Link degradation may be intermittent or not yet affecting control quality.",
        )

    if not link_moderate and (lateral_significant or apollo_internal_definitive):
        if apollo_internal_definitive:
            return (
                "apollo_capability",
                "high",
                "Communication link is healthy but Apollo internal state shows clear anomalies "
                "(speed=0, engage not ready, planning stalled, etc.). "
                "Root cause is Apollo's internal control/planning logic.",
            )
        confidence = "high" if lateral_severe else "medium"
        return (
            "apollo_capability",
            confidence,
            "Communication link is healthy but lateral tracking quality is poor. "
            "Root cause is likely Apollo's lateral control algorithm or parameter tuning.",
        )

    if not link_moderate and not lateral_significant and not apollo_internal_significant:
        return (
            "inconclusive",
            "low",
            "Neither link nor lateral metrics show clear issues. "
            "The failure may be in a different domain (longitudinal, scenario setup, etc.) "
            "or the run was too short to evaluate.",
        )

    return (
        "inconclusive",
        "low",
        "Insufficient or conflicting evidence to determine root cause.",
    )


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(result: DiagnosticResult) -> str:
    """Generate a structured diagnostic report as a string."""
    lines: List[str] = []
    sep = "=" * 72

    lines.append(sep)
    lines.append("  LATERAL CONTROL QUALITY DIAGNOSTIC REPORT")
    lines.append(sep)
    lines.append(f"  Run directory : {result.run_dir}")
    lines.append(f"  Generated at  : {result.timestamp}")
    lines.append(f"  Root cause    : {result.root_cause}")
    lines.append(f"  Confidence    : {result.root_cause_confidence}")
    lines.append(sep)
    lines.append("")
    lines.append(f"SUMMARY: {result.summary}")
    lines.append("")

    # --- Link Metrics ---
    if result.link_metrics:
        lm = result.link_metrics
        lines.append("-" * 72)
        lines.append("  COMMUNICATION LINK METRICS")
        lines.append("-" * 72)
        lines.append(f"  Control RX count              : {lm.control_rx_count}")
        lines.append(f"  Control TX count              : {lm.control_tx_count}")
        lines.append(f"  RX/TX ratio                   : {lm.rx_tx_ratio:.4f}")
        lines.append(f"  Publish exceptions            : {lm.control_publish_exception_count}")
        lines.append(f"  Publish errors                : {lm.publish_errors}")
        lines.append(f"  Localization count            : {lm.loc_count}")
        lines.append(f"  Chassis count                 : {lm.chassis_count}")
        lines.append(f"  Loc/control ratio             : {lm.loc_control_ratio:.4f}")
        lines.append(f"  Control latency               : {_fmt_ms(lm.control_latency_ms)}")
        lines.append(f"  Control message age           : {_fmt_ms(lm.control_message_age_ms)}")
        lines.append(f"  Planning message age (final)  : {_fmt_ms(lm.planning_message_age_ms)}  (*)")
        lines.append(f"  GT state age (wall, final)    : {_fmt_ms(lm.gt_state_age_wall_ms)}  (*)")
        lines.append(f"  GT state age (sim, final)     : {_fmt_ms(lm.gt_state_age_sim_ms)}  (*)")
        lines.append(f"  Planning lateral contract     : valid={lm.planning_lateral_contract_valid} "
                     f"reason={lm.planning_lateral_contract_reason}  (*)")
        lines.append(f"  Control anomaly               : active={lm.control_anomaly_active} "
                     f"count={lm.control_anomaly_count}")
        lines.append(f"  (*) Final snapshot — may reflect end-of-scenario state (vehicle stopped).")
        lines.append(f"      See Lateral Metrics for planning age during active driving.")
        lines.append("")

    # --- Lateral Metrics ---
    if result.lateral_metrics:
        lm = result.lateral_metrics
        lines.append("-" * 72)
        lines.append("  LATERAL CONTROL METRICS")
        lines.append("-" * 72)
        lines.append(f"  Total geometry samples        : {lm.total_samples}")
        lines.append(f"  Samples after routing         : {lm.samples_after_routing}")
        lines.append(f"  Samples with planning         : {lm.samples_with_planning}")
        lines.append(f"  Control duration              : {lm.control_duration_s:.1f}s")
        lines.append(f"  e_y (CTE) mean                : {lm.e_y_mean_m:.4f}m")
        lines.append(f"  e_y |mean|                    : {lm.e_y_abs_mean_m:.4f}m")
        lines.append(f"  e_y max |abs|                 : {lm.e_y_max_abs_m:.4f}m")
        lines.append(f"  e_y range                     : [{lm.e_y_min_m:.4f}, {lm.e_y_max_m:.4f}]")
        lines.append(f"  e_y start -> end              : {lm.e_y_start_m:.4f} -> {lm.e_y_end_m:.4f}")
        lines.append(f"  e_y |deviation|>0.5m count    : {lm.e_y_deviation_count} ({lm.e_y_deviation_ratio:.1%})")
        lines.append(f"  e_y |severe|>1.0m count       : {lm.e_y_severe_count} ({lm.e_y_severe_ratio:.1%})")
        lines.append(f"  e_y 1st-half |mean|           : {lm.e_y_first_half_abs_mean_m:.4f}m")
        lines.append(f"  e_y 2nd-half |mean|           : {lm.e_y_second_half_abs_mean_m:.4f}m")
        lines.append(f"  e_psi mean                    : {lm.e_psi_mean_deg:.2f} deg")
        lines.append(f"  e_psi max |abs|               : {lm.e_psi_max_abs_deg:.2f} deg")
        lines.append(f"  Guard cycles total            : {lm.guard_cycles_total}")
        lines.append(f"  Guard cycles modified         : {lm.guard_cycles_modified}")
        lines.append(f"  Sustained guard active ratio  : {lm.sustained_guard_active_ratio:.1%}")
        lines.append(f"  Sustained guard applied ratio : {lm.sustained_guard_applied_ratio:.1%}")
        lines.append(f"  Max steer guard reduction     : {lm.max_steer_guard_reduction:.4f}")
        lines.append(f"  Planning age mean             : {lm.planning_age_mean_ms:.0f}ms")
        lines.append(f"  Planning age max              : {lm.planning_age_max_ms:.0f}ms")
        lines.append(f"  Planning stale ratio (>300ms) : {lm.planning_age_stale_ratio:.1%}")
        lines.append(f"  Trajectory valid ratio        : {lm.trajectory_valid_ratio:.1%}")
        lines.append("")

    # --- Apollo Internal Metrics ---
    if result.apollo_internal_metrics and result.apollo_internal_metrics.raw_control_count > 0:
        am = result.apollo_internal_metrics
        lines.append("-" * 72)
        lines.append("  APOLLO CONTROL INTERNAL STATE")
        lines.append("-" * 72)
        lines.append(f"  Raw control messages          : {am.raw_control_count}")
        lines.append(f"  Apollo speed mean             : {am.apollo_speed_mean:.2f} m/s")
        lines.append(f"  Apollo speed=0 ratio          : {am.apollo_speed_zero_ratio:.1%}")
        lines.append(f"  Engage state values           : {am.engage_state_values}")
        lines.append(f"  Engage READY (2) ratio        : {am.engage_ready_ratio:.1%}")
        lines.append(f"  Engage ENGAGED (3) ratio      : {am.engage_engaged_ratio:.1%}")
        lines.append(f"  Drive mode values             : {am.drive_mode_values}")
        lines.append(f"  steer_target saturated ratio  : {am.steer_target_saturated_ratio:.1%}")
        lines.append(f"  steer_angle saturated ratio   : {am.steer_angle_saturated_ratio:.1%}")
        lines.append(f"  steer_angle_limited mean      : {am.steer_angle_limited_mean:.1f}")
        lines.append(f"  Apollo lat_error mean         : {am.apollo_lat_error_mean:.3f}m")
        lines.append(f"  Apollo lat_error max          : {am.apollo_lat_error_max:.3f}m")
        lines.append(f"  Apollo head_error mean abs    : {am.apollo_head_error_mean_abs:.3f}rad ({am.apollo_head_error_mean_abs*57.3:.1f}°)")
        lines.append(f"  Apollo head_error max abs     : {am.apollo_head_error_max_abs:.3f}rad ({am.apollo_head_error_max_abs*57.3:.1f}°)")
        lines.append(f"  Latest replan stalled         : {am.latest_replan_stalled}")
        lines.append(f"  Latest replan max age         : {am.latest_replan_max_age_s:.1f}s")
        lines.append(f"  traj_fraction=0 ratio         : {am.trajectory_fraction_zero_ratio:.1%}")
        lines.append(f"  Apollo internal healthy       : {am.apollo_internal_healthy}")
        lines.append("")

    # --- Issues ---
    if result.link_issues:
        lines.append("-" * 72)
        lines.append("  LINK ISSUES FOUND")
        lines.append("-" * 72)
        for i, issue in enumerate(result.link_issues, 1):
            lines.append(f"  [{i}] {issue}")
        lines.append("")

    if result.lateral_issues:
        lines.append("-" * 72)
        lines.append("  LATERAL ISSUES FOUND")
        lines.append("-" * 72)
        for i, issue in enumerate(result.lateral_issues, 1):
            lines.append(f"  [{i}] {issue}")
        lines.append("")

    if result.apollo_internal_issues:
        lines.append("-" * 72)
        lines.append("  APOLLO INTERNAL ISSUES FOUND")
        lines.append("-" * 72)
        for i, issue in enumerate(result.apollo_internal_issues, 1):
            lines.append(f"  [{i}] {issue}")
        lines.append("")

    # --- Evidence ---
    if result.key_evidence:
        lines.append("-" * 72)
        lines.append("  KEY EVIDENCE")
        lines.append("-" * 72)
        for i, ev in enumerate(result.key_evidence, 1):
            lines.append(f"  [{i}] {ev}")
        lines.append("")

    # --- Recommendations ---
    if result.recommendations:
        lines.append("-" * 72)
        lines.append("  RECOMMENDATIONS")
        lines.append("-" * 72)
        for i, rec in enumerate(result.recommendations, 1):
            lines.append(f"  [{i}] {rec}")
        lines.append("")

    lines.append(sep)
    lines.append("  END OF REPORT")
    lines.append(sep)

    return "\n".join(lines)


def _fmt_ms(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    return f"{val:.2f}ms"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def diagnose_run(run_dir: Path) -> DiagnosticResult:
    """Run full diagnosis on a single run directory."""
    artifacts = run_dir / "artifacts"
    result = DiagnosticResult(
        run_dir=str(run_dir),
        timestamp=datetime.now().isoformat(),
    )

    # Load data
    link = load_link_metrics(artifacts)
    lat = load_lateral_metrics(artifacts)
    apm = load_apollo_internal_metrics(artifacts)
    cmd_mat = load_command_materialization(artifacts)
    summary = load_summary(run_dir)

    result.link_metrics = link
    result.lateral_metrics = lat
    result.apollo_internal_metrics = apm

    # Diagnose
    link_thresh = LinkThresholds()
    lat_thresh = LateralThresholds()

    result.link_issues = diagnose_link(link, lat, link_thresh)
    result.lateral_issues = diagnose_lateral(lat, link, lat_thresh)
    result.apollo_internal_issues = diagnose_apollo_internal(apm)

    # Classify
    root_cause, confidence, explanation = classify_root_cause(
        result.link_issues, result.lateral_issues, result.apollo_internal_issues, link, lat, apm,
    )
    result.root_cause = root_cause
    result.root_cause_confidence = confidence
    result.summary = explanation

    # Build evidence
    evidence: List[str] = []

    if link.control_rx_count > 0:
        evidence.append(
            f"Apollo Control published {link.control_rx_count} commands, "
            f"bridge transmitted {link.control_tx_count} to CARLA "
            f"(rx/tx ratio={link.rx_tx_ratio:.3f})"
        )
    else:
        evidence.append("No Apollo Control commands received — Control module may not have started")

    if link.control_publish_exception_count > 0:
        evidence.append(
            f"{link.control_publish_exception_count} control publish exceptions "
            f"(rate={link.exception_rate:.4f})"
        )

    if lat.samples_after_routing > 0:
        evidence.append(
            f"During Apollo active control ({lat.control_duration_s:.1f}s): "
            f"CTE range [{lat.e_y_min_m:.3f}, {lat.e_y_max_m:.3f}]m, "
            f"|e_y| mean={lat.e_y_abs_mean_m:.3f}m, "
            f"max |e_y|={lat.e_y_max_abs_m:.3f}m"
        )

        trend = "improving" if lat.e_y_second_half_abs_mean_m < lat.e_y_first_half_abs_mean_m * 0.8 else (
            "worsening" if lat.e_y_second_half_abs_mean_m > lat.e_y_first_half_abs_mean_m * 1.3 else "stable"
        )
        evidence.append(
            f"CTE trend: {trend} (1st-half |mean|={lat.e_y_first_half_abs_mean_m:.3f}m, "
            f"2nd-half |mean|={lat.e_y_second_half_abs_mean_m:.3f}m)"
        )

    if lat.sustained_guard_applied_ratio > 0.3:
        evidence.append(
            f"Bridge sustained_lateral_guard reduced steer in {lat.sustained_guard_applied_ratio:.0%} "
            f"of control cycles (max reduction={lat.max_steer_guard_reduction:.2f}) — "
            f"Apollo was commanding saturated steer but guard limited it"
        )

    if link.planning_lateral_contract_valid is False:
        evidence.append(
            f"Planning lateral contract INVALID: {link.planning_lateral_contract_reason}"
        )

    stage = cmd_mat.get("command_path_stage", "unknown")
    layer = cmd_mat.get("first_divergence_layer", "unknown")
    reason = cmd_mat.get("first_divergence_reason", "")
    evidence.append(f"Command path stage: {stage}, divergence layer: {layer}, reason: {reason}")

    fail_reason = summary.get("fail_reason", "")
    if fail_reason:
        evidence.append(f"Run failure reason: {fail_reason}")

    # Apollo internal evidence
    if apm.raw_control_count > 0:
        if apm.apollo_speed_zero_ratio > 0.8:
            evidence.append(
                f"Apollo ControlCommand.speed=0 in {apm.apollo_speed_zero_ratio:.0%} of cycles "
                f"— Apollo thinks vehicle is stationary despite actual movement"
            )
        if apm.engage_engaged_ratio < 0.5:
            evidence.append(
                f"Apollo engage_state not ENGAGED: ready={apm.engage_ready_ratio:.0%}, "
                f"engaged={apm.engage_engaged_ratio:.0%}, drive_mode={apm.drive_mode_values}"
            )
        if apm.steer_target_saturated_ratio > 0.5:
            evidence.append(
                f"Apollo steering_target saturated in {apm.steer_target_saturated_ratio:.0%} of cycles"
            )
        if apm.latest_replan_stalled:
            evidence.append(
                f"Apollo Planning stopped replanning: max replan age={apm.latest_replan_max_age_s:.1f}s"
            )

    result.key_evidence = evidence

    # Build recommendations
    recs: List[str] = []

    if result.root_cause in ("link_fault", "both"):
        if link.control_latency_ms and link.control_latency_ms > link_thresh.max_control_latency_ms:
            recs.append("Investigate bridge internal processing latency; check CPU load and I/O bottlenecks")
        if link.planning_message_age_ms and link.planning_message_age_ms > link_thresh.max_planning_age_ms:
            recs.append("Planning data is stale — check Apollo Planning module output rate and cyber channel health")
        if link.control_publish_exception_count > 0:
            recs.append("Investigate control publish exceptions — check bridge error logs for root cause")
        if link.planning_lateral_contract_valid is False:
            recs.append("Fix planning lateral contract validity — ensure Planning module produces timely trajectories")

    if result.root_cause in ("apollo_capability", "both"):
        if lat.e_y_max_abs_m > 1.0:
            recs.append("Apollo lateral controller cannot maintain lane — review PID/MPC gains and reference line quality")
        if lat.sustained_guard_applied_ratio > 0.5:
            recs.append(
                "Apollo outputs saturated steer (100%) but guard limits it. "
                "Consider: (a) tuning Apollo's lateral controller to avoid saturation, "
                "or (b) increasing sustained_lateral_guard_max_abs_steer to allow stronger corrections"
            )
        if lat.e_y_second_half_abs_mean_m > lat.e_y_first_half_abs_mean_m * 1.2:
            recs.append("CTE is worsening over time — Apollo may have incorrect reference line or map mismatch")
        if lat.e_y_abs_mean_m > 0.3:
            recs.append(
                "Mean CTE > 0.3m indicates systematic offset — check Apollo HD map alignment with CARLA Town01, "
                "verify coordinate transform calibration"
            )
        recs.append(
            "Compare lateral_geometry_debug.csv against Apollo planning trajectory to determine "
            "if the error is in trajectory generation or trajectory tracking"
        )
        # Apollo internal specific recommendations
        if apm.apollo_speed_zero_ratio > 0.8:
            recs.append(
                "Apollo ControlCommand.speed=0 despite vehicle moving. "
                "Check /apollo/canbus/chassis subscription in Apollo Control module. "
                "Control module may not be reading chassis speed correctly."
            )
        if apm.engage_engaged_ratio < 0.5:
            recs.append(
                "Apollo Control not entering ENGAGED state. "
                "Check engage command flow and Control module initialization sequence."
            )
        if apm.latest_replan_stalled:
            recs.append(
                "Apollo Planning stopped replanning. Check Apollo Planning logs for "
                "reference line errors (e.g., 'not on previous reference line'). "
                "Vehicle may have driven outside the reference line coverage area."
            )

    if not recs:
        recs.append("No specific recommendations — re-run with longer duration to gather more data")

    result.recommendations = recs

    return result


def find_latest_run(base_dir: Path = Path("runs")) -> Optional[Path]:
    """Find the most recent verify_* or phase1_* run directory."""
    if not base_dir.exists():
        return None
    run_dirs = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and (d / "summary.json").exists()],
        key=lambda d: (d / "summary.json").stat().st_mtime,
        reverse=True,
    )
    return run_dirs[0] if run_dirs else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lateral Control Quality Diagnostic Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s runs/verify_v14_periodic_flush_20260626_151748
  %(prog)s --latest
  %(prog)s --latest --json  # machine-readable output
        """,
    )
    parser.add_argument(
        "run_dir", nargs="?", default=None,
        help="Path to a run directory (containing summary.json and artifacts/)",
    )
    parser.add_argument(
        "--latest", action="store_true",
        help="Diagnose the most recent run in runs/",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output machine-readable JSON instead of human-readable report",
    )
    parser.add_argument(
        "--runs-base", default="runs",
        help="Base directory for --latest (default: runs/)",
    )
    args = parser.parse_args()

    run_dir: Optional[Path] = None

    if args.latest:
        run_dir = find_latest_run(Path(args.runs_base))
        if run_dir is None:
            print("ERROR: no runs found with summary.json", file=sys.stderr)
            sys.exit(1)
        print(f"[INFO] Using latest run: {run_dir}", file=sys.stderr)
    elif args.run_dir:
        run_dir = Path(args.run_dir)
        if not run_dir.exists():
            print(f"ERROR: run directory not found: {run_dir}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)

    result = diagnose_run(run_dir)

    if args.json:
        # Machine-readable output
        output = {
            "run_dir": result.run_dir,
            "timestamp": result.timestamp,
            "root_cause": result.root_cause,
            "root_cause_confidence": result.root_cause_confidence,
            "summary": result.summary,
            "link_issues": result.link_issues,
            "lateral_issues": result.lateral_issues,
            "apollo_internal_issues": result.apollo_internal_issues,
            "key_evidence": result.key_evidence,
            "recommendations": result.recommendations,
        }
        if result.link_metrics:
            output["link_metrics"] = {
                "control_rx_count": result.link_metrics.control_rx_count,
                "control_tx_count": result.link_metrics.control_tx_count,
                "rx_tx_ratio": result.link_metrics.rx_tx_ratio,
                "control_publish_exception_count": result.link_metrics.control_publish_exception_count,
                "publish_errors": result.link_metrics.publish_errors,
                "control_latency_ms": result.link_metrics.control_latency_ms,
                "control_message_age_ms": result.link_metrics.control_message_age_ms,
                "planning_message_age_ms": result.link_metrics.planning_message_age_ms,
                "gt_state_age_wall_ms": result.link_metrics.gt_state_age_wall_ms,
                "planning_lateral_contract_valid": result.link_metrics.planning_lateral_contract_valid,
                "planning_lateral_contract_reason": result.link_metrics.planning_lateral_contract_reason,
                "control_anomaly_active": result.link_metrics.control_anomaly_active,
                "control_anomaly_count": result.link_metrics.control_anomaly_count,
            }
        if result.lateral_metrics:
            output["lateral_metrics"] = {
                "samples_after_routing": result.lateral_metrics.samples_after_routing,
                "control_duration_s": result.lateral_metrics.control_duration_s,
                "e_y_mean_m": result.lateral_metrics.e_y_mean_m,
                "e_y_abs_mean_m": result.lateral_metrics.e_y_abs_mean_m,
                "e_y_max_abs_m": result.lateral_metrics.e_y_max_abs_m,
                "e_y_deviation_ratio": result.lateral_metrics.e_y_deviation_ratio,
                "e_y_severe_ratio": result.lateral_metrics.e_y_severe_ratio,
                "e_y_first_half_abs_mean_m": result.lateral_metrics.e_y_first_half_abs_mean_m,
                "e_y_second_half_abs_mean_m": result.lateral_metrics.e_y_second_half_abs_mean_m,
                "sustained_guard_applied_ratio": result.lateral_metrics.sustained_guard_applied_ratio,
                "max_steer_guard_reduction": result.lateral_metrics.max_steer_guard_reduction,
                "planning_age_mean_ms": result.lateral_metrics.planning_age_mean_ms,
                "planning_age_stale_ratio": result.lateral_metrics.planning_age_stale_ratio,
            }
        if result.apollo_internal_metrics and result.apollo_internal_metrics.raw_control_count > 0:
            am = result.apollo_internal_metrics
            output["apollo_internal_metrics"] = {
                "raw_control_count": am.raw_control_count,
                "apollo_speed_mean": am.apollo_speed_mean,
                "apollo_speed_zero_ratio": am.apollo_speed_zero_ratio,
                "engage_state_values": am.engage_state_values,
                "engage_ready_ratio": am.engage_ready_ratio,
                "engage_engaged_ratio": am.engage_engaged_ratio,
                "steer_target_saturated_ratio": am.steer_target_saturated_ratio,
                "apollo_lat_error_mean": am.apollo_lat_error_mean,
                "apollo_lat_error_max": am.apollo_lat_error_max,
                "apollo_head_error_mean_abs": am.apollo_head_error_mean_abs,
                "apollo_head_error_max_abs": am.apollo_head_error_max_abs,
                "latest_replan_stalled": am.latest_replan_stalled,
                "latest_replan_max_age_s": am.latest_replan_max_age_s,
                "trajectory_fraction_zero_ratio": am.trajectory_fraction_zero_ratio,
                "apollo_internal_healthy": am.apollo_internal_healthy,
            }
        print(json.dumps(output, indent=2, ensure_ascii=False))
    else:
        print(generate_report(result))

    # Exit code reflects root cause
    exit_codes = {
        "link_fault": 2,
        "apollo_capability": 3,
        "both": 4,
        "inconclusive": 1,
    }
    sys.exit(exit_codes.get(result.root_cause, 1))


if __name__ == "__main__":
    main()
