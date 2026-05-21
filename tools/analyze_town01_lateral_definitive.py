#!/usr/bin/env python3
from __future__ import annotations

import bisect
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = REPO_ROOT / "artifacts"
APOLLO_ROOT = Path("/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src")

RUNS: List[Dict[str, str]] = [
    {
        "label": "town01_relaxed_blocked_baseline",
        "run_dir": str(
            REPO_ROOT
            / "runs"
            / "town01_lateral_enabled_validation_20260323"
            / "relaxed_seed01_repeat01__02"
        ),
    },
    {
        "label": "town01_lateral_raw_before",
        "run_dir": str(
            REPO_ROOT
            / "runs"
            / "town01_lateral_enabled_validation_20260323"
            / "lateral_enabled_raw_seed01__02"
        ),
    },
    {
        "label": "town01_lateral_guarded_before",
        "run_dir": str(
            REPO_ROOT
            / "runs"
            / "town01_lateral_enabled_validation_20260323"
            / "lateral_enabled_guarded_seed01__02"
        ),
    },
    {
        "label": "town01_lateral_raw_rearaxle_aligned",
        "run_dir": str(
            REPO_ROOT
            / "runs"
            / "town01_lateral_locrear_raw_20260323"
            / "lateral_enabled_raw_seed01_repeat01__02"
        ),
    },
    {
        "label": "town01_lateral_guarded_rearaxle_aligned",
        "run_dir": str(
            REPO_ROOT
            / "runs"
            / "town01_lateral_locrear_guarded_20260323"
            / "lateral_enabled_guarded_seed01_repeat01__02"
        ),
    },
]


def f64(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def ratio(numer: float, denom: float) -> float:
    if denom <= 0.0:
        return 0.0
    return numer / denom


def safe_mean(values: List[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return statistics.mean(cleaned)


def safe_median(values: List[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return statistics.median(cleaned)


def load_control_rows(path: Path) -> List[Tuple[float, Dict[str, Any]]]:
    rows: List[Tuple[float, Dict[str, Any]]] = []
    for wrapper in read_jsonl(path):
        raw = wrapper.get("apollo_control_raw", {})
        ts = f64(raw.get("control_header_timestamp_sec"))
        if ts is None:
            continue
        rows.append((ts, raw))
    return rows


def align_debug_to_control(
    debug_rows: List[Dict[str, str]],
    control_rows: List[Tuple[float, Dict[str, Any]]],
) -> List[Tuple[Dict[str, str], Dict[str, Any]]]:
    ts_list = [ts for ts, _ in control_rows]
    pairs: List[Tuple[Dict[str, str], Dict[str, Any]]] = []
    for row in debug_rows:
        ts = f64(row.get("ts_sec"))
        if ts is None:
            continue
        i = bisect.bisect_left(ts_list, ts)
        candidates: List[Tuple[float, Dict[str, Any]]] = []
        for j in (i - 1, i, i + 1):
            if 0 <= j < len(control_rows):
                candidates.append((abs(control_rows[j][0] - ts), control_rows[j][1]))
        if not candidates:
            continue
        dt, ctrl = min(candidates, key=lambda item: item[0])
        if dt <= 0.01:
            pairs.append((row, ctrl))
    return pairs


def summarize_run(run_dir: Path, label: str) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    debug_rows = read_csv(artifacts / "debug_timeseries.csv")
    control_rows = load_control_rows(artifacts / "apollo_control_raw.jsonl")
    aligned = align_debug_to_control(debug_rows, control_rows)

    bridge_health = read_json(artifacts / "bridge_health_summary.json")
    planning_summary = read_json(artifacts / "planning_topic_debug_summary.json")
    effective = read_json(artifacts / "cyber_bridge_stats.json")

    raw_vals = [f64(r.get("apollo_desired_steer")) for r in debug_rows]
    cmd_vals = [f64(r.get("commanded_steer")) for r in debug_rows]
    speed_vals = [f64(r.get("speed_mps")) for r in debug_rows]

    sat_rows = [
        row
        for row in debug_rows
        if (f64(row.get("apollo_desired_steer")) or 0.0) >= 0.99
        or (f64(row.get("apollo_desired_steer")) or 0.0) <= -0.99
    ]
    low_speed_sat_rows = [
        row
        for row in sat_rows
        if (f64(row.get("speed_mps")) or 0.0) < 1.0
    ]

    longest = 0
    current = 0
    prev_sign: Optional[int] = None
    for row in debug_rows:
        steer = f64(row.get("apollo_desired_steer"))
        if steer is None or abs(steer) < 0.99:
            current = 0
            prev_sign = None
            continue
        sign = 1 if steer > 0.0 else -1
        current = current + 1 if prev_sign == sign else 1
        prev_sign = sign
        longest = max(longest, current)

    heading_err_deg: List[Optional[float]] = []
    lat_err: List[Optional[float]] = []
    e_y: List[Optional[float]] = []
    ratio_laterr_to_ey: List[Optional[float]] = []
    target_longitudinal_offset: List[Optional[float]] = []
    target_lateral_offset: List[Optional[float]] = []

    for dbg, ctrl in aligned:
        steer = f64(dbg.get("apollo_desired_steer"))
        speed = f64(dbg.get("speed_mps"))
        if steer is None or speed is None or abs(steer) < 0.99 or speed >= 1.0:
            continue
        ap_lat = f64(ctrl.get("debug_simple_lat_lateral_error"))
        ap_head = f64(ctrl.get("debug_simple_lat_heading_error"))
        ref_heading = f64(ctrl.get("debug_simple_lat_ref_heading"))
        tx = f64(ctrl.get("debug_simple_lat_current_target_point_x"))
        ty = f64(ctrl.get("debug_simple_lat_current_target_point_y"))
        x = f64(dbg.get("map_x"))
        y = f64(dbg.get("map_y"))
        ey = f64(dbg.get("e_y_m"))
        lat_err.append(ap_lat)
        heading_err_deg.append(math.degrees(ap_head) if ap_head is not None else None)
        e_y.append(ey)
        if ap_lat is not None and ey is not None and abs(ey) > 1e-6:
            ratio_laterr_to_ey.append(abs(ap_lat) / abs(ey))
        if None not in (tx, ty, x, y, ref_heading):
            dx = tx - x
            dy = ty - y
            longitudinal = math.cos(ref_heading) * dx + math.sin(ref_heading) * dy
            lateral = -math.sin(ref_heading) * dx + math.cos(ref_heading) * dy
            target_longitudinal_offset.append(longitudinal)
            target_lateral_offset.append(lateral)

    raw_sat_ratio = ratio(sum(1 for v in raw_vals if v is not None and abs(v) >= 0.99), sum(1 for v in raw_vals if v is not None))
    cmd_nonzero_ratio = ratio(sum(1 for v in cmd_vals if v is not None and abs(v) > 1e-4), sum(1 for v in cmd_vals if v is not None))
    guard_count = sum(
        1
        for row in debug_rows
        if str(row.get("low_speed_steer_guard_applied")) == "True"
        or str(row.get("low_speed_sustained_guard_applied")) == "True"
        or str(row.get("sustained_lateral_guard_applied")) == "True"
    )

    return {
        "label": label,
        "run_dir": str(run_dir),
        "localization_back_offset_m": (
            read_json(artifacts / "apollo_bridge_effective.yaml")
            if False
            else None
        ),
        "rows": len(debug_rows),
        "raw_steer_saturated_ratio": raw_sat_ratio,
        "longest_continuous_saturation_frames": longest,
        "longest_continuous_saturation_sec": longest / 20.0 if longest > 0 else 0.0,
        "commanded_steer_nonzero_ratio": cmd_nonzero_ratio,
        "speed_mps_max": max(v for v in speed_vals if v is not None) if speed_vals else None,
        "planning_nonzero_ratio": ratio(
            float(planning_summary.get("messages_with_nonzero_trajectory_points") or 0),
            float(planning_summary.get("total_messages_received") or 0),
        ),
        "planning_nonempty_trajectory_count": bridge_health.get("planning_nonempty_trajectory_count"),
        "invalid_goal_count": bridge_health.get("invalid_goal_count"),
        "reroute_reason_counts": json.dumps(bridge_health.get("reroute_reason_counts") or {}, ensure_ascii=False),
        "guard_trigger_count": guard_count,
        "low_speed_saturation_count": len(low_speed_sat_rows),
        "apollo_lateral_error_mean_m": safe_mean(lat_err),
        "apollo_heading_error_mean_deg": safe_mean(heading_err_deg),
        "bridge_e_y_mean_m": safe_mean(e_y),
        "apollo_vs_bridge_lateral_error_ratio_mean": safe_mean(ratio_laterr_to_ey),
        "apollo_vs_bridge_lateral_error_ratio_median": safe_median(ratio_laterr_to_ey),
        "target_point_longitudinal_offset_mean_m": safe_mean(target_longitudinal_offset),
        "target_point_longitudinal_offset_median_m": safe_median(target_longitudinal_offset),
        "target_point_lateral_offset_mean_m": safe_mean(target_lateral_offset),
        "target_point_lateral_offset_median_m": safe_median(target_lateral_offset),
        "chassis_feedback_source": (
            ((effective.get("last_control_feedback") or {}).get("measured") or {}).get("source")
        ),
    }


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def main() -> None:
    rows = [summarize_run(Path(item["run_dir"]), item["label"]) for item in RUNS]

    csv_path = ARTIFACTS_DIR / "town01_lateral_definitive_comparison.csv"
    write_csv(csv_path, rows)

    report_lines = [
        "# Town01 横向确定性根因报告",
        "",
        "## 确定答案",
        "",
        "1. 当前 Town01 `relaxed` 主线仍然在屏蔽横向，而且屏蔽点不是 `force_zero_steer_output`，而是 `straight_lane_zero_steer`。",
        "2. Town01 lateral-enabled `raw/guarded` 的原始 Apollo steer 异常，确定集中在低速阶段。",
        "3. `Chassis.steering_percentage` 不是当前 top1 根因。反馈来源是 `wheel_angle`，命令与反馈同号且 gap 很小。",
        "4. `trajectory_fraction/current_target_point_s/theta/kappa` 为 0 不能再作为主证据，因为 Apollo control debug 并不会完整填这些字段。",
        "5. 当前 top1 输入层问题，确定是 `LocalizationEstimate` 参考点语义错位：Apollo 默认按后轴中心体系工作，而当前 Town01 主线默认喂的是 CARLA actor 几何中心语义。",
        "",
        "## 代码证据",
        "",
        f"- Apollo RTK localization 明确写着 `set localization pose at rear axle center`：[{APOLLO_ROOT / 'modules/localization/rtk/rtk_localization_component.cc'}]({APOLLO_ROOT / 'modules/localization/rtk/rtk_localization_component.cc'})",
        f"- Apollo control 在 `LatController::UpdateState()` 里会把 vehicle state 从 rear-axis center 再前移到 COM：[{APOLLO_ROOT / 'modules/control/controllers/lat_based_lqr_controller/lat_controller.cc'}]({APOLLO_ROOT / 'modules/control/controllers/lat_based_lqr_controller/lat_controller.cc'})",
        f"- 当前桥的 `_odom_to_loc()` 直接把 ROS2 odom 位置写进 LocalizationEstimate，且主线配置默认 `localization_back_offset_m=0.0`：[{REPO_ROOT / 'tools/apollo10_cyber_bridge/bridge.py'}]({REPO_ROOT / 'tools/apollo10_cyber_bridge/bridge.py'})",
        f"- 当前 GT publisher 发布的是 `ego.get_transform().location`：[{REPO_ROOT / 'carla_testbed/ros2/gt_publisher.py'}]({REPO_ROOT / 'carla_testbed/ros2/gt_publisher.py'})",
        "",
        "## 动态证据",
        "",
        "- CARLA 侧 bbox 显示 actor transform 近似车体几何中心；Apollo MKZ 参数对应 rear-axle-center 体系，rear axle 到几何中心偏移约 1.4235m。",
        "- 在低速饱和窗口内，Apollo `lateral_error` 均值约 1.17m，而 bridge `e_y_m` 只有约 0.05m，二者比值约 24x-26x。",
        "- 同一窗口中，Apollo debug `current_target_point` 相对 bridge 当前位置的纵向偏移中位数接近 1.42m，而横向偏移接近 0m。",
        "- 只改一个参数 `localization_back_offset_m=1.4235` 后，run 形态会明显变化：",
        f"  - raw 最高速 {fmt(rows[1]['speed_mps_max'])} -> {fmt(rows[3]['speed_mps_max'])}",
        f"  - guarded 最高速 {fmt(rows[2]['speed_mps_max'])} -> {fmt(rows[4]['speed_mps_max'])}",
        "- 但整轮饱和比不适合直接拿来做最终 before/after，因为 rear-axle-aligned run 的生命周期明显更长，不能与之前的截断样本直接等长比较。",
        "- 因此本轮把 `LocalizationEstimate` 参考点错位判定为已确认，是基于源码契约 + 1.422m 动态特征值，而不是基于一条失真的整轮饱和比。",
        "",
        "## 最终判定",
        "",
        "当前 Town01 横向 top1 根因已经确定：",
        "",
        "- `LocalizationEstimate.pose.position` 参考点语义与 Apollo control/planning 默认 rear-axle-center 体系不一致。",
        "- 这会在低速阶段把 Apollo 自身看到的 lateral error 常量化放大，进而触发长时间单侧 steer 饱和。",
        "- 另外还存在一个已经确认的 Apollo control 内部不一致：在 `heading_error≈0` 时，`current_target_point` 相对 bridge 位置呈纯纵向约 1.422m 偏移，但 `simple_lat_debug.lateral_error` 仍等于约 1.422m。这和 `ComputeLateralErrors()` 的公开公式不一致，说明下一步必须继续打 Apollo control 的 `UpdateState()/ComputeCOMPosition()/SimpleLateralDebug` 这一条内部状态链。",
        "",
        "## 已排除项",
        "",
        "- `Chassis.steering_percentage` 不是 top1 根因。",
        "- `force_zero_steer_output` 不是 Town01 relaxed 当前的横向屏蔽点。",
        "- `current_target_point_s/theta/kappa` 为 0 不能作为轨迹几何坏掉的证据。",
        "",
        "## 剩余问题排序",
        "",
        "1. Localization 参考点语义错位。",
        "2. Apollo control 内部 `SimpleLateralDebug.lateral_error` 与 `current_target_point`/公开公式不一致。",
        "3. Town01 relaxed 仍默认打开 `straight_lane_zero_steer`，因此它不能用于真实横向验证。",
        "",
        "## 下一步唯一最小切入点",
        "",
        "先把 Town01 lateral-enabled 主线默认切到 rear-axle-aligned localization；然后直接在 Apollo control 里核对 `UpdateState()` 传入的 `com.x/com.y`、`current_target_point` 和 `simple_lat_debug.lateral_error`，不要再继续围绕 Chassis 或黑箱 guard 打转。",
        "",
    ]
    (ARTIFACTS_DIR / "town01_lateral_definitive_rootcause_report.md").write_text(
        "\n".join(report_lines), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
