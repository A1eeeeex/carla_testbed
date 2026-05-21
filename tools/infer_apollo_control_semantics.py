#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import statistics
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml


DEFAULT_LAG_GRID_SEC = [step * 0.05 for step in range(0, 17)]


@dataclass
class ArtifactSet:
    run_name: str
    artifacts_dir: Path
    raw_control_path: Optional[Path]
    bridge_decode_path: Optional[Path]
    vehicle_response_path: Optional[Path]
    geometry_path: Optional[Path]


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _load_jsonl(path: Optional[Path]) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open() as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_csv(path: Optional[Path]) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="") as fp:
        return [dict(row) for row in csv.DictReader(fp)]


def _load_json_dict(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _allowed_run_dirs_from_summary(path: Optional[Path]) -> Optional[set[str]]:
    payload = _load_json_dict(path)
    captures = payload.get("captures")
    if not isinstance(captures, list):
        return None
    allowed = {
        str(item.get("run_dir", "") or "")
        for item in captures
        if isinstance(item, dict) and bool(item.get("capture_valid"))
    }
    return {item for item in allowed if item}


def _find_artifact_sets(root: Path, *, allowed_run_dirs: Optional[set[str]] = None) -> List[ArtifactSet]:
    sets: List[ArtifactSet] = []
    for raw_path in sorted(root.rglob("artifacts/apollo_control_raw.jsonl")):
        artifacts_dir = raw_path.parent
        run_dir = str(artifacts_dir.parent.resolve())
        if allowed_run_dirs is not None and run_dir not in allowed_run_dirs:
            continue
        run_name = str(artifacts_dir.parent.relative_to(root))
        sets.append(
            ArtifactSet(
                run_name=run_name,
                artifacts_dir=artifacts_dir,
                raw_control_path=raw_path,
                bridge_decode_path=artifacts_dir / "bridge_control_decode.jsonl",
                vehicle_response_path=artifacts_dir / "carla_vehicle_response.csv",
                geometry_path=artifacts_dir / "lateral_geometry_debug.csv",
            )
        )
    return sets


def _extract_raw_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = record.get("apollo_control_raw")
    return payload if isinstance(payload, dict) else {}


def _extract_series(records: Sequence[Dict[str, Any]], field: str, *, nested: bool = False) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for record in records:
        ts = _safe_float(record.get("ts_sec"))
        if ts is None:
            continue
        source = _extract_raw_payload(record) if nested else record
        value = _safe_float(source.get(field))
        if value is None:
            continue
        out.append((ts, value))
    return out


def _extract_csv_series(records: Sequence[Dict[str, Any]], field: str) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for record in records:
        ts = _safe_float(record.get("ts_sec"))
        value = _safe_float(record.get(field))
        if ts is None or value is None:
            continue
        out.append((ts, value))
    return out


def _nearest_value(
    target_ts: float,
    series_ts: Sequence[float],
    series_vals: Sequence[float],
    *,
    tolerance_sec: float,
) -> Optional[float]:
    if not series_ts:
        return None
    idx = bisect.bisect_left(series_ts, target_ts)
    candidates: List[int] = []
    if idx < len(series_ts):
        candidates.append(idx)
    if idx > 0:
        candidates.append(idx - 1)
    best_idx: Optional[int] = None
    best_dist = float("inf")
    for cand in candidates:
        dist = abs(series_ts[cand] - target_ts)
        if dist < best_dist:
            best_dist = dist
            best_idx = cand
    if best_idx is None or best_dist > tolerance_sec:
        return None
    return series_vals[best_idx]


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) < 8 or len(xs) != len(ys):
        return None
    try:
        mean_x = statistics.fmean(xs)
        mean_y = statistics.fmean(ys)
    except statistics.StatisticsError:
        return None
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 1e-9 or var_y <= 1e-9:
        return None
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return cov / math.sqrt(var_x * var_y)


def _best_lag_correlation(
    source_series: Sequence[Tuple[float, float]],
    target_series: Sequence[Tuple[float, float]],
    *,
    lags_sec: Sequence[float],
    tolerance_sec: float = 0.20,
    absolute: bool = False,
) -> Dict[str, Any]:
    if not source_series or not target_series:
        return {"best_corr": None, "best_lag_sec": None, "samples": 0}
    target_ts = [ts for ts, _ in target_series]
    target_vals = [float(val) for _, val in target_series]
    best_corr: Optional[float] = None
    best_lag: Optional[float] = None
    best_samples = 0
    for lag in lags_sec:
        xs: List[float] = []
        ys: List[float] = []
        for src_ts, src_val in source_series:
            target_val = _nearest_value(src_ts + float(lag), target_ts, target_vals, tolerance_sec=tolerance_sec)
            if target_val is None:
                continue
            xs.append(abs(float(src_val)) if absolute else float(src_val))
            ys.append(abs(float(target_val)) if absolute else float(target_val))
        corr = _pearson(xs, ys)
        if corr is None:
            continue
        if best_corr is None or abs(corr) > abs(best_corr):
            best_corr = corr
            best_lag = float(lag)
            best_samples = len(xs)
    return {"best_corr": best_corr, "best_lag_sec": best_lag, "samples": best_samples}


def _series_stats(series: Sequence[Tuple[float, float]]) -> Dict[str, Any]:
    values = [float(value) for _, value in series]
    if not values:
        return {"count": 0, "max_abs": None, "stddev": None, "saturation_ratio": None}
    max_abs = max(abs(value) for value in values)
    threshold = 95.0 if max_abs > 2.0 else 0.95
    sat = sum(1 for value in values if abs(value) >= threshold) / max(len(values), 1)
    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "count": len(values),
        "max_abs": max_abs,
        "stddev": stddev,
        "saturation_ratio": sat,
    }


def _score_candidate(
    *,
    series: Sequence[Tuple[float, float]],
    against: Sequence[Tuple[str, Sequence[Tuple[float, float]], float, bool]],
) -> Dict[str, Any]:
    stats = _series_stats(series)
    score = 0.0
    evidence: Dict[str, Any] = {"series_stats": stats, "targets": {}}
    if stats["count"] < 10 or (stats["stddev"] is not None and stats["stddev"] < 1e-6):
        evidence["score"] = 0.0
        evidence["evidence_status"] = "insufficient_variation"
        return evidence
    for name, target_series, weight, absolute in against:
        result = _best_lag_correlation(series, target_series, lags_sec=DEFAULT_LAG_GRID_SEC, absolute=absolute)
        evidence["targets"][name] = result
        corr = result.get("best_corr")
        if corr is not None:
            score += abs(float(corr)) * float(weight)
    sat = stats.get("saturation_ratio")
    if sat is not None:
        score *= max(0.3, 1.0 - (0.5 * float(sat)))
    evidence["score"] = score
    evidence["evidence_status"] = "ok" if score >= 0.15 else "weak"
    return evidence


def _aggregate_lateral(artifact_sets: Sequence[ArtifactSet]) -> Dict[str, Any]:
    raw_records: List[Dict[str, Any]] = []
    decode_records: List[Dict[str, Any]] = []
    vehicle_rows: List[Dict[str, Any]] = []
    for item in artifact_sets:
        raw_records.extend(_load_jsonl(item.raw_control_path))
        decode_records.extend(_load_jsonl(item.bridge_decode_path))
        vehicle_rows.extend(_load_csv(item.vehicle_response_path))
    candidates = {
        "steering_target": _extract_series(raw_records, "steering_target", nested=True),
        "steering_percentage": _extract_series(raw_records, "steering_percentage", nested=True),
        "steering": _extract_series(raw_records, "steering", nested=True),
        "steering_rate": _extract_series(raw_records, "steering_rate", nested=True),
    }
    measured_steer = _extract_csv_series(vehicle_rows, "measured_steer_deg")
    yaw_rate = _extract_csv_series(vehicle_rows, "yaw_rate_rps")
    curvature = _extract_csv_series(vehicle_rows, "curvature")
    scored = {
        name: _score_candidate(
            series=series,
            against=(
                ("measured_steer_deg", measured_steer, 0.55, False),
                ("yaw_rate_rps", yaw_rate, 0.25, False),
                ("curvature", curvature, 0.20, False),
            ),
        )
        for name, series in candidates.items()
    }
    actuator_like = max(scored.items(), key=lambda item: float(item[1].get("score", 0.0)), default=(None, {}))[0]
    intent_like = max(
        scored.items(),
        key=lambda item: abs(
            float(
                (
                    (item[1].get("targets", {}).get("curvature", {}) or {}).get("best_corr")
                    or (item[1].get("targets", {}).get("yaw_rate_rps", {}) or {}).get("best_corr")
                    or 0.0
                )
            )
        ),
        default=(None, {}),
    )[0]
    bridge_counts = Counter(
        str(record.get("selected_steering_field", "") or "").strip()
        for record in decode_records
        if str(record.get("selected_steering_field", "") or "").strip()
    )
    recommended = actuator_like if actuator_like and scored[actuator_like].get("score", 0.0) >= 0.20 else None
    return {
        "candidate_scores": scored,
        "recommended_steering_field": recommended,
        "actuator_like_field": actuator_like,
        "intent_like_field": intent_like,
        "current_bridge_selected_counts": dict(bridge_counts),
        "evidence_status": "ok" if recommended else "insufficient",
    }


def _positive_series(series: Sequence[Tuple[float, float]]) -> List[Tuple[float, float]]:
    return [(ts, max(0.0, float(value))) for ts, value in series]


def _negative_as_positive_series(series: Sequence[Tuple[float, float]]) -> List[Tuple[float, float]]:
    return [(ts, max(0.0, -float(value))) for ts, value in series]


def _aggregate_longitudinal(artifact_sets: Sequence[ArtifactSet]) -> Dict[str, Any]:
    raw_records: List[Dict[str, Any]] = []
    decode_records: List[Dict[str, Any]] = []
    vehicle_rows: List[Dict[str, Any]] = []
    for item in artifact_sets:
        raw_records.extend(_load_jsonl(item.raw_control_path))
        decode_records.extend(_load_jsonl(item.bridge_decode_path))
        vehicle_rows.extend(_load_csv(item.vehicle_response_path))
    signed_accel = _extract_csv_series(vehicle_rows, "forward_accel_mps2")
    positive_accel = _positive_series(signed_accel)
    positive_decel = _negative_as_positive_series(signed_accel)
    signed_candidates = {
        "acceleration": _extract_series(raw_records, "acceleration", nested=True),
        "acceleration_cmd": _extract_series(raw_records, "debug_simple_lon_acceleration_cmd", nested=True),
        "acceleration_lookup": _extract_series(raw_records, "debug_simple_lon_acceleration_lookup", nested=True),
    }
    throttle_candidates = {
        "throttle": _extract_series(raw_records, "throttle", nested=True),
        "debug_throttle_cmd": _extract_series(raw_records, "debug_simple_lon_throttle_cmd", nested=True),
    }
    brake_candidates = {
        "brake": _extract_series(raw_records, "brake", nested=True),
        "debug_brake_cmd": _extract_series(raw_records, "debug_simple_lon_brake_cmd", nested=True),
    }
    signed_scores = {
        name: _score_candidate(
            series=series,
            against=(("forward_accel_mps2", signed_accel, 1.0, False),),
        )
        for name, series in signed_candidates.items()
    }
    throttle_scores = {
        name: _score_candidate(
            series=series,
            against=(("positive_accel_mps2", positive_accel, 1.0, True),),
        )
        for name, series in throttle_candidates.items()
    }
    brake_scores = {
        name: _score_candidate(
            series=series,
            against=(("positive_decel_mps2", positive_decel, 1.0, True),),
        )
        for name, series in brake_candidates.items()
    }
    signed_best = max(signed_scores.items(), key=lambda item: float(item[1].get("score", 0.0)), default=(None, {}))[0]
    throttle_best = max(throttle_scores.items(), key=lambda item: float(item[1].get("score", 0.0)), default=(None, {}))[0]
    brake_best = max(brake_scores.items(), key=lambda item: float(item[1].get("score", 0.0)), default=(None, {}))[0]
    bridge_counts = Counter(
        str(record.get("selected_signed_acceleration_field", "") or "").strip()
        for record in decode_records
        if str(record.get("selected_signed_acceleration_field", "") or "").strip()
    )
    recommended_signed = (
        signed_best if signed_best and signed_scores[signed_best].get("score", 0.0) >= 0.20 else None
    )
    return {
        "signed_candidate_scores": signed_scores,
        "throttle_candidate_scores": throttle_scores,
        "brake_candidate_scores": brake_scores,
        "recommended_signed_acceleration_field": recommended_signed,
        "recommended_throttle_like_field": throttle_best,
        "recommended_brake_like_field": brake_best,
        "current_bridge_selected_counts": dict(bridge_counts),
        "evidence_status": "ok" if recommended_signed else "insufficient",
    }


def _build_recommended_mapping_payload(
    *,
    calibration_path: str,
    lateral: Dict[str, Any],
    longitudinal: Dict[str, Any],
) -> Dict[str, Any]:
    physical_cfg: Dict[str, Any] = {
        "calibration_file": calibration_path,
        "allow_legacy_fallback": True,
    }
    preferred_steer = lateral.get("recommended_steering_field")
    if preferred_steer:
        physical_cfg["preferred_steering_field"] = preferred_steer
        physical_cfg["steering_field_priority"] = [
            preferred_steer,
            "steering_target",
            "steering_percentage",
            "steering",
            "steering_rate",
        ]
    preferred_signed = longitudinal.get("recommended_signed_acceleration_field")
    if preferred_signed:
        mapped_name = {
            "acceleration": "acceleration",
            "acceleration_cmd": "debug_simple_lon_acceleration_cmd",
            "acceleration_lookup": "debug_simple_lon_acceleration_lookup",
        }.get(preferred_signed, preferred_signed)
        physical_cfg["preferred_acceleration_field"] = mapped_name
        physical_cfg["acceleration_field_priority"] = [
            mapped_name,
            "acceleration",
            "debug_simple_lon_acceleration_cmd",
            "debug_simple_lon_acceleration_lookup",
        ]
        physical_cfg["use_top_level_acceleration"] = mapped_name == "acceleration"
        physical_cfg["use_lon_debug"] = mapped_name != "acceleration"
    return {
        "algo": {
            "apollo": {
                "bridge": {
                    "control_mapping": {
                        "actuator_mapping_mode": "physical",
                        "physical": physical_cfg,
                    }
                }
            }
        }
    }


def _write_optional_plots(output_dir: Path, lateral: Dict[str, Any], longitudinal: Dict[str, Any]) -> List[str]:
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return []
    created: List[str] = []
    plot_specs = [
        (
            "lateral_candidate_scores.png",
            lateral.get("candidate_scores", {}),
            "Lateral candidate scores",
        ),
        (
            "longitudinal_signed_candidate_scores.png",
            longitudinal.get("signed_candidate_scores", {}),
            "Longitudinal signed candidate scores",
        ),
    ]
    for filename, scores, title in plot_specs:
        if not scores:
            continue
        labels = list(scores.keys())
        values = [float((scores[label] or {}).get("score", 0.0)) for label in labels]
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(labels, values)
        ax.set_title(title)
        ax.set_ylabel("score")
        ax.tick_params(axis="x", rotation=20)
        fig.tight_layout()
        path = output_dir / filename
        fig.savefig(path)
        plt.close(fig)
        created.append(str(path))
    return created


def _markdown_report(payload: Dict[str, Any]) -> str:
    lateral = payload["lateral"]
    longitudinal = payload["longitudinal"]
    lines = [
        "# Apollo↔CARLA 控制字段语义推断报告",
        "",
        "## 结论摘要",
        "",
        f"- 横向推荐字段：`{lateral.get('recommended_steering_field') or '证据不足'}`",
        f"- 横向 actuator-like 字段：`{lateral.get('actuator_like_field') or '证据不足'}`",
        f"- 横向 intent-like 字段：`{lateral.get('intent_like_field') or '证据不足'}`",
        f"- 纵向 signed 意图字段：`{longitudinal.get('recommended_signed_acceleration_field') or '证据不足'}`",
        f"- throttle-like 字段：`{longitudinal.get('recommended_throttle_like_field') or '证据不足'}`",
        f"- brake-like 字段：`{longitudinal.get('recommended_brake_like_field') or '证据不足'}`",
        "",
        "## 横向候选字段",
        "",
    ]
    for name, evidence in (lateral.get("candidate_scores", {}) or {}).items():
        targets = evidence.get("targets", {}) or {}
        corr = (targets.get("measured_steer_deg", {}) or {}).get("best_corr")
        lag = (targets.get("measured_steer_deg", {}) or {}).get("best_lag_sec")
        saturation_ratio = (evidence.get("series_stats", {}) or {}).get("saturation_ratio")
        lines.append(
            "- `%s`: score=%.3f, 与 measured_steer_deg 最佳相关=%.3f, 最佳滞后=%s s, 饱和率=%s"
            % (
                name,
                float(evidence.get("score", 0.0)),
                float(corr or 0.0),
                f"{lag:.2f}" if isinstance(lag, (int, float)) else "n/a",
                f"{100.0 * float(saturation_ratio):.1f}%" if saturation_ratio is not None else "n/a",
            )
        )
    lines.extend(["", "## 纵向候选字段", ""])
    for group_name, group_scores in (
        ("signed", longitudinal.get("signed_candidate_scores", {})),
        ("throttle", longitudinal.get("throttle_candidate_scores", {})),
        ("brake", longitudinal.get("brake_candidate_scores", {})),
    ):
        lines.append(f"### {group_name}")
        lines.append("")
        for name, evidence in (group_scores or {}).items():
            target_info = next(iter((evidence.get("targets", {}) or {}).values()), {})
            corr = target_info.get("best_corr")
            lag = target_info.get("best_lag_sec")
            lines.append(
                "- `%s`: score=%.3f, 最佳相关=%.3f, 最佳滞后=%s s"
                % (
                    name,
                    float(evidence.get("score", 0.0)),
                    float(corr or 0.0),
                    f"{lag:.2f}" if isinstance(lag, (int, float)) else "n/a",
                )
            )
        lines.append("")
    lines.extend(
        [
            "## 风险与证据边界",
            "",
            "- 本报告通过字段与 CARLA 实测响应的时滞对齐相关性做工程推断，不等价于 Apollo 源码级语义证明。",
            "- 若某字段样本方差过小、长期饱和、或只覆盖单一工况，会被标记为弱证据或证据不足。",
            "- 如果换车、换控制器版本、换地图或换桥接保护链，请重新跑 suite，不要直接复用旧结论。",
            "",
        ]
    )
    return "\n".join(lines)


def infer_suite(
    input_dir: Path,
    output_dir: Path,
    *,
    calibration_path: str,
    capture_validity_summary_path: Optional[Path] = None,
) -> Dict[str, Any]:
    allowed_run_dirs = _allowed_run_dirs_from_summary(capture_validity_summary_path)
    artifact_sets = _find_artifact_sets(input_dir, allowed_run_dirs=allowed_run_dirs)
    lateral = _aggregate_lateral(artifact_sets)
    longitudinal = _aggregate_longitudinal(artifact_sets)
    recommended_mapping = _build_recommended_mapping_payload(
        calibration_path=calibration_path,
        lateral=lateral,
        longitudinal=longitudinal,
    )
    payload = {
        "schema_version": 1,
        "input_dir": str(input_dir),
        "artifact_sets": [
            {
                "run_name": item.run_name,
                "artifacts_dir": str(item.artifacts_dir),
                "raw_control_path": str(item.raw_control_path) if item.raw_control_path else "",
                "bridge_decode_path": str(item.bridge_decode_path) if item.bridge_decode_path else "",
                "vehicle_response_path": str(item.vehicle_response_path) if item.vehicle_response_path else "",
                "geometry_path": str(item.geometry_path) if item.geometry_path else "",
            }
            for item in artifact_sets
        ],
        "capture_validity_summary_path": str(capture_validity_summary_path) if capture_validity_summary_path else "",
        "filtered_to_valid_captures": allowed_run_dirs is not None,
        "lateral": lateral,
        "longitudinal": longitudinal,
        "recommended_mapping_config": recommended_mapping,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "control_semantics_report.json").write_text(json.dumps(payload, indent=2))
    (output_dir / "control_semantics_report.md").write_text(_markdown_report(payload))
    (output_dir / "recommended_physical_mapping.yaml").write_text(
        yaml.safe_dump(recommended_mapping, sort_keys=False)
    )
    created_plots = _write_optional_plots(output_dir, lateral, longitudinal)
    if created_plots:
        payload["plots"] = created_plots
        (output_dir / "control_semantics_report.json").write_text(json.dumps(payload, indent=2))
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Infer Apollo control field semantics from suite artifacts")
    ap.add_argument("--input-dir", required=True)
    ap.add_argument("--output-dir", default="")
    ap.add_argument("--calibration-path", default="artifacts/carla_actuator_calibration.json")
    ap.add_argument("--capture-validity-summary", default="")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else input_dir / "artifacts"
    summary_path = Path(args.capture_validity_summary).expanduser().resolve() if args.capture_validity_summary else None
    infer_suite(
        input_dir,
        output_dir,
        calibration_path=str(args.calibration_path),
        capture_validity_summary_path=summary_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
