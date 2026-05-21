#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import classify_replan_reason, safe_float, safe_int


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _fmt(value: Any, digits: int = 3) -> str:
    num = safe_float(value)
    if num is None:
        return ""
    return f"{num:.{digits}f}"


def _window_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rel_vals = [safe_float(row.get("trajectory_relative_time_min_sec")) for row in rows]
    rel_vals = [value for value in rel_vals if value is not None]
    path_vals = [safe_float(row.get("trajectory_total_path_length")) for row in rows]
    path_vals = [value for value in path_vals if value is not None]
    total_time_vals = [safe_float(row.get("trajectory_total_time")) for row in rows]
    total_time_vals = [value for value in total_time_vals if value is not None]
    v0_vals = [safe_float(row.get("first_trajectory_point_v")) for row in rows]
    v0_vals = [value for value in v0_vals if value is not None]
    point_counts = [safe_int(row.get("trajectory_point_count")) for row in rows]
    point_counts = [value for value in point_counts if value is not None]
    replan_values = [bool(row.get("is_replan")) for row in rows]
    return {
        "streak_length": len(rows),
        "start_seq": safe_int(rows[0].get("planning_header_sequence_num")) if rows else None,
        "end_seq": safe_int(rows[-1].get("planning_header_sequence_num")) if rows else None,
        "rel_min_first": rel_vals[0] if rel_vals else None,
        "rel_min_last": rel_vals[-1] if rel_vals else None,
        "rel_min_min": min(rel_vals) if rel_vals else None,
        "rel_min_max": max(rel_vals) if rel_vals else None,
        "path_len_first": path_vals[0] if path_vals else None,
        "path_len_last": path_vals[-1] if path_vals else None,
        "path_len_min": min(path_vals) if path_vals else None,
        "path_len_max": max(path_vals) if path_vals else None,
        "total_time_min": min(total_time_vals) if total_time_vals else None,
        "total_time_max": max(total_time_vals) if total_time_vals else None,
        "v0_first": v0_vals[0] if v0_vals else None,
        "v0_last": v0_vals[-1] if v0_vals else None,
        "v0_min": min(v0_vals) if v0_vals else None,
        "v0_max": max(v0_vals) if v0_vals else None,
        "point_count_min": min(point_counts) if point_counts else None,
        "point_count_max": max(point_counts) if point_counts else None,
        "replan_true_count": sum(1 for value in replan_values if value),
    }


def _first_path_fallback_index(rows: List[Dict[str, Any]]) -> Optional[int]:
    for idx, row in enumerate(rows):
        if str(row.get("trajectory_type") or "").strip() == "PATH_FALLBACK":
            return idx
    return None


def _precursor_window_before_first_fallback(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fallback_idx = _first_path_fallback_index(rows)
    if fallback_idx is None or fallback_idx <= 0:
        return []
    cursor = fallback_idx - 1
    window: List[Dict[str, Any]] = []
    while cursor >= 0:
        row = rows[cursor]
        if str(row.get("trajectory_type") or "").strip() != "NORMAL":
            break
        if classify_replan_reason(row.get("replan_reason")) != "current_time_smaller":
            break
        window.append(row)
        cursor -= 1
    return list(reversed(window))


def _normal_context_before_first_fallback(rows: List[Dict[str, Any]], lookback: int = 6) -> List[Dict[str, Any]]:
    fallback_idx = _first_path_fallback_index(rows)
    if fallback_idx is None or fallback_idx <= 0:
        return []
    selected: List[Dict[str, Any]] = []
    cursor = fallback_idx - 1
    while cursor >= 0 and len(selected) < lookback:
        row = rows[cursor]
        if str(row.get("trajectory_type") or "").strip() != "NORMAL":
            break
        selected.append(row)
        cursor -= 1
    return list(reversed(selected))


def _longest_normal_current_time_smaller_window(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    for row in rows:
        if (
            str(row.get("trajectory_type") or "").strip() == "NORMAL"
            and classify_replan_reason(row.get("replan_reason")) == "current_time_smaller"
        ):
            current.append(row)
            if len(current) > len(best):
                best = list(current)
        else:
            current = []
    return best


def _case_summary(label: str, run_dir: Path) -> Dict[str, Any]:
    planning_path = run_dir / "artifacts" / "planning_topic_debug.jsonl"
    rows = _load_jsonl(planning_path)
    fallback_idx = _first_path_fallback_index(rows)
    fallback_row = rows[fallback_idx] if fallback_idx is not None else None
    precursor_kind = "longest_normal_current_time_smaller"
    if fallback_row is not None:
        precursor = _precursor_window_before_first_fallback(rows)
        if precursor:
            precursor_kind = "before_first_fallback_current_time_smaller"
        else:
            precursor = _normal_context_before_first_fallback(rows)
            precursor_kind = "before_first_fallback_normal_context"
    else:
        precursor = _longest_normal_current_time_smaller_window(rows)
    precursor_summary = _window_summary(precursor)
    return {
        "label": label,
        "run_dir": str(run_dir),
        "has_first_fallback": fallback_row is not None,
        "first_fallback_seq": safe_int(fallback_row.get("planning_header_sequence_num")) if fallback_row else None,
        "first_fallback_reason_family": classify_replan_reason(fallback_row.get("replan_reason")) if fallback_row else None,
        "first_fallback_path_len": safe_float(fallback_row.get("trajectory_total_path_length")) if fallback_row else None,
        "first_fallback_v0": safe_float(fallback_row.get("first_trajectory_point_v")) if fallback_row else None,
        "precursor_kind": precursor_kind,
        "precursor": precursor_summary,
    }


def render_report(cases: List[Dict[str, Any]]) -> str:
    lines = [
        "# Town01 Current-Time-Smaller Control Report",
        "",
        "## Key Conclusion",
        "",
    ]
    unstable = next((case for case in cases if case["label"] == "219_repeat"), None)
    stable_repeat = next((case for case in cases if case["label"] == "097_repeat"), None)
    smoke = next((case for case in cases if case["label"] == "219_smoke"), None)
    if unstable and stable_repeat and smoke:
        lines.append(
            "- `current_time_smaller` 本身不是充分根因：`097_repeat` 也会在 `NORMAL` 轨迹上连续出现该信号。"
        )
        lines.append(
            "- 当前更强分裂点是：`219_repeat` 会从多帧 `current_time_smaller + NORMAL` 更早塌到短 `PATH_FALLBACK`，"
            f"其中 precursor streak=`{unstable['precursor']['streak_length']}`，fallback_path_len=`{_fmt(unstable['first_fallback_path_len'])}`；"
            f"`097_repeat` 的对应 NORMAL current-time-smaller window 仍维持 path_len=`{_fmt(stable_repeat['precursor']['path_len_min'])}..{_fmt(stable_repeat['precursor']['path_len_max'])}`。"
        )
        lines.append(
            f"- `219_smoke` 则不带这种前驱，它在 first fallback 前的 `trajectory_relative_time_min_sec` 仍是负值窗口 "
            f"`{_fmt(smoke['precursor']['rel_min_min'])}..{_fmt(smoke['precursor']['rel_min_max'])}`，"
            f"first fallback 触发 family 仍是 `{smoke['first_fallback_reason_family']}`。"
        )
        lines.append(
            "- 当前最像根因的分裂是 precursor NORMAL 轨迹自身的“horizon 形状”："
            f"`219_repeat` 已经是低速短 horizon `v0={_fmt(unstable['precursor']['v0_min'])}..{_fmt(unstable['precursor']['v0_max'])}` "
            f"`path_len={_fmt(unstable['precursor']['path_len_min'])}..{_fmt(unstable['precursor']['path_len_max'])}` "
            f"`point_count={unstable['precursor']['point_count_min']}..{unstable['precursor']['point_count_max']}`；"
            f"`219_smoke` 还是 richer normal plan `v0={_fmt(smoke['precursor']['v0_min'])}..{_fmt(smoke['precursor']['v0_max'])}` "
            f"`path_len={_fmt(smoke['precursor']['path_len_min'])}..{_fmt(smoke['precursor']['path_len_max'])}` "
            f"`point_count={smoke['precursor']['point_count_min']}..{smoke['precursor']['point_count_max']}`；"
            f"`097_repeat` 虽然同样有 `current_time_smaller`，但仍维持长 horizon `v0={_fmt(stable_repeat['precursor']['v0_min'])}..{_fmt(stable_repeat['precursor']['v0_max'])}` "
            f"`path_len={_fmt(stable_repeat['precursor']['path_len_min'])}..{_fmt(stable_repeat['precursor']['path_len_max'])}`。"
        )
        lines.append(
            "- 进一步看，三组 case 的 precursor `trajectory_total_time` 都基本锁在 "
            f"`{_fmt(unstable['precursor']['total_time_min'])}..{_fmt(unstable['precursor']['total_time_max'])}` / "
            f"`{_fmt(smoke['precursor']['total_time_min'])}..{_fmt(smoke['precursor']['total_time_max'])}` / "
            f"`{_fmt(stable_repeat['precursor']['total_time_min'])}..{_fmt(stable_repeat['precursor']['total_time_max'])}`，"
            "所以更像是空间 horizon 先塌，而不是时间 horizon 先塌。"
        )
    lines.extend(
        [
            "",
            "## Case Summary",
            "",
            "| label | precursor_kind | precursor_seq | precursor_streak | precursor_rel_min_range | precursor_path_len_range | precursor_total_time_range | precursor_v0_range | precursor_point_count_range | precursor_replan_true_count | first_fallback_seq | first_fallback_reason | first_fallback_path_len | first_fallback_v0 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for case in cases:
        precursor = case["precursor"]
        lines.append(
            "| "
            + " | ".join(
                [
                    case["label"],
                    str(case["precursor_kind"]),
                    f"{precursor.get('start_seq') or ''}..{precursor.get('end_seq') or ''}",
                    str(precursor.get("streak_length") or 0),
                    f"{_fmt(precursor.get('rel_min_min'))}..{_fmt(precursor.get('rel_min_max'))}",
                    f"{_fmt(precursor.get('path_len_min'))}..{_fmt(precursor.get('path_len_max'))}",
                    f"{_fmt(precursor.get('total_time_min'))}..{_fmt(precursor.get('total_time_max'))}",
                    f"{_fmt(precursor.get('v0_min'))}..{_fmt(precursor.get('v0_max'))}",
                    f"{precursor.get('point_count_min') or ''}..{precursor.get('point_count_max') or ''}",
                    str(precursor.get("replan_true_count") or 0),
                    str(case.get("first_fallback_seq") or ""),
                    str(case.get("first_fallback_reason_family") or ""),
                    _fmt(case.get("first_fallback_path_len")),
                    _fmt(case.get("first_fallback_v0")),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare current_time_smaller precursor windows across Town01 runs.")
    parser.add_argument(
        "--case",
        action="append",
        required=True,
        help="Case in label=run_dir form. May be repeated.",
    )
    parser.add_argument("--report", required=True, help="Markdown report output path.")
    args = parser.parse_args()

    cases: List[Dict[str, Any]] = []
    for item in args.case:
        if "=" not in item:
            raise SystemExit(f"Invalid --case {item!r}; expected label=run_dir")
        label, raw_path = item.split("=", 1)
        run_dir = Path(raw_path).expanduser().resolve()
        cases.append(_case_summary(label.strip(), run_dir))
    report_path = Path(args.report).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(cases), encoding="utf-8")
    print(f"[town01-current-time-smaller-control] rows={len(cases)} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
