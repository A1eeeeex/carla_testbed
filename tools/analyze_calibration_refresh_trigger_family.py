#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence


DEFAULT_INPUT_CSV = Path("artifacts/calibration_low_y_refresh_trigger_family_weekend.csv")
DEFAULT_REPORT = Path("artifacts/calibration_low_y_refresh_trigger_family_weekend.md")
DEFAULT_SUMMARY_CSV = Path("artifacts/calibration_low_y_refresh_trigger_family_weekend.csv")
DEFAULT_LABELS: Sequence[str] = (
    "low_y_truthful_runtime_retry",
    "low_y_truthful_eventobs",
    "low_y_truthful_eventobs_fresh",
    "low_y_truthful_historical_same_road",
    "low_y_truthful_historical_alt_road",
    "low_y_truthful_tailcheck_startcarla",
    "low_y_truthful_goalvaliditycheck_reuse",
)
RESPONSE_AWARE_SHORT = "low_y_truthful_tailcheck_startcarla"
RESPONSE_AWARE_LONG = "low_y_truthful_goalvaliditycheck_reuse"


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_float(value: Any) -> float | None:
    text = _text(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _fmt_float(value: Any, digits: int = 3, empty: str = "") -> str:
    out = _safe_float(value)
    if out is None:
        return empty
    return f"{out:.{digits}f}"


def _fmt_sec(value: Any, empty: str = "") -> str:
    text = _fmt_float(value, empty=empty)
    if not text:
        return empty
    return f"{text}s"


def _fmt_len(value: Any, empty: str = "m") -> str:
    text = _fmt_float(value, empty="")
    if not text:
        return empty
    return f"{text}m"


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="") as fp:
        return [dict(row) for row in csv.DictReader(fp)]


def _write_csv(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _select_rows(rows: Sequence[Dict[str, str]], labels: Sequence[str]) -> List[Dict[str, str]]:
    by_label = {row.get("label", ""): row for row in rows}
    missing = [label for label in labels if label not in by_label]
    if missing:
        raise SystemExit(f"missing labels from input csv: {', '.join(missing)}")
    return [by_label[label] for label in labels]


def _final_branch(row: Dict[str, str]) -> str:
    return (
        f"{row.get('final_branch_family') or 'unknown'} / "
        f"len~{row.get('final_route_segment_total_length_m') or '?'}m / "
        f"goal={row.get('final_reroute_goal_mode') or '?'}"
    )


def _final_reroute_summary(row: Dict[str, str]) -> str:
    return (
        f"reason={row.get('final_reroute_reason') or ''}, "
        f"ts={row.get('final_reroute_timestamp') or ''}, "
        f"ready={row.get('final_reroute_route_debug_ready') or ''}, "
        f"fb={row.get('final_reroute_fallback_applied') or ''}, "
        f"sent={row.get('final_reroute_routing_request_sent') or ''}"
    )


def _first_event_summary(row: Dict[str, str], *, ts_key: str, delta_key: str, before_key: str) -> str:
    return (
        f"ts={row.get(ts_key) or ''}, "
        f"delta={_fmt_sec(row.get(delta_key))}, "
        f"before={row.get(before_key) or ''}"
    )


def _pre_refresh_evidence(row: Dict[str, str]) -> str:
    return (
        f"routing={row.get('pre_refresh_routing_event_count') or '0'}, "
        f"goal_validity={row.get('pre_refresh_goal_validity_count') or '0'}, "
        f"planning={row.get('pre_refresh_planning_msg_count') or '0'}, "
        f"route_debug={row.get('pre_refresh_route_debug_msg_count') or '0'}, "
        f"upstream_route={row.get('transition_to_refresh_upstream_route_segment_debug_count') or '0'}, "
        f"last_routing_ts={row.get('last_routing_event_timestamp') or ''}"
    )


def _refresh_input_summary(row: Dict[str, str]) -> str:
    return (
        f"pre_gv={row.get('last_pre_refresh_goal_validity_goal_mode') or '?'}/"
        f"{row.get('last_pre_refresh_goal_validity_goal_source') or '?'}@"
        f"{_fmt_sec(row.get('delta_last_pre_refresh_goal_validity_minus_final_reroute_sec'))}, "
        f"final_gv={row.get('final_goal_validity_goal_mode') or '?'}/"
        f"{row.get('final_goal_validity_goal_source') or '?'}, "
        f"recent={row.get('final_goal_validity_signature') or 'none'}/"
        f"{row.get('final_goal_validity_recent_route_debug_present') or ''}/"
        f"{row.get('final_goal_validity_recent_route_debug_fresh') or ''}"
    )


def _refresh_pivot_summary(row: Dict[str, str]) -> str:
    return (
        f"event_rows={row.get('final_routing_event_match_count') or '0'}, "
        f"pre={row.get('last_pre_refresh_visible_route_debug_signature') or 'none'}@"
        f"{_fmt_sec(row.get('delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec'))}, "
        f"post={row.get('first_post_refresh_visible_route_debug_signature') or 'none'}@"
        f"{_fmt_sec(row.get('delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec'))}, "
        f"gv_rows={row.get('transition_to_refresh_goal_validity_count') or '0'}"
    )


def _per_run_read(row: Dict[str, str]) -> List[str]:
    return [
        f"### `{row.get('label') or ''}`",
        "",
        f"- run: [{Path(row.get('run_dir') or '').name}]({row.get('run_dir') or ''})",
        f"- summary: `status={row.get('summary_status') or ''}, fail_reason={row.get('fail_reason') or ''}`",
        f"- final reroute boundary: `reason={row.get('final_reroute_reason') or ''}, pre_goal={row.get('final_reroute_pre_fallback_goal_mode') or ''}, pre_source={row.get('final_reroute_pre_fallback_goal_source') or ''}, goal={row.get('final_reroute_goal_mode') or ''}, goal_source={row.get('final_reroute_goal_source') or ''}, fallback={row.get('final_reroute_fallback_applied') or ''}, routing_request_sent={row.get('final_reroute_routing_request_sent') or ''}, invalid_reason={row.get('final_reroute_invalid_goal_reason') or ''}, route_debug_ready={row.get('final_reroute_route_debug_ready') or ''}, last_route_debug_seen={row.get('final_reroute_last_route_debug_event_seen') or ''}, planning_msg_snapshot={row.get('final_reroute_planning_msg_count_snapshot') or ''}`",
        f"- reroute sequence: `count={row.get('reroute_decision_row_count') or ''}, reasons={row.get('reroute_reason_sequence') or ''}, reroutes_after_first_planning={row.get('reroute_rows_at_or_after_first_planning_count') or ''}, reroutes_after_first_route_debug={row.get('reroute_rows_at_or_after_first_route_debug_count') or ''}`",
        f"- timing boundary: `adapter_start_done_ts={row.get('adapter_start_done_timestamp') or ''}, scenario_build_done_ts={row.get('scenario_build_done_timestamp') or ''}, final_reroute_ts={row.get('final_reroute_timestamp') or ''}, last_routing_send_ts={row.get('last_routing_send_timestamp') or ''}, last_routing_event_ts={row.get('last_routing_event_timestamp') or ''}, routing_delta={_fmt_sec(row.get('delta_last_routing_event_minus_final_reroute_sec'))}, routing_response_after_send={_fmt_sec(row.get('delta_first_routing_response_minus_last_routing_send_sec'))}, routing_success_response_after_send={_fmt_sec(row.get('delta_first_success_routing_response_minus_last_routing_send_sec'))}, first_planning_ts={row.get('first_planning_timestamp') or ''}, first_route_debug_ts={row.get('first_route_debug_timestamp') or ''}, planning_after_send={_fmt_sec(row.get('delta_first_planning_minus_last_routing_send_sec'))}, planning_after_routing={_fmt_sec(row.get('delta_first_planning_minus_last_routing_event_sec'))}, planning_after_response={_fmt_sec(row.get('delta_first_planning_minus_first_routing_response_after_last_send_sec'))}, planning_after_success_response={_fmt_sec(row.get('delta_first_planning_minus_first_success_routing_response_after_last_send_sec'))}, route_debug_after_send={_fmt_sec(row.get('delta_first_route_debug_minus_last_routing_send_sec'))}, planning_delta={_fmt_sec(row.get('delta_first_planning_minus_final_reroute_sec'))}, route_debug_after_routing={_fmt_sec(row.get('delta_first_route_debug_minus_last_routing_event_sec'))}, route_debug_after_response={_fmt_sec(row.get('delta_first_route_debug_minus_first_routing_response_after_last_send_sec'))}, route_debug_after_success_response={_fmt_sec(row.get('delta_first_route_debug_minus_first_success_routing_response_after_last_send_sec'))}, route_debug_delta={_fmt_sec(row.get('delta_first_route_debug_minus_final_reroute_sec'))}`",
        f"- pre-refresh evidence: `routing_before={row.get('routing_event_before_final_reroute') or ''}, goal_validity_rows={row.get('pre_refresh_goal_validity_count') or ''}, planning_before={row.get('planning_before_final_reroute') or ''}, route_debug_before={row.get('route_debug_before_final_reroute') or ''}, routing_rows={row.get('pre_refresh_routing_event_count') or ''}, planning_rows={row.get('pre_refresh_planning_msg_count') or ''}, route_debug_rows={row.get('pre_refresh_route_debug_msg_count') or ''}`",
        f"- transition-to-refresh upstream materialization: `transition_ts={row.get('transition_reroute_timestamp') or ''}, upstream_route_segment_rows={row.get('transition_to_refresh_upstream_route_segment_debug_count') or ''}, upstream_reference_line_rows={row.get('transition_to_refresh_upstream_reference_line_debug_count') or ''}, upstream_lane_follow_rows={row.get('transition_to_refresh_upstream_lane_follow_debug_count') or ''}, first_upstream_route_segment_ts={row.get('first_transition_to_refresh_upstream_route_segment_debug_timestamp') or ''}, upstream_after_transition={_fmt_sec(row.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_transition_sec'))}, upstream_vs_refresh={_fmt_sec(row.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_final_reroute_sec'))}, upstream_vs_first_response={_fmt_sec(row.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_routing_response_after_last_send_sec'))}, upstream_vs_first_success_response={_fmt_sec(row.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_success_routing_response_after_last_send_sec'))}, first_upstream_route_segment_len={_fmt_len(row.get('first_transition_to_refresh_upstream_route_segment_debug_total_length_m'))}, first_upstream_route_segment_sig={row.get('first_transition_to_refresh_upstream_route_segment_debug_signature') or 'none'}`",
        f"- refresh input contract: `pre_invalid={row.get('final_reroute_pre_fallback_invalid_goal') or ''}, pre_invalid_reason={row.get('final_reroute_pre_fallback_invalid_goal_reason') or ''}, pre_goal_distance={_fmt_len(row.get('final_reroute_pre_fallback_goal_distance_m'))}, pre_ref_count={row.get('final_reroute_pre_fallback_reference_line_count') or ''}, pre_route_segments={row.get('final_reroute_pre_fallback_route_segment_count') or ''}, recent_route_debug_seen={row.get('final_reroute_recent_route_debug_seen') or ''}, recent_route_debug_age={_fmt_sec(row.get('final_reroute_recent_route_debug_age_sec'))}, recent_route_debug_len={_fmt_len(row.get('final_reroute_recent_route_debug_total_length_m'))}, recent_route_debug_sig={row.get('final_reroute_recent_route_debug_signature') or 'none'}, recent_route_debug_provider={row.get('final_reroute_recent_route_debug_provider_status') or ''}, recent_route_debug_lane_follow={row.get('final_reroute_recent_route_debug_lane_follow_map_status') or ''}`",
        f"- goal-validity boundary: `transition_to_refresh_goal_validity_rows={row.get('transition_to_refresh_goal_validity_count') or ''}, last_pre_goal_validity_ts={row.get('last_pre_refresh_goal_validity_timestamp') or ''}, last_pre_goal_validity={row.get('last_pre_refresh_goal_validity_goal_mode') or ''}/{row.get('last_pre_refresh_goal_validity_goal_source') or ''}, last_pre_invalid={row.get('last_pre_refresh_goal_validity_invalid_goal') or ''}({row.get('last_pre_refresh_goal_validity_invalid_goal_reason') or ''}), last_pre_len={_fmt_len(row.get('last_pre_refresh_goal_validity_total_length_m'))}, last_pre_sig={row.get('last_pre_refresh_goal_validity_signature') or 'none'}, last_pre_recent={row.get('last_pre_refresh_goal_validity_recent_route_debug_present') or ''}/{row.get('last_pre_refresh_goal_validity_recent_route_debug_fresh') or ''}, final_goal_validity_ts={row.get('final_goal_validity_timestamp') or ''}, final_goal_validity={row.get('final_goal_validity_goal_mode') or ''}/{row.get('final_goal_validity_goal_source') or ''}, final_invalid={row.get('final_goal_validity_invalid_goal') or ''}({row.get('final_goal_validity_invalid_goal_reason') or ''}), final_len={_fmt_len(row.get('final_goal_validity_total_length_m'))}, final_route_segments={row.get('final_goal_validity_route_segment_count') or ''}, final_ref_count={row.get('final_goal_validity_reference_line_count') or ''}, final_sig={row.get('final_goal_validity_signature') or 'none'}, final_recent={row.get('final_goal_validity_recent_route_debug_present') or ''}/{row.get('final_goal_validity_recent_route_debug_fresh') or ''}, final_lane_follow={row.get('final_goal_validity_lane_follow_map_status') or ''}, final_provider={row.get('final_goal_validity_reference_line_provider_status') or ''}`",
        f"- refresh pivot: `routing_event_rows={row.get('final_routing_event_match_count') or ''}, pre_visible_route_debug_rows={row.get('pre_refresh_visible_route_debug_count') or ''}, last_pre_visible_ts={row.get('last_pre_refresh_visible_route_debug_timestamp') or ''}, last_pre_visible_len={_fmt_len(row.get('last_pre_refresh_visible_route_debug_total_length_m'))}, last_pre_visible_sig={row.get('last_pre_refresh_visible_route_debug_signature') or 'none'}, pre_delta={_fmt_sec(row.get('delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec'))}, post_visible_route_debug_rows={row.get('post_refresh_visible_route_debug_count') or ''}, first_post_visible_ts={row.get('first_post_refresh_visible_route_debug_timestamp') or ''}, first_post_visible_len={_fmt_len(row.get('first_post_refresh_visible_route_debug_total_length_m'))}, first_post_visible_sig={row.get('first_post_refresh_visible_route_debug_signature') or 'none'}, post_delta={_fmt_sec(row.get('delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec'))}`",
        "",
    ]


def _render_report(rows: Sequence[Dict[str, str]], *, generated_at_local: str, source_csv: Path) -> str:
    short_rows = [row for row in rows if row.get("final_branch_family") == "short"]
    long_rows = [row for row in rows if row.get("final_branch_family") == "long"]
    response_short = next((row for row in rows if row.get("label") == RESPONSE_AWARE_SHORT), None)
    response_long = next((row for row in rows if row.get("label") == RESPONSE_AWARE_LONG), None)

    lines: List[str] = [
        "# Calibration Low-Y Refresh Trigger Family Weekend",
        "",
        f"- generated_at_local: `{generated_at_local}`",
        f"- source_csv: `{source_csv}`",
        "",
        "## Timing Table",
        "",
        "| label | candidate | final branch | final reroute | first planning | first route debug | pre-refresh evidence | refresh input | refresh pivot |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row.get('label') or ''}`",
                    f"`{row.get('candidate_signature') or ''}`",
                    f"`{_final_branch(row)}`",
                    f"`{_final_reroute_summary(row)}`",
                    f"`{_first_event_summary(row, ts_key='first_planning_timestamp', delta_key='delta_first_planning_minus_final_reroute_sec', before_key='planning_before_final_reroute')}`",
                    f"`{_first_event_summary(row, ts_key='first_route_debug_timestamp', delta_key='delta_first_route_debug_minus_final_reroute_sec', before_key='route_debug_before_final_reroute')}`",
                    f"`{_pre_refresh_evidence(row)}`",
                    f"`{_refresh_input_summary(row)}`",
                    f"`{_refresh_pivot_summary(row)}`",
                ]
            )
            + " |"
        )

    lines.extend(["", "## Per-Run Read", ""])
    for row in rows:
        lines.extend(_per_run_read(row))

    lines.extend(["## Conclusions", ""])
    if short_rows:
        lines.append(
            "- The short-branch fresh runs share the same boundary shape: "
            + "; ".join(
                f"`{row.get('label')}`: planning_after_routing={_fmt_sec(row.get('delta_first_planning_minus_last_routing_event_sec'))}, route_debug_after_routing={_fmt_sec(row.get('delta_first_route_debug_minus_last_routing_event_sec'))}, pre_refresh_planning={row.get('pre_refresh_planning_msg_count') or '0'}, pre_refresh_route_debug={row.get('pre_refresh_route_debug_msg_count') or '0'}"
                for row in short_rows
            )
            + "."
        )
    if long_rows:
        lines.append(
            "- The long-branch reference runs differ before provider failure, not after it: "
            + "; ".join(
                f"`{row.get('label')}`: planning_after_routing={_fmt_sec(row.get('delta_first_planning_minus_last_routing_event_sec'))}, route_debug_after_routing={_fmt_sec(row.get('delta_first_route_debug_minus_last_routing_event_sec'))}, pre_refresh_planning={row.get('pre_refresh_planning_msg_count') or '0'}, pre_refresh_route_debug={row.get('pre_refresh_route_debug_msg_count') or '0'}"
                for row in long_rows
            )
            + "."
        )
    lines.append(
        "- `routing_event_debug` timing by itself does not explain the current split: "
        + "; ".join(
            f"`{row.get('label')}`: routing_delta={_fmt_sec(row.get('delta_last_routing_event_minus_final_reroute_sec'))}, routing_rows={row.get('pre_refresh_routing_event_count') or '0'}, planning_after_routing={_fmt_sec(row.get('delta_first_planning_minus_last_routing_event_sec'))}"
            for row in rows
        )
        + "."
    )
    lines.append("- That pushes the remaining question one layer deeper: routing publication is already present before final refresh in all compared runs, so the decisive difference is between routing emission and planning/stage5 evidence arrival.")
    lines.append(
        "- In the current truthful corpus, the final `long_phase_refresh` row is a decision/update boundary rather than a new routing send: "
        + "; ".join(
            f"`{row.get('label')}`: routing_request_sent={row.get('final_reroute_routing_request_sent') or 'false'}"
            for row in rows
        )
        + "."
    )
    lines.append("- No compared run emits a matching `long_phase_refresh` row in `routing_event_debug`; the refresh boundary is only visible in `reroute_decision_debug` plus route-debug rows.")
    if response_short and response_long:
        lines.append(
            "- The response-aware low-y pair now makes the post-send window auditable in two stages: "
            f"`{response_short.get('label')}`: response_after_send={_fmt_sec(response_short.get('delta_first_routing_response_minus_last_routing_send_sec'))}, success_response_after_send={_fmt_sec(response_short.get('delta_first_success_routing_response_minus_last_routing_send_sec'))}, planning_after_response={_fmt_sec(response_short.get('delta_first_planning_minus_first_routing_response_after_last_send_sec'))}, route_debug_after_response={_fmt_sec(response_short.get('delta_first_route_debug_minus_first_routing_response_after_last_send_sec'))}; "
            f"`{response_long.get('label')}`: response_after_send={_fmt_sec(response_long.get('delta_first_routing_response_minus_last_routing_send_sec'))}, success_response_after_send={_fmt_sec(response_long.get('delta_first_success_routing_response_minus_last_routing_send_sec'))}, planning_after_response={_fmt_sec(response_long.get('delta_first_planning_minus_first_routing_response_after_last_send_sec'))}, route_debug_after_response={_fmt_sec(response_long.get('delta_first_route_debug_minus_first_routing_response_after_last_send_sec'))}."
        )
        lines.append(
            "- The response-aware low-y pair also shares the same reroute decision sequence, but not the same ordering against evidence arrival: "
            f"`{response_short.get('label')}`: reroutes={response_short.get('reroute_decision_row_count') or ''}, sequence={response_short.get('reroute_reason_sequence') or ''}, reroutes_after_first_planning={response_short.get('reroute_rows_at_or_after_first_planning_count') or ''}, reroutes_after_first_route_debug={response_short.get('reroute_rows_at_or_after_first_route_debug_count') or ''}; "
            f"`{response_long.get('label')}`: reroutes={response_long.get('reroute_decision_row_count') or ''}, sequence={response_long.get('reroute_reason_sequence') or ''}, reroutes_after_first_planning={response_long.get('reroute_rows_at_or_after_first_planning_count') or ''}, reroutes_after_first_route_debug={response_long.get('reroute_rows_at_or_after_first_route_debug_count') or ''}."
        )
        lines.append("- Because the final `long_phase_refresh` row is already the last reroute/update boundary in both response-aware runs, the short branch currently records no later reroute boundary after its first planning/route-debug rows finally appear.")
        lines.append(
            "- The response-aware pair also now exposes a refresh-window pivot rather than only a timing gap: "
            f"`{response_short.get('label')}`: pre_visible={response_short.get('last_pre_refresh_visible_route_debug_signature') or 'none'}@{_fmt_sec(response_short.get('delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec'))}, post_visible={response_short.get('first_post_refresh_visible_route_debug_signature') or 'none'}@{_fmt_sec(response_short.get('delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec'))}, goal_source={response_short.get('final_goal_validity_goal_source') or ''}; "
            f"`{response_long.get('label')}`: pre_visible={response_long.get('last_pre_refresh_visible_route_debug_signature') or 'none'}@{_fmt_sec(response_long.get('delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec'))}, post_visible={response_long.get('first_post_refresh_visible_route_debug_signature') or 'none'}@{_fmt_sec(response_long.get('delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec'))}, goal_source={response_long.get('final_goal_validity_goal_source') or ''}."
        )
        lines.append(
            "- The same pair also shows that recorded routing-response timing is not the full explanation: "
            f"`{response_short.get('label')}`: upstream_vs_first_response={_fmt_sec(response_short.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_routing_response_after_last_send_sec'))}, upstream_vs_first_success_response={_fmt_sec(response_short.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_success_routing_response_after_last_send_sec'))}; "
            f"`{response_long.get('label')}`: upstream_vs_first_response={_fmt_sec(response_long.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_routing_response_after_last_send_sec'))}, upstream_vs_first_success_response={_fmt_sec(response_long.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_success_routing_response_after_last_send_sec'))}."
        )
    lines.append(
        "- The pre-refresh split is not only a stage5 artifact. The same compared family also diverges in upstream route-segment materialization: "
        + "; ".join(
            f"`{row.get('label')}`: upstream_route_rows={row.get('transition_to_refresh_upstream_route_segment_debug_count') or '0'}, upstream_reference_line_rows={row.get('transition_to_refresh_upstream_reference_line_debug_count') or '0'}, upstream_lane_follow_rows={row.get('transition_to_refresh_upstream_lane_follow_debug_count') or '0'}, first_upstream={row.get('first_transition_to_refresh_upstream_route_segment_debug_signature') or 'none'}@{_fmt_sec(row.get('delta_first_transition_to_refresh_upstream_route_segment_debug_minus_transition_sec'))}"
            for row in rows
        )
        + "."
    )
    lines.append(
        "- The refresh input contract is no longer implicit: "
        + "; ".join(
            f"`{row.get('label')}`: pre_invalid={row.get('final_reroute_pre_fallback_invalid_goal') or 'false'}({row.get('final_reroute_pre_fallback_invalid_goal_reason') or ''}), recent={row.get('final_reroute_recent_route_debug_signature') or 'none'}@{_fmt_sec(row.get('final_reroute_recent_route_debug_age_sec'))}, recent_lane_follow={row.get('final_reroute_recent_route_debug_lane_follow_map_status') or ''}"
            for row in rows
        )
        + "."
    )
    lines.append(
        "- The goal-validity row itself now makes the refresh flip explicit: "
        + "; ".join(
            f"`{row.get('label')}`: pre_gv={row.get('last_pre_refresh_goal_validity_goal_mode') or ''}/{row.get('last_pre_refresh_goal_validity_goal_source') or ''}@{_fmt_sec(row.get('delta_last_pre_refresh_goal_validity_minus_final_reroute_sec'))}, final_gv={row.get('final_goal_validity_goal_mode') or ''}/{row.get('final_goal_validity_goal_source') or ''}, final_invalid={row.get('final_goal_validity_invalid_goal') or 'false'}({row.get('final_goal_validity_invalid_goal_reason') or ''}), final_sig={row.get('final_goal_validity_signature') or 'none'}, recent={row.get('final_goal_validity_recent_route_debug_present') or ''}/{row.get('final_goal_validity_recent_route_debug_fresh') or ''}"
            for row in rows
        )
        + "."
    )
    lines.append("- That means the long branch does not pivot to fallback from an otherwise identical refresh boundary. It reaches `long_phase_refresh` with upstream route-segment materialization already in place, while the short branch reaches the same reroute boundary with no upstream route-segment materialization and no recent route-debug snapshot.")
    lines.append("- The strongest current calibration split is now timing on the final `long_phase_refresh` boundary: the short family reaches final refresh with no planning/route-debug evidence yet, while the long family already has planning/route-debug rows before refresh.")
    lines.append("- More specifically, the long response-aware branch holds a short visible window until just before refresh and then pivots almost immediately to a fallback-ahead long window, while the short branch has no pre-refresh window and only later surfaces the scenario-xy long window.")
    lines.append("- This makes `high-y vs low-y` an insufficient explanation on its own. The more predictive question is whether planning/route-debug has already arrived before final refresh, because that is the point where the same truthful candidate family can still diverge into `scenario_xy` short vs fallback-bearing long branches.")
    lines.append("- The next minimal push should target what materializes the upstream route-segment / lane-follow / reference-line debug rows before final refresh on the long branch, because that upstream materialization is now the direct source of the recent route-debug snapshot that separates `scenario_goal_file` retention from `invalid_goal_fallback_ahead` pivot.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the calibration low-y refresh-trigger family report.")
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV), help="Source reroute timing CSV.")
    parser.add_argument("--report", default=str(DEFAULT_REPORT), help="Markdown report path.")
    parser.add_argument("--summary-csv", default=str(DEFAULT_SUMMARY_CSV), help="Filtered CSV output path.")
    parser.add_argument("--label", action="append", default=[], help="Label to include. Defaults to the curated truthful low-y family order.")
    args = parser.parse_args()

    input_csv = Path(args.input_csv).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    summary_csv_path = Path(args.summary_csv).expanduser().resolve()
    labels = tuple(args.label) if args.label else DEFAULT_LABELS

    rows = _load_csv_rows(input_csv)
    selected_rows = _select_rows(rows, labels)
    generated_at_local = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    report = _render_report(selected_rows, generated_at_local=generated_at_local, source_csv=input_csv)
    _write_csv(summary_csv_path, selected_rows)
    _write_text(report_path, report)


if __name__ == "__main__":
    main()
