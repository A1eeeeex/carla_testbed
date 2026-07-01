#!/usr/bin/env python3
"""
Diagnose Apollo Planning empty trajectory issues from run artifacts.

Usage:
    python scripts/diagnose_planning_empty.py <run_dir_or_artifacts_path>

Example:
    python scripts/diagnose_planning_empty.py runs/1782788006__followstop__apollo__ros2-native__carla-town01-route-health__town01

Analyzes:
    - Empty trajectory rate and patterns
    - reference_line / routing field presence in planning debug
    - map_contract validity
    - routing event timeline (startup vs long-phase)
    - unstable reroute guard triggers
    - Planning stall detection (consecutive empty trajectories)
    - Causal chain from map_contract_invalid → empty trajectory
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def find_artifacts_dir(path: Path) -> Optional[Path]:
    """Find the artifacts directory from a run directory or direct path."""
    if path.is_dir():
        artifacts = path / "artifacts"
        if artifacts.is_dir():
            return artifacts
        # Maybe the path itself is the artifacts dir
        if (path / "planning_topic_debug.jsonl").exists():
            return path
    return None


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load a JSONL file."""
    if not path.exists():
        return []
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def analyze_planning_debug(artifacts: Path) -> Dict[str, Any]:
    """Analyze planning_topic_debug.jsonl."""
    path = artifacts / "planning_topic_debug.jsonl"
    events = load_jsonl(path)
    if not events:
        return {"error": f"no data in {path}"}

    empty = [e for e in events if e.get("trajectory_point_count", -1) == 0]
    nonempty = [e for e in events if e.get("trajectory_point_count", -1) > 0]

    # Diagnosis distribution
    diag_counter = Counter(e.get("planning_debug_diagnosis", "?") for e in events)
    diag_empty = Counter(e.get("planning_debug_diagnosis", "?") for e in empty)

    # reference_line_count in non-empty events
    ref_counts_nonempty = [e.get("reference_line_count", 0) for e in nonempty]
    ref_nonzero = sum(1 for r in ref_counts_nonempty if r > 0)

    # routing_segment_count in empty events
    seg_empty = Counter(e.get("routing_segment_count", -1) for e in empty)

    # Empty reason guesses
    reason_counter = Counter(e.get("planning_empty_reason_guess", "?") for e in empty)

    # Sequence number gaps
    seqs = [e.get("planning_header_sequence_num", -1) for e in events]
    gaps = []
    for i in range(1, len(seqs)):
        gap = seqs[i] - seqs[i - 1]
        if gap > 1:
            gaps.append((i, seqs[i - 1], seqs[i], gap))

    # Find first non-empty
    first_nonempty_idx = None
    for i, e in enumerate(events):
        if e.get("trajectory_point_count", 0) > 0:
            first_nonempty_idx = i
            break

    # Transition context
    transition = None
    if first_nonempty_idx is not None:
        start = max(0, first_nonempty_idx - 3)
        end = min(len(events), first_nonempty_idx + 4)
        transition = []
        for j in range(start, end):
            ev = events[j]
            transition.append({
                "index": j,
                "seq": ev.get("planning_header_sequence_num"),
                "pt": ev.get("trajectory_point_count"),
                "diag": ev.get("planning_debug_diagnosis"),
                "ref": ev.get("reference_line_count"),
                "seg": ev.get("routing_segment_count"),
                "ts": ev.get("timestamp"),
                "is_first_nonempty": j == first_nonempty_idx,
            })

    # Timing
    first_ts = events[0].get("timestamp", 0)
    last_ts = events[-1].get("timestamp", 0)

    return {
        "total_messages": len(events),
        "nonempty_messages": len(nonempty),
        "empty_messages": len(empty),
        "empty_ratio": len(empty) / max(len(events), 1),
        "time_span_sec": last_ts - first_ts if first_ts and last_ts else None,
        "diagnosis_distribution": dict(diag_counter.most_common()),
        "empty_diagnosis_distribution": dict(diag_empty.most_common()),
        "reference_line_in_nonempty": {
            "total": len(ref_counts_nonempty),
            "nonzero_count": ref_nonzero,
            "nonzero_ratio": ref_nonzero / max(len(ref_counts_nonempty), 1),
            "max_count": max(ref_counts_nonempty) if ref_counts_nonempty else 0,
        },
        "empty_routing_segment_distribution": dict(seg_empty.most_common()),
        "empty_reason_distribution": dict(reason_counter.most_common()),
        "sequence_gaps": gaps[:20],
        "first_nonempty_transition": transition,
        "point_counts": {
            "max": max(e.get("trajectory_point_count", 0) for e in events),
            "mean": sum(e.get("trajectory_point_count", 0) for e in events) / max(len(events), 1),
        },
    }


def analyze_route_segment_debug(artifacts: Path) -> Dict[str, Any]:
    """Analyze planning_route_segment_debug.jsonl."""
    path = artifacts / "planning_route_segment_debug.jsonl"
    events = load_jsonl(path)
    if not events:
        return {"error": f"no data in {path}"}

    lf_status = Counter(e.get("lane_follow_map_status", "?") for e in events)
    ref_status = Counter(e.get("reference_line_provider_status", "?") for e in events)
    route_status = Counter(e.get("create_route_segments_status", "?") for e in events)
    reason = Counter(e.get("planning_empty_reason_guess", "?") for e in events)
    path_end = Counter(str(e.get("path_end_like_condition", "?")) for e in events)

    # Find first with route_segment_count > 0
    first_seg_idx = None
    for i, e in enumerate(events):
        if e.get("route_segment_count", 0) > 0:
            first_seg_idx = i
            break

    # routing_lane_window changes
    sig_changes = []
    prev_sig = None
    for i, e in enumerate(events):
        sig = e.get("routing_lane_window_signature", "")
        if sig and sig != "none" and sig != prev_sig:
            prev_sig = sig
            sig_changes.append({
                "index": i,
                "ts": e.get("timestamp"),
                "signature": sig[:120],
                "lane_window_count": e.get("routing_lane_window_count"),
            })

    return {
        "total_events": len(events),
        "lane_follow_map_status": dict(lf_status.most_common()),
        "reference_line_provider_status": dict(ref_status.most_common()),
        "create_route_segments_status": dict(route_status.most_common()),
        "empty_reason_distribution": dict(reason.most_common()),
        "path_end_like_condition": dict(path_end.most_common()),
        "first_with_route_segment_index": first_seg_idx,
        "routing_lane_window_changes": sig_changes,
    }


def analyze_routing_events(artifacts: Path) -> List[Dict[str, Any]]:
    """Analyze routing_event_debug.jsonl."""
    path = artifacts / "routing_event_debug.jsonl"
    events = load_jsonl(path)
    summary = []
    for e in events:
        summary.append({
            "ts": e.get("timestamp"),
            "phase": e.get("routing_phase"),
            "request_index": e.get("routing_request_index"),
            "reroute_reason": e.get("reroute_reason"),
            "goal_mode": e.get("goal_mode"),
            "goal_dist_m": e.get("goal_distance_m"),
            "ref_line_count": e.get("reference_line_count"),
            "route_seg_count": e.get("route_segment_count"),
            "lf_map_status": e.get("lane_follow_map_status"),
            "map_contract_invalid": e.get("map_contract_invalid"),
            "routing_skipped_unstable": e.get("routing_skipped_due_to_unstable_reference_line"),
        })
    return summary


def analyze_reroute_decisions(artifacts: Path) -> List[Dict[str, Any]]:
    """Analyze reroute_decision_debug.jsonl."""
    path = artifacts / "reroute_decision_debug.jsonl"
    events = load_jsonl(path)
    summary = []
    for e in events:
        triggers = e.get("trigger_condition_snapshot", {})
        summary.append({
            "ts": e.get("timestamp"),
            "phase": e.get("routing_phase"),
            "reroute_reason": e.get("reroute_reason"),
            "map_contract_invalid": triggers.get("map_contract_invalid"),
            "unstable_guard_active": triggers.get("unstable_long_reroute_guard_active"),
            "unstable_guard_triggers": triggers.get("unstable_long_reroute_guard_triggers"),
            "ref_line_count": triggers.get("reference_line_count"),
            "route_seg_count": triggers.get("route_segment_count"),
            "lf_map_status": triggers.get("lane_follow_map_status"),
        })
    return summary


def analyze_health_summary(artifacts: Path) -> Dict[str, Any]:
    """Analyze bridge_health_summary.json."""
    path = artifacts / "bridge_health_summary.json"
    if not path.exists():
        return {"error": f"{path} not found"}
    try:
        data = json.loads(path.read_text())
    except Exception:
        return {"error": "parse failed"}

    cmd_mat = data.get("command_materialization", {})
    return {
        "transport_mode": data.get("bridge_transport", {}).get("transport_mode"),
        "routing_request_count": data.get("bridge_transport", {}).get("routing_request_count"),
        "routing_response_count": data.get("bridge_transport", {}).get("routing_response_count"),
        "planning_message_count": data.get("bridge_transport", {}).get("planning_message_count"),
        "planning_nonempty_count": data.get("bridge_transport", {}).get("planning_nonempty_trajectory_count"),
        "command_stage": cmd_mat.get("command_stage"),
        "first_blocking_layer": cmd_mat.get("first_blocking_layer"),
        "first_blocking_reason": cmd_mat.get("first_blocking_reason"),
        "last_blocking_reason": cmd_mat.get("last_blocking_reason"),
        "reroute_reason_counts": data.get("bridge_transport", {}).get("reroute_reason_counts"),
        "lateral_guard": data.get("lateral_guard", {}),
    }


def diagnose(artifacts: Path) -> Dict[str, Any]:
    """Run full diagnosis and return structured results."""
    planning = analyze_planning_debug(artifacts)
    route_seg = analyze_route_segment_debug(artifacts)
    routing = analyze_routing_events(artifacts)
    reroute = analyze_reroute_decisions(artifacts)
    health = analyze_health_summary(artifacts)

    # Compute causal chain
    causal_chain = []
    empty_ratio = planning.get("empty_ratio", 0)

    if empty_ratio > 0.3:
        causal_chain.append(f"HIGH empty trajectory rate: {empty_ratio:.1%}")

    # Check map_contract_invalid
    map_invalid = False
    for ev in routing:
        if ev.get("map_contract_invalid"):
            map_invalid = True
            break
    if not map_invalid:
        for ev in reroute:
            if ev.get("map_contract_invalid"):
                map_invalid = True
                break
    if map_invalid:
        causal_chain.append("map_contract_invalid=true → bridge map ≠ runtime map")

    # Check unstable reroute guard
    guard_active = False
    for ev in reroute:
        if ev.get("unstable_guard_active"):
            guard_active = True
            triggers = ev.get("unstable_guard_triggers", [])
            causal_chain.append(f"unstable_long_reroute_guard ACTIVE triggers={triggers}")
            break
    if guard_active:
        causal_chain.append("→ long-phase routing BLOCKED")
        causal_chain.append("→ Apollo only has 30m startup route")
        causal_chain.append("→ Planning reaches end of short reference line")
        causal_chain.append("→ trajectory_point=0 (empty)")
        causal_chain.append("→ Control enters safe hold (brake=15, steer=0)")
        causal_chain.append("→ vehicle drifts → CTE grows → LANE_INVASION")

    # Check routing phases
    phases = [ev.get("phase") for ev in routing if ev.get("phase")]
    if "long" not in phases:
        causal_chain.append("WARNING: No long-phase routing event found")

    ref_nonzero_ratio = planning.get("reference_line_in_nonempty", {}).get("nonzero_ratio", 0)
    if ref_nonzero_ratio == 0 and empty_ratio < 0.9:
        causal_chain.append(
            f"reference_line_nonempty_ratio={ref_nonzero_ratio:.1%} — "
            "debug.planning_data.reference_line is empty even in non-empty trajectories"
        )

    return {
        "planning": planning,
        "route_segment": route_seg,
        "routing_events": routing,
        "reroute_decisions": reroute,
        "health": health,
        "causal_chain": causal_chain,
    }


def print_report(result: Dict[str, Any]) -> None:
    """Print a human-readable diagnosis report."""
    planning = result.get("planning", {})
    route_seg = result.get("route_segment", {})
    routing = result.get("routing_events", [])
    reroute = result.get("reroute_decisions", [])
    health = result.get("health", {})
    causal = result.get("causal_chain", [])

    print("=" * 70)
    print("  Apollo Planning Empty Trajectory Diagnosis Report")
    print("=" * 70)

    # Causal chain
    if causal:
        print("\n>>> CAUSAL CHAIN:")
        for i, step in enumerate(causal, 1):
            print(f"  {i}. {step}")

    # Planning summary
    print("\n>>> PLANNING MESSAGE SUMMARY:")
    print(f"  Total messages:    {planning.get('total_messages', '?')}")
    print(f"  Non-empty:         {planning.get('nonempty_messages', '?')}")
    print(f"  Empty:             {planning.get('empty_messages', '?')}")
    print(f"  Empty ratio:       {planning.get('empty_ratio', 0):.1%}")
    print(f"  Time span:         {planning.get('time_span_sec', '?'):.1f}s" if planning.get('time_span_sec') else "  Time span: N/A")
    print(f"  Max trajectory pts:{planning.get('point_counts', {}).get('max', '?')}")
    print(f"  Mean trajectory pts:{planning.get('point_counts', {}).get('mean', 0):.1f}")

    # Reference line in non-empty
    ref_info = planning.get("reference_line_in_nonempty", {})
    print(f"\n>>> REFERENCE LINE IN NON-EMPTY TRAJECTORIES:")
    print(f"  Non-empty msgs:    {ref_info.get('total', '?')}")
    print(f"  With ref_line > 0: {ref_info.get('nonzero_count', '?')}")
    print(f"  Nonzero ratio:     {ref_info.get('nonzero_ratio', 0):.1%}")

    # Diagnosis distribution
    diag = planning.get("diagnosis_distribution", {})
    if diag:
        print(f"\n>>> DIAGNOSIS DISTRIBUTION:")
        for d, c in sorted(diag.items(), key=lambda x: -x[1])[:5]:
            pct = c / max(planning.get('total_messages', 1), 1) * 100
            print(f"  {d}: {c} ({pct:.1f}%)")

    # Empty reason
    reasons = planning.get("empty_reason_distribution", {})
    if reasons:
        print(f"\n>>> EMPTY TRAJECTORY REASONS:")
        for r, c in sorted(reasons.items(), key=lambda x: -x[1])[:5]:
            print(f"  {r}: {c}")

    # Route segment debug
    lf_status = route_seg.get("lane_follow_map_status", {})
    if lf_status:
        print(f"\n>>> LANE FOLLOW MAP STATUS:")
        for s, c in sorted(lf_status.items(), key=lambda x: -x[1]):
            pct = c / max(route_seg.get('total_events', 1), 1) * 100
            print(f"  {s}: {c} ({pct:.1f}%)")

    # Routing events
    if routing:
        print(f"\n>>> ROUTING EVENTS ({len(routing)}):")
        for ev in routing:
            skip_flag = " [SKIPPED]" if ev.get("routing_skipped_unstable") else ""
            print(f"  ts={ev.get('ts', '?'):.2f} phase={ev.get('phase')} "
                  f"reason={ev.get('reroute_reason')} goal={ev.get('goal_mode')} "
                  f"dist={ev.get('goal_dist_m', 0):.1f}m "
                  f"map_invalid={ev.get('map_contract_invalid')}{skip_flag}")

    # Reroute decisions
    if reroute:
        print(f"\n>>> REROUTE DECISIONS ({len(reroute)}):")
        for ev in reroute:
            triggers = ev.get("unstable_guard_triggers", [])
            guard = "ACTIVE" if ev.get("unstable_guard_active") else "inactive"
            print(f"  ts={ev.get('ts', '?'):.2f} phase={ev.get('phase')} "
                  f"reason={ev.get('reroute_reason')} guard={guard}")
            if triggers:
                for t in triggers:
                    print(f"    trigger: {t}")

    # Health
    if health and "error" not in health:
        print(f"\n>>> HEALTH SUMMARY:")
        print(f"  Transport mode:    {health.get('transport_mode')}")
        print(f"  Command stage:     {health.get('command_stage')}")
        print(f"  First blocker:     {health.get('first_blocking_layer')}:{health.get('first_blocking_reason')}")
        print(f"  Last blocker:      {health.get('last_blocking_reason')}")
        reroute_counts = health.get("reroute_reason_counts", {})
        if reroute_counts:
            print(f"  Reroute reasons:")
            for r, c in sorted(reroute_counts.items(), key=lambda x: -x[1]):
                print(f"    {r}: {c}")

    # Transition
    transition = planning.get("first_nonempty_transition")
    if transition:
        print(f"\n>>> FIRST NON-EMPTY TRANSITION:")
        for t in transition:
            marker = ">>>" if t.get("is_first_nonempty") else "   "
            print(f"  {marker} idx={t['index']} seq={t['seq']} pt={t['pt']} "
                  f"ref={t['ref']} seg={t['seg']} diag={t['diag']}")

    # Routing lane window changes
    sig_changes = route_seg.get("routing_lane_window_changes", [])
    if sig_changes:
        print(f"\n>>> ROUTING LANE WINDOW CHANGES:")
        for sc in sig_changes:
            print(f"  idx={sc['index']} ts={sc.get('ts', 0):.2f} lw={sc['lane_window_count']}")
            print(f"    sig={sc['signature']}")

    print("\n" + "=" * 70)
    print("  Diagnosis complete.")
    print("=" * 70)


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <run_dir_or_artifacts_path>")
        print(f"Example: {sys.argv[0]} runs/1782788006__followstop__apollo__ros2-native__carla-town01-route-health__town01")
        sys.exit(1)

    path = Path(sys.argv[1])
    artifacts = find_artifacts_dir(path)
    if artifacts is None:
        print(f"ERROR: Cannot find artifacts in {path}")
        sys.exit(1)

    print(f"Analyzing artifacts in: {artifacts}")
    result = diagnose(artifacts)
    print_report(result)

    # Optionally output JSON
    if "--json" in sys.argv:
        print("\n--- JSON OUTPUT ---")
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
