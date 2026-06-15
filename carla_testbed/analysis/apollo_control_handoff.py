from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION = "apollo_control_handoff.v1"

FAILURE_STAGE_PRIORITY = (
    "process_health",
    "input_readiness",
    "control_channel",
    "bridge_receive",
    "raw_decode",
    "mapping_and_apply",
    "vehicle_response",
)

CRASH_SIGNATURES: tuple[tuple[str, str], ...] = (
    ("tcmalloc_invalid_free", r"tcmalloc.*invalid free|invalid free"),
    ("segfault", r"SIGSEGV|segmentation fault"),
    ("abort", r"SIGABRT|\babort(?:ed)?\b|core dumped"),
    ("protobuf_error", r"protobuf.*descriptor|descriptor.*protobuf"),
    ("dag_load_failure", r"failed to load DAG|dag load"),
    ("module_exited", r"module exited|component exited|process exited"),
    ("unknown", r"cannot create reader|cannot create writer"),
)

RAW_FIELDS = (
    "apollo_steer_raw",
    "apollo_raw_steer",
    "raw_steer",
    "steering_target",
    "throttle_raw",
    "raw_throttle",
    "brake_raw",
    "raw_brake",
)
MAPPED_FIELDS = (
    "bridge_steer_mapped",
    "mapped_steer",
    "throttle_mapped",
    "brake_mapped",
    "mapped_carla_steer_cmd",
    "mapped_throttle_cmd",
    "mapped_brake_cmd",
    "commanded_steer",
    "commanded_throttle",
    "commanded_brake",
)
APPLIED_FIELDS = ("carla_steer_applied", "applied_steer", "throttle_applied", "brake_applied")


def analyze_apollo_control_handoff(
    *,
    run_dir: str | Path | None = None,
    summary: str | Path | None = None,
    manifest: str | Path | None = None,
    timeseries: str | Path | None = None,
    cyber_bridge_stats: str | Path | None = None,
    planning_summary: str | Path | None = None,
    control_health: str | Path | None = None,
    control_attribution: str | Path | None = None,
    control_logs_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser() if run_dir else None
    paths = _resolve_inputs(
        root=root,
        summary=summary,
        manifest=manifest,
        timeseries=timeseries,
        cyber_bridge_stats=cyber_bridge_stats,
        planning_summary=planning_summary,
        control_health=control_health,
        control_attribution=control_attribution,
        control_logs_dir=control_logs_dir,
    )
    summary_payload = _read_json(paths["summary"])
    manifest_payload = _read_json(paths["manifest"])
    cyber_stats = _read_json(paths["cyber_bridge_stats"])
    planning_payload = _read_json(paths["planning_summary"])
    channel_stats = _read_json(paths["channel_stats"])
    control_health_payload = _read_json(paths["control_health"])
    control_attribution_payload = _read_json(paths["control_attribution"])
    control_handoff_debug = _read_json(paths["control_handoff_summary"])
    control_survival = _read_json(paths["control_survival"])
    rows = _read_rows(paths["timeseries"])
    decode_rows = _read_jsonl(paths["control_decode_debug"])
    apply_rows = _read_jsonl(paths["direct_control_apply"])
    logs = _read_logs(paths["control_logs"])

    missing_inputs = _missing_inputs(paths)
    source = {key: _path_text(value) for key, value in paths.items() if key != "control_logs"}
    source["control_logs"] = [_path_text(path) for path in paths["control_logs"]]

    process_health = _process_health(
        summary=summary_payload,
        control_handoff_debug=control_handoff_debug,
        control_survival=control_survival,
        logs=logs,
        log_paths=paths["control_logs"],
    )
    input_readiness = _input_readiness(
        summary=summary_payload,
        cyber_bridge_stats=cyber_stats,
        planning_summary=planning_payload,
        channel_stats=channel_stats,
        control_handoff_debug=control_handoff_debug,
        rows=rows,
    )
    control_channel = _control_channel(
        summary=summary_payload,
        cyber_bridge_stats=cyber_stats,
        channel_stats=channel_stats,
        decode_rows=decode_rows,
        rows=rows,
        upstream_ok=process_health["status"] in {"pass", "warn"}
        and input_readiness["status"] in {"pass", "warn"},
    )
    bridge_receive = _bridge_receive(
        cyber_bridge_stats=cyber_stats,
        decode_rows=decode_rows,
        control_channel=control_channel,
    )
    raw_decode = _raw_decode(decode_rows=decode_rows, rows=rows, bridge_receive=bridge_receive)
    mapping_and_apply = _mapping_and_apply(
        cyber_bridge_stats=cyber_stats,
        rows=rows,
        decode_rows=decode_rows,
        apply_rows=apply_rows,
        control_health=control_health_payload,
        control_attribution=control_attribution_payload,
        raw_decode=raw_decode,
    )
    vehicle_response = _vehicle_response(rows=rows, mapping_and_apply=mapping_and_apply)

    sections = {
        "process_health": process_health,
        "input_readiness": input_readiness,
        "control_channel": control_channel,
        "bridge_receive": bridge_receive,
        "raw_decode": raw_decode,
        "mapping_and_apply": mapping_and_apply,
        "vehicle_response": vehicle_response,
    }
    failure_stage, blocking_reasons, warnings = _verdict_sections(sections)
    warnings.extend(_mark_downstream_insufficient(sections, failure_stage))
    missing_fields = sorted(
        {
            str(field)
            for section in sections.values()
            for field in (section.get("missing_fields") or [])
            if field
        }
    )
    if failure_stage == "none":
        verdict = "warn" if warnings else "pass"
    elif failure_stage == "insufficient_data":
        verdict = "insufficient_data"
    else:
        verdict = "fail"

    return {
        "schema_version": APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION,
        "run_id": _first_text(summary_payload, "run_id", manifest_payload, "run_id", default=root.name if root else None),
        "route_id": _first_text(summary_payload, "route_id", manifest_payload, "route_id"),
        "scenario_class": _first_text(summary_payload, "scenario_class", manifest_payload, "scenario_class"),
        "backend": _first_text(summary_payload, "backend", manifest_payload, "backend"),
        "transport_mode": _first_text(summary_payload, "transport_mode", manifest_payload, "transport_mode"),
        "evidence_level": _evidence_level(channel_stats=channel_stats, cyber_bridge_stats=cyber_stats, decode_rows=decode_rows),
        "process_health": process_health,
        "input_readiness": input_readiness,
        "control_channel": control_channel,
        "bridge_receive": bridge_receive,
        "raw_decode": raw_decode,
        "mapping_and_apply": mapping_and_apply,
        "vehicle_response": vehicle_response,
        "failure_stage": failure_stage,
        "blocking_reasons": blocking_reasons,
        "warnings": sorted(set(warnings)),
        "missing_inputs": missing_inputs,
        "missing_fields": missing_fields,
        "status": verdict,
        "verdict": verdict,
        "source": source,
        "interpretation_boundary": (
            "This report localizes the control handoff evidence chain. A source-control "
            "semantics breakpoint does not by itself prove Apollo algorithm limitation; "
            "route/reference-line and matched/target semantics still need separate evidence."
        ),
    }


def write_apollo_control_handoff_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    report_path = out / "apollo_control_handoff_report.json"
    summary_path = out / "apollo_control_handoff_summary.md"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {
        "apollo_control_handoff_report": str(report_path),
        "apollo_control_handoff_summary": str(summary_path),
    }


def analyze_and_write_apollo_control_handoff(
    *,
    out_dir: str | Path,
    run_dir: str | Path | None = None,
    summary: str | Path | None = None,
    manifest: str | Path | None = None,
    timeseries: str | Path | None = None,
    cyber_bridge_stats: str | Path | None = None,
    planning_summary: str | Path | None = None,
    control_health: str | Path | None = None,
    control_attribution: str | Path | None = None,
    control_logs_dir: str | Path | None = None,
) -> dict[str, str]:
    report = analyze_apollo_control_handoff(
        run_dir=run_dir,
        summary=summary,
        manifest=manifest,
        timeseries=timeseries,
        cyber_bridge_stats=cyber_bridge_stats,
        planning_summary=planning_summary,
        control_health=control_health,
        control_attribution=control_attribution,
        control_logs_dir=control_logs_dir,
    )
    return write_apollo_control_handoff_report(report, out_dir)


def ensure_apollo_control_handoff_report(
    run_dir: str | Path,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    report_path = root / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"
    existing = _given_or_find(
        root,
        None,
        [
            "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
            "apollo_control_handoff_report.json",
        ],
    )
    if existing is not None and not refresh:
        report = _read_json(existing)
        if not (
            _has_control_handoff_regeneration_inputs(root)
            and _control_handoff_report_needs_regeneration(report)
        ):
            return {
                "status": "existing",
                "path": str(existing),
                "report_status": report.get("verdict"),
                "failure_stage": report.get("failure_stage"),
            }
    if existing is not None and refresh and not _has_control_handoff_regeneration_inputs(root):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        if existing != report_path:
            report_path.write_text(existing.read_text(encoding="utf-8"), encoding="utf-8")
        report = _read_json(report_path)
        return {
            "status": "existing_report_copied",
            "path": str(report_path),
            "report_status": report.get("verdict"),
            "failure_stage": report.get("failure_stage"),
            "source_report": str(existing),
        }
    report = analyze_apollo_control_handoff(run_dir=root)
    outputs = write_apollo_control_handoff_report(report, report_path.parent)
    return {
        "status": "generated",
        "path": outputs["apollo_control_handoff_report"],
        "summary_path": outputs["apollo_control_handoff_summary"],
        "report_status": report.get("verdict"),
        "failure_stage": report.get("failure_stage"),
    }


def ensure_apollo_control_handoff_reports_for_root(
    suite_or_run_root: str | Path,
    *,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    root = Path(suite_or_run_root).expanduser()
    run_dirs = _discover_run_dirs(root)
    return [
        {
            "run_dir": str(run_dir),
            **ensure_apollo_control_handoff_report(run_dir, refresh=refresh),
        }
        for run_dir in run_dirs
    ]


def _discover_run_dirs(root: Path) -> list[Path]:
    if (root / "summary.json").exists() or (root / "manifest.json").exists():
        return [root]
    if not root.exists():
        return []
    return sorted(
        {
            path.parent
            for path in root.rglob("summary.json")
            if path.is_file() and "analysis" not in path.parts
        }
    )


def _has_control_handoff_regeneration_inputs(root: Path) -> bool:
    raw_evidence_patterns = (
        "artifacts/cyber_bridge_stats.json",
        "cyber_bridge_stats.json",
        "artifacts/channel_stats.json",
        "channel_stats.json",
        "artifacts/control_handoff_summary.json",
        "control_handoff_summary.json",
        "artifacts/apollo_control_deferred_survival.json",
        "apollo_control_deferred_survival.json",
        "artifacts/planning_topic_debug_summary.json",
        "planning_topic_debug_summary.json",
        "artifacts/control_decode_debug.jsonl",
        "artifacts/bridge_control_decode.jsonl",
        "artifacts/direct_bridge_control_apply.jsonl",
    )
    if any((root / pattern).exists() for pattern in raw_evidence_patterns):
        return True
    return any(_control_log_paths(root))


def _control_handoff_report_needs_regeneration(report: Mapping[str, Any]) -> bool:
    if report.get("schema_version") != APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION:
        return True
    status = str(report.get("verdict") or report.get("status") or "").strip().lower()
    if status == "fail":
        return False
    blockers = {str(item) for item in report.get("blocking_reasons") or [] if item}
    runtime_missing = "control_runtime_messages_missing" in blockers
    no_control_channel = not isinstance(report.get("control_channel"), Mapping) or _nested_number(
        report, "control_channel", "message_count"
    ) is None
    no_bridge_receive = not isinstance(report.get("bridge_receive"), Mapping) or _nested_number(
        report, "bridge_receive", "control_rx_count"
    ) is None
    no_apply = not isinstance(report.get("mapping_and_apply"), Mapping) or _nested_number(
        report, "mapping_and_apply", "apply_control_count"
    ) is None
    return status == "insufficient_data" and (
        runtime_missing or no_control_channel or no_bridge_receive or no_apply
    )


def _resolve_inputs(
    *,
    root: Path | None,
    summary: str | Path | None,
    manifest: str | Path | None,
    timeseries: str | Path | None,
    cyber_bridge_stats: str | Path | None,
    planning_summary: str | Path | None,
    control_health: str | Path | None,
    control_attribution: str | Path | None,
    control_logs_dir: str | Path | None,
) -> dict[str, Any]:
    logs_root = Path(control_logs_dir).expanduser() if control_logs_dir else root
    return {
        "summary": _given_or_find(root, summary, ["summary.json"]),
        "manifest": _given_or_find(root, manifest, ["manifest.json"]),
        "timeseries": _given_or_find(root, timeseries, ["timeseries.csv", "timeseries.jsonl"]),
        "cyber_bridge_stats": _given_or_find(
            root,
            cyber_bridge_stats,
            ["artifacts/cyber_bridge_stats.json", "cyber_bridge_stats.json"],
        ),
        "planning_summary": _given_or_find(
            root,
            planning_summary,
            ["artifacts/planning_topic_debug_summary.json", "planning_topic_debug_summary.json"],
        ),
        "channel_stats": _given_or_find(root, None, ["artifacts/channel_stats.json", "channel_stats.json"]),
        "control_health": _given_or_find(
            root,
            control_health,
            ["analysis/control_health/control_health_report.json", "control_health_report.json"],
        ),
        "control_attribution": _given_or_find(
            root,
            control_attribution,
            ["analysis/control_attribution/control_attribution_report.json", "control_attribution_report.json"],
        ),
        "control_handoff_summary": _given_or_find(
            root,
            None,
            ["artifacts/control_handoff_summary.json", "control_handoff_summary.json"],
        ),
        "control_survival": _given_or_find(
            root,
            None,
            [
                "artifacts/apollo_control_deferred_survival.json",
                "apollo_control_deferred_survival.json",
            ],
        ),
        "control_decode_debug": _given_or_find(
            root,
            None,
            [
                "artifacts/control_decode_debug.jsonl",
                "artifacts/bridge_control_decode.jsonl",
                "control_decode_debug.jsonl",
                "bridge_control_decode.jsonl",
            ],
        ),
        "direct_control_apply": _given_or_find(
            root,
            None,
            ["artifacts/direct_bridge_control_apply.jsonl", "direct_bridge_control_apply.jsonl"],
        ),
        "control_logs": _control_log_paths(logs_root),
    }


def _process_health(
    *,
    summary: Mapping[str, Any],
    control_handoff_debug: Mapping[str, Any],
    control_survival: Mapping[str, Any],
    logs: Sequence[str],
    log_paths: Sequence[Path],
) -> dict[str, Any]:
    evidence_available = bool(summary or control_handoff_debug or control_survival or logs or log_paths)
    log_text = "\n".join(logs)
    crash_reason, crash_signature = _detect_crash(log_text)
    debug_crash = _first_text(control_handoff_debug, "control_crash_reason", summary, "control_crash_reason")
    if debug_crash and debug_crash not in {"none", "null"}:
        crash_reason = str(debug_crash)
        crash_signature = crash_signature or str(debug_crash)
    started = _first_bool(
        control_handoff_debug.get("control_process_started"),
        control_handoff_debug.get("process_started"),
        control_handoff_debug.get("control_started_pid_seen"),
        control_survival.get("control_started_pid_seen"),
        control_survival.get("control_present_after_first_nonzero_planning"),
        summary.get("control_process_started"),
    )
    pid = _first_number(control_handoff_debug.get("pid"), control_handoff_debug.get("control_pid"), summary.get("control_pid"))
    if started is None and pid is not None:
        started = True
    alive_after_5s = _first_bool(
        control_handoff_debug.get("alive_after_5s"),
        control_handoff_debug.get("control_survived_5s"),
        control_survival.get("control_survived_5s"),
        control_survival.get("control_survived_10s"),
        summary.get("control_alive_after_5s"),
    )
    alive_at_end = _first_bool(
        control_handoff_debug.get("alive_at_end"),
        control_handoff_debug.get("control_final_process_alive"),
        control_survival.get("control_present_at_end"),
        summary.get("control_alive_at_end"),
    )
    exit_code = _first_number(
        control_handoff_debug.get("exit_code"),
        control_handoff_debug.get("control_exit_code"),
        summary.get("control_exit_code"),
    )
    crash_detected = bool(crash_reason and crash_reason != "unknown") or _first_bool(
        control_handoff_debug.get("crash_detected"),
        control_handoff_debug.get("control_crash_detected"),
        summary.get("control_crash_detected"),
    ) is True
    core_dump_detected = "core dumped" in log_text.lower() or _first_bool(
        control_handoff_debug.get("core_dump_detected"),
        summary.get("core_dump_detected"),
    ) is True
    missing_fields: list[str] = []
    if not evidence_available:
        status = "insufficient_data"
        missing_fields.append("process_health.evidence")
    elif started is False:
        status = "fail"
    elif crash_detected:
        status = "fail"
    elif exit_code is not None and exit_code != 0:
        status = "fail"
        crash_reason = crash_reason or "module_exited"
    elif alive_after_5s is False or alive_at_end is False:
        status = "fail"
        crash_reason = crash_reason or "module_exited"
    elif started is None:
        status = "insufficient_data"
        missing_fields.append("process_health.started")
    else:
        status = "pass"
    return {
        "expected": True,
        "started": started,
        "pid": int(pid) if pid is not None else None,
        "alive_after_5s": alive_after_5s,
        "alive_at_end": alive_at_end,
        "exit_code": int(exit_code) if exit_code is not None else None,
        "crash_detected": crash_detected,
        "crash_reason": crash_reason,
        "crash_signature": crash_signature,
        "core_dump_detected": core_dump_detected,
        "survival_probe_available": bool(control_survival),
        "survival_probe_window_sec": _num(control_survival.get("probe_window_sec")),
        "log_paths": [str(path) for path in log_paths],
        "log_tail": _tail_lines(logs),
        "missing_fields": missing_fields,
        "status": status,
    }


def _input_readiness(
    *,
    summary: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
    planning_summary: Mapping[str, Any],
    channel_stats: Mapping[str, Any],
    control_handoff_debug: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    control_handoff_inputs = control_handoff_debug.get("inputs")
    if not isinstance(control_handoff_inputs, Mapping):
        control_handoff_inputs = {}
    planning_channel = _channel(channel_stats, "/apollo/planning")
    localization_channel = _channel(channel_stats, "/apollo/localization/pose")
    chassis_channel = _channel(channel_stats, "/apollo/canbus/chassis")
    planning_count = _first_number(
        planning_summary.get("message_count"),
        planning_summary.get("planning_message_count"),
        control_handoff_inputs.get("planning_message_count"),
        control_handoff_debug.get("planning_message_count"),
        summary.get("planning_message_count"),
        _channel_count(planning_channel),
    )
    nonempty = _first_number(
        planning_summary.get("nonempty_trajectory_count"),
        planning_summary.get("nonempty_count"),
        control_handoff_inputs.get("planning_nonempty_count"),
        summary.get("planning_nonempty_count"),
    )
    if nonempty is None and _first_bool(summary.get("planning_materialized")) is True:
        nonempty = 1.0
    localization_count = _first_number(
        cyber_bridge_stats.get("loc_count"),
        cyber_bridge_stats.get("localization_count"),
        _channel_count(localization_channel),
        _row_available_count(rows, "localization_timestamp", "ego_x"),
    )
    chassis_count = _first_number(
        cyber_bridge_stats.get("chassis_count"),
        _channel_count(chassis_channel),
        _row_available_count(rows, "chassis_timestamp", "chassis_available"),
    )
    localization_hz = _first_number(cyber_bridge_stats.get("loc_hz"), _channel_hz(localization_channel))
    chassis_hz = _first_number(cyber_bridge_stats.get("chassis_hz"), _channel_hz(chassis_channel))
    pad_required = _first_bool(control_handoff_debug.get("control_pad_required"), summary.get("control_pad_required"))
    pad_enabled = _first_bool(control_handoff_debug.get("control_pad_enabled"), summary.get("control_pad_enabled"))
    missing_fields: list[str] = []
    warnings: list[str] = []
    reasons: list[str] = []
    if planning_count is None:
        missing_fields.append("input_readiness.planning_message_count")
    elif planning_count <= 0:
        reasons.append("planning_missing")
    if nonempty is None:
        warnings.append("planning_nonempty_count_unknown")
    elif nonempty <= 0:
        reasons.append("planning_empty")
    if localization_count is None:
        missing_fields.append("input_readiness.localization_count")
    elif localization_count <= 0:
        reasons.append("localization_missing")
    if chassis_count is None:
        missing_fields.append("input_readiness.chassis_count")
    elif chassis_count <= 0:
        reasons.append("chassis_missing")
    if pad_required is True and pad_enabled is False:
        reasons.append("control_pad_missing")
    elif pad_required is None:
        warnings.append("control_pad_requirement_unknown")
    if reasons:
        status = "fail"
    elif missing_fields and not (planning_count or localization_count or chassis_count):
        status = "insufficient_data"
    else:
        status = "warn" if warnings else "pass"
    return {
        "planning_available": bool(planning_count and planning_count > 0),
        "planning_message_count": int(planning_count) if planning_count is not None else None,
        "planning_nonempty_count": int(nonempty) if nonempty is not None else None,
        "localization_count": int(localization_count) if localization_count is not None else None,
        "localization_hz": localization_hz,
        "chassis_count": int(chassis_count) if chassis_count is not None else None,
        "chassis_hz": chassis_hz,
        "control_pad_required": pad_required,
        "control_pad_enabled": pad_enabled,
        "failure_reasons": reasons,
        "warnings": warnings,
        "missing_fields": missing_fields,
        "status": status,
    }


def _control_channel(
    *,
    summary: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
    channel_stats: Mapping[str, Any],
    decode_rows: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    upstream_ok: bool,
) -> dict[str, Any]:
    channel = _channel(channel_stats, "/apollo/control")
    count = _first_number(
        _channel_count(channel),
        cyber_bridge_stats.get("control_rx_count"),
        summary.get("apollo_control_raw_row_count"),
        summary.get("control_consume_row_count"),
        len(decode_rows) if decode_rows else None,
        _row_available_count(rows, "apollo_steer_raw", "throttle_raw", "brake_raw"),
    )
    hz = _first_number(_channel_hz(channel), cyber_bridge_stats.get("control_rx_hz"))
    source = "channel_stats" if channel else ("bridge_stats" if cyber_bridge_stats else ("decode_debug" if decode_rows else None))
    sample = _first_raw_sample(decode_rows, rows)
    missing_fields: list[str] = []
    if count is None:
        missing_fields.append("control_channel.message_count")
        status = "insufficient_data"
    elif count <= 0 and upstream_ok:
        status = "fail"
    elif count <= 0:
        status = "insufficient_data"
    else:
        status = "pass"
    return {
        "name": "/apollo/control",
        "message_count": int(count) if count is not None else None,
        "hz": hz,
        "first_timestamp": channel.get("first_timestamp") if channel else None,
        "last_timestamp": channel.get("last_timestamp") if channel else None,
        "max_gap_ms": channel.get("max_gap_ms") if channel else None,
        "timestamp_monotonic": channel.get("timestamp_monotonic") if channel else None,
        "sequence_monotonic": channel.get("sequence_monotonic") if channel else None,
        "raw_sample": sample,
        "source": source,
        "missing_fields": missing_fields,
        "status": status,
    }


def _bridge_receive(
    *,
    cyber_bridge_stats: Mapping[str, Any],
    decode_rows: Sequence[Mapping[str, Any]],
    control_channel: Mapping[str, Any],
) -> dict[str, Any]:
    rx_count = _first_number(
        cyber_bridge_stats.get("control_rx_count"),
        len(decode_rows) if decode_rows else None,
    )
    channel_count = _num(control_channel.get("message_count"))
    missing_fields: list[str] = []
    if rx_count is None:
        missing_fields.append("bridge_receive.control_rx_count")
        status = "insufficient_data"
    elif channel_count is not None and channel_count > 0 and rx_count <= 0:
        status = "fail"
    elif rx_count <= 0:
        status = "insufficient_data"
    else:
        status = "pass"
    return {
        "control_rx_count": int(rx_count) if rx_count is not None else None,
        "rx_hz": _num(cyber_bridge_stats.get("control_rx_hz")),
        "first_control_rx_time_s": _num(cyber_bridge_stats.get("first_control_rx_time_s")),
        "last_control_rx_time_s": _num(cyber_bridge_stats.get("last_control_rx_time_s")),
        "raw_fields_seen": sorted(_fields_seen(decode_rows, RAW_FIELDS)),
        "missing_fields": missing_fields,
        "status": status,
    }


def _raw_decode(
    *,
    decode_rows: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    bridge_receive: Mapping[str, Any],
) -> dict[str, Any]:
    fields = _fields_seen(decode_rows, RAW_FIELDS) | _row_fields_seen(rows, RAW_FIELDS)
    raw_available = bool(fields)
    steer_available = bool(_fields_seen(decode_rows, ("apollo_steer_raw", "raw_steer", "steering_target")) | _row_fields_seen(rows, ("apollo_steer_raw", "apollo_raw_steer", "raw_steer")))
    throttle_available = bool(_fields_seen(decode_rows, ("throttle_raw", "raw_throttle", "throttle")) | _row_fields_seen(rows, ("throttle_raw",)))
    brake_available = bool(_fields_seen(decode_rows, ("brake_raw", "raw_brake", "brake")) | _row_fields_seen(rows, ("brake_raw",)))
    rx_count = _num(bridge_receive.get("control_rx_count"))
    missing_fields: list[str] = []
    if not raw_available:
        missing_fields.append("raw_decode.raw_control")
    if rx_count and rx_count > 0 and not raw_available:
        status = "fail"
    elif not raw_available:
        status = "insufficient_data"
    else:
        status = "pass"
    return {
        "raw_control_available": raw_available,
        "apollo_steer_raw_available": steer_available,
        "throttle_raw_available": throttle_available,
        "brake_raw_available": brake_available,
        "field_names": sorted(fields),
        "message_schema": "dict_or_timeseries",
        "missing_fields": missing_fields,
        "status": status,
    }


def _mapping_and_apply(
    *,
    cyber_bridge_stats: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    decode_rows: Sequence[Mapping[str, Any]],
    apply_rows: Sequence[Mapping[str, Any]],
    control_health: Mapping[str, Any],
    control_attribution: Mapping[str, Any],
    raw_decode: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = control_health.get("metrics")
    if not isinstance(metrics, Mapping):
        metrics = {}
    bridge_log = metrics.get("control_bridge_log")
    if not isinstance(bridge_log, Mapping):
        bridge_log = {}
    attribution = control_attribution.get("attribution")
    if not isinstance(attribution, Mapping):
        attribution = {}
    mapped_fields = _row_fields_seen(rows, MAPPED_FIELDS) | _fields_seen(decode_rows, MAPPED_FIELDS)
    applied_fields = _row_fields_seen(rows, APPLIED_FIELDS) | _fields_seen(apply_rows, APPLIED_FIELDS)
    timeseries_mapped_count = _nonzero_row_count(rows, "bridge_steer_mapped", "throttle_mapped", "brake_mapped")
    decoded_mapped_count = _first_number(
        _nested_number(metrics, "control_decode_debug", "nonzero_mapped_control_frames"),
        _nonzero_flat_row_count(
            decode_rows,
            "bridge_steer_mapped",
            "mapped_steer",
            "mapped_carla_steer_cmd",
            "commanded_steer",
            "throttle_mapped",
            "mapped_throttle_cmd",
            "commanded_throttle",
            "brake_mapped",
            "mapped_brake_cmd",
            "commanded_brake",
        ),
    )
    mapped_count = timeseries_mapped_count
    mapped_count_source = "timeseries" if timeseries_mapped_count is not None else None
    if (mapped_count is None or mapped_count <= 0) and decoded_mapped_count is not None:
        mapped_count = decoded_mapped_count
        mapped_count_source = "control_decode_debug.jsonl"
        if not mapped_fields:
            mapped_fields.add("control_decode_debug.mapped_control")
    applied_count = _first_number(
        cyber_bridge_stats.get("control_tx_count"),
        bridge_log.get("final_applied_count"),
        len(apply_rows) if apply_rows else None,
        _nonzero_row_count(rows, "carla_steer_applied", "throttle_applied", "brake_applied"),
    )
    mapped_applied_steer_error = _first_number(
        metrics.get("mapped_applied_steer_abs_error_p95"),
        _nested_number(attribution, "mapped_to_applied_steer_consistency", "abs_error_p95"),
    )
    missing_fields: list[str] = []
    warnings: list[str] = []
    if not mapped_fields:
        missing_fields.append("mapping_and_apply.mapped_control")
    if not applied_fields:
        missing_fields.append("mapping_and_apply.applied_control")
    if _num(mapped_applied_steer_error) is not None and _num(mapped_applied_steer_error) > 0.20:
        warnings.append("mapped_applied_steer_mismatch")
    if raw_decode.get("status") not in {"pass", "warn"}:
        status = "insufficient_data"
    elif mapped_count and mapped_count > 0 and (applied_count is None or applied_count <= 0):
        status = "fail"
    elif missing_fields:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"
    return {
        "actuator_mapping_mode": _first_text(control_health, "actuator_mapping_mode", default=_first_text_from_rows(rows, "actuator_mapping_mode")),
        "steer_scale": _first_number(control_health.get("steer_scale"), _first_text_from_rows(rows, "steer_scale")),
        "steering_sign": _first_text(control_health, "steering_sign"),
        "calibration_profile_id": _first_text(control_health, "calibration_profile_id", default=_first_text_from_rows(rows, "calibration_profile_id")),
        "raw_mapped_applied_available": bool(mapped_fields and applied_fields),
        "mapped_control_fields": sorted(mapped_fields),
        "applied_control_fields": sorted(applied_fields),
        "nonzero_mapped_frames": mapped_count,
        "nonzero_mapped_frames_source": mapped_count_source,
        "apply_control_count": int(applied_count) if applied_count is not None else None,
        "mapped_applied_steer_abs_error_p95": mapped_applied_steer_error,
        "control_latency_p95_ms": _first_number(metrics.get("control_latency_p95_ms"), _percentile(_series(rows, "control_latency_ms"), 0.95)),
        "missing_fields": missing_fields,
        "warnings": warnings,
        "status": status,
    }


def _vehicle_response(
    *,
    rows: Sequence[Mapping[str, Any]],
    mapping_and_apply: Mapping[str, Any],
) -> dict[str, Any]:
    speeds = _series(rows, "ego_speed", "speed_mps", "v_mps")
    route_s = _series(rows, "route_s")
    route_completion = _series(rows, "route_completion")
    yaw_rate = _series(rows, "ego_yaw_rate", "yaw_rate")
    applied_count = _num(mapping_and_apply.get("apply_control_count"))
    steer_active = any(abs(value) > 0.02 for value in _series(rows, "carla_steer_applied", "applied_steer"))
    speed_delta = (max(speeds) - min(speeds)) if speeds else None
    route_s_delta = (route_s[-1] - route_s[0]) if len(route_s) >= 2 else None
    route_completion_delta = (
        route_completion[-1] - route_completion[0] if len(route_completion) >= 2 else None
    )
    yaw_response = max(abs(value) for value in yaw_rate) if yaw_rate else None
    missing_fields: list[str] = []
    if not rows:
        status = "insufficient_data"
        missing_fields.append("vehicle_response.timeseries")
    elif (
        applied_count
        and applied_count > 0
        and (speed_delta is None or speed_delta < 0.05)
        and (route_s_delta is None or route_s_delta < 0.05)
        and (route_completion_delta is None or route_completion_delta < 0.01)
    ):
        status = "fail"
    elif steer_active and yaw_response is None:
        status = "insufficient_data"
        missing_fields.append("vehicle_response.ego_yaw_rate")
    elif steer_active and yaw_response is not None and yaw_response < 0.001:
        status = "fail"
    else:
        status = "pass"
    return {
        "speed_delta_mps": speed_delta,
        "route_s_delta_m": route_s_delta,
        "route_completion_delta": route_completion_delta,
        "yaw_rate_abs_max_rad_s": yaw_response,
        "steer_active": steer_active,
        "missing_fields": missing_fields,
        "status": status,
    }


def _verdict_sections(sections: Mapping[str, Mapping[str, Any]]) -> tuple[str, list[str], list[str]]:
    warnings: list[str] = []
    insufficient: list[str] = []
    for name in FAILURE_STAGE_PRIORITY:
        section = sections[name]
        status = str(section.get("status") or "")
        warnings.extend(str(item) for item in (section.get("warnings") or []) if item)
        if status == "fail":
            return name, [f"{name}_failed"], warnings
        if status == "insufficient_data":
            insufficient.append(name)
    if insufficient:
        return "insufficient_data", [f"{insufficient[0]}_insufficient_data"], warnings
    return "none", [], warnings


def _mark_downstream_insufficient(
    sections: Mapping[str, Mapping[str, Any]],
    failure_stage: str,
) -> list[str]:
    if failure_stage not in FAILURE_STAGE_PRIORITY:
        return []
    index = FAILURE_STAGE_PRIORITY.index(failure_stage)
    warnings: list[str] = []
    for name in FAILURE_STAGE_PRIORITY[index + 1 :]:
        section = sections.get(name)
        if not isinstance(section, dict):
            continue
        original_status = section.get("status")
        if original_status in {"pass", "warn", "fail"}:
            section["status"] = "insufficient_data"
            section.setdefault("warnings", [])
            if isinstance(section["warnings"], list):
                section["warnings"].append(f"blocked_by_upstream_{failure_stage}")
            warnings.append(f"{name}_blocked_by_upstream_{failure_stage}")
    return warnings


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Apollo Control Handoff Summary",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- run_id: `{report.get('run_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- scenario_class: `{report.get('scenario_class')}`",
        f"- backend: `{report.get('backend')}`",
        f"- evidence_level: `{report.get('evidence_level')}`",
        f"- verdict: `{report.get('verdict')}`",
        f"- failure_stage: `{report.get('failure_stage')}`",
        f"- blocking_reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
        "",
        "| layer | status | key evidence |",
        "| --- | --- | --- |",
    ]
    for key in FAILURE_STAGE_PRIORITY:
        section = report.get(key) if isinstance(report.get(key), Mapping) else {}
        evidence = ""
        if key == "process_health":
            evidence = f"started={section.get('started')} crash={section.get('crash_detected')}"
        elif key == "input_readiness":
            evidence = (
                f"planning={section.get('planning_message_count')} "
                f"loc={section.get('localization_count')} chassis={section.get('chassis_count')}"
            )
        elif key == "control_channel":
            evidence = f"count={section.get('message_count')} hz={section.get('hz')}"
        elif key == "bridge_receive":
            evidence = f"rx={section.get('control_rx_count')}"
        elif key == "mapping_and_apply":
            evidence = f"apply={section.get('apply_control_count')}"
        elif key == "vehicle_response":
            evidence = (
                f"route_s_delta={section.get('route_s_delta_m')} "
                f"route_completion_delta={section.get('route_completion_delta')} "
                f"speed_delta={section.get('speed_delta_mps')}"
            )
        lines.append(f"| {key} | {section.get('status')} | {evidence} |")
    lines.extend(
        [
            "",
            "## Missing",
            f"- missing_inputs: `{', '.join(report.get('missing_inputs') or []) or 'none'}`",
            f"- missing_fields: `{', '.join(report.get('missing_fields') or []) or 'none'}`",
            "",
            "This report is an evidence gate. It does not tune steer_scale, enable physical mapping, or prove planning quality.",
            "",
        ]
    )
    return "\n".join(lines)


def _given_or_find(root: Path | None, explicit: str | Path | None, relatives: Sequence[str]) -> Path | None:
    if explicit:
        path = Path(explicit).expanduser()
        if path.exists():
            return path
        return path
    if root is None:
        return None
    for relative in relatives:
        candidate = root / relative
        if candidate.exists():
            return candidate
    for relative in relatives:
        name = Path(relative).name
        matches = [path for path in root.rglob(name) if path.is_file()]
        if matches:
            return max(matches, key=lambda path: path.stat().st_mtime)
    return None


def _control_log_paths(root: Path | None) -> list[Path]:
    if root is None or not root.exists():
        return []
    names = (
        "artifacts/cyber_control_bridge.out.log",
        "artifacts/cyber_control_bridge.err.log",
        "artifacts/control.out.log",
        "artifacts/control.err.log",
        "cyber_control_bridge.out.log",
        "cyber_control_bridge.err.log",
        "control.out.log",
        "control.err.log",
    )
    paths = [root / name for name in names if (root / name).exists()]
    for pattern in ("*control*.log", "*control*.out", "*control*.err"):
        for path in root.rglob(pattern):
            if path.is_file() and path not in paths:
                paths.append(path)
    return sorted(paths)


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _read_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix == ".jsonl":
        return _read_jsonl(path)
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except OSError:
        return []


def _read_logs(paths: Sequence[Path]) -> list[str]:
    logs: list[str] = []
    for path in paths:
        try:
            logs.extend(path.read_text(encoding="utf-8", errors="replace").splitlines())
        except OSError:
            continue
    return logs


def _detect_crash(log_text: str) -> tuple[str | None, str | None]:
    if not log_text:
        return None, None
    for reason, pattern in CRASH_SIGNATURES:
        match = re.search(pattern, log_text, flags=re.IGNORECASE)
        if match:
            return reason, match.group(0)
    return None, None


def _channel(stats: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    channels = stats.get("channels")
    if not isinstance(channels, Mapping):
        return {}
    payload = channels.get(name)
    return payload if isinstance(payload, Mapping) else {}


def _channel_count(channel: Mapping[str, Any]) -> float | None:
    return _first_number(channel.get("message_count"), channel.get("count"))


def _channel_hz(channel: Mapping[str, Any]) -> float | None:
    return _first_number(channel.get("hz"), channel.get("rate_hz"))


def _fields_seen(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> set[str]:
    seen: set[str] = set()
    for row in rows:
        flattened = _flatten(row)
        for field in fields:
            value = flattened.get(field)
            if value not in (None, ""):
                seen.add(field)
    return seen


def _row_fields_seen(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> set[str]:
    seen: set[str] = set()
    for row in rows:
        for field in fields:
            if row.get(field) not in (None, ""):
                seen.add(field)
    return seen


def _first_raw_sample(
    decode_rows: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    for row in decode_rows:
        flat = _flatten(row)
        sample = {key: flat.get(key) for key in RAW_FIELDS if flat.get(key) not in (None, "")}
        if sample:
            return sample
    for row in rows:
        sample = {key: row.get(key) for key in RAW_FIELDS if row.get(key) not in (None, "")}
        if sample:
            return sample
    return None


def _flatten(payload: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}

    def visit(prefix: str, value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                visit(str(key), item)
        else:
            out[prefix] = value

    visit("", payload)
    return out


def _row_available_count(rows: Sequence[Mapping[str, Any]], *fields: str) -> int | None:
    if not rows:
        return None
    count = sum(1 for row in rows if any(row.get(field) not in (None, "") for field in fields))
    return count if count else None


def _nonzero_row_count(rows: Sequence[Mapping[str, Any]], *fields: str) -> int | None:
    if not rows:
        return None
    count = 0
    seen = False
    for row in rows:
        values = [_num(row.get(field)) for field in fields]
        if any(value is not None for value in values):
            seen = True
        if any(value is not None and abs(value) > 1e-6 for value in values):
            count += 1
    return count if seen else None


def _nonzero_flat_row_count(rows: Sequence[Mapping[str, Any]], *fields: str) -> int | None:
    if not rows:
        return None
    count = 0
    seen = False
    for row in rows:
        flattened = _flatten(row)
        values = [_num(flattened.get(field)) for field in fields]
        if any(value is not None for value in values):
            seen = True
        if any(value is not None and abs(value) > 1e-6 for value in values):
            count += 1
    return count if seen else None


def _series(rows: Sequence[Mapping[str, Any]], *fields: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        for field in fields:
            value = _num(row.get(field))
            if value is not None:
                values.append(value)
                break
    return values


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    index = (len(cleaned) - 1) * q
    lower = int(math.floor(index))
    upper = int(math.ceil(index))
    if lower == upper:
        return cleaned[lower]
    return cleaned[lower] * (upper - index) + cleaned[upper] * (index - lower)


def _nested_number(payload: Mapping[str, Any], *path: str) -> float | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return _num(current)


def _first_text(*items: Any, default: str | None = None) -> str | None:
    for index in range(0, len(items), 2):
        mapping = items[index] if index < len(items) else None
        key = items[index + 1] if index + 1 < len(items) else None
        if isinstance(mapping, Mapping) and key is not None:
            value = mapping.get(str(key))
            if value not in (None, ""):
                return str(value)
    return default


def _first_text_from_rows(rows: Sequence[Mapping[str, Any]], field: str) -> str | None:
    for row in rows:
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def _first_number(*values: Any) -> float | None:
    for value in values:
        result = _num(value)
        if result is not None:
            return result
    return None


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        parsed = _bool(value)
        if parsed is not None:
            return parsed
    return None


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "pass", "ok"}:
            return True
        if text in {"false", "0", "no", "n", "fail"}:
            return False
    return None


def _tail_lines(lines: Sequence[str], limit: int = 20) -> list[str]:
    return [str(line) for line in lines[-limit:]]


def _path_text(path: Path | None) -> str | None:
    return None if path is None else str(path)


def _missing_inputs(paths: Mapping[str, Any]) -> list[str]:
    required = ("summary", "manifest", "timeseries")
    return [key for key in required if paths.get(key) is None]


def _evidence_level(
    *,
    channel_stats: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
    decode_rows: Sequence[Mapping[str, Any]],
) -> str:
    native = bool(_channel(channel_stats, "/apollo/control"))
    bridge = bool(cyber_bridge_stats or decode_rows)
    if native and bridge:
        return "mixed"
    if native:
        return "apollo_native"
    if bridge:
        return "bridge_derived"
    return "insufficient_data"
