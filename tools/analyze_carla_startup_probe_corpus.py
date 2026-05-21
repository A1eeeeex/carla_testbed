#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from collections import defaultdict
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.run_town01_route_health import _startup_probe_attempt_rows


def _launch_record(attempt_row: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = attempt_row.get("launcher_diagnostics") or {}
    launch_records = diagnostics.get("launch_records") or []
    if launch_records and isinstance(launch_records[-1], dict):
        return launch_records[-1]
    return {}


def _command_list(launch_record: Dict[str, Any]) -> List[str]:
    command = launch_record.get("command") or []
    if isinstance(command, list):
        return [str(item) for item in command]
    return []


def _launch_shape(attempt_row: Dict[str, Any]) -> str:
    launch_record = _launch_record(attempt_row)
    command = _command_list(launch_record)
    joined = " ".join(command)
    display = str(launch_record.get("display") or "")
    sdl = str(launch_record.get("sdl_videodriver") or "")
    if "-RenderOffScreen" in command and "-quality-level=Low" in joined:
        return "render_offscreen_low_quality"
    if "-RenderOffScreen" in command:
        return "render_offscreen"
    if "-windowed" in command and "-ResX=1280" in command and "-ResY=720" in command:
        if sdl == "x11":
            return "x11_windowed_1280x720"
        return "windowed_1280x720"
    if "-windowed" in command and "-ResX=960" in command and "-ResY=540" in command and "-quality-level=Low" in joined:
        return "lowres_low_quality"
    if display == ":99":
        return "nested_display_replay"
    if display == ":0":
        return "default_display"
    return "other"


def _flatten_attempt_row(probe_path: Path, attempt_row: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = attempt_row.get("launcher_diagnostics") or {}
    display_probe = diagnostics.get("display_probe") or {}
    launch_record = _launch_record(attempt_row)
    command = _command_list(launch_record)
    retry_policy = attempt_row.get("_retry_policy") or {}
    no_retry_families = retry_policy.get("no_retry_failure_families") or []
    if not isinstance(no_retry_families, list):
        no_retry_families = [no_retry_families]
    return {
        "sample": probe_path.parent.parent.name,
        "probe_path": str(probe_path),
        "attempt": attempt_row.get("attempt"),
        "status": attempt_row.get("status"),
        "failure_family": attempt_row.get("failure_family"),
        "rpc_ready": bool(attempt_row.get("rpc_ready")),
        "world_ready": bool(attempt_row.get("world_ready")),
        "display": display_probe.get("display") or launch_record.get("display") or "",
        "monitor_count": display_probe.get("monitor_count"),
        "sdl_videodriver": launch_record.get("sdl_videodriver") or "",
        "render_offscreen_in_cmd": bool(launch_record.get("render_offscreen_in_cmd")),
        "launch_shape": _launch_shape(attempt_row),
        "command": " ".join(command),
        "probe_retry_policy_present": bool(retry_policy),
        "retry_max_attempts": retry_policy.get("max_attempts"),
        "retry_delay_sec": retry_policy.get("retry_delay_sec"),
        "retry_no_retry_families": ",".join(
            sorted(str(item).strip() for item in no_retry_families if str(item).strip())
        ),
        "retry_eligible": attempt_row.get("retry_eligible"),
        "retry_decision_reason": attempt_row.get("retry_decision_reason") or "",
    }


def _iter_flat_rows() -> Iterable[Dict[str, Any]]:
    for probe_path in sorted(REPO_ROOT.glob("runs/**/carla_boot/carla_startup_probe.json")):
        try:
            payload = json.loads(probe_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        retry_policy = payload.get("retry_policy") or {}
        if not isinstance(retry_policy, dict):
            retry_policy = {}
        for attempt_row in _startup_probe_attempt_rows(payload):
            enriched_attempt_row = dict(attempt_row)
            enriched_attempt_row["_retry_policy"] = retry_policy
            yield _flatten_attempt_row(probe_path, enriched_attempt_row)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _markdown_table(rows: List[List[str]]) -> str:
    if not rows:
        return ""
    header = "| " + " | ".join(rows[0]) + " |"
    sep = "| " + " | ".join("---" for _ in rows[0]) + " |"
    body = ["| " + " | ".join(row) + " |" for row in rows[1:]]
    return "\n".join([header, sep, *body])


def _write_report(path: Path, rows: List[Dict[str, Any]], report_title: str) -> None:
    total_attempts = len(rows)
    family_counts = Counter(row["failure_family"] for row in rows)
    display_rows = [row for row in rows if row["display"]]
    display_world_ready_rows = [row for row in display_rows if row["world_ready"]]
    monitor1_world_ready_rows = [row for row in display_world_ready_rows if row["monitor_count"] == 1]
    default_display_rows = [row for row in rows if row["launch_shape"] == "default_display"]
    default_display_family_counts = Counter(row["failure_family"] for row in default_display_rows)
    monitor1_x11_window_fail_rows = [
        row
        for row in rows
        if row["launch_shape"] == "x11_windowed_1280x720" and not row["world_ready"]
    ]
    rpc_ready_nonworld_rows = [row for row in rows if row["rpc_ready"] and not row["world_ready"]]
    rpc_ready_nonworld_family_counts = Counter(row["failure_family"] for row in rpc_ready_nonworld_rows)
    latest_rpc_ready_nonworld_rows = sorted(
        rpc_ready_nonworld_rows,
        key=lambda row: (row["sample"], int(row.get("attempt") or 0), row["probe_path"]),
    )[-8:]
    retry_policy_probe_rows = {
        str(row["probe_path"]): row for row in rows if bool(row.get("probe_retry_policy_present"))
    }
    retry_decision_rows = [
        row
        for row in rows
        if row.get("retry_eligible") in (True, False) or str(row.get("retry_decision_reason") or "")
    ]
    retry_policy_config_counts = Counter(
        (
            str(row.get("retry_no_retry_families") or "<none>"),
            str(row.get("retry_max_attempts") if row.get("retry_max_attempts") is not None else "<missing>"),
            str(row.get("retry_delay_sec") if row.get("retry_delay_sec") is not None else "<missing>"),
        )
        for row in retry_policy_probe_rows.values()
    )
    latest_retry_decision_rows = sorted(
        retry_decision_rows,
        key=lambda row: (row["sample"], int(row.get("attempt") or 0), row["probe_path"]),
    )[-8:]
    nonretry_decision_rows = [
        row
        for row in retry_decision_rows
        if row.get("retry_eligible") is False
        and str(row.get("retry_decision_reason") or "").startswith("nonretry_family:")
    ]
    default_display_probe_groups = defaultdict(list)
    for row in default_display_rows:
        default_display_probe_groups[str(row["probe_path"])].append(row)
    default_display_retry_recoveries = []
    default_display_retry_recovery_counts = Counter()
    for probe_path, group_rows in sorted(default_display_probe_groups.items()):
        ordered_rows = sorted(group_rows, key=lambda row: int(row.get("attempt") or 0))
        first_world_ready_index = next((index for index, row in enumerate(ordered_rows) if row["world_ready"]), None)
        if first_world_ready_index is None or first_world_ready_index == 0:
            continue
        preceding_rows = ordered_rows[:first_world_ready_index]
        last_failure = preceding_rows[-1]
        default_display_retry_recovery_counts[str(last_failure["failure_family"])] += 1
        default_display_retry_recoveries.append(
            {
                "sample": str(ordered_rows[first_world_ready_index]["sample"]),
                "probe_path": probe_path,
                "failure_attempt": str(last_failure["attempt"]),
                "failure_family": str(last_failure["failure_family"]),
                "success_attempt": str(ordered_rows[first_world_ready_index]["attempt"]),
                "command": str(ordered_rows[first_world_ready_index]["command"]),
            }
        )
    shape_counts = Counter((row["launch_shape"], row["failure_family"]) for row in rows)

    family_table = [["failure_family", "count"]]
    for family, count in sorted(family_counts.items(), key=lambda item: (-item[1], item[0])):
        family_table.append([str(family), str(count)])

    shape_table = [["launch_shape", "failure_family", "count"]]
    for (shape, family), count in sorted(shape_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
        shape_table.append([shape, family, str(count)])

    monitor1_world_table = [["sample", "launch_shape", "display", "command"]]
    for row in monitor1_world_ready_rows[:8]:
        monitor1_world_table.append([row["sample"], row["launch_shape"], row["display"], row["command"] or "<none>"])

    x11_fail_table = [["sample", "failure_family", "display", "monitor_count", "command"]]
    for row in monitor1_x11_window_fail_rows[:8]:
        x11_fail_table.append(
            [
                row["sample"],
                row["failure_family"],
                row["display"] or "<missing>",
                str(row["monitor_count"]),
                row["command"] or "<none>",
            ]
        )

    rpc_ready_nonworld_family_table = [["failure_family", "count"]]
    for family, count in sorted(rpc_ready_nonworld_family_counts.items(), key=lambda item: (-item[1], item[0])):
        rpc_ready_nonworld_family_table.append([str(family), str(count)])

    retry_policy_config_table = [["no_retry_families", "max_attempts", "retry_delay_sec", "probe_count"]]
    for (families, max_attempts, retry_delay_sec), count in sorted(
        retry_policy_config_counts.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2]),
    ):
        retry_policy_config_table.append([families, max_attempts, retry_delay_sec, str(count)])

    default_display_family_table = [["failure_family", "count"]]
    for family, count in sorted(default_display_family_counts.items(), key=lambda item: (-item[1], item[0])):
        default_display_family_table.append([str(family), str(count)])

    latest_retry_decisions_table = [
        ["sample", "attempt", "failure_family", "retry_eligible", "retry_decision_reason"]
    ]
    for row in latest_retry_decision_rows:
        latest_retry_decisions_table.append(
            [
                row["sample"],
                str(row["attempt"]),
                str(row["failure_family"]),
                str(row["retry_eligible"]),
                str(row["retry_decision_reason"] or "<missing>"),
            ]
        )

    latest_rpc_ready_nonworld_table = [["sample", "attempt", "failure_family", "launch_shape", "display", "command"]]
    for row in latest_rpc_ready_nonworld_rows:
        latest_rpc_ready_nonworld_table.append(
            [
                row["sample"],
                str(row["attempt"]),
                str(row["failure_family"]),
                str(row["launch_shape"]),
                row["display"] or "<missing>",
                row["command"] or "<none>",
            ]
        )

    default_display_retry_recovery_table = [["preceding_failure_family", "count"]]
    for family, count in sorted(default_display_retry_recovery_counts.items(), key=lambda item: (-item[1], item[0])):
        default_display_retry_recovery_table.append([str(family), str(count)])

    default_display_retry_recovery_samples_table = [
        ["sample", "failure_attempt", "failure_family", "success_attempt", "command"]
    ]
    for row in default_display_retry_recoveries:
        default_display_retry_recovery_samples_table.append(
            [
                row["sample"],
                row["failure_attempt"],
                row["failure_family"],
                row["success_attempt"],
                row["command"] or "<none>",
            ]
        )

    report = f"""# {report_title}

## Key Conclusion

- 我把 `runs/**/carla_boot/carla_startup_probe.json` 全量按同一 canonical 口径归一化后，当前结论比之前更准确：
  - `monitor_count = 1` **并不等于** “一定 world_not_ready”
  - 现有 corpus 里已经有 `Monitors: 1` 且成功 `world_ready` 的样本
  - 因此，当前真正的问题不是“显示器存在”本身，而是更窄的：
    - `x11/no-xrandr + windowed replay`
    - `nested display replay`
    - `render_offscreen / lowres_low_quality`
    这些特定 bring-up 形态如何掉进各自的 failure family

## Corpus Snapshot

- total normalized attempts: `{total_attempts}`
- attempts with non-empty display metadata: `{len(display_rows)}`
- display-aware `world_ready` attempts: `{len(display_world_ready_rows)}`
- `Monitors: 1` and `world_ready` attempts: `{len(monitor1_world_ready_rows)}`
- `x11_windowed_1280x720` non-`world_ready` attempts: `{len(monitor1_x11_window_fail_rows)}`

## Failure Family Counts

{_markdown_table(family_table)}

## Launch Shape By Canonical Family

{_markdown_table(shape_table)}

## RPC-Ready But Not World-Ready Families

- canonical rows in this bucket: `{len(rpc_ready_nonworld_rows)}`

{_markdown_table(rpc_ready_nonworld_family_table)}

## Retry Policy Coverage

- recorded `retry_policy` probe count: `{len(retry_policy_probe_rows)}`
- recorded retry-decision attempt count: `{len(retry_decision_rows)}`
- recorded nonretry-family stop decisions: `{len(nonretry_decision_rows)}`

## Recorded Retry Policy Configs

{_markdown_table(retry_policy_config_table)}

## `default_display` Bring-Up Split

- canonical `default_display` rows: `{len(default_display_rows)}`

{_markdown_table(default_display_family_table)}

## Latest Recorded Retry Decisions

{_markdown_table(latest_retry_decisions_table)}

## `default_display` Retry Recoveries

- probes that later reached `world_ready` after an earlier non-`world_ready` attempt: `{len(default_display_retry_recoveries)}`

{_markdown_table(default_display_retry_recovery_table)}

{_markdown_table(default_display_retry_recovery_samples_table)}

## Latest RPC-Ready Followup Blockers

{_markdown_table(latest_rpc_ready_nonworld_table)}

## `Monitors: 1` World-Ready Samples

{_markdown_table(monitor1_world_table)}

## `x11/no-xrandr + 1280x720` Non-World-Ready Samples

{_markdown_table(x11_fail_table)}

## Practical Meaning

- 当前 corpus 已经直接说明：
  - `Monitors: 1` 的 `default_display` 既可以 `world_ready`
  - 也可以在 `rpc_ready` 之后掉进 follow-up failure families
- 而且这层分叉还不是完全同一种“死失败”：
  - `rpc_ready_world_not_ready_eof_alive` 已经出现过同 probe 内后续 `attempt 2 -> world_ready` 的恢复样本
  - 当前 `rpc_ready_followup_missing_eof_alive` 还没有对应的同 probe 恢复样本
- 所以当前阻塞更像是：
  - **同一条 `default_display + --ros2` bring-up 路径的 follow-up 分叉**
  - 而不是 generic `monitor_count`、也不是“只要有显示器就不行”
- 另外，当前 normalized corpus 还没有真正覆盖到新的主线 retry instrumentation：
  - 还没有带 `retry_policy` / `retry_eligible` / `retry_decision_reason` 的 live probe rows
  - 所以这份 followup report 现在已经准备好消费新 mainline policy
  - 但它当前提供的是下轮 live 的 observability closure，不是伪造出来的 policy-live proof

## First Remaining Problem

- 当前环境第一问题进一步收窄成：
  - 为什么同一条 `default_display + --ros2 + Town01` bring-up 路径
    有时能到 `world_ready`
  - 但最近几条 live Town01 continuation 又会落进：
    - `rpc_ready_followup_missing_eof_alive`
    - `rpc_ready_world_not_ready_eof_alive`
  - 并且为什么后者已经出现 `attempt 2` recover，而前者在当前 corpus 里还没有

## Next Natural Step

- 后续环境 probe 不再把 `monitor_count` 当成主解释变量
- 优先只盯两条最有信息量的 bring-up tactic：
  - 对 `rpc_ready_world_not_ready_eof_alive` 保留 bounded same-probe retry
  - 对 `rpc_ready_followup_missing_eof_alive` 单独隔离 `default_display + --ros2` 的 `world_ready` follow-up
- 同时，下一条主线 Town01 live probe 应直接带上新的 retry instrumentation：
  - 让这份 report 自动显示：
    - `retry_policy`
    - `retry_eligible`
    - `retry_decision_reason`
  - 从而确认 `rpc_ready_followup_missing_eof_alive` 是否真的走到了 isolate-first no-retry 分支
- 一旦拿到新的稳定 `world_ready` 样本，立刻回到 Apollo 主问题：
  - `town01_rh_spawn219_goal046`
  - `stage6_reference_line_generation_guard`
  - reroute landing-frame bridge 在线验证
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze normalized CARLA startup probe corpus")
    parser.add_argument(
        "--output-csv",
        default=str(REPO_ROOT / "artifacts" / "town01_carla_startup_probe_corpus_summary_20260326.csv"),
        help="Where to write the normalized startup probe CSV summary",
    )
    parser.add_argument(
        "--output-report",
        default=str(REPO_ROOT / "artifacts" / "town01_carla_startup_probe_corpus_report_20260326.md"),
        help="Where to write the markdown report",
    )
    parser.add_argument(
        "--report-title",
        default="Town01 CARLA Startup Probe Corpus Report 2026-03-26",
        help="Markdown H1 title to use in the generated report",
    )
    args = parser.parse_args()

    rows = list(_iter_flat_rows())
    if not rows:
        raise SystemExit("No startup probe rows found")
    csv_path = Path(args.output_csv).expanduser().resolve()
    report_path = Path(args.output_report).expanduser().resolve()
    _write_csv(csv_path, rows)
    _write_report(report_path, rows, str(args.report_title))
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
