from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

BAGUANG_ASSIST_DEBT_REPORT_VERSION = "baguang_assist_debt_report.v1"

ASSIST_DEBT_CSV_FIELDS = [
    "stack",
    "assist",
    "status",
    "in_config_baseline",
    "in_current_comparison",
    "positive_removal_runs",
    "blocking_runs",
    "planned_profiles",
    "recommendation",
]


def analyze_baguang_assist_debt(
    *,
    config_path: str | Path = "configs/experiments/baguang_assist_reduction.yaml",
    comparison_report_path: str | Path = (
        "runs/analysis/baguang_stack_comparison_20260531_reduced_assists/"
        "baguang_stack_comparison_report.json"
    ),
    reduction_roots: Iterable[str | Path] = ("runs/assist_reduction",),
) -> dict[str, Any]:
    config = _read_yaml(Path(config_path).expanduser())
    comparison = _read_json(Path(comparison_report_path).expanduser())
    manifests = _find_reduction_manifests(reduction_roots)
    rows = _manifest_rows(manifests)
    config_baseline = _baseline_assists_from_config(config)
    profile_expected = _profile_expected_assists_from_config(config)
    current_assists = _current_assists_from_comparison(comparison)
    lane_event_contract = _lane_event_contract_from_comparison(comparison)
    assist_results = _build_assist_results(
        config_baseline=config_baseline,
        profile_expected=profile_expected,
        current_assists=current_assists,
        rows=rows,
        lane_event_contract=lane_event_contract,
    )
    verdict = _assist_debt_verdict(assist_results, lane_event_contract)
    return {
        "schema_version": BAGUANG_ASSIST_DEBT_REPORT_VERSION,
        "config_path": str(Path(config_path).expanduser()),
        "comparison_report_path": str(Path(comparison_report_path).expanduser()),
        "reduction_manifest_paths": [str(path) for path in manifests],
        "lane_event_contract": lane_event_contract,
        "assist_results": assist_results,
        "verdict": verdict,
        "claim_boundary": {
            "can_claim_unassisted_natural_driving": False,
            "reason": "Assist-debt report classifies reduction evidence; it does not prove unassisted natural driving.",
        },
    }


def write_baguang_assist_debt_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_assist_debt_report.json"
    csv_path = out / "baguang_assist_debt_report.csv"
    md_path = out / "baguang_assist_debt_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(csv_path, report.get("assist_results") or [])
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "csv": str(csv_path), "summary": str(md_path)}


def _build_assist_results(
    *,
    config_baseline: Mapping[str, set[str]],
    profile_expected: Mapping[tuple[str, str], set[str]],
    current_assists: Mapping[str, set[str]],
    rows: list[dict[str, Any]],
    lane_event_contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    evidence: dict[tuple[str, str], dict[str, Any]] = {}
    all_stacks = sorted(set(config_baseline) | set(current_assists))
    for stack in all_stacks:
        for assist in sorted(config_baseline.get(stack, set()) | current_assists.get(stack, set())):
            evidence[(stack, assist)] = {
                "stack": stack,
                "assist": assist,
                "in_config_baseline": assist in config_baseline.get(stack, set()),
                "in_current_comparison": assist in current_assists.get(stack, set()),
                "positive_removal_runs": [],
                "blocking_runs": [],
                "planned_profiles": [],
            }

    for row in rows:
        stack = str(row.get("stack") or "")
        profile_id = str(row.get("profile_id") or "")
        expected = _expected_assists_from_row(row)
        baseline = config_baseline.get(stack, set())
        if not stack or not profile_id or profile_id.endswith("_assisted_baseline"):
            continue
        if not expected and (stack, profile_id) in profile_expected:
            expected = profile_expected[(stack, profile_id)]
        removed = sorted(baseline - expected)
        for assist in removed:
            item = evidence.setdefault(
                (stack, assist),
                {
                    "stack": stack,
                    "assist": assist,
                    "in_config_baseline": assist in baseline,
                    "in_current_comparison": assist in current_assists.get(stack, set()),
                    "positive_removal_runs": [],
                    "blocking_runs": [],
                    "planned_profiles": [],
                },
            )
            status = str(row.get("status") or "")
            run_ref = _run_ref(row)
            if status == "success":
                item["positive_removal_runs"].append(run_ref)
            elif status == "failed":
                if assist == "lane_invasion_event_disabled" and _lane_event_is_quarantined(lane_event_contract):
                    item["positive_removal_runs"].append(
                        {
                            **run_ref,
                            "status": "quarantined",
                            "reason": "lane_invasion_failure_quarantined_by_contract",
                        }
                    )
                else:
                    item["blocking_runs"].append(run_ref)
            elif status in {"dry_run", "planned", "skipped"}:
                item["planned_profiles"].append(profile_id)

    output: list[dict[str, Any]] = []
    for key in sorted(evidence):
        item = evidence[key]
        item["status"] = _assist_status(item, lane_event_contract)
        item["recommendation"] = _assist_recommendation(item)
        output.append(item)
    return output


def _assist_status(item: Mapping[str, Any], lane_event_contract: Mapping[str, Any]) -> str:
    if item.get("assist") == "lane_invasion_event_disabled" and _lane_event_is_quarantined(lane_event_contract):
        return "quarantined_by_lane_event_contract"
    positive = bool(item.get("positive_removal_runs"))
    blocking = bool(item.get("blocking_runs"))
    if positive and blocking:
        return "mixed_evidence"
    if positive:
        return "removable_once"
    if blocking:
        return "blocking_when_removed_once"
    if item.get("in_current_comparison"):
        return "active_in_current_comparison"
    if item.get("planned_profiles"):
        return "planned_not_run"
    return "unverified"


def _assist_recommendation(item: Mapping[str, Any]) -> str:
    status = str(item.get("status") or "")
    if status == "removable_once":
        return "repeat the removal probe before promoting this assist as removable"
    if status == "blocking_when_removed_once":
        return "debug the failed removal run before attempting further reduction"
    if status == "quarantined_by_lane_event_contract":
        return "keep lane-event disabled for Baguang until OpenDRIVE/CARLA lane-event contract is fixed"
    if status == "active_in_current_comparison":
        return "schedule a targeted removal probe or document why this assist is required"
    if status == "planned_not_run":
        return "run the planned profile and refresh this report"
    if status == "mixed_evidence":
        return "repeat controlled probes; evidence is not stable enough for promotion"
    return "collect online reduction evidence"


def _assist_debt_verdict(
    assist_results: list[Mapping[str, Any]],
    lane_event_contract: Mapping[str, Any],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for item in assist_results:
        status = str(item.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    blockers = [
        f"{item.get('stack')}:{item.get('assist')}"
        for item in assist_results
        if item.get("status") == "blocking_when_removed_once"
    ]
    active = [
        f"{item.get('stack')}:{item.get('assist')}"
        for item in assist_results
        if item.get("status") == "active_in_current_comparison"
    ]
    return {
        "status": "warn" if blockers or active else "pass",
        "status_counts": status_counts,
        "blocking_assists": blockers,
        "active_assists": active,
        "lane_event_quarantined": _lane_event_is_quarantined(lane_event_contract),
        "next_priority": _next_priority(blockers, active, assist_results),
    }


def _next_priority(
    blockers: list[str],
    active: list[str],
    assist_results: list[Mapping[str, Any]],
) -> str:
    if blockers:
        return f"debug blocker {_first_by_priority(blockers)}"
    if active:
        return f"run targeted reduction for {_first_by_priority(active)}"
    removable = [
        f"{item.get('stack')}:{item.get('assist')}"
        for item in assist_results
        if item.get("status") == "removable_once"
    ]
    if removable:
        return f"repeat positive removal {removable[0]}"
    return "collect more assist-reduction evidence"


def _first_by_priority(items: list[str]) -> str:
    priority = [
        "apollo:straight_lane_lateral_stabilizer",
        "apollo:straight_acc_override",
        "autoware:recording_timeout_sec_5",
        "autoware:goal_planner_module_disabled",
        "autoware:planning_common_dynamics_override",
        "autoware:speed_feedback_bridge_profile",
        "apollo:carla_direct_transport",
    ]
    for candidate in priority:
        if candidate in items:
            return candidate
    return items[0]


def _baseline_assists_from_config(config: Mapping[str, Any]) -> dict[str, set[str]]:
    stacks = _as_mapping(config.get("stacks"))
    output: dict[str, set[str]] = {}
    for stack, cfg_raw in stacks.items():
        profiles = _as_mapping(cfg_raw).get("profiles") or []
        baseline_id = f"{stack}_assisted_baseline"
        baseline = next(
            (
                profile
                for profile in profiles
                if isinstance(profile, Mapping) and profile.get("profile_id") == baseline_id
            ),
            None,
        )
        output[str(stack)] = set(str(item) for item in ((baseline or {}).get("expected_assists") or []))
    return output


def _profile_expected_assists_from_config(config: Mapping[str, Any]) -> dict[tuple[str, str], set[str]]:
    stacks = _as_mapping(config.get("stacks"))
    output: dict[tuple[str, str], set[str]] = {}
    for stack, cfg_raw in stacks.items():
        for profile in _as_mapping(cfg_raw).get("profiles") or []:
            if not isinstance(profile, Mapping):
                continue
            profile_id = str(profile.get("profile_id") or "")
            output[(str(stack), profile_id)] = set(str(item) for item in profile.get("expected_assists") or [])
    return output


def _current_assists_from_comparison(comparison: Mapping[str, Any]) -> dict[str, set[str]]:
    output: dict[str, set[str]] = {}
    for result in comparison.get("stack_results") or []:
        if not isinstance(result, Mapping):
            continue
        stack = str(result.get("stack") or "")
        output.setdefault(stack, set()).update(str(item) for item in result.get("declared_assists") or [])
    return output


def _lane_event_contract_from_comparison(comparison: Mapping[str, Any]) -> dict[str, Any]:
    boundary = _as_mapping(comparison.get("claim_boundary"))
    return dict(_as_mapping(boundary.get("lane_event_contract")))


def _lane_event_is_quarantined(lane_event_contract: Mapping[str, Any]) -> bool:
    return (
        lane_event_contract.get("status") == "quarantined"
        and lane_event_contract.get("quarantine_recommended") is True
        and lane_event_contract.get("lane_invasion_event_can_be_used_as_hard_gate") is False
    )


def _find_reduction_manifests(roots: Iterable[str | Path]) -> list[Path]:
    paths: list[Path] = []
    for root in roots:
        p = Path(root).expanduser()
        if p.is_file() and p.name == "assist_reduction_manifest.json":
            paths.append(p)
        elif p.is_dir():
            paths.extend(sorted(p.rglob("assist_reduction_manifest.json")))
    return sorted(dict.fromkeys(paths))


def _manifest_rows(manifest_paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in manifest_paths:
        payload = _read_json(path)
        if payload.get("dry_run") is True:
            continue
        for row in payload.get("runs") or []:
            if not isinstance(row, Mapping):
                continue
            item = dict(row)
            item["_manifest_path"] = str(path)
            rows.append(item)
    return rows


def _expected_assists_from_row(row: Mapping[str, Any]) -> set[str]:
    raw = row.get("expected_assists")
    if isinstance(raw, list):
        return {str(item) for item in raw}
    raw_json = row.get("expected_assists_json")
    if isinstance(raw_json, str) and raw_json.strip():
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            parsed = []
        if isinstance(parsed, list):
            return {str(item) for item in parsed}
    return set()


def _run_ref(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "profile_id": row.get("profile_id"),
        "status": row.get("status"),
        "failure_reason": row.get("failure_reason") or row.get("summary_exit_reason"),
        "run_dir": row.get("actual_run_dir") or row.get("run_dir"),
        "summary_path": row.get("summary_path"),
        "manifest_path": row.get("_manifest_path"),
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload if isinstance(payload, dict) else {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ASSIST_DEBT_CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            payload = {field: row.get(field) for field in ASSIST_DEBT_CSV_FIELDS}
            payload["positive_removal_runs"] = len(row.get("positive_removal_runs") or [])
            payload["blocking_runs"] = len(row.get("blocking_runs") or [])
            payload["planned_profiles"] = "|".join(str(item) for item in row.get("planned_profiles") or [])
            writer.writerow(payload)


def _summary_markdown(report: Mapping[str, Any]) -> str:
    verdict = _as_mapping(report.get("verdict"))
    lines = [
        "# Baguang Assist Debt Report",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{verdict.get('status')}`",
        f"- next_priority: `{verdict.get('next_priority')}`",
        f"- lane_event_quarantined: `{verdict.get('lane_event_quarantined')}`",
        "",
        "## Assist Results",
    ]
    for item in report.get("assist_results") or []:
        if not isinstance(item, Mapping):
            continue
        lines.extend(
            [
                "",
                f"- `{item.get('stack')}:{item.get('assist')}`",
                f"  - status: `{item.get('status')}`",
                f"  - in_current_comparison: `{item.get('in_current_comparison')}`",
                f"  - positive_removal_runs: `{len(item.get('positive_removal_runs') or [])}`",
                f"  - blocking_runs: `{len(item.get('blocking_runs') or [])}`",
                f"  - recommendation: {item.get('recommendation')}",
            ]
        )
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "This report is an evidence ledger for assist reduction. It does not prove unassisted natural driving.",
            "",
        ]
    )
    return "\n".join(lines)
