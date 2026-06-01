#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.algorithms.channel_contract import (
    check_channel_stats_file,
    load_channel_contract,
)
from carla_testbed.algorithms.inventory import load_algorithm_inventory
from carla_testbed.algorithms.replay_digest import (
    compare_replay_digest,
    load_replay_digest,
    write_replay_comparison_report,
)
from carla_testbed.algorithms.reproduction import (
    load_reproduction_report,
    validate_reproduction_report,
)
from carla_testbed.algorithms.reproduction_gate import (
    evaluate_reproduction_gate,
    load_reproduction_gate_config,
)
from carla_testbed.algorithms.shadow_mode import (
    analyze_shadow_mode_timeseries,
    write_shadow_mode_report,
)
from carla_testbed.algorithms.variant import load_algorithm_variant

STATUS_RANK = {"pass": 0, "warn": 1, "fail": 2, "blocked": 3, "insufficient_data": 2}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check Apollo reproduction artifacts without starting Apollo or CARLA."
    )
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--variant", type=Path, required=True)
    parser.add_argument("--reproduction-report", type=Path, required=True)
    parser.add_argument("--channel-stats", type=Path)
    parser.add_argument("--channel-contract", type=Path, default=Path("configs/algorithms/apollo_channel_contract.yaml"))
    parser.add_argument("--replay-golden", type=Path)
    parser.add_argument("--replay-candidate", type=Path)
    parser.add_argument("--shadow-timeseries", type=Path)
    parser.add_argument("--shadow-config", type=Path, default=Path("configs/algorithms/apollo_shadow_mode_check.yaml"))
    parser.add_argument("--route-health", type=Path)
    parser.add_argument("--ab-report", type=Path)
    parser.add_argument("--calibration-report", type=Path)
    parser.add_argument("--gate-config", type=Path, default=Path("configs/algorithms/apollo_reproduction_gate.yaml"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--allow-warn-as-success", action="store_true")
    return parser


def _load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    payload["_source_path"] = str(path)
    return payload


def _combine_status(statuses: list[str]) -> str:
    status = "pass"
    for item in statuses:
        normalized = item if item in STATUS_RANK else "fail"
        if STATUS_RANK[normalized] > STATUS_RANK[status]:
            status = normalized
    return status


def _write_markdown(path: Path, report: dict[str, Any]) -> None:
    lines = [
        "# Apollo Reproduction Check",
        "",
        f"- status: `{report['status']}`",
        f"- variant_id: `{report.get('variant_id')}`",
        f"- can_run_closed_loop_eval: `{report['gate'].get('can_run_closed_loop_eval')}`",
        f"- can_claim_algorithm_limitation: `{report['gate'].get('can_claim_algorithm_limitation')}`",
        "",
        "## Component Status",
    ]
    for name, status in sorted((report.get("component_statuses") or {}).items()):
        lines.append(f"- `{name}`: `{status}`")
    lines.extend(["", "## Blocking Reasons"])
    blocking = report["gate"].get("blocking_reasons") or []
    if blocking:
        lines.extend(f"- `{reason}`" for reason in blocking)
    else:
        lines.append("- none")
    lines.extend(["", "## Missing Artifacts"])
    missing = report["gate"].get("missing_artifacts") or []
    if missing:
        lines.extend(f"- `{item}`" for item in missing)
    else:
        lines.append("- none")
    lines.extend(["", "## Generated Reports"])
    for name, output in sorted((report.get("generated_reports") or {}).items()):
        lines.append(f"- `{name}`: `{output}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_check(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, str]]:
    out_dir = args.out.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory = load_algorithm_inventory(args.inventory)
    variant = load_algorithm_variant(args.variant)
    reproduction = load_reproduction_report(args.reproduction_report)
    reproduction_validation = validate_reproduction_report(reproduction)

    generated_reports: dict[str, str] = {}
    component_statuses: dict[str, str] = {
        "inventory": (inventory.get("_validation") or {}).get("status", "pass"),
        "variant": (variant.get("_validation") or {}).get("status", "pass"),
        "reproduction_report": reproduction_validation.status,
    }

    adapter_report = None
    if args.channel_stats:
        contract = load_channel_contract(args.channel_contract)
        adapter_check = check_channel_stats_file(contract, args.channel_stats)
        adapter_report = adapter_check.report
        adapter_path = out_dir / "adapter_contract_report.json"
        adapter_path.write_text(json.dumps(adapter_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        generated_reports["adapter_contract_report"] = str(adapter_path)
        component_statuses["adapter_contract_report"] = adapter_check.status

    replay_report = None
    if args.replay_golden or args.replay_candidate:
        if not args.replay_golden or not args.replay_candidate:
            raise ValueError("--replay-golden and --replay-candidate must be provided together")
        golden = load_replay_digest(args.replay_golden)
        candidate = load_replay_digest(args.replay_candidate)
        replay_comparison = compare_replay_digest(golden, candidate, None)
        replay_outputs = write_replay_comparison_report(
            out_dir / "replay_digest",
            replay_comparison,
            golden_path=args.replay_golden,
            candidate_path=args.replay_candidate,
        )
        replay_report = replay_comparison.report
        generated_reports.update({f"replay_digest.{key}": value for key, value in replay_outputs.items()})
        component_statuses["replay_comparison_report"] = replay_comparison.status

    shadow_report = None
    if args.shadow_timeseries:
        shadow_report = analyze_shadow_mode_timeseries(args.shadow_timeseries, args.shadow_config)
        shadow_outputs = write_shadow_mode_report(shadow_report, out_dir / "shadow_mode")
        generated_reports.update({f"shadow_mode.{key}": value for key, value in shadow_outputs.items()})
        component_statuses["shadow_mode_report"] = str(shadow_report.get("status") or "fail")

    route_health = _load_json(args.route_health)
    ab_report = _load_json(args.ab_report)
    calibration_report = _load_json(args.calibration_report)
    gate_config = load_reproduction_gate_config(args.gate_config)
    gate = evaluate_reproduction_gate(
        reproduction,
        route_health=route_health,
        ab_report=ab_report,
        calibration_report=calibration_report,
        config=gate_config,
    )
    component_statuses["reproduction_gate"] = gate["status"]

    status = _combine_status(list(component_statuses.values()))
    report = {
        "schema_version": "apollo_reproduction_check.v1",
        "status": status,
        "variant_id": variant.get("variant_id"),
        "inventory_path": str(args.inventory),
        "variant_path": str(args.variant),
        "reproduction_report_path": str(args.reproduction_report),
        "component_statuses": component_statuses,
        "gate": gate,
        "generated_reports": generated_reports,
        "embedded_reports": {
            "adapter_contract_report": adapter_report,
            "replay_comparison_report": replay_report,
            "shadow_mode_report": shadow_report,
        },
    }
    json_path = out_dir / "apollo_reproduction_check.json"
    md_path = out_dir / "apollo_reproduction_check.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(md_path, report)
    return report, {"json": str(json_path), "markdown": str(md_path)}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report, outputs = run_check(args)
    except Exception as exc:
        print(f"apollo reproduction check failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"status": report["status"], "outputs": outputs}, indent=2, sort_keys=True))
    if report["status"] == "pass":
        return 0
    if report["status"] == "warn" and args.allow_warn_as_success:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
