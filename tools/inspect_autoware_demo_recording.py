#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from carla_testbed.analysis.autoware_evidence import analyze_autoware_evidence_run_dir


def _file_ok(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except Exception:
        return False


def _dir_has_bag(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    if (path / "metadata.yaml").exists():
        return True
    return any(path.rglob("*.db3")) or any(path.rglob("*.mcap"))


def _find_run_dirs(root: Path) -> List[Path]:
    resolved = root.expanduser().resolve()
    if (resolved / "summary.json").exists():
        return [resolved]
    return sorted({path.parent for path in resolved.rglob("summary.json") if path.parent.name != "artifacts"})


def _inspect_run(run_dir: Path) -> Dict[str, Any]:
    evidence = analyze_autoware_evidence_run_dir(run_dir)
    carla_video = run_dir / "video" / "dual_cam" / "demo_third_person.mp4"
    rviz_video = run_dir / "video" / "rviz" / "autoware_rviz.mp4"
    rosbag_dir = run_dir / "rosbag2" / "autoware_demo"
    control_log = run_dir / "artifacts" / "autoware_control.jsonl"
    summary = run_dir / "summary.json"
    timeseries = run_dir / "timeseries.csv"
    checks = {
        "carla_recording": _file_ok(carla_video),
        "rviz_recording": _file_ok(rviz_video),
        "rosbag": _dir_has_bag(rosbag_dir),
        "control_log": _file_ok(control_log),
        "summary": _file_ok(summary),
        "timeseries": _file_ok(timeseries),
    }
    if not checks["carla_recording"]:
        status = "missing_carla_recording"
    elif not checks["rviz_recording"]:
        status = "missing_rviz_recording"
    elif not checks["rosbag"]:
        status = "missing_rosbag"
    elif not checks["control_log"]:
        status = "missing_control_log"
    elif not checks["summary"] or not checks["timeseries"]:
        status = "missing_run_artifacts"
    else:
        status = "ready"
    return {
        "run_dir": str(run_dir),
        "status": status,
        "checks": checks,
        "acceptance_gate": {
            "schema_version": evidence.get("schema_version"),
            "artifact_completeness_status": evidence.get("artifact_completeness_status"),
            "can_compare_with_apollo": evidence.get("can_compare_with_apollo"),
            "missing_artifacts": evidence.get("missing_artifacts") or [],
        },
        "artifacts": {
            "carla_video": str(carla_video),
            "rviz_video": str(rviz_video),
            "rosbag_dir": str(rosbag_dir),
            "control_log": str(control_log),
            "summary": str(summary),
            "timeseries": str(timeseries),
        },
    }


def build_inspection(root: Path) -> Dict[str, Any]:
    run_dirs = _find_run_dirs(root)
    runs = [_inspect_run(path) for path in run_dirs]
    if not runs:
        status = "no_runs_found"
    else:
        status = "ready" if all(item["status"] == "ready" for item in runs) else runs[0]["status"]
    return {
        "schema_version": "autoware_demo_recording_inspection.v1",
        "batch_root": str(root.expanduser().resolve()),
        "status": status,
        "run_count": len(runs),
        "ready_count": sum(1 for item in runs if item["status"] == "ready"),
        "runs": runs,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Autoware Demo Recording Inspection",
        "",
        f"- status: `{payload['status']}`",
        f"- run_count: `{payload['run_count']}`",
        f"- ready_count: `{payload['ready_count']}`",
        "",
        "| run_dir | status | CARLA | RViz | rosbag | control_log | summary | timeseries |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for item in payload.get("runs", []):
        checks = item.get("checks", {})
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                item.get("run_dir", ""),
                item.get("status", ""),
                bool(checks.get("carla_recording")),
                bool(checks.get("rviz_recording")),
                bool(checks.get("rosbag")),
                bool(checks.get("control_log")),
                bool(checks.get("summary")),
                bool(checks.get("timeseries")),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect Autoware CARLA/RViz/rosbag demo recording outputs.")
    parser.add_argument("root", type=Path, help="Run dir or batch root.")
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    return parser


def main() -> int:
    args = _parser().parse_args()
    payload = build_inspection(args.root)
    artifacts_dir = args.root.expanduser().resolve() / "artifacts"
    json_out = args.json_out or artifacts_dir / "autoware_demo_recording_inspection.json"
    md_out = args.md_out or artifacts_dir / "autoware_demo_recording_inspection.md"
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
