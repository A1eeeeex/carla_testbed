#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _route_id_from_summary(path: Path) -> str:
    summary = _load_json(path)
    route_id = str(summary.get("route_id") or "").strip()
    if route_id:
        return route_id
    metadata = summary.get("scenario_metadata")
    if isinstance(metadata, dict):
        route_id = str(metadata.get("route_id") or "").strip()
        if route_id:
            return route_id
    return path.parent.name


def _find_run_dirs(batch_root: Path) -> List[Path]:
    resolved = batch_root.expanduser().resolve()
    if (resolved / "summary.json").exists():
        return [resolved]
    run_dirs = []
    for summary_path in resolved.rglob("summary.json"):
        # Skip copied or nested summaries that are not the run-level summary.
        if summary_path.parent.name == "artifacts":
            continue
        run_dirs.append(summary_path.parent)
    return sorted(set(run_dirs), key=lambda path: str(path))


def _count_pngs(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    return sum(1 for item in path.glob("*.png") if item.is_file())


def _file_ok(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except Exception:
        return False


def _inspect_carla_recording(run_dir: Path, *, min_frames: int) -> Dict[str, Any]:
    dual_cam_dir = run_dir / "video" / "dual_cam"
    raw_tp = dual_cam_dir / "raw_tp"
    third_person_mp4 = dual_cam_dir / "demo_third_person.mp4"
    hud_mp4 = dual_cam_dir / "demo_third_person_hud.mp4"
    frame_count = _count_pngs(raw_tp)
    video_ok = _file_ok(third_person_mp4) or _file_ok(hud_mp4)
    frames_ok = frame_count >= int(min_frames)
    status = "ok" if frames_ok or video_ok else "missing"
    return {
        "status": status,
        "raw_tp": str(raw_tp),
        "raw_tp_frame_count": frame_count,
        "min_frames": int(min_frames),
        "frames_ok": frames_ok,
        "demo_third_person_mp4": str(third_person_mp4),
        "demo_third_person_mp4_exists": _file_ok(third_person_mp4),
        "demo_third_person_hud_mp4": str(hud_mp4),
        "demo_third_person_hud_mp4_exists": _file_ok(hud_mp4),
        "video_ok": video_ok,
    }


def _inspect_dreamview_recording(run_dir: Path, *, min_frames: int, required: bool) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    status_path = artifacts / "dreamview_recording_status.json"
    manifest_path = artifacts / "dreamview_capture_manifest.json"
    status = _load_json(status_path)
    manifest = _load_json(manifest_path)
    recording_status = str(status.get("recording_status") or manifest.get("recording_status") or "").strip()
    recording_success = bool(status.get("recording_success") or manifest.get("recording_success"))
    output_video_path = Path(str(manifest.get("output_video_path") or status.get("output_video_path") or ""))
    output_video_generated = bool(manifest.get("output_video_generated") or status.get("output_video_generated"))
    frame_count = int(manifest.get("frame_count") or status.get("frame_count") or 0)
    if str(output_video_path) and not output_video_path.is_absolute():
        output_video_path = run_dir / output_video_path
    video_ok = output_video_generated and _file_ok(output_video_path)
    frames_ok = frame_count >= int(min_frames)
    if recording_success and (video_ok or frames_ok):
        derived_status = "ok"
    elif not required and recording_status in {"", "disabled"}:
        derived_status = "disabled"
    elif not status and not manifest:
        derived_status = "missing"
    else:
        derived_status = "failed"
    return {
        "status": derived_status,
        "required": bool(required),
        "status_path": str(status_path),
        "status_exists": status_path.exists(),
        "manifest_path": str(manifest_path),
        "manifest_exists": manifest_path.exists(),
        "recording_status": recording_status,
        "recording_success": recording_success,
        "output_video_path": str(output_video_path) if str(output_video_path) else "",
        "output_video_generated": output_video_generated,
        "output_video_exists": _file_ok(output_video_path) if str(output_video_path) else False,
        "frame_count": frame_count,
        "min_frames": int(min_frames),
        "frames_ok": frames_ok,
        "failure_types": status.get("failure_types") or manifest.get("failure_types") or [],
        "failure_messages": status.get("failure_messages") or manifest.get("failure_messages") or [],
    }


def build_inspection(
    batch_root: Path,
    *,
    require_carla: bool = True,
    require_dreamview: bool = False,
    min_carla_frames: int = 10,
    min_dreamview_frames: int = 10,
) -> Dict[str, Any]:
    resolved = batch_root.expanduser().resolve()
    run_dirs = _find_run_dirs(resolved)
    route_reports: List[Dict[str, Any]] = []
    for run_dir in run_dirs:
        summary_path = run_dir / "summary.json"
        route_report = {
            "run_dir": str(run_dir),
            "summary_path": str(summary_path),
            "route_id": _route_id_from_summary(summary_path),
            "carla": _inspect_carla_recording(run_dir, min_frames=min_carla_frames),
            "dreamview": _inspect_dreamview_recording(
                run_dir,
                min_frames=min_dreamview_frames,
                required=require_dreamview,
            ),
        }
        route_reports.append(route_report)

    carla_failures = [
        item for item in route_reports if require_carla and item["carla"].get("status") != "ok"
    ]
    dreamview_failures = [
        item for item in route_reports if require_dreamview and item["dreamview"].get("status") != "ok"
    ]
    if not route_reports:
        status = "no_runs_found"
    elif carla_failures:
        status = "missing_carla_recording"
    elif dreamview_failures:
        status = "missing_dreamview_recording"
    else:
        status = "ready"
    return {
        "batch_root": str(resolved),
        "status": status,
        "route_count": len(route_reports),
        "require_carla": bool(require_carla),
        "require_dreamview": bool(require_dreamview),
        "min_carla_frames": int(min_carla_frames),
        "min_dreamview_frames": int(min_dreamview_frames),
        "carla_ok_count": sum(1 for item in route_reports if item["carla"].get("status") == "ok"),
        "dreamview_ok_count": sum(1 for item in route_reports if item["dreamview"].get("status") == "ok"),
        "routes": route_reports,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Demo Recording Inspection",
        "",
        f"- status: `{payload['status']}`",
        f"- batch_root: `{payload['batch_root']}`",
        f"- route_count: `{payload['route_count']}`",
        f"- carla_ok_count: `{payload['carla_ok_count']}`",
        f"- dreamview_ok_count: `{payload['dreamview_ok_count']}`",
        f"- require_carla: `{payload['require_carla']}`",
        f"- require_dreamview: `{payload['require_dreamview']}`",
        "",
        "| route_id | CARLA | CARLA frames | CARLA mp4 | Dreamview | Dreamview frames | Dreamview mp4 |",
        "|---|---|---:|---|---|---:|---|",
    ]
    for item in payload.get("routes", []):
        carla = item.get("carla", {})
        dreamview = item.get("dreamview", {})
        lines.append(
            "| `{}` | `{}` | {} | `{}` | `{}` | {} | `{}` |".format(
                item.get("route_id", ""),
                carla.get("status", ""),
                carla.get("raw_tp_frame_count", 0),
                bool(carla.get("video_ok")),
                dreamview.get("status", ""),
                dreamview.get("frame_count", 0),
                bool(dreamview.get("output_video_exists")),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect Town01 CARLA and Dreamview demo recording outputs.")
    parser.add_argument("batch_root", type=Path)
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    parser.add_argument("--require-carla", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-dreamview", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--min-carla-frames", type=int, default=10)
    parser.add_argument("--min-dreamview-frames", type=int, default=10)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    batch_root = args.batch_root.expanduser().resolve()
    payload = build_inspection(
        batch_root,
        require_carla=bool(args.require_carla),
        require_dreamview=bool(args.require_dreamview),
        min_carla_frames=int(args.min_carla_frames),
        min_dreamview_frames=int(args.min_dreamview_frames),
    )
    default_artifacts = batch_root / "artifacts"
    json_out = args.json_out or (default_artifacts / "town01_demo_recording_inspection.json")
    md_out = args.md_out or (default_artifacts / "town01_demo_recording_inspection.md")
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    print(json.dumps({k: payload[k] for k in ["status", "route_count", "carla_ok_count", "dreamview_ok_count"]}, indent=2))


if __name__ == "__main__":
    main()
