#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
import sys
import time
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tbio.carla.launcher import CarlaLauncher, _external_carla_python_candidates
from tools.run_town01_route_health import _load_dotenv, _startup_probe_failure_family, write_json


MODE_PRESETS: Dict[str, Dict[str, str]] = {
    "default_display": {
        "label": "Default Display",
        "extra_args": "--ros2",
    },
    "lowres_window": {
        "label": "Low-Res Window",
        "extra_args": "--ros2 -windowed -ResX=960 -ResY=540",
    },
    "render_offscreen": {
        "label": "RenderOffScreen",
        "extra_args": "--ros2 -RenderOffScreen",
    },
    "lowres_low_quality": {
        "label": "Low-Res Low-Quality",
        "extra_args": "--ros2 -windowed -ResX=960 -ResY=540 -quality-level=Low",
    },
    "render_offscreen_low_quality": {
        "label": "RenderOffScreen Low-Quality",
        "extra_args": "--ros2 -RenderOffScreen -quality-level=Low",
    },
    "render_offscreen_no_ros2": {
        "label": "RenderOffScreen No-ROS2",
        "extra_args": "-RenderOffScreen",
    },
    "lowres_low_quality_no_ros2": {
        "label": "Low-Res Low-Quality No-ROS2",
        "extra_args": "-windowed -ResX=960 -ResY=540 -quality-level=Low",
    },
}


def _classify_attempt(row: Dict[str, Any]) -> str:
    status = str(row.get("status") or "")
    if status == "world_ready":
        return "world_ready"
    diagnostics = row.get("launcher_diagnostics") or {}
    probe_style = str(row.get("probe_style") or "active")
    launch_records = diagnostics.get("launch_records") or []
    last_record = launch_records[-1] if launch_records else {}
    exit_code = last_record.get("exit_code")
    if exit_code is not None:
        tail = "\n".join(str(item) for item in (last_record.get("server_log_tail") or []))
        lowered = tail.lower()
        if "out of memory on vulkan" in lowered:
            return "early_exit_vulkan_oom"
        return "early_exit"
    port_snapshot = diagnostics.get("target_port_snapshot") or []
    any_port_open = any(bool(item.get("open")) for item in port_snapshot if isinstance(item, dict))
    latest_server_tail = "\n".join(str(item) for item in (diagnostics.get("latest_server_log_tail") or []))
    lowered_tail = latest_server_tail.lower()
    if probe_style == "passive":
        if "error retrieving stream id" in lowered_tail or "failed to read header" in lowered_tail:
            return "passive_eof_bridge"
        if any_port_open:
            return "passive_ports_open_no_eof"
        return "passive_no_listener_dead_state"
    family = _startup_probe_failure_family(row)
    if family and family not in {"unknown", "starting"}:
        return family
    handshake_ready = bool(diagnostics.get("rpc_handshake_ready"))
    if any_port_open and not handshake_ready:
        return "ports_open_handshake_dead"
    if not any_port_open and not handshake_ready:
        return "no_listener_dead_state"
    return status or "unknown"


def _external_world_probe(*, host: str, port: int, timeout_s: float) -> Dict[str, Any]:
    script = (
        "import json, sys\n"
        "try:\n"
        "    import carla\n"
        "    client = carla.Client(sys.argv[1], int(sys.argv[2]))\n"
        "    client.set_timeout(float(sys.argv[3]))\n"
        "    client.get_server_version()\n"
        "    world = client.get_world()\n"
        "    print(json.dumps({'ok': True, 'world_map': world.get_map().name}))\n"
        "except Exception as exc:\n"
        "    print(json.dumps({'ok': False, 'error': f'{exc.__class__.__name__}: {exc}'}))\n"
        "    raise SystemExit(1)\n"
    )
    candidates = _external_carla_python_candidates()
    if not candidates:
        return {
            "ok": False,
            "error": "no_external_carla_python_candidate",
            "candidates": [],
        }
    attempts: list[Dict[str, Any]] = []
    for candidate in candidates:
        try:
            result = subprocess.run(
                [
                    candidate,
                    "-c",
                    script,
                    str(host),
                    str(int(port)),
                    str(float(timeout_s)),
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=max(2.0, float(timeout_s) + 2.0),
            )
        except Exception as exc:
            attempts.append(
                {
                    "python": candidate,
                    "ok": False,
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
            )
            continue
        try:
            payload = json.loads((result.stdout or "{}").strip() or "{}")
        except json.JSONDecodeError:
            payload = {"ok": False, "error": "invalid_probe_json", "stdout": result.stdout}
        payload["python"] = candidate
        payload["returncode"] = result.returncode
        if result.stderr:
            payload["stderr_tail"] = result.stderr.splitlines()[-10:]
        attempt_record = dict(payload)
        attempts.append(attempt_record)
        if result.returncode == 0 and payload.get("ok"):
            return {**attempt_record, "attempts": list(attempts)}
    return {
        "ok": False,
        "error": "external_world_probe_failed",
        "candidates": candidates,
        "attempts": attempts,
    }


def _write_report(report_path: Path, rows: List[Dict[str, Any]]) -> None:
    lines: List[str] = [
        f"# CARLA Startup Mode Probe {datetime.now().strftime('%Y-%m-%d %H:%M:%S %z')}",
        "",
        "## Summary",
        "",
        "| mode | label | probe_style | classification | status | rpc_ready | world_ready | extra_args |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row['mode']}`",
                    row["label"],
                    f"`{row['probe_style']}`",
                    f"`{row['classification']}`",
                    f"`{row['status']}`",
                    f"`{row['rpc_ready']}`",
                    f"`{row['world_ready']}`",
                    f"`{row['extra_args']}`",
                ]
            )
            + " |"
        )
    for row in rows:
        lines.extend(
            [
                "",
                f"## {row['label']}",
                "",
                f"- mode: `{row['mode']}`",
                f"- probe_style: `{row['probe_style']}`",
                f"- classification: `{row['classification']}`",
                f"- status: `{row['status']}`",
                f"- rpc_ready: `{row['rpc_ready']}`",
                f"- world_ready: `{row['world_ready']}`",
                f"- extra_args: `{row['extra_args']}`",
            ]
        )
        error = row.get("error")
        if error:
            lines.append(f"- error: `{error}`")
        diagnostics = row.get("launcher_diagnostics") or {}
        process_pid = diagnostics.get("process_pid")
        process_alive = diagnostics.get("process_alive")
        if process_pid is not None:
            lines.append(f"- process_pid: `{process_pid}`")
        if process_alive is not None:
            lines.append(f"- process_alive: `{process_alive}`")
        port_snapshot = diagnostics.get("target_port_snapshot") or []
        if port_snapshot:
            lines.extend(["- target_port_snapshot:"])
            for item in port_snapshot:
                if not isinstance(item, dict):
                    continue
                port = item.get("port")
                opened = item.get("open")
                ss_lines = item.get("ss_lines") or []
                lines.append(f"  - `{port}` open=`{opened}` listeners={len(ss_lines)}")
        launch_records = diagnostics.get("launch_records") or []
        if launch_records:
            last = launch_records[-1]
            exit_code = last.get("exit_code")
            if exit_code is not None:
                lines.append(f"- last_exit_code: `{exit_code}`")
            if last.get("server_log_tail"):
                lines.append("- server_log_tail_head:")
                for line in (last.get("server_log_tail") or [])[:8]:
                    lines.append(f"  - `{str(line)}`")
        latest_server_tail = diagnostics.get("latest_server_log_tail") or []
        if latest_server_tail:
            lines.append("- latest_server_log_tail_tail:")
            for line in latest_server_tail[-8:]:
                lines.append(f"  - `{str(line)}`")
        latest_ue_tail = diagnostics.get("latest_ue_log_tail") or []
        if latest_ue_tail:
            lines.append("- latest_ue_log_tail_tail:")
            for line in latest_ue_tail[-8:]:
                lines.append(f"  - `{str(line)}`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe CARLA startup modes under the unified launcher")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--carla-root", default="")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--town", default="Town01")
    parser.add_argument("--timeout-sec", type=float, default=20.0)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument(
        "--probe-style",
        choices=("active", "passive"),
        default="active",
        help="active=probe RPC/world readiness, passive=do not touch CARLA client and only observe startup state",
    )
    parser.add_argument(
        "--modes",
        default="default_display,lowres_window,render_offscreen,lowres_low_quality",
        help="Comma-separated preset ids",
    )
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    artifacts_dir = batch_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    dotenv = _load_dotenv()
    carla_root = Path(args.carla_root or os.environ.get("CARLA_ROOT") or dotenv.get("CARLA_ROOT") or "").expanduser().resolve()
    if not carla_root.exists():
        raise SystemExit("CARLA_ROOT not set or invalid")

    mode_ids = [item.strip() for item in str(args.modes).split(",") if item.strip()]
    rows: List[Dict[str, Any]] = []

    for mode_id in mode_ids:
        preset = MODE_PRESETS.get(mode_id)
        if preset is None:
            raise SystemExit(f"Unknown mode preset: {mode_id}")
        run_dir = batch_root / mode_id
        run_dir.mkdir(parents=True, exist_ok=True)
        launcher = CarlaLauncher(
            carla_root=carla_root,
            host="127.0.0.1",
            port=int(args.carla_port),
            town=str(args.town),
            extra_args=str(preset["extra_args"]),
            foreground=False,
            run_dir=run_dir,
            stop_reused_on_exit=False,
        )
        row: Dict[str, Any] = {
            "mode": mode_id,
            "label": str(preset["label"]),
            "extra_args": str(preset["extra_args"]),
            "probe_style": str(args.probe_style),
            "status": "starting",
            "rpc_ready": False,
            "world_ready": False,
        }
        try:
            launcher.start()
            if args.probe_style == "passive":
                deadline = time.time() + float(args.timeout_sec)
                while time.time() < deadline:
                    if launcher.proc is not None and launcher.proc.poll() is not None:
                        row["status"] = "passive_process_exited"
                        break
                    sleep_s = min(float(args.poll_sec), max(0.1, deadline - time.time()))
                    if sleep_s <= 0:
                        break
                    time.sleep(sleep_s)
                else:
                    row["status"] = "passive_wait_complete"
            else:
                if launcher.wait_ready(timeout_s=float(args.timeout_sec), poll_s=min(float(args.poll_sec), 1.0)):
                    row["status"] = "rpc_ready"
                    row["rpc_ready"] = True
                    world_probe = _external_world_probe(
                        host="127.0.0.1",
                        port=int(args.carla_port),
                        timeout_s=float(args.timeout_sec),
                    )
                    row["world_probe"] = world_probe
                    if world_probe.get("ok"):
                        row["status"] = "world_ready"
                        row["world_ready"] = True
                        row["world_map"] = str(world_probe.get("world_map") or "")
                    else:
                        row["status"] = "world_not_ready"
                        row["error"] = str(world_probe.get("error") or "world_not_ready")
                else:
                    row["status"] = "rpc_not_ready"
                    row["error"] = "rpc_not_ready"
        except Exception as exc:
            row["status"] = "launcher_error"
            row["error"] = repr(exc)
        finally:
            row["launcher_diagnostics"] = launcher.diagnostics_snapshot(probe_rpc=(args.probe_style != "passive"))
            row["classification"] = _classify_attempt(row)
            rows.append(row)
            write_json(artifacts_dir / "carla_startup_mode_probe.json", {"rows": rows})
            try:
                launcher.stop()
            except Exception:
                pass

    report_path = artifacts_dir / "carla_startup_mode_probe_report.md"
    _write_report(report_path, rows)
    print(f"[carla-startup-probe] wrote {artifacts_dir / 'carla_startup_mode_probe.json'}")
    print(f"[carla-startup-probe] wrote {report_path}")


if __name__ == "__main__":
    main()
