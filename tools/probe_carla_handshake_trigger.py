#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
import sys
import time
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tbio.carla.launcher import CarlaLauncher, _load_carla_module
from tools.probe_carla_startup_modes import MODE_PRESETS
from tools.run_town01_route_health import _load_dotenv, write_json


def _tail_has_eof_bridge(tail: list[str]) -> bool:
    lowered = "\n".join(str(item) for item in tail).lower()
    return "error retrieving stream id" in lowered or "failed to read header" in lowered


def _classify_handshake_effect(pre_tail: list[str], post_tail: list[str], post_ports_open: bool) -> str:
    pre_has_eof = _tail_has_eof_bridge(pre_tail)
    post_has_eof = _tail_has_eof_bridge(post_tail)
    if pre_has_eof:
        return "eof_already_present_before_handshake"
    if post_has_eof:
        return "handshake_triggered_eof_bridge"
    if post_ports_open:
        return "handshake_no_eof_ports_still_open"
    return "handshake_no_eof_no_listener"


def _write_report(report_path: Path, row: Dict[str, Any]) -> None:
    pre = row.get("pre_handshake") or {}
    post = row.get("post_handshake") or {}
    lines = [
        f"# CARLA Handshake Trigger Probe {datetime.now().strftime('%Y-%m-%d %H:%M:%S %z')}",
        "",
        "## Summary",
        "",
        f"- mode: `{row['mode']}`",
        f"- extra_args: `{row['extra_args']}`",
        f"- python_executable: `{row.get('python_executable')}`",
        f"- python_version: `{row.get('python_version')}`",
        f"- passive_wait_sec: `{row['passive_wait_sec']}`",
        f"- post_handshake_wait_sec: `{row['post_handshake_wait_sec']}`",
        f"- handshake_status: `{row['handshake_status']}`",
        f"- handshake_classification: `{row.get('handshake_classification')}`",
    ]
    if row.get("handshake_error"):
        lines.append(f"- handshake_error: `{row['handshake_error']}`")
    if row.get("server_version"):
        lines.append(f"- server_version: `{row['server_version']}`")
    lines.extend(
        [
            "",
            "## Pre Handshake",
            "",
            f"- process_alive: `{pre.get('process_alive')}`",
            f"- target_ports_open: `{[(item.get('port'), item.get('open')) for item in (pre.get('target_port_snapshot') or [])]}`",
        ]
    )
    pre_tail = pre.get("latest_server_log_tail") or []
    if pre_tail:
        lines.append("- latest_server_log_tail_tail:")
        for line in pre_tail[-8:]:
            lines.append(f"  - `{str(line)}`")
    lines.extend(
        [
            "",
            "## Post Handshake",
            "",
            f"- process_alive: `{post.get('process_alive')}`",
            f"- target_ports_open: `{[(item.get('port'), item.get('open')) for item in (post.get('target_port_snapshot') or [])]}`",
        ]
    )
    post_tail = post.get("latest_server_log_tail") or []
    if post_tail:
        lines.append("- latest_server_log_tail_tail:")
        for line in post_tail[-8:]:
            lines.append(f"  - `{str(line)}`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe whether a late CARLA client handshake triggers the EOF bridge")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--mode", default="lowres_low_quality", choices=sorted(MODE_PRESETS.keys()))
    parser.add_argument("--carla-root", default="")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--town", default="Town01")
    parser.add_argument("--passive-wait-sec", type=float, default=60.0)
    parser.add_argument("--post-handshake-wait-sec", type=float, default=5.0)
    parser.add_argument("--handshake-timeout-sec", type=float, default=5.0)
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    artifacts_dir = batch_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    dotenv = _load_dotenv()
    carla_root = Path(args.carla_root or os.environ.get("CARLA_ROOT") or dotenv.get("CARLA_ROOT") or "").expanduser().resolve()
    if not carla_root.exists():
        raise SystemExit("CARLA_ROOT not set or invalid")

    preset = MODE_PRESETS[str(args.mode)]
    run_dir = batch_root / str(args.mode)
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
        "mode": str(args.mode),
        "extra_args": str(preset["extra_args"]),
        "passive_wait_sec": float(args.passive_wait_sec),
        "post_handshake_wait_sec": float(args.post_handshake_wait_sec),
        "handshake_status": "not_run",
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
    }
    try:
        carla = _load_carla_module()
    except Exception as exc:
        row["handshake_status"] = "preflight_import_failed"
        row["handshake_error"] = repr(exc)
        write_json(artifacts_dir / "carla_handshake_trigger_probe.json", row)
        _write_report(artifacts_dir / "carla_handshake_trigger_probe_report.md", row)
        raise SystemExit(f"CARLA Python API import failed before launch: {exc}") from exc
    try:
        launcher.start()
        time.sleep(float(args.passive_wait_sec))
        pre = launcher.diagnostics_snapshot(probe_rpc=False)
        row["pre_handshake"] = pre

        try:
            client = carla.Client("127.0.0.1", int(args.carla_port))
            client.set_timeout(float(args.handshake_timeout_sec))
            row["server_version"] = str(client.get_server_version())
            row["handshake_status"] = "server_version_ok"
        except Exception as exc:
            row["handshake_status"] = "server_version_failed"
            row["handshake_error"] = repr(exc)

        time.sleep(float(args.post_handshake_wait_sec))
        post = launcher.diagnostics_snapshot(probe_rpc=False)
        row["post_handshake"] = post
        row["handshake_classification"] = _classify_handshake_effect(
            list(pre.get("latest_server_log_tail") or []),
            list(post.get("latest_server_log_tail") or []),
            any(bool(item.get("open")) for item in (post.get("target_port_snapshot") or []) if isinstance(item, dict)),
        )
        write_json(artifacts_dir / "carla_handshake_trigger_probe.json", row)
        _write_report(artifacts_dir / "carla_handshake_trigger_probe_report.md", row)
        print(f"[carla-handshake-probe] wrote {artifacts_dir / 'carla_handshake_trigger_probe.json'}")
        print(f"[carla-handshake-probe] wrote {artifacts_dir / 'carla_handshake_trigger_probe_report.md'}")
    finally:
        try:
            launcher.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
