#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Tuple


def _ensure_paths(apollo_root: Path, pb_root: Path) -> None:
    for path in (apollo_root, apollo_root / "cyber" / "python", pb_root):
        text = str(path.resolve())
        if text not in sys.path:
            sys.path.insert(0, text)


def _import_cyber_and_pb(apollo_root: Path, pb_root: Path):
    _ensure_paths(apollo_root, pb_root)
    try:
        from cyber.python.cyber_py3 import cyber  # type: ignore
    except Exception:
        from cyber_py3 import cyber  # type: ignore
    from modules.common_msgs.routing_msgs import routing_pb2  # type: ignore
    return cyber, routing_pb2


def _parse_xyz(text: str) -> Tuple[float, float, float]:
    parts = [p.strip() for p in text.split(",")]
    if len(parts) not in (2, 3):
        raise ValueError(f"invalid point '{text}', expected x,y[,z]")
    x = float(parts[0])
    y = float(parts[1])
    z = float(parts[2]) if len(parts) == 3 else 0.0
    return x, y, z


def _set_header(header, ts: float, seq: int, module_name: str) -> None:
    if hasattr(header, "timestamp_sec"):
        header.timestamp_sec = ts
    if hasattr(header, "sequence_num"):
        header.sequence_num = seq
    if hasattr(header, "module_name"):
        header.module_name = module_name


def main() -> int:
    ap = argparse.ArgumentParser(description="Send Apollo routing request via CyberRT")
    ap.add_argument("--apollo-root", type=Path, default=Path(os.environ.get("APOLLO_ROOT", "")))
    ap.add_argument("--pb-root", type=Path, default=Path(__file__).resolve().parent / "pb")
    ap.add_argument("--request-channel", default="/apollo/routing_request")
    ap.add_argument("--response-channel", default="/apollo/routing_response")
    ap.add_argument("--start", required=True, help="x,y[,z]")
    ap.add_argument("--end", required=True, help="x,y[,z]")
    ap.add_argument("--wait-response-sec", type=float, default=3.0)
    args = ap.parse_args()

    if not args.apollo_root or not str(args.apollo_root):
        raise RuntimeError("apollo-root is required (or set APOLLO_ROOT)")
    if not args.apollo_root.exists():
        raise RuntimeError(f"apollo-root not found: {args.apollo_root}")
    if not args.pb_root.exists():
        raise RuntimeError(f"pb-root not found: {args.pb_root}; run gen_pb2.sh first")

    cyber, routing_pb2 = _import_cyber_and_pb(args.apollo_root, args.pb_root)
    sx, sy, sz = _parse_xyz(args.start)
    ex, ey, ez = _parse_xyz(args.end)

    got_response = {"ok": False}

    def _on_resp(_msg):
        got_response["ok"] = True
        print("[routing] response received")

    cyber.init("tb_routing_request_sender")
    node = cyber.Node("tb_routing_request_sender")
    writer_fn = getattr(node, "create_writer", None) or getattr(node, "CreateWriter", None)
    reader_fn = getattr(node, "create_reader", None) or getattr(node, "CreateReader", None)
    if writer_fn is None:
        raise RuntimeError("cyber node has no create_writer/CreateWriter")
    if reader_fn is not None:
        reader_fn(args.response_channel, routing_pb2.RoutingResponse, _on_resp)

    writer = writer_fn(args.request_channel, routing_pb2.RoutingRequest)
    req = routing_pb2.RoutingRequest()
    _set_header(req.header, time.time(), 1, "tb_routing_request_sender")

    wp0 = req.waypoint.add()
    wp1 = req.waypoint.add()
    if hasattr(wp0, "pose"):
        wp0.pose.x, wp0.pose.y, wp0.pose.z = sx, sy, sz
        wp1.pose.x, wp1.pose.y, wp1.pose.z = ex, ey, ez
    else:
        # Some proto revisions expose direct x/y.
        if hasattr(wp0, "x"):
            wp0.x, wp0.y, wp0.z = sx, sy, sz
            wp1.x, wp1.y, wp1.z = ex, ey, ez

    writer.write(req)
    print(f"[routing] sent request {args.request_channel} start=({sx},{sy},{sz}) end=({ex},{ey},{ez})")

    deadline = time.time() + max(0.0, args.wait_response_sec)
    while time.time() < deadline and not got_response["ok"]:
        time.sleep(0.1)

    if got_response["ok"]:
        print("[routing] success")
    else:
        print("[routing] no response observed within timeout")

    cyber.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
