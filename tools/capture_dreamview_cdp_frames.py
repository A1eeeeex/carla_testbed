#!/usr/bin/env python3
"""Capture Apollo Dreamview frames directly through Chrome DevTools Protocol."""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from dreamview_chrome_cdp_auto import _CDPWebSocket, _target_ws_url


def _connect(port: int, url: str) -> _CDPWebSocket:
    client = _CDPWebSocket(_target_ws_url(port, url))
    client.call("Page.bringToFront", {}, timeout_s=2.0)
    client.call("Page.enable", {}, timeout_s=2.0)
    return client


def capture_frames(url: str, port: int, out_dir: Path, duration_s: float, fps: float) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    index_path = out_dir / "frames_index.jsonl"
    target_interval = 1.0 / max(fps, 0.1)
    deadline = time.time() + max(duration_s, 0.1)
    frame_idx = 0
    failures = 0
    client: _CDPWebSocket | None = None
    started_at = time.time()
    with index_path.open("w", encoding="utf-8") as index_fp:
        while time.time() < deadline:
            frame_start = time.time()
            try:
                if client is None:
                    client = _connect(port, url)
                response = client.call(
                    "Page.captureScreenshot",
                    {"format": "png", "captureBeyondViewport": False},
                    timeout_s=5.0,
                )
                data = ((response.get("result") or {}).get("data") or "")
                if not data:
                    raise RuntimeError("empty screenshot payload")
                path = out_dir / f"{frame_idx:06d}.png"
                path.write_bytes(base64.b64decode(data))
                index_fp.write(
                    json.dumps(
                        {
                            "frame": frame_idx,
                            "path": str(path),
                            "captured_at": datetime.now(timezone.utc).isoformat(),
                            "wall_time_s": round(time.time() - started_at, 6),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
                index_fp.flush()
                frame_idx += 1
            except Exception:
                failures += 1
                if client is not None:
                    client.close()
                client = None
                time.sleep(min(1.0, target_interval))
            sleep_s = target_interval - (time.time() - frame_start)
            if sleep_s > 0:
                time.sleep(sleep_s)
    if client is not None:
        client.close()
    return {
        "schema_version": "dreamview_cdp_capture.v1",
        "url": url,
        "port": port,
        "frames_dir": str(out_dir),
        "frame_count": frame_idx,
        "failures": failures,
        "duration_s": round(time.time() - started_at, 3),
        "fps_requested": fps,
        "frames_index": str(index_path),
    }


def encode_video(frames_dir: Path, output: Path, fps: float) -> bool:
    output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-framerate",
        str(fps),
        "-i",
        str(frames_dir / "%06d.png"),
        "-c:v",
        "libx264",
        "-vf",
        "scale=trunc(iw/2)*2:trunc(ih/2)*2",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-movflags",
        "+faststart",
        str(output),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    (output.parent / "ffmpeg_cdp_encode.log").write_text(
        "\n".join(["cmd=" + " ".join(cmd), f"returncode={result.returncode}", result.stdout, result.stderr])
        + "\n",
        encoding="utf-8",
    )
    return result.returncode == 0 and output.exists() and output.stat().st_size > 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8888")
    parser.add_argument("--port", type=int, default=9222)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--duration-sec", type=float, default=30.0)
    parser.add_argument("--fps", type=float, default=10.0)
    parser.add_argument("--encode", action="store_true")
    parser.add_argument("--video-out", default="")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    summary = capture_frames(args.url, args.port, out_dir, args.duration_sec, args.fps)
    if args.encode:
        video_out = Path(args.video_out) if args.video_out else out_dir.parent / "dreamview_cdp_capture.mp4"
        summary["video_path"] = str(video_out)
        summary["video_encoded"] = encode_video(out_dir, video_out, args.fps)
    (out_dir.parent / "dreamview_cdp_capture_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get("frame_count", 0) > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
