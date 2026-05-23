#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import os
import shlex
import shutil
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.run_town01_demo_showcase import (
    AUTO_BROWSER_CMD,
    DEFAULT_CHAIN,
    DEFAULT_CONFIG,
    PLAYWRIGHT_BROWSER_CMD,
    _parse_capture_region,
    resolve_dreamview_browser_cmd,
)

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_demo_readiness_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_demo_readiness_20260522.md"

WhichFn = Callable[[str], str | None]
ImportFn = Callable[[str], object]


def _check(name: str, status: str, detail: str, *, severity: str = "info", data: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "severity": severity,
        "detail": detail,
        "data": data or {},
    }


def _command_binary(command: str, url: str = "http://127.0.0.1:8888") -> str:
    raw = str(command or "").strip()
    if not raw:
        return ""
    if "{url}" in raw:
        raw = raw.replace("{url}", url)
    else:
        raw = f"{raw} {url}"
    parts = shlex.split(raw)
    return parts[0] if parts else ""


def _binary_exists(binary: str, *, which: WhichFn) -> bool:
    if not binary:
        return False
    path = Path(binary).expanduser()
    if path.is_absolute() or "/" in binary:
        return path.exists()
    return which(binary) is not None


def build_readiness_payload(
    *,
    config_path: Path = DEFAULT_CONFIG,
    chain_path: Path = DEFAULT_CHAIN,
    browser_cmd: str = AUTO_BROWSER_CMD,
    capture_mode: str = "tick_snapshot",
    capture_region: str = "1280x720+0,0",
    use_fixed_region: bool = True,
    require_browser: bool = True,
    require_video_encoder: bool = True,
    which: WhichFn = shutil.which,
    importer: ImportFn = importlib.import_module,
    env: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    env_map = env if env is not None else os.environ
    resolved_config = config_path.expanduser().resolve()
    resolved_chain = chain_path.expanduser().resolve()
    checks.append(
        _check(
            "demo_config",
            "ok" if resolved_config.exists() else "failed",
            str(resolved_config),
            severity="error" if not resolved_config.exists() else "info",
        )
    )
    checks.append(
        _check(
            "online_chain_script",
            "ok" if resolved_chain.exists() else "failed",
            str(resolved_chain),
            severity="error" if not resolved_chain.exists() else "info",
        )
    )

    resolved_browser_cmd, browser_source = resolve_dreamview_browser_cmd(browser_cmd, which=which)
    binary = _command_binary(resolved_browser_cmd)
    browser_ok = _binary_exists(binary, which=which)
    fallback_ok = which("xdg-open") is not None or which("gio") is not None
    playwright_unavailable = browser_source == "playwright:unavailable"
    if resolved_browser_cmd:
        checks.append(
            _check(
                "dreamview_browser_command",
                "ok" if browser_ok else ("failed" if require_browser else "warn"),
                binary or "empty browser command",
                severity="error" if require_browser and not browser_ok else ("warning" if not browser_ok else "info"),
                data={"browser_cmd": resolved_browser_cmd, "browser_source": browser_source},
            )
        )
    else:
        checks.append(
            _check(
                "dreamview_browser_command",
                "failed" if playwright_unavailable and require_browser else ("ok" if fallback_ok else ("failed" if require_browser else "warn")),
                (
                    "Playwright wrapper/global CLI unavailable"
                    if playwright_unavailable
                    else ("xdg-open/gio fallback" if fallback_ok else "no browser command or opener found")
                ),
                severity=(
                    "error"
                    if require_browser and (playwright_unavailable or not fallback_ok)
                    else ("warning" if not fallback_ok else "info")
                ),
                data={"browser_cmd": "", "browser_source": browser_source},
            )
        )

    try:
        region = _parse_capture_region(capture_region)
        region_status = "ok" if (region is not None or not use_fixed_region) else "failed"
        region_detail = "not required" if region is None and not use_fixed_region else str(region)
    except Exception as exc:
        region_status = "failed"
        region_detail = str(exc)
    checks.append(
        _check(
            "dreamview_capture_region",
            region_status,
            region_detail,
            severity="error" if region_status == "failed" else "info",
            data={"capture_region": capture_region, "use_fixed_region": bool(use_fixed_region)},
        )
    )

    mode = str(capture_mode or "").strip().lower()
    if mode == "tick_snapshot":
        try:
            importer("PIL.ImageGrab")
            pil_status = "ok"
            pil_detail = "PIL.ImageGrab import ok"
        except Exception as exc:
            pil_status = "failed"
            pil_detail = str(exc)
        checks.append(
            _check(
                "dreamview_tick_snapshot_dependency",
                pil_status,
                pil_detail,
                severity="error" if pil_status == "failed" else "info",
            )
        )

    ffmpeg = which("ffmpeg")
    checks.append(
        _check(
            "dreamview_video_encoder",
            "ok" if ffmpeg else ("failed" if require_video_encoder else "warn"),
            ffmpeg or "ffmpeg not found",
            severity="error" if require_video_encoder and not ffmpeg else ("warning" if not ffmpeg else "info"),
        )
    )

    display = str(env_map.get("DISPLAY") or "").strip()
    checks.append(
        _check(
            "display_environment",
            "ok" if display else "warn",
            display or "DISPLAY is empty; browser/screenshot capture may need an active desktop or virtual display",
            severity="warning" if not display else "info",
        )
    )

    failed = [item for item in checks if item.get("status") == "failed"]
    warnings = [item for item in checks if item.get("status") == "warn"]
    if failed:
        status = "failed"
    elif warnings:
        status = "ready_with_warnings"
    else:
        status = "ready"
    return {
        "status": status,
        "failed_count": len(failed),
        "warning_count": len(warnings),
        "checks": checks,
        "recommended_demo_command": (
            "/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/run_town01_demo_showcase.py "
            "--mode short --record-dreamview --dreamview-auto-open "
            "--dreamview-open-wait-page "
            f"--dreamview-browser-cmd {shlex.quote(browser_cmd)} "
            f"--dreamview-capture-mode {shlex.quote(capture_mode)} "
            f"--dreamview-capture-region {shlex.quote(capture_region)} "
            "--dreamview-use-fixed-region --require-recording-ready"
        ),
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Demo Readiness",
        "",
        f"- status: `{payload['status']}`",
        f"- failed_count: `{payload['failed_count']}`",
        f"- warning_count: `{payload['warning_count']}`",
        "",
        "| check | status | severity | detail |",
        "|---|---|---|---|",
    ]
    for item in payload.get("checks", []):
        lines.append(
            "| `{}` | `{}` | `{}` | {} |".format(
                item.get("name", ""),
                item.get("status", ""),
                item.get("severity", ""),
                str(item.get("detail", "")).replace("|", "\\|"),
            )
        )
    lines.extend(
        [
            "",
            "## Recommended Demo Command",
            "",
            "```bash",
            str(payload.get("recommended_demo_command", "")),
            "```",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check local readiness for automated Town01 CARLA + Dreamview demo recording.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--chain", type=Path, default=DEFAULT_CHAIN)
    parser.add_argument(
        "--browser-cmd",
        default=PLAYWRIGHT_BROWSER_CMD,
        help="Use 'auto' to pick a browser/fallback opener, or 'playwright' for the local Playwright CLI wrapper.",
    )
    parser.add_argument("--capture-mode", default="tick_snapshot")
    parser.add_argument("--capture-region", default="1280x720+0,0")
    parser.add_argument("--use-fixed-region", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-browser", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-video-encoder", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_readiness_payload(
        config_path=args.config,
        chain_path=args.chain,
        browser_cmd=str(args.browser_cmd),
        capture_mode=str(args.capture_mode),
        capture_region=str(args.capture_region),
        use_fixed_region=bool(args.use_fixed_region),
        require_browser=bool(args.require_browser),
        require_video_encoder=bool(args.require_video_encoder),
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"status": payload["status"], "failed_count": payload["failed_count"], "warning_count": payload["warning_count"]}, indent=2))
    raise SystemExit(2 if payload["status"] == "failed" else 0)


if __name__ == "__main__":
    main()
