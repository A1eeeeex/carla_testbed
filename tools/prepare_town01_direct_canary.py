#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PYTHON = Path("/home/ubuntu/miniconda3/envs/carla16/bin/python3")
DEFAULT_MARKER = Path("/tmp/town01_direct_stale_republish_097_canary_root.txt")


def _default_python() -> str:
    return str(DEFAULT_PYTHON if DEFAULT_PYTHON.exists() else Path(sys.executable))


def _batch_id(routes: str) -> str:
    route_label = routes.replace(",", "_").replace(" ", "")
    return f"town01_direct_stale_republish_{route_label}_canary_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare the next Town01 ros2_gt vs carla_direct canary command without starting CARLA/Apollo."
    )
    parser.add_argument("--routes", default="097", help="Comma-separated canonical route selectors. Default: 097.")
    parser.add_argument("--durations", default="30", help="Comma-separated durations in seconds. Default: 30.")
    parser.add_argument("--out-root", type=Path, default=Path("runs/ab"), help="Parent directory for the batch root.")
    parser.add_argument("--batch-id", help="Optional deterministic batch id for tests or manual naming.")
    parser.add_argument("--python", default=_default_python(), help="Python executable to embed in the command.")
    parser.add_argument("--marker", type=Path, default=DEFAULT_MARKER, help="File that records the generated RUN_ROOT.")
    parser.add_argument("--write-script", type=Path, help="Optional path to write an executable shell script.")
    parser.add_argument(
        "--require-hard-gate-pass",
        action="store_true",
        help="Add hard-gate requirement. Use only when routes include 097,217,031.",
    )
    parser.add_argument(
        "--include-diagnostic-curves",
        action="store_true",
        help="Include diagnostic curve routes in the generated command.",
    )
    return parser


def build_command(args: argparse.Namespace) -> dict[str, str]:
    batch_id = args.batch_id or _batch_id(args.routes)
    run_root = args.out_root / batch_id
    command_parts = [
        args.python,
        "tools/run_town01_direct_ab.py",
        "--route-config",
        "configs/routes/town01/canonical_five.yaml",
        "--durations",
        args.durations,
        "--baseline",
        "ros2_gt",
        "--candidate",
        "carla_direct",
        "--routes",
        args.routes,
        "--continue-on-failure",
        "--carla-ignore-memory-preflight",
        "--analyze-after-run",
        "--require-steering-normalization-mode",
        "legacy_double_percent",
        "--require-direct-control-apply-mode",
        "frame_flush_only",
        "--require-direct-stale-world-frame-policy",
        "always_republish",
        "--require-direct-transport-contract-aligned",
        "--require-direct-bridge-cadence-ratio-min",
        "0.8",
        "--out",
        "$RUN_ROOT",
    ]
    if args.require_hard_gate_pass:
        command_parts.insert(command_parts.index("--require-steering-normalization-mode"), "--require-hard-gate-pass")
    if args.include_diagnostic_curves:
        command_parts.insert(command_parts.index("--continue-on-failure"), "--include-diagnostic-curves")

    run_root_text = str(run_root)
    shell_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"cd {shlex.quote(str(REPO_ROOT))}",
        f'RUN_ROOT={shlex.quote(run_root_text)}',
        f'echo "$RUN_ROOT" | tee {shlex.quote(str(args.marker))}',
        " ".join(shlex.quote(item) if item != "$RUN_ROOT" else item for item in command_parts),
    ]
    return {
        "run_root": run_root_text,
        "marker": str(args.marker),
        "command": shell_lines[-1],
        "script": "\n".join(shell_lines) + "\n",
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_command(args)
    if args.write_script:
        args.write_script.parent.mkdir(parents=True, exist_ok=True)
        args.write_script.write_text(payload["script"], encoding="utf-8")
        args.write_script.chmod(0o755)
        payload["script_path"] = str(args.write_script)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
