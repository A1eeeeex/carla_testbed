#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_postprocess import run_phase1_postprocess  # noqa: E402
from carla_testbed.experiments.phase1_scenario_binding import bind_phase1_scenario_to_run  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Explicitly bind an existing run to a Phase 1 scenario spec; does not prove behavior."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument(
        "--role-alias",
        action="append",
        default=[],
        help="Canonical-to-runtime role alias, e.g. lead_vehicle=front. Repeatable.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--postprocess", action="store_true", help="Run Phase 1 postprocess after binding.")
    args = parser.parse_args(argv)

    report = bind_phase1_scenario_to_run(
        args.run_dir,
        args.scenario,
        role_aliases=_parse_aliases(args.role_alias),
        overwrite=args.overwrite,
    )
    payload: dict[str, object] = {"binding": report}
    if args.postprocess:
        payload["postprocess"] = run_phase1_postprocess(args.run_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _parse_aliases(items: list[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        key = key.strip()
        value = value.strip()
        if not sep or not key or not value:
            raise SystemExit(f"invalid --role-alias {item!r}; expected canonical=runtime")
        aliases[key] = value
    return aliases


if __name__ == "__main__":
    raise SystemExit(main())
