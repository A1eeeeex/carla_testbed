#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def main() -> int:
    parser = argparse.ArgumentParser(description="Compile a fixed-scene template into a resolved storyboard")
    parser.add_argument("--template", required=True, help="fixed_scene_template YAML or scenario YAML with fixed_scene block")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args()

    template = load_fixed_scene_template(args.template)
    storyboard = compile_fixed_scene_template(template)
    out_dir = Path(args.out).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    output = out_dir / "fixed_scene_resolved.json"
    output.write_text(json.dumps(storyboard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
