#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.run_unified_calibration_pipeline import (
    _baseline_reference_summary_markdown,
    _baseline_reference_summary_payload,
)


DEFAULT_INPUT = "artifacts/calibration_baseline_reference_followup_weekend_20260328.json"
DEFAULT_OUTPUT_JSON = "artifacts/calibration_baseline_reference_summary_followup_weekend_20260328.json"
DEFAULT_OUTPUT_MD = "artifacts/calibration_baseline_reference_summary_followup_weekend_20260328.md"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a compact baseline reference summary from a baseline reference artifact.")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-json", default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD)
    args = parser.parse_args()

    input_path = Path(args.input).expanduser()
    if not input_path.is_absolute():
        input_path = (REPO_ROOT / input_path).resolve()
    output_json_path = Path(args.output_json).expanduser()
    if not output_json_path.is_absolute():
        output_json_path = (REPO_ROOT / output_json_path).resolve()
    output_md_path = Path(args.output_md).expanduser()
    if not output_md_path.is_absolute():
        output_md_path = (REPO_ROOT / output_md_path).resolve()

    payload = _load_json(input_path)
    summary = _baseline_reference_summary_payload(payload)

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.write_text(_baseline_reference_summary_markdown(summary), encoding="utf-8")
    print(f"[calibration-baseline-reference-summary] input={input_path}")
    print(f"[calibration-baseline-reference-summary] written_json={output_json_path}")
    print(f"[calibration-baseline-reference-summary] written_md={output_md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
