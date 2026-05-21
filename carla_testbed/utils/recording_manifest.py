from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


def output_entry(path: Path | str) -> Dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    return {
        "path": str(resolved),
        "exists": resolved.exists(),
        "is_dir": resolved.is_dir(),
        "is_file": resolved.is_file(),
    }


def build_recording_manifest(
    *,
    mode: str,
    expected_outputs: Iterable[Path | str],
    actual_outputs: Iterable[Path | str],
    required_for_acceptance: bool,
    status: str,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_version": 1,
        "mode": str(mode or "").strip(),
        "expected_outputs": [output_entry(item) for item in expected_outputs],
        "actual_outputs": [output_entry(item) for item in actual_outputs],
        "status": str(status or "").strip() or "unknown",
        "required_for_acceptance": bool(required_for_acceptance),
    }
    if extra:
        payload.update(dict(extra))
    return payload


def write_recording_manifest(path: Path | str, payload: Dict[str, Any]) -> Path:
    output_path = Path(path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return output_path


def render_recording_manifest_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = [
        "# Recording Manifest",
        "",
        f"- mode: `{payload.get('mode', '')}`",
        f"- status: `{payload.get('status', '')}`",
        f"- required_for_acceptance: `{payload.get('required_for_acceptance', False)}`",
        "",
        "## Expected Outputs",
        "",
    ]
    expected = list(payload.get("expected_outputs", []) or [])
    if expected:
        for item in expected:
            lines.append(f"- `{item.get('path', '')}` exists=`{item.get('exists', False)}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Actual Outputs", ""])
    actual = list(payload.get("actual_outputs", []) or [])
    if actual:
        for item in actual:
            lines.append(f"- `{item.get('path', '')}` exists=`{item.get('exists', False)}`")
    else:
        lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"
