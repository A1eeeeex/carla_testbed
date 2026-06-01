from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("tools/prepare_town01_direct_canary.py")


def test_prepare_direct_canary_prints_strict_command(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--batch-id",
            "test_canary",
            "--out-root",
            str(tmp_path / "runs" / "ab"),
            "--marker",
            str(tmp_path / "canary_root.txt"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["run_root"].endswith("runs/ab/test_canary")
    assert payload["marker"].endswith("canary_root.txt")
    assert "tools/run_town01_direct_ab.py" in payload["command"]
    assert "--routes 097" in payload["command"]
    assert "--require-steering-normalization-mode legacy_double_percent" in payload["command"]
    assert "--require-direct-control-apply-mode frame_flush_only" in payload["command"]
    assert "--require-direct-stale-world-frame-policy always_republish" in payload["command"]
    assert "--require-direct-transport-contract-aligned" in payload["command"]
    assert "--require-direct-bridge-cadence-ratio-min 0.8" in payload["command"]
    assert "--require-hard-gate-pass" not in payload["command"]


def test_prepare_direct_canary_can_write_script_with_hard_gate(tmp_path: Path) -> None:
    script_path = tmp_path / "run_canary.sh"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--batch-id",
            "hard_gate_canary",
            "--routes",
            "097,217,031",
            "--require-hard-gate-pass",
            "--write-script",
            str(script_path),
            "--marker",
            str(tmp_path / "hard_gate_root.txt"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["script_path"] == str(script_path)
    script = script_path.read_text(encoding="utf-8")
    assert script.startswith("#!/usr/bin/env bash")
    assert "RUN_ROOT=runs/ab/hard_gate_canary" in script
    assert "tee " in script
    assert "--routes 097,217,031" in script
    assert "--require-hard-gate-pass" in script
    assert "--require-direct-bridge-cadence-ratio-min 0.8" in script
