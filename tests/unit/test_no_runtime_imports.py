from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

FORBIDDEN_IMPORT_PATTERNS = [
    re.compile(r"^\s*import\s+carla\b", re.MULTILINE),
    re.compile(r"^\s*from\s+carla\b", re.MULTILINE),
    re.compile(r"^\s*import\s+rclpy\b", re.MULTILINE),
    re.compile(r"^\s*from\s+rclpy\b", re.MULTILINE),
    re.compile(r"^\s*import\s+cyber\b", re.MULTILINE),
    re.compile(r"^\s*from\s+cyber\b", re.MULTILINE),
    re.compile(r"^\s*import\s+cyber_py\b", re.MULTILINE),
    re.compile(r"^\s*from\s+cyber_py\b", re.MULTILINE),
    re.compile(r"apollo.*pb2"),
]


def _python_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("*.py"))


def test_core_contracts_and_adapter_base_do_not_import_runtime_stacks() -> None:
    targets = [
        ROOT / "carla_testbed" / "core",
        ROOT / "carla_testbed" / "contracts",
        ROOT / "carla_testbed" / "adapters" / "base.py",
    ]
    violations: list[str] = []

    for target in targets:
        for path in _python_files(target):
            text = path.read_text(encoding="utf-8")
            for pattern in FORBIDDEN_IMPORT_PATTERNS:
                if pattern.search(text):
                    violations.append(f"{path.relative_to(ROOT)} matches {pattern.pattern}")

    assert not violations, "runtime stack imports leaked into core boundaries:\n" + "\n".join(violations)


def test_record_package_lightweight_import_does_not_import_carla() -> None:
    sys.modules.pop("carla", None)
    record = importlib.import_module("carla_testbed.record")

    assert "carla" not in sys.modules
    assert record.ROUTE_CURVE_P0_FIELDS


def test_runner_and_sim_package_imports_do_not_import_carla() -> None:
    sys.modules.pop("carla", None)
    runner = importlib.import_module("carla_testbed.runner")
    sim = importlib.import_module("carla_testbed.sim")

    assert "carla" not in sys.modules
    assert runner.FrameContext
    assert "tick_world" in sim.__all__
