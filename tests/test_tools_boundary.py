from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import yaml

ALLOWLIST_PATH = Path("configs/tools_boundary_allowlist.yaml")
TOOLS_DIR = Path("tools")
MAX_NEW_RUN_TOOL_LINES = 300
BOUNDARY_HEADERS = ("OPERATIONAL HELPER", "LEGACY")


def test_tools_boundary_allowlist_is_well_formed() -> None:
    cfg = _load_allowlist()
    assert cfg["schema_version"] == "tools_boundary_allowlist.v1"
    entries = cfg["entries"]
    assert entries
    seen: set[str] = set()
    for entry in entries:
        path = str(entry.get("path") or "")
        assert path.startswith("tools/run_") and path.endswith(".py")
        assert path not in seen
        seen.add(path)
        assert Path(path).exists(), path
        assert entry.get("status") in {"legacy", "operational_helper"}
        assert entry.get("migration_target"), path
        assert entry.get("allowed_to_grow") is False, path


def test_allowlisted_run_tools_have_boundary_headers() -> None:
    for path in _allowlisted_paths():
        header = "\n".join(path.read_text(encoding="utf-8").splitlines()[:12])
        assert any(marker in header for marker in BOUNDARY_HEADERS), path
        assert "Do not add new platform logic here" in header, path


def test_new_run_tools_stay_thin_and_runtime_neutral() -> None:
    allowlisted = _allowlisted_paths()
    violations: list[str] = []
    for path in sorted(TOOLS_DIR.glob("run_*.py")):
        if path in allowlisted:
            continue
        text = path.read_text(encoding="utf-8")
        line_count = len(text.splitlines())
        if line_count > MAX_NEW_RUN_TOOL_LINES:
            violations.append(
                f"{path}: {line_count} lines exceeds {MAX_NEW_RUN_TOOL_LINES}; add to allowlist only as legacy debt"
            )
        patterns = _core_logic_patterns(path, text)
        if patterns:
            violations.append(f"{path}: move core/runtime logic to package modules ({', '.join(patterns)})")
    assert violations == []


def test_non_allowlisted_run_tools_delegate_to_package_modules_or_stay_small() -> None:
    allowlisted = _allowlisted_paths()
    offenders: list[str] = []
    for path in sorted(TOOLS_DIR.glob("run_*.py")):
        if path in allowlisted:
            continue
        text = path.read_text(encoding="utf-8")
        line_count = len(text.splitlines())
        imports_package = "carla_testbed." in text
        if line_count > 180 and not imports_package:
            offenders.append(f"{path}: medium-sized run tool should delegate to carla_testbed package modules")
    assert offenders == []


def _core_logic_patterns(path: Path, text: str) -> list[str]:
    patterns: list[str] = []
    tree = ast.parse(text, filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name == "carla" or name.startswith("carla."):
                    patterns.append("direct CARLA import")
                if name == "cyber" or name.startswith("cyber."):
                    patterns.append("direct CyberRT import")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "carla" or module.startswith("carla."):
                patterns.append("direct CARLA import")
            if module == "cyber" or module.startswith("cyber."):
                patterns.append("direct CyberRT import")
        elif isinstance(node, ast.ClassDef):
            patterns.append("class definition")
        elif isinstance(node, ast.FunctionDef):
            if node.name != "main" and _function_line_count(node) > 120:
                patterns.append(f"large function {node.name}")
    if "from dataclasses import" in text or "@dataclass" in text:
        patterns.append("dataclass")
    return sorted(set(patterns))


def _function_line_count(node: ast.FunctionDef) -> int:
    if node.end_lineno is None or node.lineno is None:
        return 0
    return int(node.end_lineno) - int(node.lineno) + 1


def _allowlisted_paths() -> set[Path]:
    entries = _load_allowlist()["entries"]
    return {Path(str(entry["path"])) for entry in entries}


def _load_allowlist() -> dict[str, Any]:
    payload = yaml.safe_load(ALLOWLIST_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    assert isinstance(payload.get("entries"), list)
    return payload
