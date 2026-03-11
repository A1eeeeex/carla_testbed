#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.env import (
    list_local_config_files,
    load_host_config,
    resolve_repo_root,
)


def _path_status(path_text: str, repo_root: Path) -> Dict[str, Any]:
    p = Path(path_text).expanduser()
    if not p.is_absolute():
        p = (repo_root / p).resolve()
    return {
        "raw": path_text,
        "resolved": str(p),
        "exists": p.exists(),
        "is_dir": p.is_dir(),
    }


def _collect_checks(host_cfg: Dict[str, Any], repo_root: Path) -> Dict[str, Dict[str, Any]]:
    paths = host_cfg.get("paths", {}) if isinstance(host_cfg.get("paths"), dict) else {}
    checks: Dict[str, Dict[str, Any]] = {}
    for key in [
        "carla_root",
        "apollo_root",
        "ros_workspace",
        "data_root",
        "log_root",
        "map_root",
        "model_root",
        "output_root",
    ]:
        raw = str(paths.get(key) or "").strip()
        if not raw:
            checks[key] = {"configured": False}
            continue
        checks[key] = {"configured": True, **_path_status(raw, repo_root)}
    return checks


def _render_text(repo_root: Path, local_files: List[Path], checks: Dict[str, Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append(f"repo_root: {repo_root}")
    lines.append("local_config_files:")
    if local_files:
        for path in local_files:
            lines.append(f"  - {path}")
    else:
        lines.append("  - (none)")
    lines.append("")
    lines.append("path_checks:")
    for key, value in checks.items():
        if not value.get("configured"):
            lines.append(f"  - {key}: NOT SET")
            continue
        state = "OK" if value.get("exists") else "MISSING"
        lines.append(f"  - {key}: {state} ({value.get('resolved')})")
    lines.append("")
    lines.append("env_overrides:")
    lines.append(f"  - CARLA_ROOT={os.environ.get('CARLA_ROOT', '')}")
    lines.append(f"  - APOLLO_ROOT={os.environ.get('APOLLO_ROOT', '')}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="检查本机私有配置是否可用")
    ap.add_argument("--repo-root", type=Path, default=None, help="仓库根目录（默认自动推断）")
    ap.add_argument("--strict", action="store_true", help="发现缺失路径时返回非 0")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    args = ap.parse_args()

    repo_root = (args.repo_root or resolve_repo_root()).resolve()
    local_files = list_local_config_files(repo_root)
    host_cfg = load_host_config(repo_root)
    checks = _collect_checks(host_cfg, repo_root)

    missing_config = [k for k, v in checks.items() if v.get("configured") and (not v.get("exists"))]
    payload = {
        "repo_root": str(repo_root),
        "local_config_files": [str(p) for p in local_files],
        "checks": checks,
        "missing_configured_paths": missing_config,
    }

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(_render_text(repo_root, local_files, checks))
        if not local_files:
            print("\n[WARN] 未检测到 local 配置文件，当前仅使用 project.yaml 与环境变量。")

    if args.strict and missing_config:
        print(f"\n[ERROR] 配置了但不存在的路径: {', '.join(missing_config)}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
