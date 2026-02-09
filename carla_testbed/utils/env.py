from __future__ import annotations

import os
from pathlib import Path
import yaml


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_local_config(repo_root: Path) -> dict:
    cfg_path = repo_root / "configs" / "local.yaml"
    if cfg_path.exists():
        with open(cfg_path, "r") as f:
            return yaml.safe_load(f) or {}
    return {}


def resolve_carla_root(override: str | None = None) -> Path:
    repo_root = resolve_repo_root()
    local_cfg = load_local_config(repo_root)
    root = override or os.environ.get("CARLA_ROOT") or local_cfg.get("carla", {}).get("root")
    if not root:
        raise RuntimeError(
            "CARLA_ROOT 未设置。请设置环境变量 CARLA_ROOT 或在 configs/local.yaml 中配置 carla.root。\n"
            "示例：export CARLA_ROOT=/path/to/CARLA_0.9.16"
        )
    root_path = Path(root).expanduser().resolve()
    if not root_path.exists():
        raise RuntimeError(f"CARLA_ROOT 路径不存在: {root_path}")
    return root_path


def resolve_carla_wheel_dir(carla_root: Path) -> Path:
    wheel_dir = carla_root / "PythonAPI" / "carla" / "dist"
    if not wheel_dir.exists():
        raise RuntimeError(f"未找到 CARLA wheel 目录: {wheel_dir}")
    wheels = list(wheel_dir.glob("carla-*.whl"))
    if not wheels:
        raise RuntimeError(f"{wheel_dir} 下未找到 carla-*.whl，请先编译或下载 wheel")
    return wheel_dir
