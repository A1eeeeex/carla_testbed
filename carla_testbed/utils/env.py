from __future__ import annotations

import os
from pathlib import Path

import yaml

PROJECT_CONFIG_REL = Path("configs/project.yaml")
LOCAL_CONFIG_DIR_REL = Path("configs")
LOCAL_CONFIG_MAIN = "local.yaml"


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _deep_update(base: dict, patch: dict) -> dict:
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _load_yaml_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"配置文件必须是字典结构: {path}")
    return data


def load_project_config(repo_root: Path) -> dict:
    return _load_yaml_dict(repo_root / PROJECT_CONFIG_REL)


def list_local_config_files(repo_root: Path) -> list[Path]:
    cfg_dir = repo_root / LOCAL_CONFIG_DIR_REL
    paths: list[Path] = []

    main_cfg = cfg_dir / LOCAL_CONFIG_MAIN
    if main_cfg.exists():
        paths.append(main_cfg)

    for path in sorted(cfg_dir.glob("local.*.yaml")):
        # local.example.yaml 仅作为样板，不参与运行时配置合并。
        if path.name.endswith(".example.yaml"):
            continue
        paths.append(path)

    extra = str(os.environ.get("CARLA_TESTBED_LOCAL_CONFIGS", "")).strip()
    if extra:
        for raw in extra.split(","):
            token = raw.strip()
            if not token:
                continue
            p = Path(token).expanduser()
            if not p.is_absolute():
                p = (repo_root / p).resolve()
            if p.exists():
                paths.append(p)

    dedup: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        rp = path.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        dedup.append(rp)
    return dedup


def load_local_config(repo_root: Path) -> dict:
    merged: dict = {}
    for cfg_path in list_local_config_files(repo_root):
        merged = _deep_update(merged, _load_yaml_dict(cfg_path))
    return merged


def load_host_config(repo_root: Path) -> dict:
    merged = load_project_config(repo_root)
    merged = _deep_update(merged, load_local_config(repo_root))
    return merged


def _resolve_path(repo_root: Path, raw: str) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (repo_root / p).resolve()
    return p.resolve()


def _carla_root_candidates(repo_root: Path, override: str | None) -> list[tuple[str, str]]:
    host_cfg = load_host_config(repo_root)
    carla_cfg = (host_cfg.get("carla", {}) or {}) if isinstance(host_cfg.get("carla"), dict) else {}
    paths_cfg = (host_cfg.get("paths", {}) or {}) if isinstance(host_cfg.get("paths"), dict) else {}
    candidates: list[tuple[str, str]] = []
    if override:
        candidates.append(("override", str(override)))
    env_root = str(os.environ.get("CARLA_ROOT", "")).strip()
    if env_root:
        candidates.append(("env.CARLA_ROOT", env_root))
    cfg_root = str(carla_cfg.get("root") or "").strip()
    if cfg_root:
        candidates.append(("host_config.carla.root", cfg_root))
    path_root = str(paths_cfg.get("carla_root") or "").strip()
    if path_root:
        candidates.append(("host_config.paths.carla_root", path_root))
    return candidates


def resolve_carla_root(override: str | None = None, *, strict: bool = True) -> Path:
    repo_root = resolve_repo_root()
    candidates = _carla_root_candidates(repo_root, override)
    if not candidates:
        hint_lines = [
            "未找到 CARLA_ROOT，请至少配置一种来源：",
            "1) 环境变量 CARLA_ROOT",
            "2) configs/local.yaml 的 carla.root 或 paths.carla_root",
            "3) configs/local.<hostname>.yaml（自动加载）",
            "4) CARLA_TESTBED_LOCAL_CONFIGS=/path/a.yaml,/path/b.yaml",
            "参考样板：configs/local.example.yaml 与 .env.example",
        ]
        raise RuntimeError("\n".join(hint_lines))

    source, raw = candidates[0]
    root_path = _resolve_path(repo_root, raw)
    if strict and not root_path.exists():
        raise RuntimeError(
            f"CARLA_ROOT 路径不存在: {root_path}\n"
            f"来源: {source}\n"
            "可通过环境变量 CARLA_ROOT 或本地配置覆盖。"
        )
    return root_path


def resolve_carla_wheel_dir(carla_root: Path) -> Path:
    wheel_dir = carla_root / "PythonAPI" / "carla" / "dist"
    if not wheel_dir.exists():
        raise RuntimeError(f"未找到 CARLA wheel 目录: {wheel_dir}")
    wheels = list(wheel_dir.glob("carla-*.whl"))
    if not wheels:
        raise RuntimeError(f"{wheel_dir} 下未找到 carla-*.whl，请先编译或下载 wheel")
    return wheel_dir
