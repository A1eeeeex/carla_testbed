from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _ensure_ros2_arg(extra_args: str, need_ros2: bool) -> str:
    args: List[str] = shlex.split(extra_args or "")
    if need_ros2 and "--ros2" not in args:
        args.append("--ros2")
    return " ".join(args)


@dataclass
class CarlaLaunchPolicy:
    start: bool
    host: str
    port: int
    town: str
    extra_args: str
    foreground: bool
    carla_root: Optional[str]
    need_ros2_native: bool


def resolve_carla_launch_policy(cfg: Dict[str, Any]) -> CarlaLaunchPolicy:
    runtime_cfg = cfg.get("runtime", {}) or {}
    runtime_carla = runtime_cfg.get("carla", {}) or {}
    carla_cfg = cfg.get("carla", {}) or {}
    run_cfg = cfg.get("run", {}) or {}
    scenario_cfg = cfg.get("scenario", {}) or {}
    io_cfg = cfg.get("io", {}) or {}
    autoware_cfg = cfg.get("algo", {}).get("autoware", {}) if isinstance(cfg.get("algo"), dict) else {}

    need_ros2_native = bool(scenario_cfg.get("publish_ros2_native")) or str(io_cfg.get("mode", "")).lower() == "ros2_native"
    start = _as_bool(runtime_carla.get("start", runtime_cfg.get("start_carla")), default=False)
    host = str(runtime_carla.get("host") or carla_cfg.get("host") or autoware_cfg.get("carla_host") or "localhost")
    port = int(runtime_carla.get("port") or carla_cfg.get("port") or autoware_cfg.get("carla_port") or 2000)
    town = str(runtime_carla.get("town") or run_cfg.get("map") or "Town01")
    foreground = _as_bool(runtime_carla.get("foreground"), default=False)
    root = runtime_carla.get("root") or carla_cfg.get("root")
    extra_args = str(runtime_carla.get("extra_args") or carla_cfg.get("extra_args") or "")
    extra_args = _ensure_ros2_arg(extra_args, need_ros2_native)

    return CarlaLaunchPolicy(
        start=start,
        host=host,
        port=port,
        town=town,
        extra_args=extra_args,
        foreground=foreground,
        carla_root=str(root) if root else None,
        need_ros2_native=need_ros2_native,
    )


def append_followstop_launch_args(cmd: List[str], policy: CarlaLaunchPolicy) -> List[str]:
    out = list(cmd)
    if policy.start:
        out.append("--start-carla")
    out.extend(["--host", policy.host, "--port", str(policy.port), "--town", policy.town])
    if policy.carla_root:
        out.extend(["--carla-root", policy.carla_root])
    if policy.extra_args:
        out.append(f"--carla-extra-args={policy.extra_args}")
    if policy.foreground:
        out.append("--carla-foreground")
    return out
