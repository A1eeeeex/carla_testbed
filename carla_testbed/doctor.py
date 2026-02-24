from __future__ import annotations

import socket
import subprocess
import sys
import os
from pathlib import Path
from typing import List

from carla_testbed.utils.env import resolve_carla_root, resolve_carla_wheel_dir, resolve_repo_root


def check_python():
    return {
        "python": sys.executable,
        "version": sys.version,
        "venv": sys.prefix != sys.base_prefix,
    }


def check_carla():
    info = {}
    try:
        root = resolve_carla_root()
        info["carla_root"] = str(root)
        wheel_dir = resolve_carla_wheel_dir(root)
        info["carla_wheel_dir"] = str(wheel_dir)
        wheels = sorted(wheel_dir.glob("carla-*.whl"))
        info["wheels"] = [w.name for w in wheels]
        try:
            import carla  # type: ignore

            info["carla_import"] = str(carla.__file__)
        except Exception as exc:
            info["carla_import_error"] = str(exc)
            info["carla_install_hint"] = f"pip install {wheels[-1] if wheels else '<carla-wheel>'}"
    except Exception as exc:
        info["error"] = str(exc)
    return info


def check_carla_server(host="127.0.0.1", port=2000):
    res = {"host": host, "port": port}
    try:
        with socket.create_connection((host, port), timeout=2.0):
            res["reachable"] = True
    except Exception as exc:
        res["reachable"] = False
        res["error"] = str(exc)
    return res


def check_ros2():
    info = {
        "RMW_IMPLEMENTATION": os.environ.get("RMW_IMPLEMENTATION", ""),
        "ROS_DOMAIN_ID": os.environ.get("ROS_DOMAIN_ID", ""),
    }
    try:
        out = subprocess.check_output(["which", "ros2"], text=True).strip()
        info["ros2"] = out
    except Exception as exc:
        info["ros2_error"] = str(exc)
        return info
    try:
        import rclpy  # type: ignore

        info["rclpy"] = str(getattr(rclpy, "__file__", "ok"))
    except Exception as exc:
        info["rclpy_error"] = str(exc)
        info["rclpy_hint"] = "source /opt/ros/humble/setup.bash && python -c 'import rclpy'"
    return info


def render_report(items: dict) -> str:
    lines: List[str] = []
    for k, v in items.items():
        lines.append(f"[{k}]")
        if isinstance(v, dict):
            for kk, vv in v.items():
                lines.append(f"{kk}: {vv}")
        else:
            lines.append(str(v))
        lines.append("")
    return "\n".join(lines)


def doctor_main(run_dir: Path | None = None):
    repo_root = resolve_repo_root()
    ts = Path(str(int(__import__("time").time())))
    run_dir = run_dir or (repo_root / "runs" / f"doctor_{ts}")
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    report = {
        "python": check_python(),
        "carla": check_carla(),
        "carla_server": check_carla_server(),
        "ros2": check_ros2(),
    }
    txt = render_report(report)
    (artifacts / "doctor.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    doctor_main()
