from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from algo.adapters.base import Adapter


class AutowareAdapter(Adapter):
    def __init__(self):
        self.compose_proc: Optional[subprocess.Popen] = None

    def prepare(self, profile: Dict[str, Any], run_dir):
        # assume artifacts already generated and paths recorded in profile["artifacts"]
        return

    def start(self, profile: Dict[str, Any], run_dir):
        stack_cfg = profile.get("algo", {}).get("autoware", {})
        compose = stack_cfg.get("compose")
        if not compose:
            print("[autoware] compose file missing in config")
            return
        compose_path = Path(compose).resolve()
        if not compose_path.exists():
            print(f"[autoware] compose not found: {compose_path}")
            return
        env = os.environ.copy()
        domain_id = profile.get("io", {}).get("ros", {}).get("domain_id", 0)
        env["ROS_DOMAIN_ID"] = str(domain_id)
        env.setdefault("USE_SIM_TIME", "true" if profile.get("io", {}).get("ros", {}).get("use_sim_time", True) else "false")
        artifacts_dir = profile.get("artifacts", {}).get("dir")
        if artifacts_dir:
            env["IO_ARTIFACTS_DIR"] = str(Path(artifacts_dir).resolve())
        compose_clean = profile.get("runtime", {}).get("compose_clean", False)
        if compose_clean:
            down_cmd = ["docker", "compose", "-f", str(compose_path), "down"]
            print(f"[autoware] {' '.join(down_cmd)} (compose_clean)")
            subprocess.call(down_cmd, env=env)
        cmd = ["docker", "compose", "-f", str(compose_path), "up", "-d"]
        print(f"[autoware] {' '.join(cmd)}")
        try:
            self.compose_proc = subprocess.Popen(cmd, env=env)
        except FileNotFoundError:
            print("[autoware] docker compose not available")

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        compose = profile.get("algo", {}).get("autoware", {}).get("compose")
        if not compose:
            return False
        compose_path = Path(compose).resolve()
        cmd = ["docker", "compose", "-f", str(compose_path), "ps"]
        try:
            out = subprocess.check_output(cmd, text=True)
            print(out)
            return True
        except Exception as exc:
            print(f"[autoware] healthcheck failed: {exc}")
            return False

    def stop(self, profile: Dict[str, Any], run_dir):
        compose = profile.get("algo", {}).get("autoware", {}).get("compose")
        if not compose:
            return
        compose_path = Path(compose).resolve()
        cmd = ["docker", "compose", "-f", str(compose_path), "down"]
        try:
            subprocess.check_call(cmd)
        except Exception as exc:
            print(f"[autoware] stop failed: {exc}")

    def get_control_topics(self, profile: Dict[str, Any]):
        return ["/vehicle/engage", "/vehicle/vehicle_command"]
