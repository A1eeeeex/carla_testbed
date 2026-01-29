from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

# Ensure local backends are importable when executed as a script
if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backends import AutowareDirectBackend, CyberRTBackend, Ros2NativeBackend
from scripts.smoke_test import run_smoke_test

BACKENDS = {
    "ros2_native": Ros2NativeBackend,
    "autoware_direct": AutowareDirectBackend,
    "cyberrt": CyberRTBackend,
}


def load_profile(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def main():
    ap = argparse.ArgumentParser(description="IO orchestrator entrypoint")
    ap.add_argument("--profile", type=Path, required=True, help="Profile yaml under io/contract/profiles")
    args = ap.parse_args()

    profile = load_profile(args.profile)
    backend_name = profile.get("backend")
    if backend_name not in BACKENDS:
        raise SystemExit(f"unknown backend {backend_name}; expected one of {list(BACKENDS)}")

    backend_cls = BACKENDS[backend_name]
    backend = backend_cls(profile)
    backend.start()
    backend_ok = backend.health_check()

    contract_path = Path(profile.get("contract", {}).get("canon_ros2", "io/contract/canon_ros2.yaml"))
    if not contract_path.is_absolute():
        contract_path = Path.cwd() / contract_path
    smoke_ok = run_smoke_test(contract_path, timeout=5.0)

    print("\n[run] next steps:")
    print("- ros2 topic list | grep carla")
    print("- ros2 topic echo /tb/ego/control_cmd  # publish a manual command to test闭环")
    print("- ros2 run tf2_tools view_frames -o /tmp/frames.pdf  # 可选 TF 检查")
    if backend_name == "autoware_direct":
        print("- docker compose -f algo/baselines/autoware/docker/compose.yaml logs -f")

    if not (backend_ok and smoke_ok):
        print("[run] backend or smoke test reported issues; see logs above.")


if __name__ == "__main__":
    main()
