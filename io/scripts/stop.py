from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backends import AutowareDirectBackend, CyberRTBackend, Ros2NativeBackend

BACKENDS = {
    "ros2_native": Ros2NativeBackend,
    "autoware_direct": AutowareDirectBackend,
    "cyberrt": CyberRTBackend,
}


def load_profile(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def main():
    ap = argparse.ArgumentParser(description="Stop backend resources using profile")
    ap.add_argument("--profile", type=Path, required=True)
    args = ap.parse_args()
    profile = load_profile(args.profile)
    backend_name = profile.get("backend")
    backend_cls = BACKENDS.get(backend_name)
    if not backend_cls:
        raise SystemExit(f"unknown backend {backend_name}")
    backend = backend_cls(profile)
    backend.stop()


if __name__ == "__main__":
    main()
