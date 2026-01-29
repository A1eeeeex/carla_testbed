# io/ — Contract & Orchestration Layer

Goal: single place for external interfaces (ROS2/CyberRT placeholders) shared by simulator and algorithms.

Contents
- `contract/`: canonical slot->topic/type mappings (`canon_ros2.yaml`), TF layout (`frames.yaml`), units, and runnable profiles.
- `backends/`: lifecycle wrappers for Mode-1 `ros2_native`, Mode-2 `autoware_direct`, and `cyberrt` placeholder.
- `ros2/`: health tools (`inspect_topics.py`, `time_sync_check.py`), TF sanity check, and control topic notes.
- `scripts/`: entrypoints `run.py`, `stop.py`, `smoke_test.py`, `env_check.sh`.

Usage
- Mode-1: `python io/scripts/run.py --profile io/contract/profiles/ros2_native_any_algo.yaml`
- Mode-2: `python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml`
- Stop resources: `python io/scripts/stop.py --profile <profile>`

Contract rules
- All ROS2 topics consumed/published by algorithms must be declared in `contract/canon_ros2.yaml` using slot semantics.
- TF frames required are listed in `contract/frames.yaml`; `tf/tf_sanity.py` only checks presence, not correctness.
- Units follow SI as documented in `contract/units.yaml`.

Backends
- `ros2_native`: enables CARLA native ROS2 publishers (on current sensors) and launches the control bridge.
- `autoware_direct`: generates Autoware interface configs from rig+contract and brings up docker compose.
- `cyberrt`: placeholder for future CyberRT adapter.
