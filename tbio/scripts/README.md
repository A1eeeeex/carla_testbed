# tbio/scripts/

Script entrypoints used by the unified CLI.

Primary scripts:

- `run.py`: main run orchestration for `python -m carla_testbed run`.
- `start_carla.py`: CARLA launcher helper.
- `stop.py`: shutdown helper.
- `healthcheck_ros2.py`: ROS2 health checks.
- `smoke_test.py`: smoke test flow.

Recommended invocation:

- Prefer `python -m carla_testbed <subcommand>`.
- Keep direct script calls for debugging only.

