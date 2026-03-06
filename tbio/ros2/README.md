# tbio/ros2/

ROS2 runtime tools used by the testbed.

Contents:

- `goal_engage.py`: helper for engage/goal command workflows.
- `observability.py`: ROS2 observability stream helpers.
- `control/`: control topic notes.
- `tf/`: TF sanity checks.
- `tools/`: topic probe, inspect, time sync check, control logger, conversion scripts.

Used by:

- `tbio/backends/ros2_native.py`
- `tbio/backends/cyberrt.py` (for ROS side probes/logging)

Debug first on new host:

1. `python tbio/ros2/tools/topic_probe.py ...`
2. `python tbio/ros2/tools/time_sync_check.py ...`

