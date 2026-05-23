# carla_testbed.adapters

Canonical adapter boundary for external autonomous-driving stacks.

The core runner/harness should depend on `carla_testbed.adapters.base.ADStackBackend`
or on neutral contracts, not on ROS2, CyberRT, Apollo protobufs, or Autoware
messages directly.

Current status:

- `base.py`: simulator-neutral backend protocol.
- `ros2/`: ROS2-native transitional adapter notes.
- `apollo/`: Apollo/CyberRT MVP adapter notes.

Do not move large legacy implementations here until they have a clear migration
plan. `tbio/` remains the transition implementation layer for runnable backends.
