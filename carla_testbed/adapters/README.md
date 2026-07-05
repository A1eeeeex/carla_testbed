# carla_testbed.adapters

Target lifecycle adapter boundary for external autonomous-driving stacks.

The active Phase 1 execution facade is `carla_testbed.backends.base.StackBackend`
plus `LaunchPlan`. Runtime code migrated into this namespace should implement
`carla_testbed.adapters.base.ADStackBackend` or neutral contracts, not ROS2,
CyberRT, Apollo protobufs, or Autoware messages directly.

Current status:

- `base.py`: simulator-neutral backend protocol.
- `ros2/`: ROS2-native transitional adapter notes.
- `apollo/`: Apollo/CyberRT MVP adapter notes.

Do not move large legacy implementations here until they have a clear migration
plan. `tbio/` remains the transition implementation layer for runnable backends.
