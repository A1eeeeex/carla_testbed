# ROS2 Adapter Boundary

ROS2 support is currently transitional. Runnable implementation pieces still
live in `tbio/`, `tbio/ros2/`, and `carla_testbed.ros2`.

This namespace is reserved for the future canonical adapter surface that will
implement `carla_testbed.adapters.base.ADStackBackend` without leaking ROS2
types into the core runner, scenario, sensor, recording, or evaluation layers.

Do not add new platform behavior here unless it is adapter glue behind the
backend contract.
