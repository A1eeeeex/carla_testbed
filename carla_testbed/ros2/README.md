# carla_testbed/ros2/

ROS2 publishing path for simulator ground-truth data.

Purpose:

- Publish CARLA truth streams (ego odom, objects, markers, tf) for external stacks.

Used by:

- Apollo GT flow via `tools/apollo10_cyber_bridge`.
- ROS-native observability and debugging flows.

Keep this layer simulator-focused:

- World-to-message conversion lives here.
- Stack-specific translation should stay in `tbio/` or `tools/apollo10_cyber_bridge/`.

