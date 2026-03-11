# carla_testbed/ros2/

仿真真值数据的 ROS2 发布层。

用途：

- 向外部算法栈发布 CARLA 真值流（自车里程计、障碍物、markers、tf）。

主要被以下链路使用：

- 通过 `tools/apollo10_cyber_bridge` 的 Apollo GT 链路。
- 原生 ROS2 的可观测性与调试链路。

设计边界：

- 世界坐标到 ROS 消息的转换放在此目录。
- 算法栈相关的专用转换应放在 `tbio/` 或 `tools/apollo10_cyber_bridge/`。
