# tbio/ros2/

testbed 使用的 ROS2 运行工具目录。

目录内容：

- `goal_engage.py`：engage/goal 命令辅助流程。
- `observability.py`：ROS2 可观测性流辅助。
- `control/`：控制话题约定说明。
- `tf/`：TF 自检工具。
- `tools/`：topic probe、inspect、时钟同步检查、控制日志、转换脚本。

调用方：

- `tbio/backends/ros2_native.py`
- `tbio/backends/cyberrt.py`（ROS 侧探针与日志）

新机器调试建议先跑：

1. `python tbio/ros2/tools/topic_probe.py ...`
2. `python tbio/ros2/tools/time_sync_check.py ...`
