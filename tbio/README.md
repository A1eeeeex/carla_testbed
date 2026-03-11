# tbio/

`tbio` 是仿真 harness 与外部算法栈之间的运行时集成层。

主要内容：

- `backends/`：`carla_testbed run` 使用的后端实现（`ros2_native`、`autoware_direct`、`cyberrt`）。
- `carla/`：CARLA 进程启动策略与生命周期管理。
- `contract/`：基于 IO 契约生成运行期产物。
- `ros2/`：ROS2 发布、探针、话题工具与控制日志。
- `scripts/`：供 `carla_testbed.cli` 调用的脚本入口。

在主链路中的位置：

1. `carla_testbed/runner` 驱动世界 tick。
2. 启动 `tbio/backends` 中选定后端。
3. 后端把运行时数据桥接到目标算法栈。
4. `tbio/contract` 与 `carla_testbed/record` 把产物写入 `runs/<run>/`。

栈集成建议从以下文件入手：

- `tbio/scripts/run.py`
- `tbio/backends/base.py`
