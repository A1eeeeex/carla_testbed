# tbio/scripts/

统一 CLI 调用的脚本入口目录。

主要脚本：

- `run.py`：`python -m carla_testbed run` 的核心编排逻辑。
- `start_carla.py`：CARLA 启动辅助。
- `stop.py`：资源停止辅助。
- `healthcheck_ros2.py`：ROS2 健康检查。
- `smoke_test.py`：smoke 流程。

调用建议：

- 优先使用 `python -m carla_testbed <subcommand>`。
- 直接调用脚本主要用于调试场景。
