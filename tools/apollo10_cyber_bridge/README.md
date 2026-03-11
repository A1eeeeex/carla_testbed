# apollo10_cyber_bridge/

用于 followstop Apollo 链路的 ROS2 -> Apollo 桥接模块。

主要文件：

- `bridge.py`：主桥接进程。
- `config_example.yaml`：桥配置样板。
- `gen_pb2.sh`：从本地 Apollo 生成 protobuf Python 模块。
- `send_routing_request.py`：手动发送 routing 请求的辅助脚本。
- `monitor.sh`、`record_all.sh`：运行时监控/录制脚本。

核心职责：

1. 读取 ROS2 里程计和障碍物流。
2. 发布 Apollo 的 localization/chassis/perception。
3. 消费 Apollo control 并映射回 CARLA 控制话题。
4. 管理自动 routing，并输出运行统计与健康摘要。

关键运行产物（位于 `runs/<run>/artifacts`）：

- `cyber_bridge_stats.json`
- `bridge_health_summary.json`
- `debug_timeseries.csv`
- `control_decode_debug.jsonl`

关键配置域：

- `bridge.auto_routing`
- `bridge.traffic_light`
- `bridge.front_obstacle_behavior`
- `bridge.control_mapping`

迁移说明：

- 该模块依赖 `tools/apollo10_cyber_bridge/pb/` 下生成的 pb2 文件。
- 当 Apollo 版本变化时，请重新执行 `gen_pb2.sh` 并检查导入路径是否兼容。
