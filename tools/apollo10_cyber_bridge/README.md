# apollo10_cyber_bridge/

用于 Apollo ground-truth 链路的桥接模块。

主要文件：

- `bridge.py`：主桥接进程。
- `carla_direct_transport.py`：Town01 候选直连 transport。直接从 CARLA 读取 GT 并直接把 control 写回 CARLA，不经过 ROS2 GT/control transport。
- `actuator_mapping.py`：执行器标定查表与 physical 映射辅助模块。
- `control_mapping.py`：`legacy` / `physical` 控制换算层。
- `bridge_policy.py`：桥侧 guard / fallback / policy 计数与摘要。
- `bridge_observer.py`：桥侧 timing / debug / observability 摘要层。
- `ingress_egress.py`：ROS2 / CyberRT 进出通道摘要层。
- `config_example.yaml`：桥配置样板。
- `gen_pb2.sh`：从本地 Apollo 生成 protobuf Python 模块。
- `send_routing_request.py`：手动发送 routing 请求的辅助脚本。
- `monitor.sh`、`record_all.sh`：运行时监控/录制脚本。

核心职责：

1. 在 `ros2_gt` 模式下读取 ROS2 里程计/障碍物流；在 `carla_direct` 模式下直接读取 CARLA world/actors。
2. 发布 Apollo 的 localization/chassis/perception。
3. 消费 Apollo control，并按 transport mode 写回：
   - `ros2_gt`: 通过 ROS2 control bridge 回 CARLA
   - `carla_direct`: 直接 `actor.apply_control`
4. 管理自动 routing，并输出运行统计与健康摘要。

Town01 direct candidate 额外约定：

- `algo.apollo.direct_bridge.route_command_mode=cyber_direct`
  - 明确当前 routing / lane-follow / external-command 走 CyberRT 直发路径，不依赖 Dreamview 发命令。
- `algo.apollo.direct_bridge.require_no_ros2_runtime=true`
  - direct transport 允许在 bridge 进程里不 source ROS2 runtime；如果当前配置仍需要 ROS2，必须由 summary 明示。

当前结构分层：

1. `ingress_egress`
   - ROS2 GT / direct CARLA / control bridge / CyberRT channel contract
2. `control_mapping`
   - `legacy` / `physical` / calibration 映射
3. `bridge_policy`
   - low-speed guard、trajectory-contract guard、fallback
4. `bridge_observer`
   - timing、debug artifacts、simple_lat / route observability 摘要
5. `carla_direct_transport`
   - Town01 候选直连 transport，只负责 CARLA 读写，不做 tick owner

关键运行产物（位于 `runs/<run>/artifacts`）：

- `cyber_bridge_stats.json`
- `bridge_health_summary.json`
- `bridge_transport_summary.json`
  - 重点看：
    - `transport_mode`
    - `uses_ros2_gt`
    - `uses_ros2_control_bridge`
    - `requires_ros2_reexec`
    - `route_command_mode`
    - `route_command_path`
- `command_materialization_summary.json`
  - 重点看：
    - `command_path_stage`
    - `first_divergence_layer`
    - `first_divergence_reason`
  - 用途：
    - 当在线窗口停在 `routing=0 / planning=0 / control_tx=0` 时，
      直接判断 first divergence 更像卡在 `command_path`、`routing_materialization`、
      `planning_materialization`、`control_materialization`，还是 `actuation`
- `debug_timeseries.csv`
- `control_decode_debug.jsonl`
- `carla_vehicle_characteristics.json`
- `carla_actuator_calibration.json`（physical 模式使用时）
- `direct_bridge_stats.json`（`carla_direct` 模式）
- `direct_bridge_actor_snapshot.json`（`carla_direct` 模式）
- `direct_bridge_control_apply.jsonl`（`carla_direct` 模式）

关键配置域：

- `algo.apollo.transport_mode`
- `algo.apollo.direct_bridge`
  - `route_command_mode`
  - `require_no_ros2_runtime`
- `bridge.auto_routing`
- `bridge.traffic_light`
- `bridge.front_obstacle_behavior`
- `bridge.control_mapping`

相关开环标定工具：

- `tools/calibrate_carla_actuators.py`

迁移说明：

- 该模块依赖 `tools/apollo10_cyber_bridge/pb/` 下生成的 pb2 文件。
- 当 Apollo 版本变化时，请重新执行 `gen_pb2.sh` 并检查导入路径是否兼容。
- `carla_direct` 目前是 Town01 候选 transport，作用域只限 GT truth transport A/B；它不是已验证主线，也不会覆盖现有 `ros2_gt` 默认链路。
