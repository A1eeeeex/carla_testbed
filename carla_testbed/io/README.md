# 模块定位
启用 CARLA Server 的原生 ROS2 发布能力（无需 rclpy 节点）：将已挂载的传感器/事件 actor 调用 `enable_for_ros()`，并辅助同步 Traffic Manager。该模块不构造或发送 ROS2 消息，所有话题由 CARLA UE 端直接发布。

# 目录与关键文件
- `ros2_native.py`：`Ros2NativePublisher` 薄封装，负责调用 `enable_for_ros` 与 Traffic Manager 同步模式恢复。
- `ros2/`：CARLA 官方 ros2_native 示例与 RViz 辅助脚本（仅供参考）。
- `__init__.py`：导出 `Ros2NativePublisher`。

# 对外接口（Public API）
- `Ros2NativePublisher(world, traffic_manager, ego_vehicle, rig_spec, ego_id="hero", invert_tf=True)`：
  - `setup_publishers()`：对 rig 中的传感器 actor 调用 `enable_for_ros()`，将 Traffic Manager 置为同步模式并打印最终 Transform。
  - `teardown()`：恢复 Traffic Manager 同步模式开关。

# 用法（How to Use）
- 示例脚本（推荐）：在 CARLA 服务器使用 `--ros2` 启动后运行
```bash
python examples/run_followstop.py --rig fullstack --enable-ros2-native
```
- 手动使用（伪代码）：
```python
publisher = Ros2NativePublisher(world, world.get_trafficmanager(), ego, rig_spec=rig, ego_id="hero")
publisher.setup_publishers()
# CARLA UE 端会在 /carla/<ego>/<sensor>/... 发布 ROS2 话题
...
publisher.teardown()
```

# 配置与依赖
- 需使用 `./CarlaUE4.sh --ros2` 或 `CarlaUnreal.sh --ros2` 启动服务器。
- 话题命名遵循 `/carla/<ego_id>/<sensor_id>/...`，`ego_id` 默认 `hero`，可通过 CLI `--ego-id` 自定义。
- Harness 中的 `--ros-invert-tf/--ros-keep-tf` 控制 y/pitch/yaw 是否取反，默认开启以匹配 CARLA 官方示例。

# 常见问题与排错
- 无 ROS2 话题：确认服务器以 `--ros2` 启动且示例运行时传入 `--enable-ros2-native`。
- 事件话题支持：`collision`/`lane_invasion` 传感器同样调用 `enable_for_ros()`，实际发布情况取决于 CARLA 版本；仍可在 runs/summary/timeseries 查看事件统计。
- TF 镜像：尝试切换 `--ros-keep-tf`（不取反）或调整 rig transform，再观察 `/tf`。 
