# 模块定位
负责传感器规格定义、rig 管理与事件源封装。负责从 CARLA 传感器回调采集数据、写入 run_dir/sensors，并构建 `SensorSample`。不负责渲染或控制。

# 目录与关键文件
- `specs.py`：`SensorSpec`/`SensorSample` 定义，传感器类型支持 camera/lidar/radar/imu/gnss/events。
- `rigs.py`：`SensorRig` 管理传感器生命周期、采集与落盘；`SensorRigStats`。
- `events.py`：事件源 `CollisionEventSource`、`LaneInvasionEventSource`。
- `__init__.py`：导出核心类。

# 对外接口（Public API）
- `SensorSpec(sensor_id, sensor_type, blueprint, transform, attributes, sensor_tick, enabled)`。
- `SensorSample(sensor_id, sensor_type, frame_id, timestamp, payload, meta)`。
- `SensorRig(world, ego, specs, out_dir)`：
  - `start()`：创建并附加传感器 actor，保存 meta.json。
  - `capture(frame_id, timestamp=None, return_samples=False)`：抓取最新数据，落盘（png/ply/bin/json），可返回样本字典。
  - `stop()`（若有）由上层清理 actor。
- `CollisionEventSource(world, ego)` / `LaneInvasionEventSource(world, ego)`: 监听事件，`fetch()` 获取新增事件列表。

# 数据契约（I/O）
- 输入：`SensorSpec` 列表（来自 rig_loader）；CARLA world/ego。
- 输出：文件写入 `runs/<run>/sensors/<sensor_id>/frame.ext`（camera: png, lidar: ply/bin, radar: bin, imu/gnss: json）；`meta.json` 描述传感器集合。
- 坐标系：CARLA 世界/ego(base_link)；相机 raw_data BGRA；LiDAR 点 [x,y,z,intensity] 世界->传感器坐标。

# 用法（How to Use）
- 在 Harness 内使用（已集成）：`SensorRig` 由 runner 创建，自动写入 sensors/。
- 手动采集示例：
```python
rig = SensorRig(world, ego, specs, Path("runs/tmp/sensors"))
rig.start()
samples = rig.capture(frame_id=100, timestamp=12.3, return_samples=True)
```

# 配置（Config）
- 由 `sensors_expanded.json`/rig 提供 specs；无额外 CLI。
- 文件路径：`runs/<run>/config/sensors_expanded.json`（由 rig_postprocess 生成）。

# 常见问题与排错
- PNG/PLY 未生成：检查 `SensorSpec.enabled`；确保 `capture` 每 tick 被调用。
- LiDAR 写 bin 而非 ply：CARLA save_to_disk 异常时回退 bin；确认依赖 open3d 如需 ply 读取。
- Radar payload 空：当前仅写 raw bin，解析在 record/sensor_demo；查看 note。
- 事件缺失：确认 Collision/LaneInvasion 传感器是否创建；`fetch()` 是否每 tick 调用。
- 速度/姿态偏差：检查 calibration/rig 外参；确认传感器 transform 与 rig 一致。

# 与其他模块的关系
- 上游：config/rig_loader 提供 specs；runner 调用 start/capture。
- 下游：record 使用落盘文件；io 使用 SensorSample；eval 可读取 meta/sensors。
- 调用路径：TestHarness.run -> SensorRig.start/capture -> record/io。 
