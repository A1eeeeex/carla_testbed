# 模块定位
定义在模块间传递的数据结构（dataclass）：帧数据、真值、控制命令、事件等。作为内存契约被 runner/control/io/record 等共享，不负责持久化。

# 目录与关键文件
- `frame_packet.py`：`FramePacket`（封装每 tick 的传感器样本）。
- `truth_packet.py`：`GroundTruthPacket`、`Event`、`ObjectTruth`。
- `algo_io.py`：占位接口 `AlgoOutput`。
- `__init__.py`：导出主要 dataclass。

# 对外接口（Public API）
- `FramePacket(frame_id, timestamp, samples: Dict[str, SensorSample])`。
- `GroundTruthPacket(frame_id, timestamp, ego: ObjectTruth, actors: List[ObjectTruth], events: List[Event])`。
- `Event(event_type, frame_id, timestamp, meta)`：事件元数据（collision/lane_invasion）。
- `ObjectTruth(object_id, pose, velocity, acceleration)`。
- `ControlCommand(throttle, brake, steer, reverse, hand_brake, manual_gear_shift, gear, meta)`（在 truth_packet.py 定义）。
- `SensorSample(sensor_id, sensor_type, frame_id, timestamp, payload, meta)`（在 __init__.py 中从 sensors.specs 导入）。

# 数据契约（I/O）
- 在内存中传递，不直接读写文件。被 runner 生成，record/io 消费。
- 坐标系约定：world/ego(base_link)/sensor；相机 optical 为 x 右 y 下 z 前。

# 用法（How to Use）
- 控制器返回 ControlCommand：
```python
cmd = ControlCommand(throttle=0.5, brake=0.0, steer=0.1, reverse=False, hand_brake=False, manual_gear_shift=False, gear=0, meta={"dbg":{}})
```
- 记录器使用 FramePacket：
```python
packet = FramePacket(frame_id=100, timestamp=12.3, samples={"camera_front": sample})
```

# 配置（Config）
- 无外部配置；由上层传入数据实例。

# 常见问题与排错
- 字段 None/缺失：确保控制器/runner 填充 frame_id/timestamp；payload 需符合传感器类型。
- 序列化失败：这些 dataclass 未内置 JSON 序列化，保存时需手动转换（record 已处理）。
- 坐标系混淆：保持 world x前 y右 z上；相机 optical x右 y下 z前。
- meta 过大：控制器 debug 信息尽量精简，避免 CSV/JSON 过重。
- 类型错误：严格使用浮点/布尔，避免 numpy 类型导致 JSON dump 失败。

# 与其他模块的关系
- 上游：runner/sensors/控制器 生成数据。
- 下游：record 写文件；io 转 ROS2；eval 消费。
- 调用路径：TestHarness.run -> 构造 FramePacket/GroundTruthPacket -> record/io。 
