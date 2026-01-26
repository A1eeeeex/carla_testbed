# 模块定位
提供测试台的配置解析与派生逻辑：加载 rig 预设/自定义文件，展开为传感器规格，生成校准(calibration)、时间同步(time_sync)、噪声(noise_model)与数据格式(data_format)。不负责仿真运行或消息桥接。

# 目录与关键文件
- `defaults.py`：基础默认参数与 HarnessConfig 定义。
- `rig_loader.py`：加载 rig 预设/文件，支持 `--rig`/`--rig-file`/`--rig-override`，并生成传感器规格。
- `rig_postprocess.py`：根据 rig/meta 生成 sensors_expanded、calibration、time_sync、noise_model、data_format，包含保存工具。
- `__init__.py`：导出 HarnessConfig 及默认配置。

# 对外接口（Public API）
- `HarnessConfig`（defaults.py）：运行入口的总配置 dataclass（ticks/record/ROS 开关等）。
- `load_rig_preset(name: str)`（rig_loader.py）：加载内置 rig（minimal/apollo_like/perception_lidar/perception_camera/fullstack）。
- `load_rig_file(path: Union[str, Path])`：读取自定义 rig YAML/JSON。
- `apply_overrides(rig: dict, overrides: List[str])`：应用 `key=value` 覆盖，支持点路径。
- `rig_to_specs(rig: dict) -> List[SensorSpec]`：将 rig 描述转为传感器规格列表。
- `expand_specs(meta_typed, rig_final, rig_name, ego_id="hero")`（rig_postprocess.py）：生成展开的 sensors_expanded 结构（ros2 前缀 `/carla/<ego_id>/...`）。
- `derive_calibration/derive_time_sync/derive_noise/derive_data_format`：派生各类配置文件数据。
- `save_json(path, obj)`：保存 JSON/YAML 兼容内容。

# 输出（runs/<run>/config/）
- `sensors_expanded.json`：展开后的传感器规格与 frame_id/topic 等派生字段。
- `calibration.json`：基于 rig 外参的静态 TF，世界/ego(base_link)/sensor/camera optical 坐标系。
- `time_sync.json`：sim_time，硬/软同步标记。
- `noise_model.json`、`data_format.json`。

# 用法（How to Use）
- 生成 rig 配置文件（随 run 自动生成）：
```bash
python examples/run_followstop.py --rig fullstack --ticks 100
# 生成 runs/followstop_<ts>/config/ 下各派生文件
```
- 加载自定义 rig 并覆盖字段：
```bash
python examples/run_followstop.py --rig-file myrig.yaml --rig-override camera_front.attributes.fov=110
```

# 配置（Config）
- CLI：`--rig`、`--rig-file`、`--rig-override`（逗号/多次传递，格式 key=value）。
- 环境变量：无强制，`CARLA_ROOT` 仅用于示例路径。
- 最小 rig 片段示例（YAML）：
```yaml
rig: minimal
sensors:
  - id: camera_front
    type: camera
    transform: {x: 1.5, y: 0, z: 1.6, roll: 0, pitch: 0, yaw: 0}
    attributes: {image_size_x: 1920, image_size_y: 1080, fov: 90}
```

# 常见问题与排错
- 无 `sensors_expanded.json`：检查 rig 文件路径/名称是否正确；查看 run 目录下 config 是否存在。
- 外参/TF 错位：确认 rig transform 单位为米/度，坐标系为 CARLA world（x 前 y 右 z 上）；如启用 ROS2 可尝试 `--ros-keep-tf`。
- 覆盖字段失败：检查 key 路径是否存在，点路径需匹配结构（如 camera_front.attributes.fov）。
- 时间同步字段为空：确认 `derive_time_sync` 调用后文件写入成功（runs/<run>/config/time_sync.json）。

# 与其他模块的关系
- 上游：CLI/场景选择 rig；无外部依赖。
- 下游：sensors 模块读取 specs；runner 在 run 开始时写出 calibration/time_sync；record 使用这些配置；ROS2 原生发布直接读取 actor 属性（无需 contract）。
- 调用路径：examples/run_followstop.py -> rig_loader/rig_postprocess -> runs/<run>/config。

# Roadmap
- 支持自动校验自定义 rig 的合法性（类型/属性约束）。
- 增加 rig schema/JSONSchema 校验与错误提示。 
