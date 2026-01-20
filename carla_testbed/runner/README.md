# 模块定位
负责组合场景、控制、传感器、记录器的运行框架（Harness）。处理仿真 tick、事件统计、输出配置生成、录制与终止条件。不负责场景定义或控制逻辑细节。

# 目录与关键文件
- `harness.py`：核心 `TestHarness` 实现，管理 run 状态、生成 config/recording、调用控制器。
- `__init__.py`：导出 TestHarness。

# 对外接口（Public API）
- `TestHarness(cfg: HarnessConfig)`：
  - `run(world, carla_map, ego, front, controller_cfg, out_dir, sensor_specs, rig_raw, rig_final, rig_name, enable_fail_capture, record_manager, client) -> (HarnessState, summary)`。
  - `reset()`：重置内部状态。
- `HarnessState`：运行统计（step/frame_id/timestamp/success/fail_reason/collision_count 等）。

# 数据契约（I/O）
- 输入：CARLA world/map/ego/front，控制器配置 `LegacyControllerConfig`，传感器 specs，rig/config（用于派生 calibration/time_sync/io_contract）。
- 输出：
  - `runs/<run>/config/*.json/yaml`（sensors_expanded/calibration/time_sync/noise/data_format/io_contract_*）
  - 传感器数据目录 `runs/<run>/sensors/<sensor_id>/...`
  - `runs/<run>/timeseries.csv`、`summary.json`
  - 可选视频/失败抓帧（record manager）
- 事件：收集 collision/lane_invasion，写入 GroundTruthPacket/Event。

# 用法（How to Use）
- 通过示例脚本运行（推荐）：
```bash
python examples/run_followstop.py --rig fullstack --ticks 1000 --record sensor_demo --enable-ros2-bridge
```
- 在自定义脚本中使用：
```python
from carla_testbed.runner import TestHarness
from carla_testbed.control import LegacyControllerConfig
harness = TestHarness(cfg=HarnessConfig(ticks=100))
state, summary = harness.run(world, carla_map, ego, front,
    controller_cfg=LegacyControllerConfig(controller_mode="composite"),
    out_dir=Path("runs/custom_run"),
    sensor_specs=specs,
    rig_raw=rig_raw, rig_final=rig_final, rig_name="fullstack",
    enable_fail_capture=False,
    record_manager=record_manager,
    client=client)
```

# 配置（Config）
- HarnessConfig（defaults.py 中定义）：ticks/host/port/town/rig 相关配置。
- 控制器配置：`LegacyControllerConfig`（控制模式/agent 类型）。
- CLI 参数参见 `examples/run_followstop.py`（如 `--ticks`、`--enable-fail-capture`、`--record*`、`--enable-ros2-bridge`）。

# 常见问题与排错
- config 未生成：检查 rig 是否加载成功；确保 run_dir 可写。
- 控制器导入失败：确认 `LegacyFollowStopController` 路径；CARLA PythonAPI 可用。
- 录制无输出：确认传入 RecordManager，或 CLI `--record` 是否设置；查看 `runs/<run>/video`。
- 事件未统计：确保 collision/lane invasion 传感器启用；检查 sensors_spec 是否包含事件源。
- 运行中断/异常：查看 `summary.json` 中 fail_reason、timeseries.csv、终端日志。

# 与其他模块的关系
- 上游：examples/run_followstop.py 提供 world/map/ego/front 初始化；config 提供 rig。
- 下游：sensors 捕获并写文件；control 生成命令；record 写 csv/json/mp4；io 可能发布 ROS2。
- 调用路径：入口脚本 -> TestHarness.run -> sensors rig capture -> control -> record/io。

# Roadmap
- 支持多控制器并行对比/切换。
- 增加回放模式，利用 runs/<run>/replay/recording.log 自动复现。
- 提供更细粒度的失败原因与 KPI 报告。 
