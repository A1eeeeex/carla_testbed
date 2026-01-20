# 模块定位
负责运行期数据记录与离线渲染：timeseries/summary 统计、失败抓帧、视频录制、HUD 合成，以及基于 run_dir 的统一 RecordManager。主入口由 examples/run_followstop.py 调用。

# 目录与关键文件
- `manager.py`：RecordManager/RecordOptions，按 `--record` 调度 dual_cam/hud/sensor_demo。
- `timeseries_recorder.py`：写入 `runs/<run>/timeseries.csv`。
- `summary_recorder.py`：写入 `runs/<run>/summary.json`。
- `dual_cam.py`：双相机 demo 录制（png 序列/可选 mp4）。
- `hud.py`：离线 HUD 覆盖渲染。
- `video_recorder.py`：基础视频写入工具。
- `fail_capture.py`：失败时抓取 HUD 视频/截图。
- `sensor_demo/`：传感器解释性 demo 渲染流水线（详见子目录 README）。

# 对外接口（Public API）
- `RecordManager(run_dir, rig_resolved, config_paths, args)`：
  - `run()`：根据 args.record 调用各模式（dual_cam/hud/sensor_demo）。
- `SummaryRecorder(path).write(summary_dict)`：写 run 总结。
- `TimeseriesRecorder(path).write_row(dict)`：逐 tick 记录关键指标。
- `FailFrameCapture(out_dir).capture(world, ego, front)`：失败时抓帧。

# 数据契约（I/O）
- 输入：Harness 提供的 run_dir、rig/config 路径、FramePacket/GroundTruthPacket（对 sensor_demo 来自磁盘 raw），CLI record 参数。
- 输出：
  - `runs/<run>/timeseries.csv`, `summary.json`
  - `runs/<run>/video/demo.mp4`（dual_cam/hud/sensor_demo 视模式而定）
  - `runs/<run>/sensors/<id>/<frame>`（来自 sensors 模块）

# 用法（How to Use）
- 运行并生成传感器 demo 视频：
```bash
python examples/run_followstop.py --rig fullstack --ticks 1000 --record sensor_demo --record-keep-frames
```
- 仅录双相机 png/mp4：
```bash
python examples/run_followstop.py --rig apollo_like --record dual_cam --record-output out_dir
```
- HUD 渲染已有帧：
```bash
python examples/run_followstop.py --rig minimal --record hud --record-keep-frames
```

# 配置（Config）
- CLI：`--record`（dual_cam/hud/sensor_demo，可多次）、`--record-output`、`--record-fps`、`--record-resolution`、`--record-max-lidar-points`、`--record-keep-frames`、`--record-no-lidar`、`--record-no-radar`、`--record-no-hud`。
- RecordOptions 在 manager.py 内解析自 args。

# 常见问题与排错
- 视频空白/无法打开：检查 ffmpeg 是否可用；确认 OpenCV 安装；输出路径是否有权限。
- sensor_demo 无 LiDAR：检查 sensors/<lidar>/ply 是否存在；calibration/sensors_expanded 是否匹配；HUD 会打印 “LiDAR projected 0 points”。
- Radar 全 0/sector_only：雷达数据为空或仰角超 alt_limit，查看 HUD debug（az_sign/alt_lim/v_db）。
- timeseries.csv 为空：控制器需返回 `meta.last_debug`；确保 Harness 调用了 TimeseriesRecorder。
- 路径冲突：使用 `--record-output` 指定自定义输出目录。

# 与其他模块的关系
- 上游：runner 提供 run_dir/config/sensors 输出；sensors 写原始数据。
- 下游：eval/可视化/用户阅读视频或 csv/json。
- 调用路径：examples/run_followstop.py -> RecordManager.run -> 各 recorder。

# Roadmap
- 增加 rosbag/其他日志格式的自动录制封装。
- 支持多机位/自定义布局模板。
- 增加质量检查（帧数/对齐）报告。 
