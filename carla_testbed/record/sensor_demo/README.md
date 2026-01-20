# 模块定位
离线渲染“传感器解释性 demo”：基于 run_dir 中的传感器原始文件（camera png、lidar ply/bin、radar bin、imu/gnss json），生成包含 LiDAR 投影、雷达准星/mini-map、HUD 的 mp4。仅负责离线渲染，不控制仿真。

# 目录与关键文件
- `pipeline.py`：SensorDemoRecorder 主流程（capture + offline render）。
- `overlay_lidar.py`：LiDAR 投影到相机画面，采样/深度过滤/混合绘制。
- `overlay_radar.py`：雷达投影、mini-map、速度着色、准星绘制。
- `overlay_hud.py`：HUD 文本叠加（frame/timestamp/IMU/GNSS/事件/统计）。
- `index.py`：扫描 run_dir/sensors 生成 frames.jsonl 索引。
- `chase_cam.py`：跟车相机控制（回放阶段）。
- `geometry.py`：矩阵变换/坐标转换工具。

# 对外接口（Public API）
- `SensorDemoRecorder(run_dir, rig_resolved, config_paths, opts, dt)`：
  - `start(world, ego, client)`：开始 CARLA recorder（录制 replay log）。
  - `capture(frame_id, timestamp)`：占位，当前不额外采集。
  - `stop()`：停止 recorder。
  - `finalize(timeseries_path, dt)`：生成 frames 索引并渲染 demo.mp4。
- 叠加函数（常用参数）：
  - `project_lidar_to_image(img, points, T_base_cam, T_base_lidar, K, max_points=2000, max_range=80, max_depth=60)`。
  - `project_radar_to_image(..., az_sign=1, alt_limit=0.2, v_deadband=0.3)` 返回 uv/mask/depth/vel 等。
  - `draw_radar_targets_on_image(img, radar_stats, max_range_m, topk=8, label_topk=4, static_thresh=0.3)`。
  - `draw_radar_minimap(img, depth, az, vel, fov_deg, max_range_m, origin_px, radius_px, az_sign=1, alt, alt_limit=0.2, v_deadband=0.3)`。

# 数据契约（I/O）
- 输入：`runs/<run>/sensors/<sensor_id>/`（camera png，lidar ply/bin，radar bin，imu/gnss json）、`config/calibration.json`、`config/sensors_expanded.json`。
- 输出：`runs/<run>/video/demo.mp4`；若 `--record-keep-frames`，存 `video/frames/*.png`；`frames.jsonl` 索引。
- 坐标系：world/ego(base_link) 为 CARLA（x 前 y 右 z 上），相机 optical：x 右 y 下 z 前；mini-map：前向 +X，右向 +Y（py 用减号）。

# 用法（How to Use）
- 运行仿真并渲染 demo：
```bash
python examples/run_followstop.py --rig fullstack --ticks 1000 --record sensor_demo --record-keep-frames
```
- 仅重新渲染已有 run（已录好 sensors）：
```bash
python - <<'PY'
from carla_testbed.record.sensor_demo.pipeline import SensorDemoRecorder
from argparse import Namespace
rec = SensorDemoRecorder(run_dir="runs/followstop_<ts>", rig_resolved=None, config_paths={}, opts=Namespace(
    output_dir=None, fps=None, keep_frames=True, max_lidar_points=2000, skip_lidar=False, skip_radar=False, skip_hud=False,
    record_resolution=None, record_no_lidar=False, record_no_radar=False, record_no_hud=False, radar_az_sign=1, radar_alt_limit=0.2, radar_v_deadband=0.3
), dt=0.05)
rec.finalize(None, 0.05)
PY
```

# 配置
- CLI（由 run_followstop.py 传入）：`--record sensor_demo`、`--record-fps`、`--record-resolution`、`--record-max-lidar-points`、`--record-keep-frames`、`--record-no-lidar`、`--record-no-radar`、`--record-no-hud`。
- Radar 调试参数（opts，可通过 Namespace 注入）：`radar_az_sign`（默认 1，可切换左右）、`radar_alt_limit`（默认 0.2 rad）、`radar_v_deadband`（默认 0.3 m/s）。

# 常见问题与排错
- LiDAR 0/xxxx：检查 sensors/lidar_* 是否存在；校准/内参是否匹配相机分辨率；HUD 日志会打印 front/inimg。
- Radar 点全在 0 速度：确认雷达 bin 是否包含正确列；HUD 显示 layout/速度范围，可调整 `radar_az_sign`/`alt_limit`。
- mini-map 方向不对：尝试切换 `radar_az_sign`；确认前向朝上（py 使用减号）。
- 视频尺寸错误/失真：确保第一帧分辨率正确；VideoWriter 用首帧尺寸；不要手动改 png 尺寸。
- HUD 未显示：检查 `--record-no-hud` 是否设置，或 OpenCV 安装。

# 与其他模块的关系
- 上游：sensors 模块写入 raw；config 提供 calibration/sensors_expanded；runner 触发 finalize。
- 下游：record 输出视频供用户/评审查看；不回写仿真。
- 调用路径：examples/run_followstop.py -> RecordManager(sensor_demo) -> SensorDemoRecorder.finalize -> overlay_*。

# Roadmap
- 支持雷达目标跟踪/聚类，提高科普体验。
- 增加可配置配色/布局模板。
- 引入 GPU 加速投影（可选）。 
