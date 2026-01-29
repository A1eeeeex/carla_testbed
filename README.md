CARLA 算法测试平台（仓库使用手册）
==============================

👉 新架构总览（架构收敛）
- 顶层三层次：`carla_testbed/` 专注仿真/场景/录制，`io/` 统一 I/O 契约与编排，`algo/` 承载算法栈（Autoware 等）与控制桥。
- 快速入口：  
  - Mode-1：CARLA 原生 ROS2 + 任意算法（含控制桥）  
    `python io/scripts/run.py --profile io/contract/profiles/ros2_native_any_algo.yaml`
  - Mode-2：Autoware 直连 CARLA（autoware_carla_interface）  
    `python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml`
- IO 相关逻辑已抽离到顶层 `io/`（contract/backends/tools）。`carla_testbed` 内部仅提供最小 hook，原 `carla_testbed/io` 已移除。

> 目标：基于 CARLA 搭建可复现的**算法测试平台**，聚合场景复现、传感器采集、真值输出、控制闭环、录制与评测。当前实现以本仓库内的跟停（follow-stop）控制器为核心，支持开启 CARLA 原生 ROS2 发布（`--enable-ros2-native`），CyberRT 适配尚未实现（占位接口已在蓝图与 schemas 中，扩展方式见“如何扩展”）。

------------------------------------------------------------
快速开始（5–10 分钟跑通）
------------------------------------------------------------
环境要求
- 已安装 CARLA 0.9.16（本仓库假定默认路径 `/home/ubuntu/CARLA_0.9.16`）。
- Python 3.10（推荐在已有 `carla16` conda 环境中运行）。
- 启动 CARLA 服务器：在 CARLA 目录执行 `./CarlaUE4.sh`（确保 2000 端口空闲）。

运行最小示例（生成 runs/…/summary.json）
```bash
cd /home/ubuntu/CARLA_0.9.16
# 启动服务器后另开终端运行：
cd /home/ubuntu/carla_testbed
python examples/run_followstop.py \
  --town Town01 \
  --controller composite \
  --lateral-mode dummy \
  --policy-mode acc \
  --ticks 500 \
  --rig fullstack \
  --record sensor_demo
```
预期产物：`runs/followstop_<timestamp>/timeseries.csv` 与 `summary.json`。此示例场景为直线跟停（front_idx=210, ego_idx=120），并生成 sensor_demo 视频。

若需要混合 Agent 控制纵向接管，可选参数：
```bash
python examples/run_followstop.py --controller hybrid_agent_acc --agent-type basic
```

传感器 rig（预设/自定义/覆盖）示例：
```bash
# 使用内置预设（minimal/apollo_like/perception_lidar/perception_camera/fullstack/sample_rig/sample_rig2）
python examples/run_followstop.py --rig apollo_like
# 自定义 rig 文件 + 覆盖某字段
python examples/run_followstop.py --rig-file myrig.yaml --rig-override camera_front.attributes.image_size_x=1024
# 开启失败窗口 HUD 录制
python examples/run_followstop.py --rig fullstack --enable-fail-capture
# 录制/渲染示例：dual_cam + HUD + sensor_demo
python examples/run_followstop.py --rig fullstack --record dual_cam --record hud --record sensor_demo
```

------------------------------------------------------------
核心概念与数据流
------------------------------------------------------------
ASCII 流程图（每 tick）：
```
CARLA World --(tick_world)-> frame_id, timestamp
    |                  |
    |            Scenario actors (ego/front)
    |                  |
Sensors (collision/lane invasion, optional cam/lidar/radar)
    | events           |
    v                  v
FramePacket (schemas/frame_packet.py)        GroundTruthPacket (schemas/truth_packet.py)
    \_____________________ joint into Harness _______________________/
                                |
                                v
Controller (control/legacy_controller.py -> legacy_followstop/controllers.py)
    | outputs ControlCommand (schemas/algo_io.py)
    v
Apply to ego vehicle; record timeseries; evaluate fail/success; write summary
```
- `FramePacket` / `SensorSample`：帧级传感器数据容器（可选 camera/lidar/radar/imu/gnss，默认至少事件传感器）。
- `GroundTruthPacket`：ego/front 真值与事件列表（碰撞/车道侵入）。
- `ControlCommand`：控制器输出油门/刹车/转向，包含 `last_debug` 元信息。
- `Evaluator`：尚未独立模块化；当前 Harness 内部使用基础规则（碰撞/侵线/停稳）决定成功与否。
- `Recorder`：timeseries CSV、summary JSON、可选视频。
- `Harness`：驱动 tick、调用控制器、采集事件/可选传感器、执行失败策略并落盘；可选启用 ROS2 原生发布。

------------------------------------------------------------
目录结构导览（核心模块）
------------------------------------------------------------
```
repo_root/
  carla_testbed/                  # 仿真/场景/录制核心（保持原目录层级）
    schemas/ config/ sim/ scenarios/ control/ sensors/ runner/ record/ utils/
  io/                             # 新：I/O 契约、profiles、backends、工具与脚本
    contract/ backends/ ros2/ scripts/
  algo/                           # 新：算法/栈承载（Autoware baseline、控制桥、插件占位）
    baselines/autoware/ ... , nodes/control/carla_control_bridge/, plugins/
  configs/rigs/                   # rig 预设
  examples/run_followstop.py      # 示例入口
```

模块职责（数据如何流动）
- sim：`carla_client.py` 建立 client 并确保 PythonAPI 在 sys.path；`tick.py` 开启同步 + tick；`actors.py` 提供 spawn_with_retry。
- scenarios：`followstop.py` 生成 ego/front，并让 front 刹停；返回 ActorRefs。
- control：`legacy_controller.py` 用本地 `legacy_followstop/controllers.py` 生成控制器，包装成 ControlCommand（附带 last_debug）。
- sensors：`events.py` 监听碰撞/侵线；`rigs.py::SensorRig` 按 rig specs 采集并落盘。
- runner：`harness.py` 调用 tick_world、控制器 step、应用 ControlCommand；根据 fail_strategy 决定退出；写 timeseries/summary；可选调用 SensorRig、FailFrameCapture、RecordManager。
- record：TimeseriesRecorder/SummaryRecorder 写文件；FailFrameCapture 失败单帧抓取；RecordManager 调度 dual_cam/hud/sensor_demo。
- io：`Ros2NativePublisher` 对已挂载传感器调用 `enable_for_ros()`，并协助 Traffic Manager 同步模式。
- schemas：定义 FramePacket/GroundTruthPacket/AlgoIO，供上述模块共享。

------------------------------------------------------------
运行方式详解
------------------------------------------------------------
示例脚本：`examples/run_followstop.py`
- 关键参数：
  - `--town`：CARLA 地图名，默认 Town01
  - `--ticks`：最大步数（同步 tick 次数）
  - `--controller`：`composite`（默认，使用 controllers.py 的 CompositeController）或 `hybrid_agent_acc`
  - `--lateral-mode`：`pure_pursuit` | `stanley` | `dummy`
  - `--policy-mode`：`acc`
  - `--agent-type`：hybrid 时 `basic` 或 `behavior`
  - `--takeover-dist`/`--blend-time`：hybrid 接管距离/平滑时间
  - `--front-idx` / `--ego-idx`：spawn 点索引（默认 210/120）
  - 传感器 rig 配置：
    - `--rig`：内置预设（minimal/apollo_like/perception_lidar/perception_camera/fullstack/sample_rig/sample_rig2），默认 minimal
    - `--rig-file`：自定义 rig yaml/json
    - `--rig-override`：点路径覆盖，可多次使用（如 camera_front.attributes.image_size_x=1024）
  - `--enable-fail-capture`：失败窗口 HUD 录制（run_dir/fail_window）
  - ROS2 原生发布：
    - `--enable-ros2-native`：调用 CARLA 原生接口在 `/carla/<ego>/<sensor>/...` 发布话题（需服务器 `--ros2`）
    - `--ros-invert-tf` / `--ros-keep-tf`：是否对 rig 的 y/pitch/yaw 取反（默认取反以匹配官方示例）
    - `--ego-id`：ego role_name/ros_name，默认 `hero`
    - `--enable-rviz`：在 ROS2 模式下自动启动 RViz（默认 docker，可用 `--rviz-mode local` 直接调用本机 rviz2），可配合 `--rviz-domain/--rviz-docker-image/--rviz-camera-image-suffix/--rviz-lidar-cloud-suffix`
    - `--enable-ros2-bag`：开启 rosbag2 录制，可配合 `--ros2-bag-out/--ros2-bag-storage/--ros2-bag-compress/--ros2-bag-max-size-mb/--ros2-bag-max-duration-s/--ros2-bag-topics/--ros2-bag-extra-topics/--ros2-bag-camera-image-suffix/--ros2-bag-lidar-cloud-suffix`
  - 录制/渲染（采集与合成分离）：
    - `--record dual_cam`：车内+第三人称原始视频（mp4；可选 `--record-keep-frames` 保留 png）
    - `--record hud`：基于 dual_cam 帧与 timeseries 渲染 HUD mp4
    - `--record sensor_demo`：回放 recorder.log + sensors raw，叠加 lidar 投影/雷达/IMU/事件，输出 demo.mp4
    - 公共参数：`--record-output`（默认 run_dir/video）、`--record-fps`、`--record-resolution 1920x1080`、`--record-max-lidar-points`、`--record-no-lidar`、`--record-no-radar`、`--record-no-hud`
    - 兼容旧旗标：`--record-demo`≈dual_cam，`--make-hud`≈hud（已弃用）
  - 失败策略（在 `carla_testbed/config/defaults.py` 配置）：`fail_strategy`=`fail_fast` 或 `log_and_continue`，`post_fail_steps` 控制失败后继续步数
  - 产物默认写入：`runs/followstop_<timestamp>/`

ROS2 原生发布
- 提示：原 `carla_testbed/io` 已抽离；现在由 `io/backends/ros2_native.py` 和 `io/scripts/run.py` 统一入口管理。
- 启动 CARLA：`./CarlaUE4.sh --ros2` 或 `CarlaUnreal.sh --ros2`（订阅端建议 `use_sim_time=true`）。
- 运行：`python examples/run_followstop.py --rig fullstack --enable-ros2-native [--ego-id hero --ros-invert-tf]`。
- 主题约定：`/carla/<ego_id>/<sensor_id>/...`，如 `/carla/hero/camera_front/image`、`/carla/hero/lidar_top/points`、`/carla/hero/imu`。事件传感器同样调用 `enable_for_ros()`，实际发布取决于 CARLA 版本，评测仍以 runs/summary/timeseries 为准。
- 验证：`ros2 topic list | grep /carla/hero`、`ros2 topic info /carla/hero/camera_front/image`、`ros2 topic hz /carla/hero/imu`、`rviz2` 订阅 Image/PointCloud2。
- 录包示例：`ros2 bag record /carla/hero/camera_front/image /carla/hero/lidar_top/points /carla/hero/imu /carla/hero/gnss`。
- 迁移提示：旧的 `/sim/ego/...` 话题与 QoS 契约已移除，订阅端需改为 `/carla/...`。
- RViz 可选启用：`python examples/run_followstop.py --enable-ros2-native --enable-rviz --rig fullstack`（或 `--rig-file configs/rigs/<rig>.yaml`）。默认使用 docker（镜像 `carla_testbed_rviz:humble`，自动 build），根据 rig 自动订阅首个 camera/lidar，生成 `record/rviz/rviz/native_<rig>.rviz`。若无 DISPLAY 或 Wayland 会跳过且不影响主线；可自备本机 RViz。DDS 配置 `record/rviz/config/fastrtps-profile.xml` 提升 docker/多网卡场景稳定性。
- ROS2 bag 录制（原生话题）：`python examples/run_followstop.py --enable-ros2-native --enable-ros2-bag --rig configs/rigs/<any>.yaml`。输出默认写入 `runs/<run>/ros2_bag/`，topics 自动根据 rig 生成（可用 `--ros2-bag-topics` 指定，`--ros2-bag-extra-topics` 追加，suffix 可调）。自检：`ros2 bag info <bag>`、`ros2 bag play <bag>`，确保 ROS_DOMAIN_ID 与录制时一致。

录制/渲染模式对比（record 模块）：

| MODE        | 产生素材                               | 产生 mp4 | 依赖                      | 说明 |
|-------------|----------------------------------------|----------|---------------------------|------|
| dual_cam    | 车内/第三人称 png 序列                 | 是       | ffmpeg(可选)              | 原 dual_cam 录制功能 |
| hud         | 依赖 dual_cam png + timeseries.csv     | 是       | pillow + ffmpeg(可选)     | 基于 dual_cam 叠加 HUD |
| sensor_demo | recorder.log + sensors raw + frames.jsonl | 是    | OpenCV(+open3d 可选)      | 解释性传感器可视化（LiDAR+Radar+HUD） |

高级选项（尚未完全模块化）：
- 传感器硬同步、完整评测/KPI 仍在旧脚本 `code/followstop/test_followstop_policy.py` 与 `step1_record_demo.py` 中，未移植到新框架；如需这些功能，暂用旧脚本运行。

------------------------------------------------------------
输出与回归
------------------------------------------------------------
`runs/followstop_<timestamp>/`
- `timeseries.csv`：每帧记录 frame,t,step,v_mps,throttle,brake,steer,collision_count,lane_invasion_count,dbg_*（控制器 last_debug）。
- `summary.json`：汇总 success/fail_reason/collision_count/lane_invasion_count/max_speed_mps/first_failure_step/continued_steps_after_failure/controller 配置/fail_strategy/sensor_frames_saved/dropped。
- `config/`：sensors_expanded.json、calibration.json、time_sync.json、noise_model.json、data_format.json。
- `sensors/`：原始数据（camera png；lidar ply/bin；radar bin；imu/gnss json）。
- `video/`：录制产物（dual_cam/hud/sensor_demo mp4，frames/ 可选保留）。
- `frames.jsonl`：sensor_demo 索引；`replay/recording.log`：CARLA recorder。
- `fail_window/`：启用 fail_capture 时的抓帧/HUD。

回归对比：尚未提供自动 baseline gate，可手工比对 summary/timeseries 或视频。

------------------------------------------------------------
扩展指南（入口与示例）
------------------------------------------------------------
新增 Scenario：
```python
# carla_testbed/scenarios/my_scene.py
from carla_testbed.scenarios.base import Scenario, ActorRefs
class MyScenario(Scenario):
    def __init__(self, cfg): self.cfg = cfg
    def build(self, world, carla_map, bp_lib):
        # spawn 车辆/行人...
        self.actors = ActorRefs(ego=ego, front=front)
        return self.actors
    def destroy(self): ...
```
新增 Sensor 预设：
- 在 `configs/rigs/` 增加 YAML（参考 `sample_rig.yaml`），或用 `--rig-override` 动态覆盖；加载逻辑在 `config/rig_loader.py::rig_to_specs`。

新增 KPI/Evaluator：
- 评测逻辑当前嵌在 `runner/harness.py`（碰撞/侵线/停稳）。可在其中添加指标写入 timeseries/summary，或新建 `eval/` 模块并从 Harness 调用。

新增 I/O Adapter：
- ROS2 已通过 CARLA 原生接口发布；如需其他中间件，可在 `io/` 下新建适配层，自行管理发布/订阅生命周期，并在 Harness 中挂载。

------------------------------------------------------------
常见问题与排错
------------------------------------------------------------
1) **无法连接 CARLA / tick 卡死**：确认服务器运行且端口 2000 空闲；同步模式下所有传感器需 listen 成功，否则 tick_world 可能超时。
2) **同步模式未恢复**：异常退出后 world 卡顿，可重启 CARLA 或调用 `restore_settings`（示例脚本 finally 已恢复）。
3) **传感器缺帧/丢帧**：SensorRig 按“取最新”策略，未实现硬同步；缺失会计入 dropped；可降低 sensor_tick 或 ticks。
4) **LiDAR 投影全 0**：检查 calibration.json 是否与 rig/FOV/分辨率匹配；HUD 会打印 front/inimg；确保未缩放 png。
5) **Radar 左右颠倒或全静止**：可调整 `radar_az_sign`、`radar_v_deadband`（sensor_demo opts）；raw bin 解析假设 vel/alt/az/depth 或 depth/az/alt/vel。
6) **ROS2 无 topic**：确认 CARLA 服务器以 `--ros2` 启动且示例传入 `--enable-ros2-native`；查看日志中的传感器 Transform；尝试 `--ros-keep-tf` 或检查 ros_name/role_name 是否与预期一致。
7) **视频生成失败**：确保 opencv/pillow/ffmpeg 可用；若缺失会提示并跳过；可用 `--record-keep-frames` 检查中间 png。
8) **性能/RTF 低**：减少 `--ticks`、降低分辨率/点数（`--record-max-lidar-points`）、关闭不必要记录；RenderOffScreen 可提速。
9) **坐标系混淆**：默认 CARLA 世界/ego（x 前 y 右 z 上）；相机 optical 投影用 x 右 y 下 z 前；TF 由 calibration.json 提供。
10) **路径/权限问题**：在仓库根运行，确保 `runs/` 可写；不要提交 `__pycache__`。

------------------------------------------------------------
Roadmap
------------------------------------------------------------
- CyberRT 适配（占位待实现）。
- 传感器硬同步/插值，完善雷达解析。
- 评测/KPI 模块化与 baseline 回归门槛。
- 录制/回放增强：追踪相机、雷达目标聚类。
- 故障注入与性能监控（RTF/资源占用）。
