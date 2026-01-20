CARLA 算法测试平台（仓库使用手册）
==============================

> 目标：基于 CARLA 搭建可复现的**算法测试平台**，聚合场景复现、传感器采集、真值输出、控制闭环、录制与评测。当前实现以本仓库内的跟停（follow-stop）控制器为核心，ROS2/CyberRT 适配尚未实现（占位接口已在蓝图与 schemas 中，扩展方式见“如何扩展”）。

------------------------------------------------------------
快速开始（5–10 分钟跑通）
------------------------------------------------------------
环境要求
- 已安装 CARLA 0.9.16（本仓库自带 Simulator 二进制）。
- Python 3.10（推荐在已有 `carla16` conda 环境中运行）。
- 启动 CARLA 服务器：在仓库根目录执行 `./CarlaUE4.sh`（确保 2000 端口空闲）。

运行最小示例（生成 runs/…/summary.json）
```bash
cd /home/ubuntu/CARLA_0.9.16
# 启动服务器后另开终端运行：
python carla_testbed/examples/run_followstop.py \
  --town Town01 \
  --controller composite \
  --lateral-mode dummy \
  --policy-mode acc \
  --ticks 1000 \
  --rig minimal \
  --record dual_cam \
  --record hud \
  --record sensor_demo
```
预期产物：`runs/followstop_<timestamp>/timeseries.csv` 与 `summary.json`。此示例场景为直线跟停（front_idx=210, ego_idx=120）。

若需要混合 Agent 控制纵向接管，可选参数：
```bash
python carla_testbed/examples/run_followstop.py --controller hybrid_agent_acc --agent-type basic
```

传感器 rig（预设/自定义/覆盖）示例：
```bash
# 使用内置预设（minimal/apollo_like/perception_lidar/perception_camera/fullstack）
python carla_testbed/examples/run_followstop.py --rig apollo_like
# 自定义 rig 文件 + 覆盖某字段
python carla_testbed/examples/run_followstop.py --rig-file myrig.yaml --rig-override camera_front.attributes.image_size_x=1024
# 开启失败窗口 HUD 录制
python carla_testbed/examples/run_followstop.py --rig fullstack --enable-fail-capture
# 录制/渲染示例（新）：dual_cam + HUD + sensor_demo
python carla_testbed/examples/run_followstop.py --rig fullstack --record dual_cam --record hud --record sensor_demo
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
Controller (control/legacy_controller.py -> code/followstop/controllers.py)
    | outputs ControlCommand (schemas/algo_io.py)
    v
Apply to ego vehicle; record timeseries; evaluate fail/success; write summary
```
- `FramePacket` / `SensorSample`：帧级传感器数据容器（当前示例仅用事件传感器；cam/lidar/radar dump可选）。
- `GroundTruthPacket`：ego/front 真值与事件列表（当前事件来源：碰撞/车道侵入）。
- `ControlCommand`：控制器输出的油门/刹车/转向，包含 `last_debug` 元信息。
- `Evaluator`：尚未独立模块化；当前 Harness 内部使用基础规则（碰撞/侵线/停稳）决定成功与否。
- `Recorder`：timeseries CSV、summary JSON。
- `Harness`：驱动 tick、调用控制器、采集事件/可选传感器、执行失败策略并落盘。

------------------------------------------------------------
目录结构导览（核心模块）
------------------------------------------------------------
```
carla_testbed/
  README.md                       # 本指南
  carla_testbed_module_blueprint.md # 目标蓝图/接口定义
  carla_testbed/
    schemas/                      # 统一数据结构：FramePacket / GroundTruthPacket / ControlCommand
    config/                       # HarnessConfig（同步模式、步数、失败策略等）
    sim/                          # CARLA 连接、同步 tick、spawn 帮助
    scenarios/                    # 场景定义（已实现 follow-stop 直线场景）
    control/                      # 控制器接口 + 对旧 controllers.py 的适配层
    sensors/                      # 事件传感器源 + 基于 rig specs 的传感器挂载
    runner/                       # Harness 主循环（fail_fast/log_and_continue、记录）
    record/                       # timeseries/summary 录制器、fail 捕获、demo 录制
    utils/                        # 预留
  examples/
    run_followstop.py             # MVP 入口：构建场景、运行 Harness、生成产物

```

模块职责（数据如何流动）
- sim：`carla_client.py` 建立 client 并确保 PythonAPI 在 sys.path；`tick.py` 开启同步 + tick；`actors.py` 提供 spawn_with_retry。
- scenarios：`followstop.py` 生成 ego/front，并让 front 刹停；返回 ActorRefs。
- control：`legacy_controller.py` 用 `controllers.build_default_controller` 生成控制器，包装成 ControlCommand（附带 last_debug）。
- sensors：`events.py` 监听碰撞/侵线；`rigs.py::SensorRig` 按 rig specs 采集并落盘；`configs/rigs/` 提供内置预设。
- runner：`harness.py` 调用 tick_world、控制器 step、应用 ControlCommand；根据 fail_strategy 决定退出；写 timeseries/summary；可选调用 SensorRig、FailFrameCapture、DemoRecorder。
- record：TimeseriesRecorder/ SummaryRecorder 写文件；FailFrameCapture 失败单帧抓取；`video_recorder.py::DemoRecorder` 录制双相机 raw/HUD/mp4（ffmpeg 可选）。
- schemas：定义 FramePacket/GroundTruthPacket/AlgoIO，供上述模块共享。

------------------------------------------------------------
运行方式详解
------------------------------------------------------------
示例脚本：`carla_testbed/examples/run_followstop.py`
- 关键参数：
  - `--town`：CARLA 地图名，默认 Town01
  - `--ticks`：最大步数（同步 tick 次数）
  - `--controller`：`composite`（默认，使用 controllers.py 的 CompositeController）或 `hybrid_agent_acc`（controllers.py 的 HybridAgentLongitudinalController）
  - `--lateral-mode`：`pure_pursuit` | `stanley` | `dummy`
  - `--policy-mode`：`acc`（当前仅 acc，传其他会报错）
  - `--agent-type`：hybrid 时 `basic` 或 `behavior`
  - `--takeover-dist`/`--blend-time`：hybrid 接管距离/平滑时间
  - `--front-idx` / `--ego-idx`：spawn 点索引（默认 210/120，直线段）
  - 传感器 rig 配置：
    - `--rig`：内置预设（minimal/apollo_like/perception_lidar/perception_camera/fullstack），默认 minimal
    - `--rig-file`：自定义 rig yaml/json
    - `--rig-override`：点路径覆盖，可多次使用（如 camera_front.attributes.image_size_x=1024）
  - `--enable-fail-capture`：失败窗口 HUD 录制（run_dir/fail_window）
  - 录制/渲染（采集与合成分离）：
    - `--record dual_cam`：车内+第三人称原始视频（mp4；可选 `--record-keep-frames` 保留 png）
    - `--record hud`：基于 dual_cam 帧与 timeseries 渲染 HUD mp4
    - `--record sensor_demo`：回放 recorder.log + sensors raw，叠加 lidar 投影/雷达扇区占位/IMU+GNSS HUD/事件提示，输出 demo.mp4
    - 公共参数：`--record-output`（默认 run_dir/video）、`--record-fps`、`--record-resolution 1920x1080`、`--record-max-lidar-points`、`--record-no-lidar`、`--record-no-radar`、`--record-no-hud`
    - 兼容旧旗标：`--record-demo`≈dual_cam，`--make-hud`≈hud（已弃用，推荐改用 --record）
  - 失败策略（在 `carla_testbed/config/defaults.py` 配置）：`fail_strategy`=`fail_fast` 或 `log_and_continue`（后者继续 `post_fail_steps` 再退出）
  - 产物默认写入：`runs/followstop_<timestamp>/`

ROS2 在线桥接（runtime bridge）
- 需先 `source /opt/ros/<distro>/setup.bash`，订阅端记得 `use_sim_time=true`（如 `ros2 param set /rviz2 use_sim_time true`）。
- 运行：`python carla_testbed/examples/run_followstop.py --rig fullstack --enable-ros2-bridge [--ros2-contract run_dir/config/io_contract_ros2.yaml]`。
- 主题约定（来自 contract）：`/clock`、`/tf_static`（transient_local）、`/sim/ego/<sensor_id_用/分段>/image_raw|points|imu|gnss`，事件 `/sim/ego/events/collision`、`/sim/ego/events/lane_invasion`（std_msgs/String，JSON payload）；雷达暂未映射，会在日志提示。
- QoS：相机/激光雷达最佳努力（depth 5）、IMU best_effort depth 50、GNSS best_effort depth 10、事件 reliable depth 50、tf_static transient_local。
- 录包（桥接只负责发布，录制请用 ros2 bag/cyber_recorder）：`ros2 bag record /clock /tf_static /sim/ego/camera/front/image_raw /sim/ego/lidar/top/points /sim/ego/imu /sim/ego/gnss`
- 验证：`ros2 topic list | grep /sim/ego`、`ros2 topic echo /clock`、`ros2 topic hz /sim/ego/imu`、RViz2 订阅 Image / PointCloud2。

录制/渲染模式对比（record 模块）：

| MODE        | 产生素材                               | 产生 mp4 | 依赖                      | 说明 |
|-------------|----------------------------------------|----------|---------------------------|------|
| dual_cam    | 车内/第三人称 png 序列                 | 是       | ffmpeg(可选)              | 原 dual_cam 录制功能 |
| hud         | 依赖 dual_cam png + timeseries.csv     | 是       | pillow + ffmpeg(可选)     | 基于 dual_cam 叠加 HUD |
| sensor_demo | recorder.log + sensors raw + frames.jsonl | 是    | OpenCV(+open3d 可选)      | 新增解释性传感器可视化；雷达为扇区占位 |

高级选项（尚未完全模块化）：
- 传感器硬同步、HUD/视频录制、完整评测/KPI 仍在旧脚本 `code/followstop/test_followstop_policy.py` 与 `step1_record_demo.py` 中，未移植到新框架；如需这些功能，暂用旧脚本运行。

------------------------------------------------------------
输出与回归
------------------------------------------------------------
`runs/followstop_<timestamp>/`
- `timeseries.csv`：每帧记录
  - 基本：frame, t, step, v_mps, throttle, brake, steer, collision_count, lane_invasion_count
  - dbg_*：来自控制器 `last_debug`（gap、限速、加速度等）
- `summary.json`：汇总
  - success, fail_reason
  - collision_count, lane_invasion_count
  - max_speed_mps
  - first_failure_step, continued_steps_after_failure
  - controller/lateral_mode/policy_mode/agent_type
  - fail_strategy/post_fail_steps
  - sensors_enabled, sensor_frames_saved, sensor_dropped
- （可选）`sensors/`：按 rig specs 保存的传感器数据；`fail_window/`：失败前 3s 的 raw png 与 HUD mp4（启用 fail_capture 时）
- （可选）`demo/`：raw_in/raw_tp png 序列、overlay_in/overlay_tp（若 make_hud），以及 mp4（若安装 ffmpeg）

回归对比：尚未实现自动 baseline/gate；可手动比对 summary/timeseries。后续可在 `eval/` 与 `runner/` 扩展 regression。

------------------------------------------------------------
扩展指南（入口与示例）
------------------------------------------------------------
新增 Scenario（例如自定义 spawn/交通流）：
```python
# carla_testbed/scenarios/my_scene.py
from carla_testbed.scenarios.base import Scenario, ActorRefs
class MyScenario(Scenario):
    def __init__(self, cfg): self.cfg = cfg
    def build(self, world, carla_map, bp_lib):
        # 使用 sim.actors.spawn_with_retry(...)
        ...
        self.actors = ActorRefs(ego=ego, front=front)
        return self.actors
    def reset(self): ...
    def destroy(self): ...
```
在 examples 中替换为 `MyScenario` 即可。

新增 SensorDriver（例如相机硬同步）：
- 参考 `sensors/rigs.py::SensorRig`，实现 start()/stop()/capture(frame_id)。需要硬同步时，可在待实现的 `sync/` 模块中对齐 FramePacket。

新增 Evaluator/KPI：
- 目前评测逻辑嵌在 Harness（碰撞/侵线/停稳）。可在 `runner/harness.py` 中扩展，或新增 `eval/` 模块调用自定义 Evaluator。

新增 I/O Adapter（ROS2/CyberRT）：
- 蓝图接口位于 `carla_testbed/carla_testbed_module_blueprint.md` 与 `schemas/algo_io.py`。当前未实现，建议在 `io/` 目录下新增 adapter，并在 Harness 中发布/订阅 FramePacket 与 ControlCommand。

------------------------------------------------------------
常见问题与排错
------------------------------------------------------------
1) **无法连接 CARLA / tick 卡死**：确保已运行 `./CarlaUE4.sh`，端口 2000 空闲；`CarlaClientManager` 默认超时 30s，可调整。
2) **同步模式未恢复**：异常退出后地图变卡顿，重新运行前可手动调用 `configure_synchronous_mode` 恢复或重启 CARLA；示例脚本在 finally 中已调用 `restore_settings`。
3) **事件传感器无数据**：碰撞/侵线源需 attach 到 ego，确认 ego 未被销毁；`CollisionEventSource.start()` 已自动 attach。
4) **timeseries dbg_* 为空**：控制器需输出 `last_debug`；确认使用的控制器来自 `code/followstop/controllers.py` 且未异常。
5) **路径/模块导入失败**：确保在仓库根运行；`sim/carla_client.py` 会自动把 `PythonAPI` 和 `PythonAPI/carla` 加入 sys.path。
6) **性能/RTF 低**：减少 `--ticks`，或关闭渲染（修改示例中的 world settings）；当前未集成性能监控。
7) **坐标系混淆**：所有 pose/速度直接使用 CARLA world 坐标；FramePacket/TruthPacket 里未转换 ENU，扩展时需自行规范。
8) **视频/HUD/传感器录制**：使用 `--record` 系列参数（dual_cam/hud/sensor_demo）；若依赖未装（pillow/ffmpeg/opencv），会在日志提示并跳过对应渲染。
9) **混合 Agent 控制失败**：若提示 agents 导入缺失，确保 PythonAPI/carla 在 sys.path，且依赖 shapely/networkx 已安装（报错信息会提示）。
10) **Fail 策略不生效**：检查 `config/defaults.py` 中 `fail_strategy` 和 `post_fail_steps`；示例脚本默认 `fail_fast`。
11) **传感器缺帧/对不齐**：当前 SensorRig 为“尽力记录”模式，基于 frame_id 取最新帧，缺失会计入 dropped；硬同步未实现，需自建 sync 模块。
12) **权限/写盘失败**：确保 run_dir 可写；传感器文件较大，磁盘不足会导致保存失败。

------------------------------------------------------------
Roadmap
------------------------------------------------------------
- ROS2/CyberRT Transport 适配（io/ros2_adapter.py / io/cyber_adapter.py）——未实现。
- 传感器采集与硬同步（camera/lidar/radar/imu/gnss + sync/hard_sync）——部分采集已支持，硬同步未实现；现阶段请用事件传感器或旧脚本。
- 评测/KPI 扩展：TTC、舒适性、速度/横摆误差；引入 baseline 回归 gate。
- 录制/回放：sensor_demo 叠加准确性（雷达解析）与回放 chase-cam 更精细的姿态控制。
- 故障注入：传感器掉帧/定位漂移/控制延迟注入（计划在 eval/与 runner/扩展）。
