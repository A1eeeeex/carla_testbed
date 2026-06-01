# 项目展示总览

这是一套面向技术展示的总览页。它不替代 [README](../README.md) 或 [reference](../reference_pack_carla_ros2_apollo/reference/00_index.md)，而是把“项目架构、当前功能、已验证进度、示例命令、明显产出”收在一页里，方便对工程同事、合作方或新接触项目的人直接讲清楚。

## 项目一句话

本项目的当前主线是：用 CARLA 生成世界与 GT，经 ROS2 与 Apollo bridge 送入 Apollo 10.0，完成 `routing / planning / control`，再把控制写回 CARLA 做闭环验证。更直白地说，它是一套 `CARLA world -> ROS2 GT -> Apollo -> control -> CARLA` 的端到端验证平台，用来回答“算法有没有真的在仿真世界里跑起来、跑得稳不稳、证据够不够硬”。参考来源见 [README](../README.md) 和 [Project Map](../reference_pack_carla_ros2_apollo/reference/01_system_overview/project_map.md)。

## 系统架构

当前系统可以按 4 层理解：

| 层 | 职责 | 代表位置 |
| --- | --- | --- |
| CARLA | 提供地图、世界、车辆动力学、交通参与者和同步 tick | [README](../README.md)、[project_map.md](../reference_pack_carla_ros2_apollo/reference/01_system_overview/project_map.md) |
| Testbed / ROS2 GT | 组织场景、推进 run、发布 `/carla/<ego>/odom`、`/tf` 等 GT | [gt_truth_simulation_pipeline.md](gt_truth_simulation_pipeline.md) |
| Apollo bridge / runtime glue | 承担 topic/channel、坐标、时间、routing send、control decode 的语义对齐 | [project_map.md](../reference_pack_carla_ros2_apollo/reference/01_system_overview/project_map.md)、[control_semantics.md](../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/control_semantics.md) |
| Apollo | 消费 localization / chassis / routing / planning 输入，输出 planning 与 control | [README](../README.md)、[current_truths.md](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md) |

主链如下：

```text
CARLA world
  -> GT publisher
  -> Apollo bridge
  -> Apollo routing / planning / control
  -> ROS2 control bridge
  -> CARLA actuation
```

如果你想看更完整的运行流，请继续看 [runtime_flow.md](../reference_pack_carla_ros2_apollo/reference/01_system_overview/runtime_flow.md) 和 [gt_truth_simulation_pipeline.md](gt_truth_simulation_pipeline.md)。

## 功能清单与进度矩阵

状态说明：

- `已验证`：已有明确 run 证据或 `current_truths.md` 支撑，能够当“当前能力”来讲。
- `已打通待收口`：链路真实打通，但行为健康度、验收或覆盖面还没完全收尾。
- `诊断中`：已经定位到较明确的问题层级，但还不能当完成能力展示。
- `未实现`：代码或接口只预留，当前不能作为能力展示。

| 功能 | 当前状态 | strongest evidence | 推荐配置 / 入口 | 是否适合对外演示 |
| --- | --- | --- | --- | --- |
| 统一 CLI / run harness | 已验证 | [README](../README.md)、[carla_testbed/cli.py](../carla_testbed/cli.py) | `python -m carla_testbed <subcommand>` | 是 |
| Apollo `followstop` 主线 | 已打通待收口 | [configs/io/examples/README.md](../configs/io/examples/README.md)、[gt_followstop_apollo_baseline.md](gt_followstop_apollo_baseline.md) | `conda run -n carla16 python tools/run_apollo_mainline.py --profile relaxed` | 谨慎 |
| `followstop` lateral-enabled stitcher 派生 | 已打通待收口 | [followstop lateral-enabled summary](../runs/followstop_apollo_gt_lateral_enabled_stitcher_v1_smoke_rerun__02/summary.json)、[bridge health](../runs/followstop_apollo_gt_lateral_enabled_stitcher_v1_smoke_rerun__02/artifacts/bridge_health_summary.json) | `python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_lateral_enabled_stitcher_v1.yaml` | 否，实验性 |
| Town01 `lane_keep 097` | 已验证 | [Truth 015~016](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md)、[097 strongest long summary](../runs/town01_capability_online_chain_20260403_010924/lane_keep__adhoc__town01_rh_spawn097_goal046__seed/lane_keep__adhoc__town01_rh_spawn097_goal046__seed__manual_online_chain__town01_rh_spawn097_goal046__02/summary.json) | `run_town01_capability_online_chain.py` + `stitcher_v1` | 是 |
| Town01 `lane_keep 217` | 已打通待收口 | [Truth 016](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md)、[217 short summary](../runs/town01_capability_online_chain_20260403_012813/lane_keep__adhoc__town01_rh_spawn217_goal046__seed/lane_keep__adhoc__town01_rh_spawn217_goal046__seed__manual_online_chain__town01_rh_spawn217_goal046__02/summary.json) | warmed batch：`217 -> 031` | 谨慎 |
| Town01 `junction 031` | 已打通待收口 | [Truth 016](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md)、[031 short summary](../runs/town01_capability_online_chain_20260403_012813/junction_traverse__adhoc__town01_rh_spawn031_goal056__seed/junction_traverse__adhoc__town01_rh_spawn031_goal056__seed__manual_online_chain__town01_rh_spawn031_goal056__02/summary.json) | warmed batch：`217 -> 031` | 谨慎 |
| Town01 `curve_lane_follow` | 诊断中 | [Truth 022](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md)、[curve217 short summary](../runs/town01_capability_online_chain_20260407_103902/curve_lane_follow__adhoc__town01_rh_spawn217_goal048__seed/curve_lane_follow__adhoc__town01_rh_spawn217_goal048__seed__night_curve_short__town01_rh_spawn217_goal048__02/summary.json)、[curve213 semantic window](../runs/town01_capability_online_chain_20260407_103902/curve_lane_follow__adhoc__town01_rh_spawn213_goal059__seed/curve_lane_follow__adhoc__town01_rh_spawn213_goal059__seed__night_curve_short__town01_rh_spawn213_goal059__02/artifacts/curve_semantic_window.finalized.json) | `run_town01_capability_online_chain.py` + `stitcher_v1` short gate | 否，正在攻关 |
| ROS2 GT / 传感器话题发布 | 已验证 | [gt_truth_simulation_pipeline.md](gt_truth_simulation_pipeline.md)、[followstop demo summary](../runs/followstop_apollo_demo_not_delete/summary.json) | `followstop_apollo_demo.yaml` 或 Apollo 主线 profile | 是 |
| ROS2 bag / 话题归档 | 已打通待收口 | [followstop_dummy.yaml](../configs/io/examples/followstop_dummy.yaml)、[examples/run_followstop.py](../examples/run_followstop.py)、[runner/harness.py](../carla_testbed/runner/harness.py) | `followstop_dummy.yaml` / `followstop_autoware.yaml` + `record.rosbag.enable=true` | 谨慎 |
| 运行产物记录（summary / timeseries / monitor） | 已验证 | [README](../README.md)、[record/README.md](../carla_testbed/record/README.md)、[followstop demo summary](../runs/followstop_apollo_demo_not_delete/summary.json) | 任一 `python -m carla_testbed run --config <yaml>` | 是 |
| 本地视频录制（dual_cam / hud / sensor_demo） | 已验证 | [record/README.md](../carla_testbed/record/README.md)、[demo summary](../runs/followstop_apollo_demo_not_delete/summary.json)、[demo_third_person.mp4](../runs/followstop_apollo_demo_not_delete/video/dual_cam/demo_third_person.mp4) | `python -m examples.run_followstop --record ...` | 是 |
| Autoware RViz operator-view 录制 | 已打通待在线复验 | [record/README.md](../carla_testbed/record/README.md)、[followstop_autoware.yaml](../configs/io/examples/followstop_autoware.yaml) | `followstop_autoware.yaml` 默认生成 `video/rviz/autoware_rviz.mp4` + `rosbag2/autoware_demo` | 谨慎 |
| Dreamview / demo 录制 | 已打通待收口 | [demo_recording_fix_report.md](../artifacts/demo_recording_fix_report.md)、[demo_recording_backward_compatibility.md](../artifacts/demo_recording_backward_compatibility.md)、[followstop_apollo_demo.yaml](../configs/io/examples/followstop_apollo_demo.yaml) | `tools/run_dreamview_recording_shortfix_regression.py` 或 `followstop_apollo_demo.yaml` | 谨慎 |
| 油门 / 刹车 / 转向开环标定 | 已打通待收口 | [actuator_regression_workflow.md](actuator_regression_workflow.md)、[apollo_carla_semantic_and_calibration_workflow.md](apollo_carla_semantic_and_calibration_workflow.md)、[carla_actuator_calibration.json](../artifacts/actuator_calibration_library/e969a34a5f43fd25/carla_actuator_calibration.json) | `apollo_actuator_calibration.yaml` + `tools/calibrate_carla_actuators.py` | 谨慎 |
| Apollo 控制语义捕获 / semantic suite | 已打通待收口 | [apollo_carla_semantic_and_calibration_workflow.md](apollo_carla_semantic_and_calibration_workflow.md)、[suite manifest](../runs/unified_calibration_weekend_smoke_conda_reordered/suite_manifest.json) | `tools/run_apollo_actuator_semantic_suite.py` | 谨慎 |
| Apollo control replay 验证 | 已打通待收口 | [actuator_regression_workflow.md](actuator_regression_workflow.md)、[run_apollo_control_replay_validation.py](../tools/run_apollo_control_replay_validation.py) | `tools/run_apollo_control_replay_validation.py` | 谨慎 |
| 执行器跟踪验证 / 语义校验 | 已打通待收口 | [actuator_regression_workflow.md](actuator_regression_workflow.md)、[apollo_carla_semantic_and_calibration_workflow.md](apollo_carla_semantic_and_calibration_workflow.md)、[apollo_actuator_tracking_validation.yaml](../configs/io/examples/apollo_actuator_tracking_validation.yaml) | `tools/validate_apollo_carla_actuator_tracking.py` | 谨慎 |
| 统一标定闭环（capture -> replay -> tracking） | 已打通待收口 | [unified_calibration_pipeline.yaml](../configs/io/examples/unified_calibration_pipeline.yaml)、[run_unified_calibration_pipeline.py](../tools/run_unified_calibration_pipeline.py)、[calibration_demo_ready_summary.md](../artifacts/calibration_demo_ready_summary.md) | `tools/run_unified_calibration_pipeline.py` | 谨慎 |
| 运行诊断 / artifacts / acceptance | 已验证 | [acceptance_criteria.md](../reference_pack_carla_ros2_apollo/reference/07_experiments_and_acceptance/acceptance_criteria.md)、[artifact_reading_guide.md](../reference_pack_carla_ros2_apollo/reference/04_runtime_and_debug/artifact_reading_guide.md) | `summary.json` + `artifacts/*.json` | 是 |
| 算法栈支持：`dummy` / `apollo` / `autoware` / `e2e` | 已打通待收口 | [README](../README.md)、[algo/registry.py](../algo/registry.py) | `python -m carla_testbed run --config <yaml>` | 谨慎；`e2e` 未实现 |

关于 Town01 的状态，需要特别强调两点：

- `lane_keep 097` 已经是当前 strongest 能力证据，可以拿来展示“Town01 车道保持已经真实做成”。
- `curve` 当前已经有 **aligned short-window** 证据，但还没有达到“过弯已完成”的口径；它目前只能展示“我们已经打到 Apollo 横向语义问题这一层”，不能展示成“能力已完成”。

## 每个功能的展示示例

### 示例 1：环境和 CLI 自检

- 展示什么：仓库有统一入口，不是散脚本集合；环境问题可以先用 `doctor` 快速筛掉。
- 推荐命令：

```bash
python -m carla_testbed doctor
```

- 运行时可见信号：命令会打印环境检查结果，并生成一次 `doctor` 运行目录。
- 运行后应查看的 artifacts：`runs/doctor_<ts>/artifacts/doctor.txt`，它能回答“本机环境现在能不能进主线”。

### 示例 2：Apollo `followstop` 主线

- 展示什么：Follow-stop 是仓库里最成熟的 Apollo 主链入口，适合展示“闭环能否跑起来、主 artifacts 是否齐”。
- 推荐命令：

```bash
conda run -n carla16 python tools/run_apollo_mainline.py --profile relaxed
```

- 运行时可见信号：会启动统一主线入口，进入 `followstop` 运行流程，而不是手工拼接多个脚本。
- 运行后应查看的 artifacts：`summary.json`、`timeseries.csv`、`artifacts/bridge_health_summary.json`、`artifacts/cyber_bridge_stats.json`。

### 示例 3：Town01 strongest `lane_keep 097`

- 展示什么：这是当前最适合拿来展示“Town01 真实车道保持已实现”的样本。
- 推荐命令：

```bash
conda run -n carla16 python \
  tools/run_town01_capability_online_chain.py \
  --enable-lateral \
  --config configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml \
  --startup-profile render_offscreen_no_ros2 \
  --carla-world-ready-timeout-sec 180 \
  --carla-launch-attempts 1 \
  --step lane_keep:town01_rh_spawn097_goal046
```

- 运行时可见信号：控制台会持续打印 `[online-chain][progress]` 行，能直接看到 step 进度、ETA、routing/planning/control live counters。
- 运行后应查看的 artifacts：`summary.json`、`artifacts/bridge_health_summary*.json`、`artifacts/control_handoff_summary.json`、`effective.yaml`。

如果你想把这个 strongest 样本直接录成 CARLA 本地 demo，而不是只保留 summary / artifacts，可以改用专门的 demo 派生配置：

```bash
conda run -n carla16 python \
  tools/run_town01_capability_online_chain.py \
  --enable-lateral \
  --config configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_demo.yaml \
  --startup-profile render_offscreen_no_ros2 \
  --carla-world-ready-timeout-sec 180 \
  --carla-launch-attempts 1 \
  --step lane_keep:town01_rh_spawn097_goal046
```

这条 demo 入口不会改变 Town01 主线行为语义，只是额外打开 `record.visual`，默认在 `runs/<run>/video/dual_cam/` 下输出第三人称视频。

如果你想一次录完当前最值得展示的 Town01 主线样本，而不是手工逐条敲命令，可以直接用录制包装脚本。它默认录：

- `lane_keep 097`
- `lane_keep 217`
- `junction 031`

推荐命令：

```bash
python3 tools/run_town01_demo_showcase.py
```

- 运行时可见信号：控制台会先打印将要录制的 step 列表，然后继续输出 `[online-chain][progress]` 行。
- 运行后应查看的 artifacts：每个 step 对应 run 目录下的 `summary.json`、`artifacts/control_handoff_summary.json`，以及 `video/dual_cam/`。

如果需要把当前过弯攻关过程也录成工程诊断 demo，可以显式追加 curve 组：

```bash
python3 tools/run_town01_demo_showcase.py --include-curve-diagnostic
```

这条命令会额外录 `curve217` 和 `curve213`，但要注意：它们当前仍属于诊断性样本，不应对外讲成“过弯能力已完成”。

### 示例 4：Town01 `curve` 正在攻关示例

- 展示什么：这不是“已完成演示”，而是用来展示当前项目已经把问题收敛到 Apollo 横向语义层。
- 推荐命令：

```bash
conda run -n carla16 python \
  tools/run_town01_capability_online_chain.py \
  --enable-lateral \
  --config configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml \
  --ticks 420 \
  --post-fail-steps 120 \
  --startup-profile render_offscreen_no_ros2 \
  --carla-world-ready-timeout-sec 180 \
  --carla-launch-attempts 1 \
  --step curve_lane_follow:town01_rh_spawn217_goal048
```

- 运行时可见信号：同样会看到 `[online-chain][progress]`，但更关键的是 short gate 结束后 `curve` 会进入结构化诊断产物。
- 运行后应查看的 artifacts：`summary.json`、`artifacts/curve_tracking_health_summary.finalized.json`、`artifacts/curve_semantic_window.finalized.json`。

### 示例 5：ROS2 GT / 传感器发布与 probe

- 展示什么：仓库不仅跑场景，还能把 GT 和关键传感器话题结构化发到 ROS2，并把 topic 健康检查落成 artifact。
- 推荐命令：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_demo.yaml \
  --run-dir runs/followstop_apollo_demo_showcase
```

- 运行时可见信号：run 会进入 Apollo followstop demo 入口，结束后 `summary.json` 里能直接看到 `ros2_publish` 和 `sensor_probe` 检查项。
- 运行后应查看的 artifacts：`artifacts/sensor_probe.json`、`artifacts/sensor_probe.log`、`summary.json` 中的 `acceptance.checks.ros2_publish`。

### 示例 6：ROS2 bag 录制

- 展示什么：原生 ROS2 模式下，除了实时发 topic，还可以把关键话题归档成 bag，方便之后做离线回放或 topic 对照。
- 推荐命令：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_dummy.yaml \
  --override record.rosbag.enable=true \
  --override record.rosbag.out=runs/followstop_dummy_rosbag_showcase/ros2_bag/bag
```

- 运行时可见信号：控制台会打印 `[ROS2 bag] recording to ...`，说明 recorder 已经真正挂上。
- 运行后应查看的 artifacts：`runs/<run>/ros2_bag/`、`summary.json` 里的 `ros2_bag_enabled / ros2_bag_out / ros2_bag_topics`。

### 示例 7：本地视频录制（dual_cam / hud / sensor_demo）

- 展示什么：仓库自带本地视频/解释性录制链，不依赖外部剪辑工具就能生成 demo 视频。
- 推荐命令：

```bash
python -m examples.run_followstop \
  --rig fullstack \
  --ticks 1000 \
  --record sensor_demo \
  --record-keep-frames
```

- 运行时可见信号：录制模式会在 run 期间持续生成 frame，并在结束时整理到 `video/` 目录。
- 运行后应查看的 artifacts：`video/dual_cam/*.mp4`、`video/demo.mp4`、`timeseries.csv`。现成示例可看 [demo_third_person.mp4](../runs/followstop_apollo_demo_not_delete/video/dual_cam/demo_third_person.mp4)。

### 示例 8：Dreamview / demo 录制能力

- 展示什么：当前仓库已经把 Dreamview 录制修到“固定区域 + tick snapshot”可用，并保留向后兼容。
- 推荐命令：

```bash
python3 tools/run_dreamview_recording_shortfix_regression.py
```

- 运行时可见信号：脚本会输出 case 级 JSON 结果，明确展示 fixed-region、remembered-region 和 fallback 路径是否成功。
- 运行后应查看的 artifacts：`artifacts/demo_recording_fix_report.md`、`artifacts/demo_recording_case_comparison.csv`，以及文档里提到的 `dreamview_recording_status.json` / `dreamview_capture_manifest.json` 口径。

### 示例 9：油门 / 刹车 / 转向开环标定

- 展示什么：仓库不只做闭环跑车，也有一整条执行器标定链，用来测 CARLA 对 throttle / brake / steer 的真实响应。
- 推荐命令：

```bash
python3 tools/calibrate_carla_actuators.py \
  --carla-host 127.0.0.1 \
  --carla-port 2000 \
  --ego-role-name hero \
  --output artifacts/carla_actuator_calibration.json
```

- 运行时可见信号：脚本会直接驱动车辆做开环采样，并输出统计结果。
- 运行后应查看的 artifacts：`artifacts/carla_actuator_calibration.json`，以及已保存的标定样本 [calibration library example](../artifacts/actuator_calibration_library/e969a34a5f43fd25/carla_actuator_calibration.json)。

### 示例 10：Apollo 控制语义捕获与 replay 验证

- 展示什么：这一步展示的不是“车能不能开”，而是“Apollo 原始 control 字段、bridge 解码后的物理目标、CARLA 实测响应”之间是否对得上。
- 推荐命令：

```bash
python3 tools/run_apollo_control_replay_validation.py \
  --capture-config configs/io/examples/followstop_apollo_gt_validation.yaml \
  --capture-config configs/io/examples/followstop_apollo_gt_relaxed.yaml \
  --validation-config configs/io/examples/apollo_actuator_tracking_validation.yaml \
  --calibration-file artifacts/carla_actuator_calibration.json \
  --output-dir runs/apollo_control_replay_validation
```

- 运行时可见信号：脚本会先抽取 Apollo 真实控制序列，再在固定场景里 replay，日志里会明确显示当前 replay 样本数和阶段进度。
- 运行后应查看的 artifacts：`artifacts/apollo_control_replay_validation.json`、`artifacts/apollo_control_replay_validation_report.md`、`artifacts/apollo_replay_sequence.csv`。

### 示例 11：执行器跟踪验证

- 展示什么：这一步不是看 Apollo 会不会开，而是看 Apollo 解码后的物理目标量和 CARLA 实际执行结果是否一致。
- 推荐命令：

```bash
python3 tools/validate_apollo_carla_actuator_tracking.py \
  --config configs/io/examples/apollo_actuator_tracking_validation.yaml \
  --calibration-file artifacts/carla_actuator_calibration.json \
  --output-dir runs/apollo_carla_tracking_validation
```

- 运行时可见信号：脚本会输出阶段性日志，告诉你当前在跑 steering、throttle 还是 brake 的 tracking 验证。
- 运行后应查看的 artifacts：`artifacts/actuator_tracking_validation.json`、`artifacts/steering_tracking_measurements.csv`、`artifacts/throttle_tracking_measurements.csv`、`artifacts/brake_tracking_measurements.csv`。

### 示例 12：统一标定闭环

- 展示什么：把语义捕获、开环标定、replay validation、tracking validation 串成一条工程化闭环，而不是手工逐步拼接。
- 推荐命令：

```bash
python3 tools/run_unified_calibration_pipeline.py \
  --config configs/io/examples/unified_calibration_pipeline.yaml \
  --output-dir runs/unified_calibration_showcase
```

- 运行时可见信号：控制台会按 `suite -> replay -> tracking -> comparison` 顺序输出阶段日志，能直接看出当前在哪个环节。
- 运行后应查看的 artifacts：`suite/suite_manifest.json`、`artifacts/replay_input_contract.json`、`artifacts/calibration_comparison_report.md`、`artifacts/calibration_demo_ready_summary.md`。

## 运行后会看到什么

这套仓库的一个特点是：它不是黑箱运行，关键证据默认都会落盘。

- 控制台 `[online-chain][progress]`
  - 说明什么：当前 step 到哪了、还要多久、routing/planning/control 是否已经 materialize。
- `runs/<run>/summary.json`
  - 说明什么：这次 run 的总结果、关键 acceptance 指标、速度、距离、状态标签。
- `runs/<run>/timeseries.csv`
  - 说明什么：逐 tick 的速度、控制、状态变化，是最直接的帧级回归入口。
- `runs/<run>/artifacts/sensor_probe.json`
  - 说明什么：ROS2 话题和关键传感器/GT 发布是否真的 materialize。
- `runs/<run>/ros2_bag/`
  - 说明什么：原生 ROS2 话题归档目录，便于离线抽查 topic、时间戳和数据完整性。
- `runs/<run>/artifacts/bridge_health_summary*.json`
  - 说明什么：bridge runtime、routing、planning materialization 是否健康。
- `runs/<run>/artifacts/control_handoff_summary.json`
  - 说明什么：planning 到 control 是否真实接上，control 是否真的在消费 planning。
- `runs/<run>/effective.yaml`
  - 说明什么：这次 run 到底是按什么配置跑的，便于复现和对照。
- `runs/<run>/video/`
  - 说明什么：本地视频录制产物，通常包含 `dual_cam`、`hud`、`sensor_demo` 或 `dreamview` 子目录。
- `runs/<run>/artifacts/curve_tracking_health_summary.finalized.json`
  - 说明什么：当前 `curve` 是“执行层不够”还是“语义层开始坏”，是过弯阶段最有价值的结构化证据。
- `artifacts/carla_actuator_calibration.json`
  - 说明什么：油门/刹车/转向标定结果，是 `physical` 映射和后续 tracking validation 的基础。
- `artifacts/apollo_control_replay_validation.json`
  - 说明什么：Apollo 真正发出的控制序列，在固定场景下 replay 后，CARLA 能否按预期执行。
- `artifacts/actuator_tracking_validation.json`
  - 说明什么：Apollo 解码目标和 CARLA 实际执行之间的偏差有多大，能回答“命令发了以后，车有没有按预期执行”。
- `suite/suite_manifest.json` / `artifacts/calibration_comparison_report.md`
  - 说明什么：统一标定闭环当前跑到了哪一步、用了哪份标定、replay/tracking 是否通过比较门槛。

如果你只想快速解释“为什么这套系统不是黑箱”，把这一节和 [artifact_reading_guide.md](../reference_pack_carla_ros2_apollo/reference/04_runtime_and_debug/artifact_reading_guide.md) 一起给别人看就够了。

## 当前边界与下一步

### 已证明的

- Town01 `lane_keep 097` 已经是 `route_health_candidate` 级别的真实能力证据，说明车道保持已经在 Town01 做成。
- `lane_keep 217` 和 `junction 031` 已恢复真实闭环，不再卡在 startup、bridge、planning 或 control survival 上游问题。
- `curve` 的 short gate 已经进入 `runtime_contract.status = aligned`，说明当前问题已经不是“lateral 没开起来”，而是“行为为什么还不够健康”。

### 还在推进的

- `followstop_apollo_gt_lateral_enabled_stitcher_v1` 已经打通真实横向控制链路，但 acceptance 仍未通过，目前仍是实验性入口。
- 传感器发布、run artifact 落盘、本地视频录制和 Dreamview 录制都已经具备可展示入口，但它们和“Town01 主线能力已完成”不是同一件事。
- ROS2 bag、执行器标定、Apollo replay validation 和统一标定闭环都已经形成独立工具链，但当前更多是工程化支撑能力，不是 Town01 capability promotion 指标。
- Town01 `217 / 031` 还没有全部收口到 `route_health_candidate`。
- Town01 `curve` 还没有达到“过弯已完成”的展示口径，当前更像“已进入 Apollo 横向语义诊断阶段”。

### 当前主 blocker

- 当前 Town01 第一 blocker 已经从“链路能否打通”推进到了“`curve` 的 Apollo lateral semantics / path-tracking 是否健康”。
- 对 `curve213`，更像是 `matched point / target point` 语义恶化。
- 对 `curve217`，更像是突发 `high-steer onset`。
- 也就是说，下一步不是继续修 bridge 小参数，而是继续沿 [current_truths.md](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md) 和 [to_verify_items.md](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/to_verify_items.md) 去收 Apollo 自身的横向语义问题。

## 推荐阅读顺序

如果别人看完这页，还想继续深入，推荐按这个顺序继续：

1. [README](../README.md)
2. [reference index](../reference_pack_carla_ros2_apollo/reference/00_index.md)
3. [startup_commands.md](../reference_pack_carla_ros2_apollo/reference/02_environment_and_versions/startup_commands.md)
4. [known_good_configs.md](../reference_pack_carla_ros2_apollo/reference/02_environment_and_versions/known_good_configs.md)
5. [current_truths.md](../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md)
