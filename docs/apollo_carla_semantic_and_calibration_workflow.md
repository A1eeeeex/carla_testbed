# Apollo↔CARLA 控制语义捕获与执行器标定工作流

## 为什么旧 run 不够可靠

旧的 `minimal / relaxed / strict` run 主要目标是让业务链路跑通，而不是暴露控制语义本身，因此有三个问题：

1. 旧 run 往往只覆盖单一工况，字段变化范围窄，容易把某个字段误判成“语义正确”。
2. bridge 里的保护链会污染观测，例如 `startup_throttle_boost`、`straight_lane_zero_steer`、`low_speed_steer_guard`、`terminal_stop_hold` 会改变最终落到 CARLA 的控制。
3. 旧 run 通常缺少分层落盘，容易只看到“最后车怎么动了”，却看不到 Apollo 原始 `ControlCommand`、bridge 解码中间量、CARLA 实测响应之间的关系。

因此，这套工具把“语义确认”和“执行器标定”拆成关联但不同的两件事：

- 语义确认：判断 Apollo 哪个字段更像意图量，哪个字段更像执行器量。
- 执行器标定：测 CARLA `steer / throttle / brake` 的实际响应，并建立反向查表。

## 一键入口

主入口：

```bash
python3 tools/run_apollo_actuator_semantic_suite.py \
  --apollo-profile configs/io/examples/apollo_semantic_suite_base.yaml \
  --map Town01 \
  --mode full \
  --carla-host 127.0.0.1 \
  --carla-port 2000
```

可选参数：

- `--apollo-profile`：Apollo semantic suite 专用 base config。默认是 `apollo_semantic_suite_base.yaml`。
- `--vehicle-profile`：额外 YAML overlay，用于换车、改车辆蓝图、改 semantic scene 参数。
- `--map`：覆盖 `run.map`。
- `--output-dir`：suite 输出目录。默认 `runs/semantic_suite_<timestamp>/`。
- `--mode semantic|calibration|full`：
  - `semantic`：只跑语义捕获场景和离线语义推断。
  - `calibration`：只跑 CARLA 开环执行器标定。
  - `full`：先跑语义捕获，再跑开环标定，并生成推荐 physical 映射配置。
- `--carla-host/port`：统一覆盖 suite 中所有 sub-run 的 CARLA 连接参数。
- `--carla-ready-timeout-sec`：suite 外层等待 CARLA `get_world()` 成功的超时，默认 `300s`。
- `--hero-ready-timeout-sec`：calibration stage 等待 `hero` 车出现的超时，默认 `240s`。
- `--carla-get-world-attempts / --carla-get-world-delay-sec`：传给每个 scene 内部 `get_world()` 重试的参数。
- `--carla-load-world-attempts / --carla-load-world-delay-sec / --carla-load-world-timeout-sec`：传给每个 scene 内部地图加载重试的参数。
- `--calibration-library-dir`：保存和复用标定结果的目录。默认 `artifacts/actuator_calibration_library/`。
- `--force-recalibration`：即使命中已保存标定，也强制重新跑一次 calibration。
- `--calibration-detail standard|exhaustive`：标定细致程度。默认 `exhaustive`，会使用更密的 steering/throttle/brake 命令、更细的速度分段和更长的采样窗口，更适合“一次标细、长期复用”。

如果机器启动较慢，建议直接拉长等待，例如：

```bash
python3 tools/run_apollo_actuator_semantic_suite.py \
  --apollo-profile configs/io/examples/apollo_semantic_suite_base.yaml \
  --mode full \
  --map Town01 \
  --carla-ready-timeout-sec 480 \
  --hero-ready-timeout-sec 360 \
  --carla-get-world-attempts 12 \
  --carla-get-world-delay-sec 5 \
  --carla-load-world-attempts 6 \
  --carla-load-world-delay-sec 6 \
  --carla-load-world-timeout-sec 180
```

## 标定结果如何保存与复用

这套工具现在会为“当前地图 + Apollo profile 内容 + vehicle overlay 内容”计算一个请求指纹。

- 如果命中已保存标定：
  - suite 会打印 `matched saved calibration`
  - `semantic` 和 `full` 会把该标定复制到当前 run 的 `artifacts/carla_actuator_calibration.json`
  - 不需要再次重跑 calibration
- 如果没有命中：
  - suite 会明确打印 warning，提示当前环境建议先做一次 calibration
  - 跑完后会自动把新标定保存到 `artifacts/actuator_calibration_library/<fingerprint>/`

也就是说，换车、换 Apollo 控制器、换地图或改 vehicle overlay 后，会因为指纹变化而提示重新标定；如果这些关键上下文不变，就可以直接复用历史标定。

当前保存目录下至少会包含：

- `carla_actuator_calibration.json`
- `metadata.json`
- `index.json`

其中 `metadata.json` 会记录：

- 请求指纹
- Apollo profile 哈希
- vehicle overlay 哈希
- 地图名
- 实际车辆 `type_id / mass / max_steer_angle_deg`
- 标定来源 run 目录

## 专用 profile 片段

这次新增的是 suite 专用 overlay，而不是替换现有 baseline。语义捕获运行时使用的是专用 driver：`carla_apollo_semantic_suite`。开环标定则单独使用 `carla_actuator_calibration`，不再借用 semantic suite 的纵向起步场景。

- `configs/io/examples/apollo_semantic_capture_lateral.yaml`
- `configs/io/examples/apollo_semantic_capture_longitudinal.yaml`
- `configs/io/examples/apollo_actuator_calibration.yaml`
- `configs/io/examples/apollo_semantic_suite_base.yaml`

设计原则：

- 保留 watchdog、超时保护。
- 尽量关掉会污染观测的强保护。
- 语义捕获默认使用 `legacy` 映射，避免 physical 先把 Apollo 原始语义重新解释一遍。
- calibration stage 使用 dummy stack + 外部 probe hold 模式，避免开环探测时被其他控制器或 harness fail-fast 抢占生命周期。

`apollo_semantic_suite_base.yaml` 相比旧 `followstop_apollo_gt_minimal.yaml` 的关键影响是：

- `force_zero_steer_output: false`
- `straight_lane_zero_steer_enabled: false`
- `low_speed_steer_guard_enabled: false`
- `startup_throttle_boost_enabled: false`
- `terminal_stop_hold.enabled: false`
- `front_obstacle_behavior.mode: normal`

也就是说，新的默认底座不会再把横向原始控制信号在 bridge 层直接压成 0，也不会默认引入起步 boost 和终点 hold 去污染纵向语义观测。

同时，这个 base profile 也把 CARLA 启动等待调得更保守：

- `runtime.carla.ready_timeout_sec: 360.0`
- `runtime.carla.post_ready_settle_sec: 8.0`
- `runtime.carla.get_world_attempts: 8`
- `runtime.carla.load_world_attempts: 5`

这部分是参考 `followstop` 现有启动方式补强的：不仅等端口打开，还要求 `get_world()` 真正成功，并在 ready 后再留一个短暂 settle 时间。

## 一键脚本会自动跑哪些场景

### 横向语义捕获

- `lat_straight_track`：直道轻微跟踪，采小幅 steering 修正。
- `lat_left_curve`：自动选择温和左弯段，采左向 steering。
- `lat_right_curve`：自动选择温和右弯段，采右向 steering。

说明：

- 新 driver 会直接扫描 CARLA waypoint，自动选取直道、左弯、右弯候选段。
- 姿态偏置仍保留为辅助激励手段，用来避免一上来就落入 steering 饱和或完全零输出区。

### 纵向语义捕获

- `lon_launch_cruise`：起步加速 + 短时巡航。
- `lon_follow_decel`：跟车减速直到接近静止。
- `lon_stop_and_go`：动态前车 stop-and-go 剧本。

说明：

- `longitudinal_stop_and_go` 由专用 scenario driver 在仿真 tick 内驱动前车执行“停车 -> 起步巡航 -> 再减速停车”。
- 这不是完整交通流模型，但已经不再依赖静态前车 + 多 run 代理样本去拼 stop-and-go。

### 开环执行器标定

- `steer cmd -> measured_steer_deg / yaw_rate / curvature`
- `throttle cmd -> accel`
- `brake cmd -> decel`

这部分由 `tools/calibrate_carla_actuators.py` 执行，但被统一纳入 suite 目录和总报告。

当前实现已经升级为 calibration-only 场景：

- 固定地图：默认 `Town01`
- 固定 ego 出生点：默认 `scenario.calibration_only.spawn_idx=120`
- 无前车：只 spawn 一辆 ego
- 严格固定出生点：默认 `strict_spawn=true`，如果该点不可用则直接失败，而不是偷偷 fallback 到别处
- scene driver：`carla_actuator_calibration`

这样做的目的，是把“借车给 probe”这件事简化到最小，不再把语义场景逻辑、前车行为、harness 验收条件混到开环标定里。

## 输出目录结构

典型输出目录：

```text
runs/semantic_suite_20260316_120000/
  suite_manifest.json
  generated_configs/
  logs/
  scenes/
    lat_straight_track/
    lat_left_curve/
    lat_right_curve/
    lon_launch_cruise/
    lon_follow_decel/
    lon_stop_and_go/
  calibration_run/
  artifacts/
    control_semantics_report.md
    control_semantics_report.json
    recommended_physical_mapping.yaml
    carla_actuator_calibration.json
```

单个 semantic scene 的关键产物在 `scenes/<scene>/artifacts/` 下：

- `apollo_control_raw.jsonl`
  - Apollo 原始 `ControlCommand` 字段快照。
  - 重点看 `steering_target / steering_percentage / steering / steering_rate / throttle / brake / acceleration / debug.simple_lon_debug.*`。
- `bridge_control_decode.jsonl`
  - bridge 解码后的 `raw_steer / raw_throttle / raw_brake`。
  - `mapping_mode`
  - `target_front_wheel_angle_deg`
  - `target_accel_mps2 / target_decel_mps2`
  - `mapped_*`
  - `physical_fallback_reason`
- `carla_vehicle_response.csv`
  - CARLA 实车响应层：
  - `speed_mps`
  - `forward_accel_mps2`
  - `yaw_rate_rps`
  - `measured_steer_deg`
  - `curvature`
  - `lateral_accel_mps2`
  - `pose_*`
  - `gear / reverse / hand_brake`
- `lateral_geometry_debug.csv`
  - `e_y_m / e_psi_deg / lane_dist_m / preview_heading_deg / reference_lane_curvature`
  - `routing_established`
  - `planning_has_trajectory`
  - `front_obstacle_gap_*`
- `debug_timeseries.csv`
  - 仍保留为总底表，方便回归分析和向后兼容。

## 语义推断是怎么做的

离线分析入口：

```bash
python3 tools/infer_apollo_control_semantics.py \
  --input-dir runs/semantic_suite_xxx \
  --output-dir runs/semantic_suite_xxx/artifacts \
  --calibration-path artifacts/carla_actuator_calibration.json
```

### 横向推断

候选字段：

- `steering_target`
- `steering_percentage`
- `steering`
- `steering_rate`

推断方法：

1. 读取 `apollo_control_raw.jsonl` 里的原始字段。
2. 读取 `carla_vehicle_response.csv` 里的 `measured_steer_deg / yaw_rate_rps / curvature`。
3. 做 0.0s 到 0.8s 的时滞扫描。
4. 计算最佳时滞下的相关性。
5. 结合字段方差和饱和率评分。

输出：

- 哪个字段更像 actuator-like 字段。
- 哪个字段更像 intent-like 字段。
- 当前 bridge 选中的字段分布。
- 是否足够支持把某字段推荐给 `physical.preferred_steering_field`。

### 纵向推断

候选字段：

- `throttle`
- `brake`
- `acceleration`
- `debug_simple_lon_acceleration_cmd`
- `debug_simple_lon_acceleration_lookup`

推断方法：

1. 读取 `forward_accel_mps2`。
2. 对 signed acceleration 候选，直接与 `forward_accel_mps2` 做时滞相关性。
3. 对 throttle-like 候选，与正向加速度做相关性。
4. 对 brake-like 候选，与正向减速度做相关性。
5. 方差不足或长期饱和时标记为弱证据。

输出：

- 推荐的 signed 意图字段。
- 推荐的 throttle-like / brake-like 字段。
- 当前 bridge 已实际命中的 signed 字段分布。

## 如何判断 bridge 用对了字段

看 `artifacts/control_semantics_report.md` 和 `artifacts/control_semantics_report.json`：

- 如果推荐字段与当前 bridge 实际命中的字段一致，且相关性分数明显高于其他候选，说明当前字段选择基本合理。
- 如果推荐字段与 bridge 当前命中字段不一致，且分数差距明显，应优先把 physical 配置切到推荐字段。
- 如果报告写明“证据不足”，不要强行切主线，应继续补场景或人工核对 Apollo 控制逻辑。

## 如何判断 actuator 标定是否可信

看 `artifacts/carla_actuator_calibration.json`：

- steering：
  - `carla_steer_cmd -> measured_steer_deg`
  - `carla_steer_cmd -> yaw_rate / curvature`
  - 反向表是否单调、是否覆盖常用区间。
- throttle：
  - speed-bin 曲线是否随油门基本单调。
  - 低速和中速 bin 是否明显不同。
- brake：
  - `deadzone_cmd` 是否合理。
  - `low_speed_hold_cmd` 是否存在。
  - `max_effective_brake_cmd` 是否明显早于 1.0 饱和。

如果曲线离散、反向表不单调、样本极少，说明这次标定不可信，应重跑 calibration stage。

## 如何判断 physical 是否可以替代 legacy

先应用 suite 产出的：

- `artifacts/carla_actuator_calibration.json`
- `artifacts/recommended_physical_mapping.yaml`

再跑回归对照：

1. `legacy`
2. `physical`

重点比较：

- steering 是否长期饱和
- `measured_steer_deg` 是否更接近目标
- `e_psi_deg` 是否下降
- `e_y_m` 是否更稳
- `planning` 空轨迹是否减少
- 起步、减速、跟停是否更平顺

如果 `physical` 只在某一轴证据不足，允许那一轴继续 fallback 到 `legacy`，不要整套强切。

## 换车 / 换算法 / 迁移环境后怎么重跑

推荐做法：

1. 准备一个新的 base Apollo config，通过 `--apollo-profile` 指向它。
2. 如有车型差异、车辆蓝图差异、spawn 差异，用 `--vehicle-profile` 提供 overlay。
3. 重新跑：

```bash
python3 tools/run_apollo_actuator_semantic_suite.py \
  --apollo-profile <new_apollo_profile.yaml> \
  --vehicle-profile <new_vehicle_overlay.yaml> \
  --map <new_town> \
  --mode full
```

4. 使用新生成的：
  - `control_semantics_report.*`
  - `carla_actuator_calibration.json`
  - `recommended_physical_mapping.yaml`

不要直接沿用旧车、旧控制器、旧环境的标定与字段结论。

## 仍然需要人工确认的地方

- Apollo 字段的源码级严格语义，这套工具给的是“实测相关性推断”，不是源码证明。
- 新 driver 的曲线候选段选择依赖 waypoint 几何启发式，不等价于 HD map 语义级路段分类。
- stop-and-go 前车脚本是轻量 lead profile，不是交通行为学模型；若后续要研究复杂交互，还需要继续扩展。
