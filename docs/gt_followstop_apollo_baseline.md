# GT Follow-Stop Apollo 基线说明书（v1）

## 1. 基线定义

当前仓库将以下链路定义为：**GT 真值最小闭环基线（v1）**。

- 基线 profile：`configs/io/examples/followstop_apollo_gt_baseline.yaml`
- 兼容历史 profile：`configs/io/examples/followstop_apollo_gt.yaml`（保留，但不再作为冻结对照入口）

基线目标：

- 用 GT localization / GT obstacles / GT chassis-like feedback 跑通 Apollo planning/control 最小闭环。
- 验证主链 `CARLA -> ROS2 -> Apollo(CyberRT) -> ROS2 -> CARLA`。
- 作为后续“去屏蔽”和“传感器仿真闭环”的对照与回退版本。

## 2. 当前基线已实现能力

基于当前代码与配置，基线已实现：

- Follow-stop 场景生成与车辆布置（`carla_testbed/scenarios/followstop.py`）。
- CARLA 同步步进驱动（`carla_testbed/runner/harness.py`，`tick_world()`）。
- ROS2 GT 发布（`/carla/<ego>/odom`、`/tf`、objects3d/markers/json，`carla_testbed/ros2/gt_publisher.py`）。
- Apollo CyberRT bridge 转换（`tools/apollo10_cyber_bridge/bridge.py`）。
- 自动 routing（startup/long 双阶段）与 lane_follow 发送（`bridge.py`）。
- traffic light 策略处理（当前 policy=ignore，`bridge.py` + adapter 配置映射）。
- Apollo control 映射与保护链（control mapping + guard + hold，`bridge.py`）。
- 控制回写到 CARLA 的 watchdog 与起步抑制保护（`algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py`）。
- 关键产物落盘：
  - `summary.json`
  - `timeseries.csv`
  - `artifacts/debug_timeseries.csv`
  - `artifacts/bridge_health_summary.json`
  - `artifacts/cyber_bridge_stats.json`
  - `artifacts/cyber_bridge*.log` / `artifacts/cyber_control_bridge*.log`

## 3. 当前关键开关与屏蔽项清单

以下均为当前基线配置与代码中的“显式开关 + 隐式屏蔽”事实。

### A. 场景层

- 地图/天气固定：`run.map=Town01`、`run.weather=ClearNoon`。
  - 来源：`configs/io/examples/followstop_apollo_gt_baseline.yaml`
- Follow-stop 双车场景：`scenario.driver=carla_followstop`。
- ego/front 固定出生点索引：`scenario.ego_idx=120`、`scenario.front_idx=210`。
- 前车自动对齐关闭：`scenario.auto_align_front_spawn=false`。
- 前车候选约束：
  - `scenario.front_min_ahead_m`
  - `scenario.front_max_ahead_m`
  - `scenario.front_max_lateral_m`
  - `scenario.front_max_heading_diff_deg`
- 场景初始化会清理世界已有动态 actor（vehicle/walker）。
  - 来源：`FollowStopScenario._clear_dynamic_actors()`
- 当前主线仅保留 follow-stop（未在该基线中叠加其他行为场景）。

### B. 感知层

- 原生 ROS2 传感器发布关闭：`scenario.publish_ros2_native=false`。
- GT 发布开启：`scenario.publish_ros2_gt=true`。
- GT 子开关：
  - `scenario.gt.publish_tf`
  - `scenario.gt.publish_odom`
  - `scenario.gt.publish_objects3d`
  - `scenario.gt.publish_markers`
  - `scenario.gt.objects_radius_m`
  - `scenario.gt.max_objects`
- 当前 Apollo 输入并非 camera/lidar/radar/imu/gnss 原生链，而是 GT 直喂：
  - odom -> localization
  - GT objects -> perception obstacles
  - CARLA control feedback -> chassis-like feedback
  - 来源：`tools/apollo10_cyber_bridge/bridge.py`
- 隐式降级：`objects3d` 不可用时会 fallback 到 `objects_json`/`objects_markers`。

### C. 规划层

- ACC/纵向裁剪相关：
  - `planning.acc_only_mode`
  - `planning.longitudinal_only_pipeline`
  - `planning.longitudinal_only_keep_lane_follow_path`
- 横向相关任务关闭：
  - `planning.disable_lane_change_path`
  - `planning.disable_lane_borrow_path`
- 规则裁剪：
  - `planning.disable_rule_based_stop_decider`
  - `planning.disable_destination_rule`
  - `planning.disable_traffic_light_rule`
- 场景裁剪：`planning.lane_follow_only_scenario=true`。
- 地图限速覆写能力存在但当前关闭：
  - `map_speed_limit.enabled=false`
  - `map_speed_limit.override_mps=23.61`
  - `map_speed_limit.restore_original=false`
  - 来源：`algo/adapters/apollo.py::_patch_apollo_map_speed_limit`
- 自动 routing 双阶段：
  - startup 阶段（短目标）
  - long 阶段（长目标）
  - 关键字段：`startup_end_ahead_m`、`startup_hold_sec`、`use_long_goal_after_move`、`freeze_after_success`。

### D. 控制层

- Apollo 控制映射缩放：
  - `control_mapping.throttle_scale`
  - `control_mapping.brake_scale`
  - `control_mapping.steer_scale`
- 制动死区：`control_mapping.brake_deadzone`。
- 起步纵向增强：`control_mapping.startup_throttle_boost_enabled`（及 add/cap）。
- 直道归零与低速转向保护：
  - `straight_lane_zero_steer_enabled`
  - `low_speed_steer_guard_enabled`
- 强制零转向输出：`control_mapping.force_zero_steer_output`。
- 纵向 override 结构预留（当前默认关闭）：
  - `control_mapping.straight_acc_override.enabled=false`
- 终端停车保持：`control_mapping.terminal_stop_hold.*`。
- 控制桥 watchdog/起步抑制：
  - `carla_control_bridge.watchdog_wait_for_first_msg`
  - `carla_control_bridge.watchdog_arm_delay_sec`
  - `carla_control_bridge.startup_brake_suppression_enabled`

## 4. 当前基线的性质与限制

- 这是一个**可运行的 GT 最小闭环基线**。
- 它**不是**完整传感器仿真闭环。
- 它**不是**完整“无屏蔽”的 Apollo 原生能力验证。
- 当前结果受到下列因素共同影响：
  - planning/task 裁剪
  - routing 目标策略
  - traffic light 策略
  - control guard / hold / watchdog
- 因此，当前运行结果不能直接外推为“完整系统真实能力上限”。

## 5. 后续版本对照原则

- 去屏蔽实验必须相对本 baseline 做增量对照，不得跳过基线直接横向结论化。
- 引入传感器仿真时，必须保留本 baseline 作为可回退版本。
- 禁止在 baseline 文件上直接叠加实验策略后覆盖提交，避免基线失真不可复现。
- 建议每次实验新增独立 profile，并记录相对 baseline 的差异清单。

三档 profile（`minimal/relaxed/strict`）及其差异矩阵见：`docs/gt_profile_matrix.md`。

## 6. 基线评价指标与工具

评估工具：`tools/evaluate_gt_baseline.py`

示例：

```bash
python tools/evaluate_gt_baseline.py --run-dir runs/<run_name>
```

输出：

- `runs/<run_name>/artifacts/gt_baseline_metrics.json`
- `runs/<run_name>/artifacts/gt_baseline_metrics.md`

### 指标定义（v1）

- `routing_success`
  - 规则：优先 `routing_success_count >= 1`。
  - 同时输出 `routing_request_count` / `routing_response_count`。
- `launch_success`
  - 规则：运行前 `15s` 内，车速是否达到 `> 0.5 m/s`。
  - 优先数据：`timeseries.csv.v_mps`，缺失时回退 `debug_timeseries.csv.speed_mps`。
- `max_speed`
  - 输出：`max_speed_mps`、`max_speed_kmh`。
  - 优先 `summary.json.max_speed_mps`，并与 timeseries/debug 做交叉检查。
- `stable_follow_gap_m`
  - 优先 gap 字段：`terminal_stop_hold_front_gap_lon_m` -> `front_obstacle_gap_lon_m` -> `front_obstacle_gap_distance_m`。
  - 规则：在 run 后段、已起步且低速样本中计算稳定跟车 gap（中位数），同时给出样本数和波动度。
- `stop_gap_m`
  - 规则：run 末段速度接近 0（默认 `<=0.3 m/s`）且 gap 有效时，取最后有效样本中位数。
  - 无法可靠计算时输出 `null` 并注明原因。
- `control_command_effective_rate`
  - 规则：在“有控制输出”的样本里，统计“车辆有响应（测得油门/刹车/转向或速度响应）”的比例。
  - 主数据源：`debug_timeseries.csv`。
- `watchdog_trigger_count`
  - 先查现有结构化统计字段；当前无直接计数时，从 `cyber_control_bridge` 日志中按 `source=watchdog` 做 best-effort 计数。
  - 若日志也不可用，输出：`watchdog_trigger_count: null` + `watchdog_metric_status: not_available`。
- `abnormal_stop_before_goal`
  - 保守规则：在末段长期近静止、且非 terminal stop hold 主导、同时目标仍远（优先 `distance_to_destination`，缺失时退化为 routing 远距线索）时判为 `true`。
  - 无足够证据时保持 `false` 或 `not_available`，不做激进推断。

## 7. 推荐最小流程

```bash
# 1) 跑冻结基线
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_baseline.yaml

# 2) 执行评估
python tools/evaluate_gt_baseline.py --run-dir runs/<run_name>

# 3) 查看输出
cat runs/<run_name>/artifacts/gt_baseline_metrics.json
```
