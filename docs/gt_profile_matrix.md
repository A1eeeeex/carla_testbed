# GT Profile 差异矩阵（minimal / relaxed / strict）

本文用于第二阶段：把 GT 真值闭环中的“屏蔽/保护/简化”整理成可切换 profile，支撑后续 ablation study。

对应配置：

- `configs/io/examples/followstop_apollo_gt_minimal.yaml`
- `configs/io/examples/followstop_apollo_gt_relaxed.yaml`
- `configs/io/examples/followstop_apollo_gt_strict.yaml`

## 1. 三档 profile 定位

- `gt_minimal`
  - 定位：保守跑通档。
  - 语义：与冻结基线 `followstop_apollo_gt_baseline.yaml` 等价（仅做显式化标注）。
- `gt_relaxed`
  - 定位：第一层 ablation。
  - 语义：在保持 GT odom/GT obstacles 的前提下，适度放开 planning/control 人工约束，优先保持可跑。
- `gt_strict`
  - 定位：尽量接近 Apollo 原生行为（GT 输入前提下）。
  - 语义：进一步减少 bridge/control 修平，保留 watchdog 等基础安全底线。

## 2. 推荐实验顺序

1. `gt_minimal`：确认环境链路稳定可跑。
2. `gt_relaxed`：观察放开后是否出现起步/跟停/路径行为变化。
3. `gt_strict`：评估接近原生行为时的稳定性边界。

## 3. Profile 差异矩阵

| 维度 | 配置字段/来源 | minimal | relaxed | strict | 这样设置的原因 | 风险 |
|---|---|---|---|---|---|---|
| 感知层输入模式 | `scenario.publish_ros2_native / publish_ros2_gt`（profile YAML） | `false / true` | `false / true` | `false / true` | 三档统一保持 GT 直喂，避免引入传感器链变量 | low |
| GT 发布内容 | `scenario.gt.*`（profile YAML） | 同 baseline | 同 baseline | 同 baseline | 保证 profile 对比焦点在 planning/control，而非输入侧变化 | low |
| 外部栈控制归属 | `algo.disable_legacy_harness_control_for_external_stack`（profile YAML）+ `examples/run_followstop.py` | `true` | `true` | `true` | 避免 harness legacy 控制与 Apollo 控制争用 | low |
| routing 基本策略 | `algo.apollo.routing.*`（profile YAML） | startup/long 双阶段；`freeze_after_success=true` | `freeze_after_success=false` | `freeze_after_success=false` | relaxed/strict 允许运行中持续更新 route 行为，减少冻结策略影响 | medium |
| lane_follow 兜底 | `algo.apollo.routing.auto_enable_lane_follow_fallback`（profile YAML）+ `bridge.py` | `true` | `true` | `false` | strict 显式关闭“自动兜底”，暴露 command 配置真实性能 | medium |
| planning 纵向裁剪 | `planning.acc_only_mode / longitudinal_only_pipeline`（profile YAML） | `true / true` | `false / false` | `false / false` | relaxed/strict 恢复非纯纵向裁剪能力 | high |
| planning 路径任务裁剪 | `disable_lane_change_path / disable_lane_borrow_path`（profile YAML） | `true / true` | `false / false` | `false / false` | 对比裁剪任务对行为的影响 | high |
| planning rule 裁剪 | `disable_rule_based_stop_decider / disable_destination_rule`（profile YAML） | `true / true` | `false / false` | `false / false` | 恢复规则后更接近原生决策 | high |
| lane_follow 场景裁剪 | `planning.lane_follow_only_scenario`（profile YAML） | `true` | `true` | `false` | strict 尽量回归默认 public road scenario 行为 | high |
| traffic light 规则 | `planning.disable_traffic_light_rule`（profile YAML） | `true` | `false` | `false` | relaxed/strict 不再关闭 traffic light 规则 | high |
| traffic light 策略 | `traffic_light.policy`（profile YAML）+ `bridge.py` | `ignore` | `force_green` | `force_green` | 代码当前仅支持 ignore/force_green；strict 选 force_green 避免“直接忽略” | medium |
| control 启动增强 | `control_mapping.startup_throttle_boost_enabled`（profile YAML） | `true` | `false` | `false` | relaxed/strict 取消起步油门补偿，减少人为修平 | medium |
| 直道零转向 | `control_mapping.straight_lane_zero_steer_enabled`（profile YAML） | `true` | `false` | `false` | relaxed/strict 移除直道强制归零干预 | high |
| 低速转向保护 | `control_mapping.low_speed_steer_guard_*`（profile YAML） | `enabled=true`（强） | `enabled=true`（弱化阈值） | `enabled=false` | relaxed 降强度，strict 关闭以接近原生输出 | high |
| 强制零转向输出 | `control_mapping.force_zero_steer_output`（profile YAML） | `true` | `false` | `false` | 该项会显著掩盖 Apollo 横向行为，relaxed/strict 关闭 | high |
| startup 刹车抑制 | `carla_control_bridge.startup_brake_suppression_enabled`（profile YAML） | `true` | `true` | `false` | strict 关闭起步抑制，减少桥接层纵向修正 | medium |
| terminal stop hold | `control_mapping.terminal_stop_hold.enabled`（profile YAML） | `true` | `true` | `true` | 作为当前跟停稳定性底线，三档暂保留 | low |
| watchdog | `carla_control_bridge.watchdog_*`（profile YAML） | 保留 | 保留 | 保留 | 安全底线，避免控制话题中断后失控 | low |

## 4. 已配置化 vs 仍隐含

### 4.1 已纯配置化差异

下列项已可直接通过 YAML profile 切换：

- planning 裁剪开关（`acc_only_mode`、lane/rule disable 等）
- traffic light policy/rule
- control guard（`startup_throttle_boost`、`straight_lane_zero_steer`、`low_speed_steer_guard`、`force_zero_steer_output`）
- startup brake suppression
- routing `freeze_after_success`
- lane_follow 自动兜底（`auto_enable_lane_follow_fallback`）
- 外部栈控制归属（`disable_legacy_harness_control_for_external_stack`）

### 4.2 仍然是代码隐含行为

- objects 输入优先级与降级路径（`objects3d -> objects_json -> markers`）
  - 来源：`tools/apollo10_cyber_bridge/bridge.py`（障碍物解析逻辑）
- follow-stop 场景启动时清理动态 actor
  - 来源：`carla_testbed/scenarios/followstop.py`
- 外部栈模式下整体流程由 adapter/harness 协同驱动（例如 adapter 未启动时的降级路径）
  - 来源：`examples/run_followstop.py`、`carla_testbed/runner/harness.py`
- 当 `traffic_light.policy=ignore` 时，adapter 会默认把 `planning.disable_traffic_light_rule` 设为 `true`（若用户未显式指定）
  - 来源：`algo/adapters/apollo.py`
- 部分 control guard 内部阈值虽然可配置，但组合逻辑（例如多条件闸门与 latch 触发序）仍是代码实现固定
  - 来源：`tools/apollo10_cyber_bridge/bridge.py`

## 5. 后续需继续解耦项

- 将 objects 输入优先级做成可配置枚举（例如 `object_source_priority`）。
- 将 routing startup/long phase 的切换判据（含速度/时间门槛）进一步指标化输出。
- 将 watchdog 触发计数从日志 best-effort 提升为结构化计数埋点。
