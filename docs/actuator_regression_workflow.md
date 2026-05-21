# 执行器回归验证流程

## 1. 目标

本流程用于比较两套映射：

- `legacy`
- `physical`

并验证 steering / throttle / brake 在开环与闭环下是否一致性更好。

## 2. 总体顺序

必须按这个顺序跑：

1. 开环标定
2. 固定场景执行器跟踪验证
3. Apollo 真实控制序列 replay 验证
4. 闭环 `validation`
5. 闭环 `relaxed`
6. 闭环 `strict`
7. `legacy vs physical` 对照

不要跳过开环、固定场景执行器跟踪验证和 Apollo 真实控制序列 replay 验证，直接评价 `physical`。

## 3. 开环标定

### 3.1 前置条件

- CARLA 已启动
- ego 车辆已存在
- 最好使用与闭环回归同一辆车、同一地图、同一版本的 CARLA

### 3.2 标定命令

```bash
python3 tools/calibrate_carla_actuators.py \
  --carla-host 127.0.0.1 \
  --carla-port 2000 \
  --ego-role-name hero \
  --output artifacts/carla_actuator_calibration.json
```

产物：

- `artifacts/carla_actuator_calibration.json`

### 3.3 开环检查项

- steering:
  - `carla_steer_cmd -> measured_steer_deg` 是否单调
  - `target_front_wheel_angle_deg -> carla_steer_cmd` 反查表是否合理
- throttle:
  - `throttle_cmd -> longitudinal_accel` 是否明显非线性
  - speed-bin 曲线是否区分了低速与中高速
  - `median_response_delay_sec` 是否异常大
- brake:
  - `brake_cmd -> longitudinal_decel` 是否单调
  - `deadzone_cmd`
  - `low_speed_hold_cmd`
  - `max_effective_brake_cmd`

## 4. 闭环 legacy 基线

## 4. 固定场景执行器跟踪验证

这一步是当前推荐的“主证明链”。

目的不是看 Apollo 规划/定位/参考线，而是只看：

- Apollo 物理目标
- bridge 映射结果
- CARLA 实际执行结果

也就是验证：

- `target_front_wheel_angle_deg -> measured_steer_deg`
- `target_accel_mps2 -> measured_forward_accel_mps2`
- `target_decel_mps2 -> measured_forward_accel_mps2`

推荐命令：

```bash
python3 tools/validate_apollo_carla_actuator_tracking.py \
  --config configs/io/examples/apollo_actuator_tracking_validation.yaml \
  --calibration-file artifacts/carla_actuator_calibration.json \
  --output-dir runs/apollo_carla_tracking_validation
```

这个脚本会自动使用：

- 固定地图
- 固定出生点
- 无前车
- `external_probe_hold`

它不会启动 Apollo 规划闭环，也不会用 `followstop/relaxed` 来混入场景级误差。

关键产物：

- `artifacts/actuator_tracking_validation.json`
- `artifacts/actuator_tracking_validation_report.md`
- `artifacts/steering_tracking_measurements.csv`
- `artifacts/throttle_tracking_measurements.csv`
- `artifacts/brake_tracking_measurements.csv`

主指标：

- steering 平均绝对误差
- throttle 平均绝对误差
- brake 平均绝对误差

只有这一步通过后，才进入闭环 `validation / relaxed / strict`。

## 5. 闭环 legacy 基线

## 5. Apollo 真实控制序列 Replay 验证

这一步比“手工目标网格”更严格。

流程是：

1. 先在专用语义场景里录一版 Apollo 真实输出的 `apollo_control_raw.jsonl`
2. 再把这段真实控制序列离线 decode 成物理目标：
   - `target_front_wheel_angle_deg`
   - `target_accel_mps2`
   - `target_decel_mps2`
3. 在固定场景里 replay 这段真实目标序列
4. 只计算：
   - `Apollo decoded target`
   - `CARLA measured response`
   的误差

推荐命令：

```bash
python3 tools/run_apollo_control_replay_validation.py \
  --capture-config configs/io/examples/followstop_apollo_gt_validation.yaml \
  --capture-config configs/io/examples/followstop_apollo_gt_relaxed.yaml \
  --validation-config configs/io/examples/apollo_actuator_tracking_validation.yaml \
  --calibration-file artifacts/carla_actuator_calibration.json \
  --output-dir runs/apollo_control_replay_validation
```

注意：

- 不推荐直接用 `apollo_semantic_capture_lateral.yaml` / `apollo_semantic_capture_longitudinal.yaml`
  作为 replay capture 来源，除非你已经确认这两组场景能稳定激发出非零 Apollo 控制。
- 当前默认推荐使用：
  - `followstop_apollo_gt_validation.yaml`
  - `followstop_apollo_gt_relaxed.yaml`
  因为仓库内已有实测表明，这两类 run 更容易产生有效的非零 steering / throttle / acceleration 序列。

关键产物：

- `artifacts/apollo_control_replay_validation.json`
- `artifacts/apollo_control_replay_validation_report.md`
- `artifacts/apollo_control_replay_validation.csv`
- `artifacts/apollo_replay_sequence.csv`

主指标：

- `mean_abs_steer_error_deg`
- `mean_abs_accel_error_mps2`
- `mean_abs_decel_error_mps2`

这一步当前是“最接近 Apollo 真实在线控制意图”的验证链。

## 6. 闭环 legacy 基线

先保留当前基线，跑 `legacy`。

注意：

- `followstop_apollo_gt_minimal.yaml` 当前保留了 `force_zero_steer_output: true`，
  它适合做“最保守跑通档 / 纵向 smoke”，不适合做横向匹配主验证。
- 做横向或整体匹配验证时，优先使用：
  - `followstop_apollo_gt_validation.yaml`
  - `followstop_apollo_gt_relaxed.yaml`
  - `followstop_apollo_gt_strict.yaml`
- 如果必须用 `minimal`，至少要显式关闭：
  - `force_zero_steer_output`
  - `straight_lane_zero_steer_enabled`
  - `low_speed_steer_guard_enabled`

示例：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt_minimal.yaml \
  --override algo.apollo.control_mapping.actuator_mapping_mode=legacy
```

同理再跑：

- `followstop_apollo_gt_validation.yaml`
- `followstop_apollo_gt_relaxed.yaml`
- `followstop_apollo_gt_strict.yaml`

建议每个 profile 至少保留一组 `legacy` run 作为冻结对照。

## 7. 闭环 physical 对照

把同一份标定文件接入 `physical`。

示例：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt_validation.yaml \
  --override algo.apollo.control_mapping.actuator_mapping_mode=physical \
  --override algo.apollo.control_mapping.physical.calibration_file=artifacts/carla_actuator_calibration.json
```

同样再跑：

- `followstop_apollo_gt_validation.yaml`
- `followstop_apollo_gt_relaxed.yaml`
- `followstop_apollo_gt_strict.yaml`

## 8. 建议保存的关键产物

每个 run 至少保留：

- `summary.json`
- `timeseries.csv`
- `artifacts/debug_timeseries.csv`
- `artifacts/cyber_bridge_stats.json`
- `artifacts/bridge_health_summary.json`
- `artifacts/control_decode_debug.jsonl`
- `artifacts/carla_vehicle_characteristics.json`
- `artifacts/carla_actuator_calibration.json`
- `artifacts/actuator_tracking_validation.json`
- `artifacts/apollo_control_replay_validation.json`

## 9. 必检指标

### 8.1 执行器匹配误差

优先看固定场景执行器跟踪验证里的：

- `abs(target_front_wheel_angle_deg - measured_steer_deg)`
- `abs(target_accel_mps2 - measured_accel_mps2)`
- `abs(target_decel_mps2 - measured_decel_mps2)`

这是当前判断“Apollo↔CARLA 是否对应上”的主指标。

### 9.2 Apollo Replay 匹配误差

优先看 Apollo replay 验证里的：

- `mean_abs_steer_error_deg`
- `mean_abs_accel_error_mps2`
- `mean_abs_decel_error_mps2`

这是当前最接近“Apollo 真发什么，CARLA 就执行成什么”的指标。

### 9.3 Steering

- steering 是否仍长期饱和
  - 看 `debug_timeseries.csv` 中：
    - `apollo_desired_steer`
    - `commanded_steer`
    - `commanded_steer_clamped`
- `measured_steer_deg` 是否更接近目标
  - 看：
    - `target_front_wheel_angle_deg`
    - `mapped_carla_steer_cmd`
    - `measured_steer_deg`
- `e_psi_deg` 是否下降
- `e_y_m` 是否更稳

### 9.4 Planning / 闭环稳定性

- `planning no trajectory` 是否下降
- `Reference lane is empty` / `s<0` 是否下降
- `routing_success_count` 是否保持不退化

### 9.5 Throttle / Brake

- throttle / brake 是否更线性
  - 看：
    - `target_accel_mps2`
    - `mapped_throttle_cmd`
    - `measured_forward_accel_mps2`
    - `target_decel_mps2`
    - `mapped_brake_cmd`
- 起步是否更平顺
- 跟停是否更平顺
- stop-go 是否更少出现“油门给了但车不走 / 小刹车拖住车”的现象

### 9.6 结果指标

- `max_speed_mps`
- `stop_gap`
- `stable_follow_gap`
- `watchdog_trigger_count`
- `control_command_effective_rate`

## 10. legacy vs physical 对照表

建议至少整理这张表：

| 项目 | legacy | physical | 结论 |
|---|---:|---:|---|
| steering saturation frames |  |  |  |
| mean abs e_psi_deg |  |  |  |
| mean abs e_y_m |  |  |  |
| planning empty trajectory count |  |  |  |
| launch success |  |  |  |
| max_speed_mps |  |  |  |
| stop_gap_m |  |  |  |
| stable_follow_gap_m |  |  |  |
| startup smoothness |  |  |  |
| follow-stop smoothness |  |  |  |

## 11. 推荐读取字段

优先看 `artifacts/debug_timeseries.csv` 的这些列：

- `actuator_mapping_mode`
- `throttle_mapping_source`
- `brake_mapping_source`
- `steer_mapping_source`
- `physical_fallback_reason`
- `target_front_wheel_angle_deg`
- `mapped_carla_steer_cmd`
- `measured_steer_deg`
- `target_accel_mps2`
- `mapped_throttle_cmd`
- `measured_forward_accel_mps2`
- `target_decel_mps2`
- `mapped_brake_cmd`
- `e_psi_deg`
- `e_y_m`

## 12. 通过标准建议

`physical` 至少应满足：

1. 固定场景执行器跟踪验证里，三轴误差明显下降
2. Apollo 真实控制序列 replay 验证里，三轴误差仍然明显下降
3. 不比 `legacy` 更差的 routing / planning 稳定性
4. steering 饱和帧显著下降，或 `measured_steer_deg` 更接近目标
5. 纵向加减速更接近目标、起步更顺、跟停更顺
6. 没有因为 calibration 缺失而长期落入 `physical_fallback_reason`

## 13. 需要人工复核的点

- Apollo 顶层 `acceleration` 与 `simple_lon_debug.acceleration_cmd` 的真实语义
- 当前 calibration 是否只适用于单一车型/地图/天气
- `control_out_type` 是否保持为 `direct`
- `localization_back_offset_m` 是否已经统一到合适的 rear-axle 语义
