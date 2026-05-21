# 执行器语义工作流说明

本文不再作为规范语义主文档维护。

## 规范入口
- 规范语义与字段优先级：
  - `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/control_semantics.md`
- 已验证结论与已排除方向：
  - `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md`
  - `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/false_leads.md`

## 这篇文档还负责什么
- 告诉你做 actuator / control mapping 调查时先看哪些 workflow 文档
- 告诉你该读哪些 artifact，而不是在这里重复规范语义

## 推荐阅读顺序
1. `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/control_semantics.md`
2. `actuator_regression_workflow.md`
3. `apollo_carla_semantic_and_calibration_workflow.md`

## 调查 actuator 语义时优先看
- `artifacts/bridge_control_decode.jsonl`
- `artifacts/apollo_control_raw.jsonl`
- `artifacts/carla_vehicle_characteristics.json`
- `artifacts/carla_actuator_calibration.json`
- `debug_timeseries.csv`

## 维护规则
- 如果得到新的稳定语义结论，更新 `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/control_semantics.md`
- 如果只是本轮回归步骤、观测顺序、脚本用法变化，更新 `docs/` workflow 文档
