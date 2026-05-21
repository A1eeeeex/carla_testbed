# Localization Reference Point Companion

本文不再承担 localization 规范语义的主维护职责。

## 规范入口
- localization / frame / timestamp 规范：
  - `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/localization_semantics.md`
- 已验证结论与待验证项：
  - `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md`
  - `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/to_verify_items.md`

## 什么时候读这篇
- 当你在做 startup geometry、`s < 0`、reference line、low-speed lateral saturation 排查时
- 当你需要一个“先看哪里”的操作说明，而不是重复读规范定义时

## 推荐阅读顺序
1. `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/localization_semantics.md`
2. `gt_truth_simulation_pipeline.md`
3. `gt_reference_line_rootcause.md`

## 这类问题优先看哪些 artifact
- `artifacts/startup_geometry_debug.jsonl`
- `artifacts/planning_route_segment_debug.jsonl`
- `artifacts/apollo_control_raw.jsonl`
- `debug_timeseries.csv`

## 维护规则
- 若得到新的稳定 reference point / frame / timestamp 结论，更新 `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/localization_semantics.md`
- 若只是某轮排障顺序、runbook、对照实验流程变化，更新 `docs/` 对应 workflow 文档
