# Vehicle Param / Actuator Consistency Companion

本文不再作为 vehicle param 与执行器语义的规范主文档。

## 规范入口
- 执行器语义规范：
  - `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/control_semantics.md`
- localization / reference point 规范：
  - `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/localization_semantics.md`
- 已验证与待验证结论：
  - `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/current_truths.md`
  - `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/to_verify_items.md`

## 这篇文档还负责什么
- 告诉你在做 vehicle param / actuator consistency 排查时，该看哪些 workflow 和 artifacts
- 不在这里重复维护规范结论

## 推荐阅读顺序
1. `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/control_semantics.md`
2. `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/localization_semantics.md`
3. `apollo_carla_semantic_and_calibration_workflow.md`

## 相关 artifact
- `artifacts/carla_vehicle_characteristics.json`
- `artifacts/carla_actuator_calibration.json`
- `artifacts/bridge_control_decode.jsonl`
- `artifacts/apollo_control_raw.jsonl`

## 维护规则
- 新的稳定参数/语义结论回写 `../reference_pack_carla_ros2_apollo/reference/`
- 新的标定步骤、脚本使用方式、回归流程回写 `docs/`
