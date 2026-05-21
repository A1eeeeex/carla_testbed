# docs/

`docs/` 是本仓库的跟踪型运行/上手/设计文档目录。

## 这里放什么
- onboarding / migration / workflow
- 运行入口与系统 walkthrough
- 帮助人类快速上手或重复操作的说明

## 这里不放什么
- 稳定 truth / contract / failure mode / durable decision
  - 这类内容放到 `../reference_pack_carla_ros2_apollo/reference/`
- 每轮一次性的 narrative 分析报告
  - 这类内容保留在 `../artifacts/` 或 `../runs/`
- 本地 prompt/scratch note
  - `prompt_usually_used.md` 不属于跟踪文档面

## 推荐阅读顺序
1. `../AGENTS.md`
2. `../reference_pack_carla_ros2_apollo/reference/00_index.md`
3. `../README.md`
4. `dual_machine_workflow.md`
5. `migration_followstop_playbook.md`
6. `apollo10_gt_sim.md`
7. `gt_truth_simulation_pipeline.md`

## 主要文档分组
- 展示与总览:
  - `project_showcase.md`
  - 面向对外/对技术同事展示的总览入口，强调当前能力、证据、命令和可见产出
- canonical 运行入口:
  - `python -m carla_testbed run`
  - `tools/run_apollo_mainline.py`
  - `tools/run_town01_capability_online_chain.py`
  - `tools/run_unified_calibration_pipeline.py`
  - `tools/run_town01_demo_showcase.py`
  - 其余 `run_*` 默认视作 internal / regression-only / historical，除非文档明确提升为主线入口
- 上手与协作:
  - `dual_machine_workflow.md`
  - `migration_followstop_playbook.md`
- Apollo GT 主链:
  - `apollo10_gt_sim.md`
  - `gt_truth_simulation_pipeline.md`
  - `gt_followstop_apollo_baseline.md`
  - `gt_profile_matrix.md`
  - `gt_reference_line_rootcause.md`
- workflow companions:
  - `actuator_regression_workflow.md`
  - `apollo_carla_semantic_and_calibration_workflow.md`
- 非规范语义 companion：
  - `actuator_semantics_report.md`
  - `localization_reference_point.md`
  - `vehicle_param_and_actuator_consistency_report.md`
  - 这些文档只做阅读路径和操作提示；规范内容以 `../reference_pack_carla_ros2_apollo/reference/03_interface_contracts/` 与 `../reference_pack_carla_ros2_apollo/reference/05_verified_findings/` 为准
