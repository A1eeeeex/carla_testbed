# docs/

`docs/` 是本仓库的跟踪型运行/上手/设计文档目录。

当前文档边界：

- Core platform: CARLA harness、config、contracts、recording、artifact workflow。
- Baseline/demo: follow-stop legacy baseline and demos。
- Transition backend: ROS2 / `tbio` compatibility layer。
- Apollo MVP: CARLA + Apollo/CyberRT ground-truth bridge work, planned or experimental depending on the config.

## 这里放什么
- onboarding / migration / workflow
- 运行入口与系统 walkthrough
- 帮助人类快速上手或重复操作的说明
- legacy/demo 边界说明，例如 `legacy.md`

## 这里不放什么
- 稳定 truth / contract / failure mode / durable decision
  - 这类内容放到 `../reference_pack/reference/`
- 每轮一次性的 narrative 分析报告
  - 这类内容保留在 `../artifacts/` 或 `../runs/`
- 当前诊断快照、证据索引、本机 run 结论
  - 这类内容不要作为稳定 docs 推送；需要留痕时放到 ignored 的
    `../artifacts/doc_drafts/`，或沉淀到 reference 的 `to_verify_items.md`
- 本地 prompt/scratch note
  - `prompt_usually_used.md` 不属于跟踪文档面

## 推荐阅读顺序
1. `../AGENTS.md`
2. `../reference_pack/reference/00_index.md`
3. `../README.md`
4. `architecture.md`
5. `configuration.md`
6. `backends.md`
7. `apollo_mvp_bridge.md`
8. `apollo_current_module_logic.md`
9. `apollo_algorithm_inventory.md`
10. `apollo_reproduction.md`
11. `apollo_town01_truth_natural_driving.md`
12. `town01_route_health.md`
13. `carla_direct_ab.md`
14. `calibration_pipeline.md`
15. `autoware_recording.md`
16. `run_artifacts.md`
17. `testing.md`
18. `legacy.md`
19. `dual_machine_workflow.md`
20. `migration_followstop_playbook.md`
21. `apollo10_gt_sim.md`
22. `gt_truth_simulation_pipeline.md`

## 主要文档分组
- 展示与总览:
  - `project_showcase.md`
  - 面向对外/对技术同事展示的总览入口，强调当前能力、证据、命令和可见产出
- canonical 运行入口:
  - `python -m carla_testbed run`
- legacy / transition / operational 入口:
  - `examples/run_followstop.py`
  - `tools/run_apollo_mainline.py`
  - `tools/run_town01_capability_online_chain.py`
  - `tools/run_unified_calibration_pipeline.py`
  - `tools/run_town01_demo_showcase.py`
  - 这些入口用于兼容、演示、回归、调试或专题批处理；不要把新的平台架构塞进这些脚本
- 上手与协作:
  - `architecture.md`
  - `configuration.md`
  - `backends.md`
  - `run_artifacts.md`
  - `testing.md`
  - `legacy.md`
  - `dual_machine_workflow.md`
  - `migration_followstop_playbook.md`
- Apollo GT 主链:
  - `apollo_mvp_bridge.md`
  - `apollo_current_module_logic.md`
  - `apollo_algorithm_inventory.md`
  - `apollo_reproduction.md`
  - `apollo_town01_truth_natural_driving.md`
  - `town01_route_health.md`
  - `carla_direct_ab.md`
  - `calibration_pipeline.md`
  - `autoware_recording.md`
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
  - 这些文档只做阅读路径和操作提示；规范内容以 `../reference_pack/reference/03_interface_contracts/` 与 `../reference_pack/reference/05_verified_findings/` 为准
