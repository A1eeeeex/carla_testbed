# tools/

项目工具脚本与集成辅助目录。

关键内容：

- `apollo10_cyber_bridge/`：ROS2 <-> Apollo CyberRT 桥及配套工具。
- `bootstrap_native.sh`：本机环境初始化脚本。
- `run_town01_route_health.py`：Town01 route-health 资产/分析入口（corpus/build/run/analyze/compare）。
- `run_apollo_mainline.py`：Apollo GT / followstop 主线入口。
- `run_town01_capability_online_chain.py`：Town01 capability 主线入口。
- `run_town01_transport_ab.py`：Town01 `ros2_gt` vs `carla_direct` A/B 入口。
- `run_unified_calibration_pipeline.py`：统一标定闭环入口。
- `run_town01_demo_showcase.py`：Town01 录制展示入口。
- `evaluate_gt_baseline.py`：单次 run 指标评估。
- `compare_gt_profiles.py`：多 profile 指标对比。
- `diagnose_startup_lane_alignment.py`：startup 起点投影/heading 对齐诊断。
- `diagnose_reference_line_failure.py`：reference line / trajectory 失败根因诊断。

约定：

- 该目录主要放运维/集成工具，不放核心业务逻辑。
- 长驻运行逻辑优先放在 `algo/` 或 `tbio/`。
- 未在 README / docs / configs 示例中被提升为主线的 `run_*`，默认按 `internal / regression-only / historical` 理解。
