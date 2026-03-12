# configs/io/examples/

可直接运行的 profile 预设目录。

常用文件：

- `followstop_dummy.yaml`：无外部算法栈，基础 harness 自检。
- `followstop_autoware.yaml`：Autoware 集成 profile。
- `followstop_apollo_demo.yaml`：Apollo 演示 profile。
- `followstop_apollo_gt_baseline.yaml`：**冻结版** Apollo GT 最小闭环基线（推荐默认入口）。
- `followstop_apollo_gt.yaml`：Apollo GT 可编辑 profile（保留，非冻结对照版本）。
- `followstop_apollo_gt_minimal.yaml`：基线等价档（保守跑通）。
- `followstop_apollo_gt_startalign.yaml`：起点投影/参考线修复档（用于排查 `s<0` / reference line 失效）。
- `followstop_apollo_gt_relaxed.yaml`：适度放开档（第一层 ablation）。
- `followstop_apollo_gt_strict.yaml`：接近原生档（高风险）。

运行示例：

```bash
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_baseline.yaml
```

迁移 smoke 推荐顺序：

1. `followstop_dummy.yaml`（先验证宿主机与 CARLA）
2. `followstop_apollo_gt_baseline.yaml`（目标闭环链路）

GT profile ablation 推荐顺序：

1. `followstop_apollo_gt_minimal.yaml`
2. `followstop_apollo_gt_startalign.yaml`
3. `followstop_apollo_gt_relaxed.yaml`
4. `followstop_apollo_gt_strict.yaml`

最小对比流程：

```bash
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_minimal.yaml --run-dir runs/gt_minimal_01
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_startalign.yaml --run-dir runs/gt_startalign_01
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_relaxed.yaml --run-dir runs/gt_relaxed_01
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_strict.yaml --run-dir runs/gt_strict_01

python tools/compare_gt_profiles.py \
  --run-dir runs/gt_minimal_01 \
  --run-dir runs/gt_startalign_01 \
  --run-dir runs/gt_relaxed_01 \
  --run-dir runs/gt_strict_01
```

输出：

- 单 run 指标：`runs/<run>/artifacts/gt_baseline_metrics.json`
- profile 对比：`runs/artifacts/gt_profile_compare.json`、`runs/artifacts/gt_profile_compare.md`
- 起点/参考线诊断：
  - `python tools/diagnose_startup_lane_alignment.py --run-dir runs/<run>`
  - `python tools/diagnose_reference_line_failure.py --run-dir runs/<run>`
