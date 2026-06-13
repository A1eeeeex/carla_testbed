# configs/io/examples/ — Transitional Profiles

This directory contains transitional / legacy runnable profiles.

These files are kept for reproducibility, demos, Town01 evidence, calibration workflows, and ongoing Apollo experiments. They should not be treated as the target location for new platform configuration. New small canonical examples should start in `configs/examples/`; reusable future profiles should move toward `configs/profiles/`.

Known limitation: many historical profiles in this directory still require machine-local values such as Apollo map roots, Docker container names, and host Python executables. Tracked profiles must use env placeholders such as `${APOLLO_MAP_ROOT}` or `${CARLA16_PYTHON}`; provide real values through `configs/local/`, environment variables, or CLI overrides.

## Apollo 主链

当前 Apollo 主链已经整理为三段式：

- `followstop_apollo_gt_minimal.yaml`
  - 最保守、最低成本的 smoke / 链路存活验证
  - 适合先看 routing / planning / control 是否基本通
- `followstop_apollo_gt_relaxed.yaml`
  - 当前默认推荐主线
  - 适合日常 Apollo 场景运行、控制验证、标定前后对比
- `followstop_apollo_gt_strict.yaml`
  - 与 relaxed 同一主链，但保留更多 Dreamview 录制、guard 和事件流
  - 适合深度排障

默认入口：

- `followstop_apollo_gt.yaml`
  - 默认别名，当前等价于 `followstop_apollo_gt_relaxed.yaml`

统一运行入口：

```bash
conda run -n carla16 python tools/run_apollo_mainline.py --profile minimal
conda run -n carla16 python tools/run_apollo_mainline.py --profile relaxed
conda run -n carla16 python tools/run_apollo_mainline.py --profile strict
```

也可以直接运行某个 yaml：

```bash
conda run -n carla16 python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt_relaxed.yaml
```

## 推荐工作流

1. 快速 smoke：`followstop_apollo_gt_minimal.yaml`
2. 日常主线验证：`followstop_apollo_gt_relaxed.yaml`
3. 深度排障 / 证据保留：`followstop_apollo_gt_strict.yaml`

如果不知道该跑哪个，直接跑：

```bash
conda run -n carla16 python tools/run_apollo_mainline.py --profile relaxed
```

## 受保护的兼容配置

以下配置保留给兼容性或特殊实验，不再作为主链默认推荐入口：

- `followstop_apollo_gt_case2_locked.yaml`
  - 保守 locked 基线，对应当前 minimal 的稳定参考
- `followstop_apollo_gt_case3_locked.yaml`
  - 可信 snap 的实验主线参考
- `followstop_apollo_gt_case3_freeze_locked.yaml`
  - freeze 变体，只用于 reroute 对照
- `followstop_apollo_gt_case3_tightened_locked.yaml`
  - 当前 relaxed/strict 的 protected reference

## Town01 Route Health

Town01 随机起点自由巡航健康场景现在以单一主线运转：

- `town01_apollo_route_health.yaml`
  - Town01 单一健康主线 canonical config
  - 统一入口通过 corpus + feature flags 驱动
  - 现已默认启用 deferred-control startup，优先保证 Apollo 跨过
    `PLANNING_READY -> CONTROL_READY`
  - 默认不把历史多 profile 当主线
- `town01_apollo_route_health_relaxed.yaml`
  - 已降级为兼容别名参考，但已同步 canonical 的 deferred-control 修复
- `town01_apollo_route_health_relaxed_probe.yaml`
  - Town01 主线的探针/排障变体
- `town01_apollo_route_health_minimal.yaml`
  - 已降级为兼容别名参考
- `town01_apollo_route_health_strict.yaml`
  - 已降级为兼容别名参考
- `town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml`
  - 当前 strongest Town01 行为恢复基线
  - `lane_keep 097` 的 strongest 证据基于这条主线
- `town01_apollo_route_health_behavior_recovery_stitcher_v1_demo.yaml`
  - strongest 基线的本地 CARLA demo 录制派生
  - 只补 `record.visual`，默认输出 `dual_cam` 第三人称视频
  - 用于展示，不替代 capability promotion 主线
- `town01_apollo_route_materialization_probe_reference_line_pullback_v1.yaml`
  - 基于 materialization probe 的 reference-line / trajectory stitcher A/B
    诊断候选
  - 用于验证 Apollo trajectory 是否继续贴 ego 漂移，而不是 claim
    natural-driving pass
  - `2026-06-13` 在线样本
    `runs/apollo_reference_line_pullback_online_20260613_151737` 为负结果：
    Chassis GT contract 已 `pass/claim_grade=true`，但 run
    `fail_reason=EGO_NOT_MOVING`，link-health primary blocker 为
    `route_establishment:apollo_routing_lane_sequence_mismatch`；因此该配置
    继续只能作为 diagnostic A/B，不能设为默认或 promotion candidate

如果希望一条命令录完当前 Town01 最适合展示的几个关键场景，可以直接用：

```bash
python3 tools/run_town01_demo_showcase.py
```

默认录制：
- `lane_keep 097`
- `lane_keep 217`
- `junction 031`

如果要把当前 curve 攻关过程也一起录成工程诊断 demo：

```bash
python3 tools/run_town01_demo_showcase.py --include-curve-diagnostic
```

说明：
- `curve` 当前仍是诊断性样本，不应当作 capability promotion 证据；
- 包装脚本本质上还是调用 `tools/run_town01_capability_online_chain.py`，只是把 Town01 demo 配置和关键 step 固化成了更适合展示的入口。

## 已降级的历史排障配置

以下文件保留给历史复现或专题排障，不再作为日常主链入口：

- `followstop_apollo_gt_baseline.yaml`
  - 历史 startup geometry 坏链路复现档
- `followstop_apollo_gt_startalign.yaml`
  - 历史 startup/reference-line 排障档
- `followstop_apollo_gt_validation.yaml`
  - 执行器/控制语义专项验证档
