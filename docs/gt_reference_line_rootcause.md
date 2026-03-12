# GT Reference Line 失效排查（聚焦 routing / lane projection / s<0）

本文用于替代“继续修 traffic light / steer guard”的排查路径，聚焦以下症状：

- `Reference lane is empty`
- `Cannot find waypoint ... s < 0`
- `planning has no trajectory point`
- `EGO_NOT_MOVING` 且 Apollo control 长期轻刹车

## 1. 从 __04 复验得到的关键证据

- routing 已成功：`routing_success_count=2`（`artifacts/cyber_bridge_stats.json`）。
- planning 失败高频：`Reference lane is empty`、`Cannot find waypoint`（`artifacts/apollo_planning.INFO`）。
- control 退化：`planning has no trajectory point`（`artifacts/apollo_control.INFO`）。
- 场景存在 ego fallback：`fallback_spawn_used.ego=true`（`artifacts/scenario_goal.json`）。

结论：主阻塞在 reference line / lane projection / reference point，不在 traffic light 或末端 steer guard。

## 2. 当前仓库里的根因优先级

1. `spawn_with_retry` fallback 行为过于激进，可能把 ego 放到与预期不一致的 lane 起点附近。
2. startup route start 与 localization reference point 的组合导致 `s<0`（典型是 `localization_back_offset_m` 与起点位置叠加）。
3. routing 成功不等于 planning 能构建 reference line；ADC 当前定位仍可能落在 route 有效段之外。

## 3. 已落地的最小修复

### 3.1 spawn fallback 修复（代码）

文件：`carla_testbed/sim/actors.py`

- 旧行为：首选点失败后按 `0,1,2...` 尝试（`max_tries=5` 时几乎必然落到地图前部）。
- 新行为：按 `preferred,+1,-1,+2,-2...` 对称就近尝试，优先保持场景语义。

### 3.2 spawn fallback 可观测性（代码）

文件：`carla_testbed/scenarios/followstop.py`

- 新增日志：当 `used_idx != requested_idx` 时打印 `[followstop][warn] ... spawn fallback`。

### 3.3 起点对齐修复档（配置）

文件：`configs/io/examples/followstop_apollo_gt_startalign.yaml`

相对 minimal 的关键差异：

- `algo.apollo.bridge.localization_back_offset_m: 0.0`
- `algo.apollo.routing.start_nudge_m: 0.0`
- `algo.apollo.routing.start_nudge_retry_step_m: 0.0`
- `algo.apollo.routing.start_nudge_min_safe_m: 0.0`
- `algo.apollo.routing.start_nudge_max_m: 0.0`

用途：先排除 startup 起点被前推、reference point 偏置导致的 `s<0` 问题。

## 4. 诊断工具（新增）

### 4.1 startup lane 对齐

```bash
python tools/diagnose_startup_lane_alignment.py --run-dir runs/<run_name>
```

输出：

- `artifacts/startup_lane_alignment_summary.json`
- `artifacts/startup_lane_alignment_summary.md`

覆盖：startup heading 对齐、route 首次请求、negative s、spawn fallback、yaw 建议。

### 4.2 reference line 根因

```bash
python tools/diagnose_reference_line_failure.py --run-dir runs/<run_name>
```

输出：

- `artifacts/reference_line_rootcause_summary.json`
- `artifacts/reference_line_rootcause_summary.md`

覆盖：routing 成功但 planning/reference line 失败、negative s、control 无轨迹、配置风险项。

## 5. 推荐最小复验顺序

1. `minimal` 跑一遍（保留基线对照）。
2. `startalign` 跑一遍（验证 reference line 是否恢复）。
3. 对两次 run 分别执行两个诊断工具。
4. 若 `startalign` 消除 `s<0` 和 `Reference lane is empty`，再继续 relaxed/strict。
