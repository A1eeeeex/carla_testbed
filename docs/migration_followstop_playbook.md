# Followstop 迁移与恢复手册

本文用于把仓库迁移到新主机，并尽快恢复 followstop + Apollo 运行链路。

## 1. 当前仓库的迁移风险

- `runs/` 运行目录体积大且强依赖宿主环境，默认不要迁移历史产物。
- 仓库同时存在新入口与兼容入口（`carla_testbed` 与 `io/scripts`），建议统一用新入口避免混淆。
- Apollo 运行依赖不在 Git 中：
  - CARLA 二进制与运行时
  - Apollo 环境/容器
  - Apollo 地图资产

## 2. 迁移前检查清单

### 2.1 代码与配置

- 确认分支干净并已 push。
- 迁移时保留这些目录：
  - `algo/`
  - `carla_testbed/`
  - `tbio/`
  - `configs/`
  - `tools/apollo10_cyber_bridge/`
  - `examples/`
  - `docs/`
- 默认排除：
  - `runs/`
  - `.venv/`
  - `dist/`
  - `dumps/`

### 2.2 主机依赖

- Python `>=3.9`
- `ffmpeg`
- `docker`（当前 Apollo 模式依赖）
- CARLA 运行时（团队当前基线：0.9.16）

### 2.3 外部资源与本地环境

- 检查 profile 引用的 Apollo 地图配置（`configs/io/examples/followstop_apollo_gt.yaml`）：
  - `algo.apollo.bridge.map_file`
- 本地私有配置：
  - `.env`（不提交）
  - `configs/local.yaml` / `configs/local.*.yaml`（不提交）

## 3. 迁移包准备

仅打包被 Git 跟踪的源码：

```bash
bash tools/prepare_migration_bundle.sh
```

默认会在 `dist/` 下生成 `.tar.gz` 包。

协作场景更推荐直接 clone：

```bash
git clone <repo-url>
```

## 4. 新主机迁移后初始化

### 4.1 基础初始化

```bash
bash tools/bootstrap_native.sh
python -m carla_testbed doctor
```

若使用 conda，请先创建并激活环境，再执行上述命令。

### 4.2 本机配置

创建 `.env`（示例）：

```dotenv
CARLA_ROOT=/path/to/CARLA_0.9.16
```

从模板生成本地配置：

```bash
cp configs/local.example.yaml configs/local.yaml
```

校验路径有效性：

```bash
python3 scripts/check_local_config.py --strict
```

### 4.3 Apollo 前置

- 确认 Docker 能启动你当前 profile 依赖的 Apollo 运行环境。
- 确认 profile 内地图路径在新机器真实存在。

## 5. 快速验收流程

### 步骤 A：仿真基础自检

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_dummy.yaml \
  --override run.ticks=120
```

预期：

- run 正常结束
- `runs/<run>/summary.json` 存在

### 步骤 B：followstop Apollo 闭环

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt.yaml \
  --override run.ticks=600 \
  --override runtime.carla.start=true
```

预期关键产物：

- `runs/<run>/artifacts/cyber_bridge_stats.json`
- `runs/<run>/artifacts/bridge_health_summary.json`
- `runs/<run>/timeseries.csv`

## 6. 迁移后的快速验收指标

- `routing_request_count >= 1`
- `loc_count > 0` 且 `chassis_count > 0`
- 若启用录制，存在 `video/dual_cam/demo_third_person.mp4`
- `timeseries.csv` 中车速能上升到大于 0

## 7. 迁移后的协作基线

- 使用功能分支，避免直接在共享长生命周期分支上改动。
- 保持本机私有配置不入库（`.env`、`configs/local.yaml`）。
- 实验参数优先用 `--override`，稳定后再回写 profile。
- 评审回归时附带 `runs/<run>/artifacts/*` 便于定位差异。
- 双机轮换开发流程见：`docs/dual_machine_workflow.md`。
