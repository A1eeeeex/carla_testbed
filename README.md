# CARLA Testbed

CARLA-based simulation testbed for autonomous driving experiments, currently prioritizing CARLA + Apollo ground-truth MVP closed loop.

This repository is not just a follow-stop demo. The intended project boundary is:

- Core platform: CARLA config, runner/harness, simulation utilities, sensors, contracts, recording, and run artifacts.
- Baseline/demo: the historical follow-stop baseline used for smoke tests, demos, and compatibility checks.
- External backend contract: ROS2 / `tbio` as a transition backend layer, plus an Apollo/CyberRT MVP adapter that is planned or experimental depending on the branch/config.

Current priority is CARLA + Apollo ground-truth closed-loop work. Autoware remains legacy experimental support and is not the equal-priority target for new platform work.

## Current Status

| Area | Status | Notes |
| --- | --- | --- |
| CARLA harness | experimental / supported | Main platform surface: config, run loop, CARLA lifecycle, recording, summaries. |
| Follow-stop baseline | legacy baseline | Useful for smoke/demo/regression, but not the future platform architecture. |
| ROS2 native backend | transitional | Still used by existing GT/control paths; keep compatible while Apollo MVP matures. |
| Apollo/CyberRT bridge | MVP planned / experimental | Current priority for CARLA + Apollo GT closed loop; do not overstate as fully complete. |
| Autoware | legacy experimental | Existing adapter/configs remain, but new work should not assume equal priority. |

## Recommended Entry Points

Config validation:

```bash
python -m carla_testbed config-validate configs/examples/smoke.yaml
```

CI-friendly smoke check. This does not start CARLA or Apollo:

```bash
python -m carla_testbed smoke --config configs/examples/smoke.yaml
```

Typed run dry-run:

```bash
python -m carla_testbed run --config configs/examples/smoke.yaml --dry-run
```

Inspect a run directory:

```bash
python -m carla_testbed inspect-run runs/smoke
```

Compile platform RunPlans without starting CARLA/Apollo/Autoware:

```bash
python -m carla_testbed suite dry-run \
  --suite configs/suites/town01_natural_driving.platform.yaml \
  --out /tmp/town01_platform_suite
```

Build evidence/gate summaries from existing artifacts:

```bash
python -m carla_testbed analyze \
  --run-dir runs/<run_id> \
  --plan /tmp/town01_platform_suite/plans/<run_id>.plan.resolved.yaml
```

These platform commands are planning and evidence-indexing surfaces. They do
not make an Apollo natural-driving pass claim without the required reports such
as `natural_driving_report.json`.

Lightweight examples:

```bash
python examples/run_smoke.py --config configs/examples/smoke.yaml
python examples/run_follow_stop_baseline.py --config configs/examples/smoke.yaml
```

For Apollo follow-stop compatibility demos, use:

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt.yaml \
  --run-dir runs/apollo_gt_demo
```

`examples/run_followstop.py` is a legacy/demo entrypoint. It remains runnable for compatibility and focused debugging, but new platform architecture should be added under `carla_testbed/`, `tbio/`, configs, or a backend/adapter layer as appropriate.

## Do Not Add New Platform Logic Here

These surfaces exist for compatibility, debugging, or regression workflows. They should not become the home for new platform architecture:

- `examples/run_followstop.py`
  - legacy/demo runner for the follow-stop baseline.
- `examples/legacy/`
  - documentation for legacy examples and migration notes.
- `io/`
  - deprecated compatibility layer and old contract wrapper surface.
- `tools/run_*.py`
  - task-specific launchers, regressions, analysis, or operational helpers.

New core behavior should be shaped around the harness, config schema, backend contract, adapter boundary, and run artifact model.

## Documentation Map

本仓库现在有 4 层文档边界：

- `AGENTS.md`
  - 仓库级执行手册与协作规则。
- `reference_pack/reference/00_index.md`
  - 规范化知识库入口；truths、contracts、playbooks、decision log 都以这里为准。
- `docs/README.md`
  - 运行、上手、迁移、workflow 类文档入口。
- `docs/backends.md`
  - backend/adapter 边界说明；区分 canonical adapter contract、transition backend、legacy IO。
- `docs/architecture.md`
  - 当前目录职责、依赖方向、runner/scenario/backend 边界。
- `docs/configuration.md`
  - typed config v0、配置优先级、local override 和 env placeholder 规则。
- `docs/apollo_mvp_bridge.md`
  - Apollo MVP bridge 边界；GT localization/chassis/obstacles/routing/control，不跑真实感知。
- `docs/apollo_current_module_logic.md`
  - 面向读者的 Apollo 当前模块逻辑说明；解释哪些是真 Apollo 模块、哪些由 CARLA truth-input 替代。
- `docs/apollo_algorithm_inventory.md`
  - Apollo 模块化算法栈清单；区分 upstream、replayed、CARLA GT port 和 Town01 tuned variant。
- `docs/apollo_reference_chain.md`
  - Apollo 官方/成熟 bridge 期望链条；配置化列出 module dependencies、required channels、GT replacement contracts 和 claim-grade evidence boundaries。
- `docs/apollo_reproduction.md`
  - Apollo L0-L5 复现层级；冻结 environment/replay/adapter/shadow/closed-loop 证据和归因规则。
- `docs/apollo_town01_truth_natural_driving.md`
  - Town01 truth-input MVP natural-driving goal and gates. Claims require
    local verification artifacts such as `natural_driving_report.json`; this is
    not full perception reproduction.
- `docs/platform_run_plan.md`
  - Offline platform planning layer; compiles platform / algorithm / scenario /
    recording / gate selections into `run_plan.v1`. It does not start
    CARLA/Apollo/Autoware and is not behavior evidence by itself.
- `docs/town01_route_health.md`
  - Town01 route-health schema、P0/P1/P2 recorder 字段、curve segment report 和 run artifact 接入。
- `docs/carla_direct_ab.md`
  - `ros2_gt` vs `carla_direct` A/B manifest、dry-run matrix、multi-metric verdict 和本地验证边界。
- `docs/calibration_pipeline.md`
  - control-actuation calibration profile/report；只解释 throttle/brake/steer response、latency 和 no-regression gates。
- `docs/autoware_recording.md`
  - Autoware demo recording evidence normalization；RViz/rosbag/CARLA video are operator evidence and must be paired with route/channel/control/natural-driving reports before Apollo comparison.
- `docs/run_artifacts.md`
  - run output 目录、manifest、resolved config、summary、events、timeseries。
- `docs/testing.md`
  - CI-friendly 单测、mock CARLA、CARLA smoke、Apollo contract、本地集成测试。
- `docs/project_showcase.md`
  - 面向技术展示的总览页；适合快速介绍架构、功能进度、示例命令与可见产出。
- `artifacts/` / `runs/`
  - 运行证据与分析输入，不是长期规范源。

当前诊断快照、证据索引和本机 run 结论不要作为稳定 `docs/` 推送；这类内容应放在 ignored 的 `artifacts/` / `runs/`，或沉淀到 `reference_pack/reference/05_verified_findings/to_verify_items.md` 后再作为待验证事实维护。

迁移与协作开发建议先看：

- `AGENTS.md`
- `reference_pack/reference/00_index.md`
- `docs/README.md`
- `docs/architecture.md`
- `docs/configuration.md`
- `docs/backends.md`
- `docs/apollo_mvp_bridge.md`
- `docs/apollo_current_module_logic.md`
- `docs/apollo_algorithm_inventory.md`
- `docs/apollo_reference_chain.md`
- `docs/apollo_reproduction.md`
- `docs/apollo_town01_truth_natural_driving.md`
- `docs/town01_route_health.md`
- `docs/carla_direct_ab.md`
- `docs/calibration_pipeline.md`
- `docs/autoware_recording.md`
- `docs/run_artifacts.md`
- `docs/testing.md`
- `docs/project_showcase.md`
- `docs/legacy.md`
- `docs/dual_machine_workflow.md`
- `docs/migration_followstop_playbook.md`
- `tbio/README.md`
- `tools/apollo10_cyber_bridge/README.md`

## 0. 协作开发与推送（2026-03）

推送到 GitHub 前，建议固定执行这组检查：

```bash
git status -sb
git remote -v
git ls-files | rg '__pycache__|\.pyc$|^dist/.*\.sha256$|^io/tools/.*\.so($|\.)|^runs/'
```

判定标准：

- `git status -sb` 应该是干净工作区。
- `git remote -v` 需要有 `origin`（为空时先 `git remote add origin <repo-url>`）。
- 第三个命令应无输出；这些文件属于本地运行产物或宿主机二进制，不应进入仓库。

本仓库当前已在 `.gitignore` 中屏蔽以下常见误提交项：

- `runs/`
- `__pycache__/`、`*.pyc`
- `dist/*.tar.gz`、`dist/*.sha256`
- `io/tools/*.so`、`io/tools/*.so.*`

如果历史上误提交过这些文件，需要先取消跟踪再提交：

```bash
git ls-files | rg '__pycache__|\.pyc$' > /tmp/tracked_pyc_list.txt
git ls-files | rg '^dist/.*\.sha256$' > /tmp/tracked_sha_list.txt
git ls-files | rg '^io/tools/.*\.so($|\.)' > /tmp/tracked_so_list.txt

xargs -a /tmp/tracked_pyc_list.txt git rm --cached
xargs -a /tmp/tracked_sha_list.txt git rm --cached
xargs -a /tmp/tracked_so_list.txt git rm --cached

git commit -m "chore: untrack generated artifacts"
```

常用推送命令：

```bash
git push -u origin main
```

新机器初始化建议：

```bash
bash scripts/setup_dev_env.sh
python3 scripts/check_local_config.py --strict
```

## 0.1 文档索引（按角色）

- 首先看：
  - `AGENTS.md`
  - `reference_pack/reference/00_index.md`
  - `docs/README.md`
- legacy/demo 边界：
  - `docs/legacy.md`
- 配置与 profile：
  - `configs/README.md`
  - `configs/io/examples/README.md`
- 运行入口与 runtime：
  - `examples/README.md`
  - `carla_testbed/README.md`
  - `tbio/README.md`
  - `tools/apollo10_cyber_bridge/README.md`

## 1. 当前可用能力

### 统一入口

推荐入口：

```bash
python -m carla_testbed <subcommand>
```

当前 CLI 子命令来自 `carla_testbed/cli.py`：

- `python -m carla_testbed doctor`
- `python -m carla_testbed run --config <yaml> [--override k=v ...]`
- `python -m carla_testbed smoke --config <yaml>`
- `python -m carla_testbed config-validate <yaml>`
- `python -m carla_testbed inspect-run <run_dir>`

注意：

- `smoke` 是 CI-friendly config smoke，默认不启动 CARLA、Apollo、CyberRT 或 ROS2。
- `run --config configs/examples/smoke.yaml --dry-run` 当前可用于 typed config 路径验证。
- 旧 ROS2 topic smoke 仍可通过 legacy wrapper 调用：

```bash
python io/scripts/smoke_test.py --contract io/contract/canon_ros2.yaml --timeout 5
```

### 兼容入口

旧入口仍在，但只是 wrapper：

- `io/scripts/run.py`
- `io/scripts/smoke_test.py`

代码里已经明确给出弃用提示，推荐继续使用 `python -m carla_testbed ...`。

### 当前兼容与运维入口家族

当前仍有多条可运行入口，主要用于兼容、运维、回归或专题分析。它们不是新的平台架构承载点：

- `python -m carla_testbed run`
  - 推荐的通用单次 run 入口
- `tools/run_apollo_mainline.py`
  - Apollo GT / follow-stop compatibility/demo launcher
- `tools/run_town01_capability_online_chain.py`
  - Town01 capability / route-health operational helper
- `tools/run_unified_calibration_pipeline.py`
  - calibration workflow helper
- `tools/run_town01_demo_showcase.py`
  - Town01 showcase recording helper

其余 `tools/run_*.py` 如果没有在 README、`docs/project_showcase.md` 或 `configs/io/examples/README.md` 明确列为主线入口，默认按下面理解：

- `internal`
- `regression-only`
- `historical`

### 算法栈状态

来自 `algo/registry.py` 和各 adapter：

- `dummy`
  - 已实现
  - `algo/adapters/dummy.py`
  - 不启动外部算法，只运行场景
- `apollo`
  - adapter / MVP bridge path experimental
  - `algo/adapters/apollo.py` + `tbio/backends/cyberrt.py`
  - 当前不要解读为 Apollo 已完整跑通所有 CARLA 场景
- `autoware`
  - legacy experimental compatibility
  - `algo/adapters/autoware.py`
- `e2e`
  - **Not implemented yet**
  - `algo/adapters/e2e.py`
  - 当前仅输出未实现提示，`healthcheck()` 固定返回 `False`

## 2. 目录结构（按当前仓库）

下面是当前仓库顶层到两层目录的结构说明（手写，等价于 `tree -L 2`）：

```text
.
├── README.md
├── algo/
│   ├── adapters/          # 算法栈 adapter（dummy / apollo / autoware / e2e）
│   ├── baselines/         # 外部栈相关资源（如 autoware docker）
│   ├── controllers/       # 控制相关辅助模块
│   ├── nodes/             # 运行时节点（如 CARLA control bridge）
│   └── plugins/           # 预留插件位
├── carla_testbed/
│   ├── config/            # HarnessConfig、rig 加载、配置输出
│   ├── control/           # 本地控制器包装
│   ├── record/            # timeseries、summary、视频、monitor
│   ├── ros2/              # GT ROS2 发布
│   ├── runner/            # TestHarness 主循环
│   ├── scenarios/         # 场景构建；follow-stop 当前为 legacy baseline
│   ├── schemas/           # FramePacket / GroundTruthPacket / ControlCommand
│   ├── sensors/           # 传感器规格与采集
│   ├── sim/               # CARLA client、tick、spawn、world settings
│   └── utils/             # 路径、环境、run 命名
├── configs/
│   ├── io/                # 示例配置（dummy / apollo / autoware）
│   └── rigs/              # 传感器 rig 预设
├── docs/                  # 跟踪的运行/上手/设计文档，不放一次性分析报告
├── dumps/                 # 调试转储（运行时可能写入）
├── examples/
│   ├── carla_examples/    # CARLA 官方/示例脚本
│   ├── run_smoke.py       # lightweight no-runtime smoke wrapper
│   ├── run_follow_stop_baseline.py # lightweight baseline CLI wrapper
│   └── run_followstop.py  # legacy/demo follow-stop entrypoint
├── io/
│   ├── backends/          # IO 后端（保留代码）
│   ├── contract/          # ROS2 合约与 profiles
│   ├── scripts/           # 兼容 wrapper 脚本
│   └── tools/             # IO 相关工具
├── runs/                  # 每次运行输出目录
├── reference_pack/
│   └── reference/         # 规范化知识库（truths / contracts / playbooks / decisions）
├── tbio/
│   ├── backends/          # 后端实现（如 CyberRT backend）
│   ├── carla/             # CARLA 启动策略与 launcher
│   ├── contract/          # 产物生成器
│   ├── ros2/              # ROS2 工具（probe / control logger / goal_engage）
│   └── scripts/           # 统一 run / healthcheck / smoke 等
└── tools/
    ├── apollo10_cyber_bridge/   # Apollo bridge 与相关工具
    └── bootstrap_native.sh      # 本机初始化脚本
```

## 3. 数据如何在模块间流动

### 代码里定义的标准数据结构

在 `carla_testbed/schemas/` 里，当前定义了这些核心结构：

- `FramePacket`（`carla_testbed/schemas/frame_packet.py`）
  - 表示一帧传感器数据
- `GroundTruthPacket`（`carla_testbed/schemas/truth_packet.py`）
  - 表示一帧真值（ego、actors、events）
- `ControlCommand`（`carla_testbed/schemas/algo_io.py`）
  - 表示控制输出
- `AlgoInput` / `AlgoOutput`
  - 表示算法输入输出封装

### 当前主运行链路里哪些是“定义了但未完全接通”

当前 `examples/run_followstop.py` + `carla_testbed/runner/harness.py` 的主路径里：

- `ControlCommand` 是实际在用的
- `FramePacket` / `GroundTruthPacket` 是 schema 层已定义的数据边界
- 但当前 harness 主路径没有把它们作为统一对象完整传递给控制器

也就是说：

- 这两类 packet 当前更像“数据模型定义”
- 当前运行主线仍然是：
  - 直接从 CARLA 取状态
  - 直接调用控制器
  - 直接写 `timeseries.csv` / `summary.json`

### 当前实际运行时的数据流

当前 `followstop` 主路径的数据流是：

```text
CARLA world
  -> Scenario build（生成 ego / front）
  -> TestHarness tick（world.tick）
  -> 本地控制器 or 外部算法栈
  -> ControlCommand
  -> ego.apply_control(...)
  -> 记录 timeseries / summary / monitor / 可选视频 / 可选 ROS2
```

如果启用 Apollo GT 栈（`configs/io/examples/followstop_apollo_gt.yaml`），实际链路是：

```text
CARLA
  -> GroundTruthRos2Publisher（/carla/<ego>/odom, /tf, /tf_static, objects）
  -> tools/apollo10_cyber_bridge/bridge.py
  -> Apollo CyberRT channels
  -> /apollo/control
  -> ROS2 /tb/ego/control_cmd
  -> algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py
  -> CARLA ego.apply_control(...)
```

详细版本可看：

- `docs/gt_truth_simulation_pipeline.md`

### KPI 当前在哪一层

当前没有独立的 `kpi/` 包。

当前 KPI / 回归结果是分散在下面几个位置生成的：

- `carla_testbed/runner/harness.py`
  - 计算成功/失败、最大速度、碰撞数、侵线数
- `carla_testbed/record/timeseries_recorder.py`
  - 写逐帧 CSV
- `carla_testbed/record/summary_recorder.py`
  - 写 `summary.json`

也就是说，当前 KPI 是：

- `timeseries.csv` 中的逐帧指标
- `summary.json` 中的汇总指标

## 4. 环境准备

### 4.1 最小依赖

当前 `pyproject.toml` 中声明的运行依赖：

- `PyYAML`
- `numpy`
- `opencv-python`
- `networkx`
- `shapely`
- `python-dotenv`

开发依赖（可选）：

- `ruff`
- `pytest`
- `pre-commit`

### 4.2 CARLA 路径解析（当前代码行为）

`carla_testbed/utils/env.py` 当前的 CARLA 根目录解析顺序是：

1. 显式 override
2. 环境变量 `CARLA_ROOT`
3. `configs/local.yaml` 的 `carla.root`
4. `configs/local.yaml` 的 `paths.carla_root`
5. `configs/local.*.yaml`（按文件名排序自动叠加）
6. `CARLA_TESTBED_LOCAL_CONFIGS` 指向的附加配置文件（逗号分隔）

也就是说：

- 当前代码不再依赖单机硬编码默认路径
- 如果没有任何本地配置，会给出清晰提示并指出可用配置来源

可用的本地配置模板：

- `.env.example`
- `configs/local.example.yaml`

### 4.3 本机初始化

当前仓库内置脚本：

```bash
bash tools/bootstrap_native.sh
```

它会做：

1. 创建 `.venv`（如果不存在）
2. `pip install -U pip`
3. 安装项目本身（优先 `pip install -e ".[dev]"`）
4. 如果 `CARLA_ROOT` 指向有效目录，自动安装最新 `carla-*.whl`
5. 运行：
   - `python -m carla_testbed doctor`

## 5. 最小可运行示例（MVP）

### 5.1 目标

下面这组命令使用当前最简单、最稳定的 `dummy` 栈，跑一轮 `followstop`，并生成：

- `runs/readme_mvp/summary.json`

注意：

- 当前代码写的是 `runs/<run>/summary.json`
- **不存在** `runs/<run>/results/summary.json`

### 5.2 命令

先准备环境：

```bash
export CARLA_ROOT=/path/to/CARLA_0.9.16
bash tools/bootstrap_native.sh
source .venv/bin/activate
```

然后执行：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_dummy.yaml \
  --run-dir runs/readme_mvp \
  --override runtime.carla.start=true
```

为什么这里要加 `--override runtime.carla.start=true`：

- `configs/io/examples/followstop_dummy.yaml` 当前默认是：
  - `runtime.carla.start: false`
- 这个 override 会让 `tbio/scripts/run.py` 把 `--start-carla` 透传给 `examples.run_followstop`
- 当前配置又启用了 `scenario.publish_ros2_native: true`
- `tbio/carla/launch_policy.py` 会自动把 `--ros2` 加到 CARLA 启动参数里

### 5.3 预期产物

跑完后至少会有：

- `runs/readme_mvp/effective.yaml`
- `runs/readme_mvp/summary.json`
- `runs/readme_mvp/timeseries.csv`
- `runs/readme_mvp/artifacts/doctor.txt`

快速验证：

```bash
python - <<'PY'
import json
from pathlib import Path
p = Path("runs/readme_mvp/summary.json")
print("exists:", p.exists())
if p.exists():
    data = json.loads(p.read_text())
    print("success:", data.get("success"))
    print("fail_reason:", data.get("fail_reason"))
    print("max_speed_mps:", data.get("max_speed_mps"))
PY
```

## 6. 其他常用运行命令

### 6.1 环境检查

```bash
python -m carla_testbed doctor
```

输出位置：

- 默认：`runs/doctor_<ts>/artifacts/doctor.txt`
- 如果由 `run` 调用：写到当前 run 的 `artifacts/doctor.txt`

### 6.2 直接运行 followstop（不经过统一 CLI）

当前直接入口仍然可用：

```bash
python -m examples.run_followstop --config configs/io/examples/followstop_dummy.yaml
```

但不推荐作为日常入口，因为：

- `python -m carla_testbed run` 会先做：
  - `.env` 加载
  - `doctor`
  - `effective.yaml`
  - `run_meta.json`
  - `runs/latest` 更新

### 6.3 Apollo GT MVP / compatibility path

当前有可用配置：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt.yaml \
  --run-dir runs/apollo_gt_demo
```

前提来自当前代码：

- `APOLLO_ROOT` 需要可解析（来自 `algo/adapters/apollo.py`）
- 若启用 Docker 模式：
  - `APOLLO_DOCKER_CONTAINER` 需要可解析
- 该路径当前用于 MVP、诊断和 compatibility demos；不要把它描述成 Apollo 已完整完成 CARLA 全场景闭环。

详细链路说明见：

- `docs/gt_truth_simulation_pipeline.md`

### 6.4 Autoware legacy experimental path

当前有配置文件：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_autoware.yaml \
  --run-dir runs/autoware_demo
```

前提是当前配置里引用的 compose / map / docker 环境可用。具体依赖由：

- `algo/adapters/autoware.py`

在启动前检查。

Autoware 保留为 legacy experimental compatibility，不是当前阶段与 Apollo 同等优先的主线。

Autoware 没有 Apollo Dreamview 这种内置网页控制台。本仓库的 Autoware
demo 录制标准是 CARLA 第三人称视频 + RViz2 operator view + rosbag2
topic 归档 + control/follow-stop diagnostics。默认
`configs/io/examples/followstop_autoware.yaml` 会尝试生成：

- `video/dual_cam/demo_third_person.mp4`
- `video/rviz/autoware_rviz.mp4`
- `rosbag2/autoware_demo/`
- `artifacts/autoware_operator_view_status.json`
- `artifacts/autoware_rosbag_status.json`

RViz/rosbag 是 operator evidence，不等同于 Autoware 自然驾驶成功；行为结论仍以
`summary.json`、`timeseries.csv` 和分析报告为准。录制后可检查：

```bash
python3 tools/inspect_autoware_demo_recording.py runs/autoware_demo
```

## 7. 每次 run 会产出什么

### 7.1 run 根目录

当前每次 `python -m carla_testbed run ...` 会创建：

- `runs/<run_name>/`

并更新：

- `runs/latest`（优先作为最新 run 的 symlink）
- `runs/LATEST.txt`

### 7.2 顶层文件

当前 run 根目录常见文件：

- `effective.yaml`
  - 这次运行真正生效的配置
- `summary.json`
  - 当前 run 的汇总结果
- `timeseries.csv`
  - 当前 run 的逐帧时间序列

注意：

- 当前没有 `results/` 子目录
- 结果文件直接写在 run 根目录

### 7.3 `artifacts/`

`artifacts/` 是跨模块的调试与回归目录。

当前常见文件包括：

- `doctor.txt`
  - 环境检查结果
- `run_meta.json`
  - 本轮运行的入口信息（config、run_dir、CARLA 启动策略）
- `sensor_mapping.yaml`
- `sensor_kit_calibration.yaml`
- `qos_overrides.yaml`
- `frames.yaml`
  - 以上四个来自 `tbio/contract/generate_artifacts.py`
  - 只有在配置里 `io.generate.*` 开启时会生成

如果 ROS2 / 外部栈相关功能开启，还会出现：

- `autoware_control.jsonl`
  - 控制话题抓取结果（Apollo / Autoware 也复用这个文件名）
- `autoware_control.log`
- `sensor_probe.json`
- `sensor_probe.log`
- `monitor.json`

如果使用 Apollo 栈，还会出现：

- `apollo_bridge_effective.yaml`
- `apollo_adapter_meta.json`
- `cyber_bridge.out.log`
- `cyber_bridge.err.log`
- `cyber_control_bridge.out.log`
- `cyber_control_bridge.err.log`
- `cyber_bridge_stats.json`
- `apollo_planning.INFO`
- `apollo_control.INFO`
- `apollo_routing.INFO`
- `apollo_external_command.INFO`

如果启用 Dreamview 相关功能，还可能出现：

- `dreamview_launch.log`
- `dreamview_open.log`
- `dreamview_url.txt`
- `dreamview_capture.mp4`
- `dreamview_record.log`
- `dreamview_record.out.log`
- `dreamview_record.err.log`

### 7.4 `config/`

当前 `run_followstop` / harness 会生成配置落盘：

- `config/sensors_rig_raw.yaml`
- `config/sensors_rig_final.yaml`
- `config/sensors_expanded.json`
- `config/calibration.json`
- `config/time_sync.json`
- `config/noise_model.json`
- `config/data_format.json`
- `config/manifest.json`

作用：

- 调试传感器 rig
- 复现实验
- 给录像和 GT 发布提供外参

### 7.5 `sensors/`

只有启用传感器采集时才会写。

当前由 `carla_testbed/sensors/` 模块负责，按传感器类型写原始文件。

### 7.6 `video/`

只有启用录制模式时才会写。

当前可由 `RecordManager` 生成：

- `dual_cam`
- `hud`
- `sensor_demo`

## 8. 回归测试（当前能直接用的做法）

### 8.1 最基本回归：比较 `summary.json`

同一配置反复运行时，先比较：

- `success`
- `fail_reason`
- `collision_count`
- `lane_invasion_count`
- `max_speed_mps`

文件：

- `runs/<run>/summary.json`

### 8.2 帧级回归：比较 `timeseries.csv`

当前最直接的帧级回归入口是：

- `runs/<run>/timeseries.csv`

它来自 `carla_testbed/record/timeseries_recorder.py`，首行表头按首次写入的字段确定。

### 8.3 ROS2 topic 自检

当前可用命令：

```bash
python tbio/scripts/healthcheck_ros2.py --config runs/readme_mvp/effective.yaml
```

说明：

- `tbio/scripts/healthcheck_ros2.py` 当前明确支持：
  - 输入配置文件路径（原始 config 或 `effective.yaml`）

### 8.4 smoke test

默认 smoke 是 config/API 级检查，不启动外部 runtime：

```bash
python -m carla_testbed smoke --config configs/examples/smoke.yaml
```

历史 ROS2 topic smoke 仍可用 legacy wrapper：

```bash
python io/scripts/smoke_test.py --contract io/contract/canon_ros2.yaml --timeout 5
```

legacy ROS2 smoke 会检查：

- `/clock` 是否存在且递增
- 合约中非 `internal`、非 `optional` 的 topic 是否存在

## 9. 常见问题 / 排错

### 9.1 同步 tick 卡住

当前主循环用的是同步模式（默认 `synchronous_mode=True`）。

排查点：

- 看 `carla_testbed/config/defaults.py`
- 看 `carla_testbed/sim/tick.py`
- 看 `timeseries.csv` 是否持续增长

如果 `timeseries.csv` 不增长，通常说明：

- CARLA 未正常推进
- 或主循环提前退出

### 9.2 CARLA settings 恢复

`examples/run_followstop.py` 在 `finally` 里会执行清理。

正常退出或 `Ctrl+C` 时，当前代码会尝试：

- 停 adapter
- 停 logger / probe
- 恢复 CARLA settings
- 销毁场景 actor
- 停 CARLA launcher

如果你怀疑 CARLA 卡在异常状态，优先检查：

- 终端是否打印 `Settings restored, exiting.`

### 9.3 传感器缺帧 / dropped

当前可从两处看：

- `summary.json`
  - `sensor_frames_saved`
  - `sensor_dropped`
- `artifacts/monitor.json`

相关实现：

- `carla_testbed/sensors/`
- `carla_testbed/record/monitor.py`

### 9.4 坐标系不一致

当前 GT 发布器默认会做：

- `y` 取反
- `pitch` 取反
- `yaw` 取反

代码：

- `carla_testbed/ros2/gt_publisher.py`

如果你在 ROS2 / Apollo 中看到朝向或左右颠倒，先核对：

- `ros_invert_tf`
- `calibration.json`
- `tf / tf_static`

### 9.5 车不动，但控制看起来在发

先看三层：

1. `summary.json`
   - `fail_reason`
   - `max_speed_mps`
2. `artifacts/autoware_control.jsonl`
   - 控制话题是否真的有消息
3. Apollo 栈时再看：
   - `artifacts/cyber_bridge_stats.json`
   - `artifacts/apollo_control.INFO`
   - `artifacts/apollo_planning.INFO`

当前代码里，外部栈控制最终都需要落到：

- `/tb/ego/control_cmd`

### 9.6 性能 / RTF

当前代码没有单独的 RTF 统计模块。

也就是说：

- **RTF 统计 Not implemented yet**

当前替代观察方式：

- 看 `timeseries.csv` 是否稳定增长
- 看 `progress` 日志是否长时间停住
- 看视频/ROS2/外部栈是否全部同时开启（这些都会明显增加负载）

### 9.7 磁盘占用

当前每轮 run 主要写到：

- `runs/<run>/`

但 Apollo 栈还会额外在宿主机保留自己的原生日志目录（由 Apollo 本身写），本仓库当前只会把本轮新增部分切片复制到 `runs/<run>/artifacts/`。

## 10. 如何扩展

### 10.1 新增场景

入口接口：

- `carla_testbed/scenarios/base.py`

当前约定是实现一个符合 `Scenario` Protocol 的类，至少提供：

- `build(world, carla_map, bp_lib)`
- `reset()`
- `destroy()`

返回对象通常是：

- `ActorRefs`

当前参考实现：

- `carla_testbed/scenarios/followstop.py`

### 10.2 新增传感器

分两层：

1. 配置层
   - `configs/rigs/*.yaml`
2. 运行层
   - `carla_testbed/sensors/specs.py`
   - `carla_testbed/sensors/rigs.py`
   - `carla_testbed/config/rig_loader.py`

当前实际入口：

- 在 rig YAML 里增加新的 `sensors[]`
- `rig_loader.rig_to_specs()` 会把 YAML 转成 `SensorSpec`

### 10.3 新增 KPI

当前没有独立 KPI 框架，所以新增 KPI 的实际入口是：

- `carla_testbed/runner/harness.py`
  - 采样和计算
- `carla_testbed/record/timeseries_recorder.py`
  - 如果要逐帧落盘
- `carla_testbed/record/summary_recorder.py`
  - 如果要汇总落盘

也就是说：

- 新 KPI 目前是“直接加到 harness 和 recorder”
- 独立 KPI 插件化 **Not implemented yet**

### 10.4 新增 I/O adapter（ROS2 / CyberRT / 其他）

当前 adapter 接口：

- `algo/adapters/base.py`

必须实现：

- `prepare(profile, run_dir)`
- `start(profile, run_dir)`
- `healthcheck(profile, run_dir)`
- `stop(profile, run_dir)`

注册入口：

- `algo/registry.py`

当前已有参考：

- `algo/adapters/dummy.py`
- `algo/adapters/apollo.py`
- `algo/adapters/autoware.py`

如果要做外部 runtime backend，当前参考：

- `tbio/backends/cyberrt.py`

### 10.5 扩展 Apollo GT 链路

当前最直接的入口文件：

- `carla_testbed/ros2/gt_publisher.py`
- `tools/apollo10_cyber_bridge/bridge.py`
- `algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py`

详细链路说明：

- `docs/gt_truth_simulation_pipeline.md`

## 11. 已知限制（按当前代码）

- `python -m carla_testbed run --config configs/examples/smoke.yaml`
  - typed config runner 尚未接入真实 CARLA loop；当前可用 `--dry-run` 或 `smoke`
  - 旧 runnable configs 仍通过 legacy runner fallback 支持
- `algo=e2e`：**Not implemented yet**
  - 当前仅保留 adapter 骨架
- 独立 KPI 模块：**Not implemented yet**
  - 当前 KPI 直接写在 harness / recorder 里
- `runs/<run>/results/summary.json`：**Not implemented yet**
  - 当前实际路径是 `runs/<run>/summary.json`
- 统一 RTF 指标：**Not implemented yet**

## 12. 相关文档

- Apollo GT 闭环链路：
  - `docs/gt_truth_simulation_pipeline.md`
- 运行器补充说明：
  - `carla_testbed/runner/README.md`
- 录制模块补充说明：
  - `carla_testbed/record/README.md`
- 传感器补充说明：
  - `carla_testbed/sensors/README.md`
