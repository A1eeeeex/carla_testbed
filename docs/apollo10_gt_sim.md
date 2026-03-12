# Apollo 10.0 GT 仿真闭环（ROS2 -> CyberRT -> CARLA 控制）

本文说明最小闭环链路：

1. testbed 在 ROS2 发布 GT（`/carla/<ego>/odom`、objects、tf）
2. `tools/apollo10_cyber_bridge/bridge.py` 将 ROS2 GT 转换为 Apollo Cyber 话题
3. Apollo 模块输出 `/apollo/control`
4. bridge 将 `ControlCommand` 映射为 ROS2 控制话题（`/tb/ego/control_cmd`）
5. `ros2_autoware_to_carla.py` 把 ROS2 控制应用到 CARLA 自车

Profile 参考：

- 冻结基线说明：`docs/gt_followstop_apollo_baseline.md`
- 三档差异矩阵：`docs/gt_profile_matrix.md`
- 起点投影修复档：`configs/io/examples/followstop_apollo_gt_startalign.yaml`
- reference line 失效排查：`docs/gt_reference_line_rootcause.md`

## 1) 前置条件

- 已有可用的 Apollo 10.0 源码并完成构建。
- `APOLLO_ROOT` 指向 Apollo 10.0 根目录。
- 同一 shell 下可用 ROS2 Humble 与 CARLA Python API。
- 若 Apollo 运行在 Docker（10.0 运行库通常建议这样做），容器建议使用 host network + host IPC：
  - `--network host`
  - `--ipc host`
  - 与宿主 testbed shell 使用相同 `ROS_DOMAIN_ID`

示例：

```bash
export APOLLO_ROOT=/path/to/apollo
export APOLLO_DOCKER_CONTAINER=apollo_dev_x86_64
source /opt/ros/humble/setup.bash
```

如需手动加载 Apollo Cyber 环境：

```bash
source "$APOLLO_ROOT/cyber/setup.bash"
```

## 2) 基于本地 Apollo 10.0 生成 pb2

```bash
bash tools/apollo10_cyber_bridge/gen_pb2.sh
```

生成文件路径：

- `tools/apollo10_cyber_bridge/pb/modules/common_msgs/.../*_pb2.py`

## 3) 运行 followstop + Apollo 适配器（GT 模式）

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt_baseline.yaml \
  --run-dir runs/followstop_apollo_gt_baseline \
  --override algo.apollo.docker.container="$APOLLO_DOCKER_CONTAINER" \
  --override algo.apollo.apollo_root="$APOLLO_ROOT"
```

说明：

- `algo.apollo.docker.enabled=true` 会启用 Apollo 容器编排（自动拉起容器/模块）。
- `algo.apollo.docker.bridge_in_container=true` 会通过 `docker exec` 在容器内运行 `bridge.py`。
- 本仓库推荐：`algo.apollo.docker.bridge_in_container=false`，让 bridge 在宿主运行（宿主有 ROS2/rclpy）。
- 若 ROS2 在宿主机，设置 `algo.apollo.ros2_setup_script`（留空则自动探测 `/opt/ros/<ROS_DISTRO>/setup.bash`）。
- 设置 `algo.apollo.cyber_domain_id`（默认 `80`）以匹配 Apollo Docker 运行域。
- 若宿主 bridge 与容器 Cyber 通道隔离，可设置非回环 `algo.apollo.cyber_ip`。
- 容器未运行时，后端可自动启动（`algo.apollo.docker.auto_start_container=true`）。
- 后端启动时会检查运行模式，输出到 `runs/<run>/artifacts/apollo_container_runtime_check.txt`。
- 后端可自动安装缺失运行库（`algo.apollo.docker.auto_install_runtime_deps=true`），并输出 `apollo_mainboard_runtime_check.log` / `apollo_runtime_deps_install.log`。
- 可由 run 脚本自动启动 Apollo 模块（`algo.apollo.docker.start_modules=true`）。
  当 `start_modules_cmd` 为空时，后端使用更稳妥默认流程：链接 `/opt/apollo/neo/lib/modules` 下的 `.so` 到 `/apollo/modules`，再直接启动 routing/prediction/planning/control 的 launch 文件。
- 支持自动 routing：`algo.apollo.routing.enable=true`，并可配置 `end_ahead_m/resend_sec/max_attempts`。
- 后端会把 `bridge.py`、bridge 配置、pb2 文件打包到容器：`/tmp/carla_testbed_apollo_bridge/<run>/`。
- `carla_control_bridge` 仍在宿主运行，订阅 ROS2 控制话题（`/tb/ego/control_cmd`）并回写 CARLA。
- 冻结基线说明文档：`docs/gt_followstop_apollo_baseline.md`。

关键产物：

- `runs/<run>/artifacts/cyber_bridge.out.log`
- `runs/<run>/artifacts/cyber_bridge.err.log`
- `runs/<run>/artifacts/cyber_bridge_stats.json`
- `runs/<run>/artifacts/cyber_bridge_healthcheck.json`
- `runs/<run>/artifacts/cyber_control_bridge.out.log`
- `runs/<run>/artifacts/cyber_control_bridge.err.log`

## 4) 检查通道与录制

查看 monitor：

```bash
bash tools/apollo10_cyber_bridge/monitor.sh
```

预期活跃通道：

- `/apollo/localization/pose`
- `/apollo/canbus/chassis`
- `/apollo/perception/obstacles`

录制全部 Cyber 通道：

```bash
bash tools/apollo10_cyber_bridge/record_all.sh runs/followstop_apollo_gt_baseline 60
```

## 5) 发送 routing 请求（可选）

```bash
python tools/apollo10_cyber_bridge/send_routing_request.py \
  --apollo-root "$APOLLO_ROOT" \
  --start "0,0,0" \
  --end "30,0,0"
```

## 6) 地图与 routing 说明

Apollo planning/control 需要 Apollo HDMap 资产，例如：

- `modules/map/data/<map_name>/base_map.bin` 或 `base_map.xml`
- 该地图对应的 routing graph 产物

若只做快速验证，可使用已有的 CARLA -> Apollo 地图资产仓库（例如 `Carla_apollo_maps`），并按其文档先完成 routing graph 生成，再启用 planning/control。

## 7) 基线评估

运行结束后可用基线评估工具生成统一指标：

```bash
python tools/evaluate_gt_baseline.py --run-dir runs/followstop_apollo_gt_baseline
```

输出：

- `runs/<run>/artifacts/gt_baseline_metrics.json`
- `runs/<run>/artifacts/gt_baseline_metrics.md`

## 8) 三档 Profile Ablation（minimal / relaxed / strict）

推荐顺序：

1. `configs/io/examples/followstop_apollo_gt_minimal.yaml`
2. `configs/io/examples/followstop_apollo_gt_startalign.yaml`
3. `configs/io/examples/followstop_apollo_gt_relaxed.yaml`
4. `configs/io/examples/followstop_apollo_gt_strict.yaml`

示例：

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

- `runs/<run>/artifacts/gt_baseline_metrics.json`
- `runs/artifacts/gt_profile_compare.json`
- `runs/artifacts/gt_profile_compare.md`

## 9) Reference Line 失败快速诊断

当出现 `Reference lane is empty` / `Cannot find waypoint ... s<0` 时，优先跑：

```bash
python tools/diagnose_startup_lane_alignment.py --run-dir runs/<run_name>
python tools/diagnose_reference_line_failure.py --run-dir runs/<run_name>
```

输出：

- `runs/<run>/artifacts/startup_lane_alignment_summary.json`
- `runs/<run>/artifacts/reference_line_rootcause_summary.json`
