# io/ —— Deprecated Legacy Compatibility Wrapper

`io/` is a deprecated compatibility layer kept for older scripts, contracts, and smoke tests. Do not add new platform architecture here.

Current project direction:

- Core platform logic goes under `carla_testbed/`.
- Canonical backend/adapter contracts start under `carla_testbed/adapters/`.
- Transition backend runtime integration goes under `tbio/`.
- Apollo/CyberRT MVP work should use the adapter/backend boundary and `tools/apollo10_cyber_bridge/`.
- New canonical config layout is planned to move away from `configs/io/examples/...` toward clearer platform config paths.

目录内容：

- `contract/`：legacy 标准 slot->topic/type 映射（`canon_ros2.yaml`）、TF 布局（`frames.yaml`）、单位定义与可运行 profile。
- `backends/`：legacy backend wrapper surface. Current runtime backends are routed through `tbio/`.
- `ros2/`：健康检查工具（`inspect_topics.py`、`time_sync_check.py`）、TF 自检与控制话题说明。
- `scripts/`：deprecated legacy wrappers around current CLI / `tbio` scripts. They should not receive new business logic.

使用方式：

- Preferred current entry: `python -m carla_testbed run --config <config.yaml>`
- Legacy Mode-1：`python io/scripts/run.py --profile io/contract/profiles/ros2_native_any_algo.yaml`
- Legacy Mode-2：`python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml`
- 停止资源：`python io/scripts/stop.py --profile <profile>`

契约约束：

- 算法消费/发布的 ROS2 话题必须在 `contract/canon_ros2.yaml` 里声明（slot 语义）。
- 必需 TF 帧定义在 `contract/frames.yaml`；`tf/tf_sanity.py` 仅检查是否存在，不验证几何正确性。
- 单位遵循 `contract/units.yaml` 中的 SI 规范。

后端说明：

- `ros2_native`：启用 CARLA 原生 ROS2 发布并拉起控制桥。
- `autoware_direct`：从 rig+contract 生成 Autoware 接口配置并启动 docker compose。
- `cyberrt`：legacy placeholder; Apollo/CyberRT MVP bridge work should not be implemented by expanding this directory as the canonical runtime layer.

Migration target:

- use `carla_testbed/adapters/base.py` for the backend contract;
- keep transition runtime implementation in `tbio/` until migrated;
- treat `io/` as read-only compatibility unless a legacy workflow needs a small fix.
