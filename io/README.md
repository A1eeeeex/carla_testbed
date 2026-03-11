# io/ —— 契约与编排层

目标：集中维护仿真与算法共享的外部接口（ROS2/CyberRT 占位）。

目录内容：

- `contract/`：标准 slot->topic/type 映射（`canon_ros2.yaml`）、TF 布局（`frames.yaml`）、单位定义与可运行 profile。
- `backends/`：Mode-1 `ros2_native`、Mode-2 `autoware_direct`、`cyberrt` 占位的生命周期封装。
- `ros2/`：健康检查工具（`inspect_topics.py`、`time_sync_check.py`）、TF 自检与控制话题说明。
- `scripts/`：`run.py`、`stop.py`、`smoke_test.py`、`env_check.sh` 入口脚本。

使用方式：

- Mode-1：`python io/scripts/run.py --profile io/contract/profiles/ros2_native_any_algo.yaml`
- Mode-2：`python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml`
- 停止资源：`python io/scripts/stop.py --profile <profile>`

契约约束：

- 算法消费/发布的 ROS2 话题必须在 `contract/canon_ros2.yaml` 里声明（slot 语义）。
- 必需 TF 帧定义在 `contract/frames.yaml`；`tf/tf_sanity.py` 仅检查是否存在，不验证几何正确性。
- 单位遵循 `contract/units.yaml` 中的 SI 规范。

后端说明：

- `ros2_native`：启用 CARLA 原生 ROS2 发布并拉起控制桥。
- `autoware_direct`：从 rig+contract 生成 Autoware 接口配置并启动 docker compose。
- `cyberrt`：未来 CyberRT 适配器占位。
