# algo/ —— 算法与栈集成层

用途：存放算法栈（如 Autoware）、适配器以及消费 IO 契约的插件节点。

目录结构：

- `baselines/autoware/`：Autoware 直连 CARLA 模式的 docker compose 与接口配置。
- `nodes/control/carla_control_bridge/`：Mode-1 下将 `AckermannDriveStamped` 桥接到 CARLA 控制的 ROS2 节点。
- `plugins/`：感知/规划/E2E 的插件占位目录。

快速开始：

- Mode-1 控制桥：`python algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py --control-topic /tb/ego/control_cmd`
- Mode-2 Autoware：`python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml`

说明：

- `plugins/` 默认是空占位，按 slot/topic 契约补充你的 ROS2 包或适配器。
- Autoware 接口配置会在每次运行时由 `io/backends/autoware_direct.py` 自动生成。
