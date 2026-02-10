# carla_testbed/ — Simulator & Harness Layer

- 只负责 CARLA 连接、场景、传感器挂载、记录与评测。
- 所有外部交互（ROS2/CyberRT）已抽离到顶层 `tbio/`；统一入口使用 `python -m carla_testbed run ...`。
- 现有示例保持不变：`python examples/run_followstop.py --rig fullstack --ticks 200` 等。开启原生 ROS2 时直接使用 `tbio/backends/ros2_native.py` 的 `Ros2NativePublisher`。

目录速览
- `sim/`：CARLA client、tick、spawn 辅助。
- `scenarios/`：场景构建（follow-stop）。
- `runner/`：Harness 与录制逻辑。
- `schemas/`：FramePacket / GroundTruthPacket / ControlCommand。
- `sensors/`：事件传感器、rig 挂载与录制。

新入口（推荐）
- Mode-1：`python -m carla_testbed run --config configs/io/examples/followstop_dummy.yaml`
- Mode-2：`python -m carla_testbed run --config configs/io/examples/followstop_autoware.yaml`
