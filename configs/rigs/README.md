# configs/rigs/

Sensor rig presets used by recording, ROS publishing, and future backend adapters.

This directory is part of the supported config surface. Rig files should be reusable across backends and should not contain machine-local paths.

使用方式：

- 通过 profile（`rig.name`）或运行参数选择。
- 由 `carla_testbed/config/rig_loader.py` 负责加载。
- 在目标 config precedence 中，rig config is applied after project/profile config and before local/CLI overrides.

约定：

- rig 文件应跨算法栈复用，避免耦合某一条链路。
- 新增传感器优先写入该目录，不要把传感器定义硬编码进运行脚本。
- Apollo/CyberRT bridge tuning belongs in backend/profile config, not in rig presets.
