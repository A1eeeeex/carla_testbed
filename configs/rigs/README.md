# configs/rigs/

录制与 ROS 发布使用的传感器 rig 预设目录。

使用方式：

- 通过 profile（`rig.name`）或运行参数选择。
- 由 `carla_testbed/config/rig_loader.py` 负责加载。

约定：

- rig 文件应跨算法栈复用，避免耦合某一条链路。
- 新增传感器优先写入该目录，不要把传感器定义硬编码进运行脚本。
