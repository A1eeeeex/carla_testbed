# tools/

项目工具脚本与集成辅助目录。

关键内容：

- `apollo10_cyber_bridge/`：ROS2 <-> Apollo CyberRT 桥及配套工具。
- `bootstrap_native.sh`：本机环境初始化脚本。

约定：

- 该目录主要放运维/集成工具，不放核心业务逻辑。
- 长驻运行逻辑优先放在 `algo/` 或 `tbio/`。
