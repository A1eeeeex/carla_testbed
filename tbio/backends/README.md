# tbio/backends/

后端层负责实现各算法栈的运行时集成。

核心文件：

- `base.py`：后端接口与生命周期契约。
- `ros2_native.py`：原生 ROS2 发布模式。
- `autoware_direct.py`：Autoware 直连模式。
- `cyberrt.py`：Apollo CyberRT 集成路径（Dreamview、bridge、control bridge）。

选择方式：

- 由配置中的 `algo.stack` 选择（`dummy`、`autoware`、`apollo`）。
- 由 `tbio/scripts/run.py` 负责启动。

关键行为：

- `start()`：启动所需进程与桥接组件。
- `on_sim_tick()`：可选的逐帧回调（用于同步采集等）。
- `health_check()`：集成状态检查。
- `stop()`：按顺序停止并做日志快照。

新增算法栈时：

1. 新建继承 `BackendBase` 的后端实现。
2. 在 `algo/adapters/*` 里完成适配注册。
3. 在 `configs/io/examples/` 增加对应 profile。
