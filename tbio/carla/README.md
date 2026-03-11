# tbio/carla/

CARLA 进程启动与策略辅助模块。

主要文件：

- `launcher.py`：启动/停止 CARLA 服务进程。
- `launch_policy.py`：启动策略解析（复用已有服务或新起进程）。

调用位置：

- `tbio/scripts/run.py` 在启动 harness 前会先处理该层逻辑。

迁移时重点检查：

- 宿主机 CARLA 路径是否有效（`CARLA_ROOT` 或配置覆盖）。
- 启动参数是否匹配当前环境（`--ros2`、offscreen、GPU 相关设置）。
