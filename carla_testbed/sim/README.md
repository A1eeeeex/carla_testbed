# 模块定位
封装 CARLA 连接、世界/actor 管理与 tick 工具。提供创建客户端、启用同步模式、spawn 车辆、统一 tick 接口。该模块不包含控制逻辑或渲染。

# 目录与关键文件
- `carla_client.py`：CarlaClientManager，负责创建/复用 client。
- `world.py`：创建 world、载入地图、spawn 车辆（ego/front）、启用/恢复同步。
- `actors.py`：车辆生成辅助（选择蓝图、spawn 点）。
- `tick.py`：`tick_world(world, timeout)` 同步 tick 工具。
- `__init__.py`：导出主要函数/类。

# 对外接口（Public API）
- `CarlaClientManager(host, port)`：`get()` 返回 `carla.Client`，自动设置超时。
- `configure_synchronous_mode(world, fps)` / `restore_settings(world, settings)`：开启/恢复同步模式。
- `spawn_vehicle(world, blueprint_id, transform, autopilot=False)`（actors.py）。
- `build_world(client, town)`（world.py，若存在）：加载 town。
- `tick_world(world, timeout=1.0)`：同步 tick，返回 snapshot。

# 数据契约（I/O）
- 输入：CARLA 服务器地址/端口、town 名称、车辆蓝图、spawn transform。
- 输出：CARLA world 对象、车辆 Actor 引用；无直接文件输出。
- 坐标系：沿用 CARLA 世界坐标（x 前 y 右 z 上）。

# 用法（How to Use）
- 在示例脚本中（已集成）：
```bash
python examples/run_followstop.py --host localhost --port 2000 --town Town01
```
- 手动创建 world 并 tick：
```python
from carla_testbed.sim import CarlaClientManager, configure_synchronous_mode, tick_world
client = CarlaClientManager("localhost", 2000).get()
world = client.get_world()
settings = configure_synchronous_mode(world, fps=20)
snapshot = tick_world(world)
# ... do work ...
restore_settings(world, settings)
```

# 配置（Config）
- CLI：`--host`、`--port`、`--town`（由入口脚本传入）。
- FPS：在 configure_synchronous_mode 中传入。

# 常见问题与排错
- 无法连接 CARLA：检查服务是否启动；host/port 是否匹配；防火墙/延迟。
- 同步模式未生效：确认 world.set_settings 返回的 settings 保存并在结束时 restore；tick 是否使用 tick_world。
- 车辆 spawn 失败：位置被占用或蓝图不存在；尝试更换 spawn 点或蓝图。
- tick 卡住：确认同步模式下所有传感器都在 listen；增加 timeout。
- Town 不正确：确保 client.load_world 成功；检查日志。

# 与其他模块的关系
- 上游：入口脚本配置 host/port/town。
- 下游：scenarios 用 world 生成车辆；sensors 依赖 world/ego；runner tick_world。
- 调用路径：examples/run_followstop.py -> CarlaClientManager/get_world -> configure_synchronous_mode -> runner.tick_world。 
