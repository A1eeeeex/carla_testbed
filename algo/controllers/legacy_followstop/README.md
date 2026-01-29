# 模块定位
本目录包含从旧仓库复制的轻量跟停控制器实现（controllers.py），供 `LegacyFollowStopController` 直接导入使用。负责计算纵向 ACC、横向 PurePursuit/路线保持、安全监督等，不负责仿真、传感器或录制。

# 关键文件
- `controllers.py`：核心控制逻辑与工具函数，导出 `build_default_controller`、`speed_mps`、`ControlState` 等。

# 对外接口
- `build_default_controller(lateral_mode, policy_mode, controller_mode, agent_type, takeover_dist_m, blend_time_s, carla_world, carla_map, ego_vehicle, front_vehicle)`：返回复合控制器实例，提供 `.step(state)`。
- `ControlState`：控制输入结构（t/dt/ego/front/world/map/v 等）。
- 工具：`speed_mps(vehicle)` 获取车速。

# 数据契约
- 输入：CARLA world/map、ego/front 车辆与 `ControlState`。
- 输出：CARLA `VehicleControl`（throttle/brake/steer/reverse 等）。

# 用法
通常不直接调用，由 `LegacyFollowStopController` 封装；如需单独测试：
```python
from carla_testbed.control.legacy_followstop import controllers
ctrl = controllers.build_default_controller(controller_mode="composite", agent_type="basic", carla_world=world, carla_map=carla_map, ego_vehicle=ego, front_vehicle=front)
cmd = ctrl.step(controllers.ControlState(t, dt, ego, front, world, carla_map, v=controllers.speed_mps(ego), d=0.0))
```

# 配置
- 依赖 `CARLA_ROOT/PythonAPI` 以导入 agents.*；已在 `LegacyFollowStopController` 中自动注入。

# 排错
- `ImportError agents.navigation`：检查 CARLA PythonAPI 路径；确保 `CARLA_ROOT` 指向有效安装。
- `AttributeError last_debug`：旧控制器可能缺少，封装已防御；查看 Harness meta。
- 控制异常/振荡：调参 `lateral_mode`、`policy_mode`、`blend_time_s` 并检查前车/ego姿态输入是否正确。

# 与其他模块关系
- 由 `LegacyFollowStopController` 动态导入；被 runner 调用，不直接与 sensors/record 交互。 
