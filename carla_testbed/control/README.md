# 模块定位
封装控制器接口与默认的跟停控制器（legacy follow-stop），向 Harness 提供 `Controller.step()` 以生成车辆控制命令。该模块不负责场景生成或仿真 tick。

# 目录与关键文件
- `base.py`：控制器抽象基类 `Controller`，定义 `reset/step`。
- `legacy_controller.py`：适配器，加载本地 `legacy_followstop/controllers.py`，包装成 `LegacyFollowStopController`。
- `legacy_followstop/controllers.py`：从旧仓库拷贝的轻量跟停控制器组合（ACC+Lateral+Safety），含 `build_default_controller` 等。

# 对外接口（Public API）
- `Controller`（base.py）：抽象类，`step(t, dt, world, carla_map, ego, front) -> ControlCommand`。
- `LegacyControllerConfig`（legacy_controller.py）：跟停控制器配置（lateral_mode/policy_mode/controller_mode/agent_type/takeover_dist/blend_time）。
- `LegacyFollowStopController(cfg, root=None, world=None, carla_map=None, ego=None, front=None)`: 创建控制器实例；`step` 返回 `ControlCommand`。

# 数据契约（I/O Contract）
- 输入：CARLA world/map、ego/front 车辆、sim_time(t)、dt。
- 输出：`ControlCommand`（throttle/brake/steer/reverse/gear/meta.last_debug）。
- 环境：需要可用的 CARLA PythonAPI/agents（从 `CARLA_ROOT/PythonAPI` 自动注入）。

# 用法（How to Use）
- 在示例脚本中使用默认控制器：
```bash
python examples/run_followstop.py --rig fullstack --controller composite
# Harness 将创建 LegacyFollowStopController 并驱动车辆
```
- 指定控制模式：
```bash
python examples/run_followstop.py --controller hybrid_agent_acc --lateral-mode pure_pursuit --policy-mode acc --agent-type basic
```

# 配置（Config）
- CLI：`--controller`、`--lateral-mode`、`--policy-mode`、`--agent-type`、`--takeover-dist`、`--blend-time`。
- 环境变量：`CARLA_ROOT`（可选，定位 PythonAPI/agents）。
- 默认 root：`/home/ubuntu/CARLA_0.9.16`。

# 常见问题与排错
- 找不到 controllers.py：检查 `carla_testbed/control/legacy_followstop` 是否存在，或环境变量 `CARLA_ROOT` 是否指向旧路径（已不再依赖外部 code/followstop）。
- `ImportError agents.*`：确保 `CARLA_ROOT/PythonAPI` 在 sys.path；脚本会自动注入，仍失败时检查 CARLA 安装。
- 控制器无输出/车辆不动：查看 `summary.json` 中 `fail_reason`，并检查 `meta.last_debug`。
- `NameError os not defined`：旧版本 bug，已修复；若出现请确认使用本地 copy。
- 路径错误/权限：运行在仓库根，生成的 run 写入 `runs/followstop_<ts>/`。

# 与其他模块的关系
- 上游：runner 传递 world/map/ego/front/timestep。
- 下游：schemas.ControlCommand 被 sim/runner 应用到车辆。
- 调用路径：examples/run_followstop.py -> TestHarness.run -> LegacyFollowStopController.step -> ControlCommand 应用。

# Roadmap
- 支持新控制器接口注册表，便于热插拔多种控制算法。
- 暴露更多调参选项（PID/LQR 等）。
- 引入可选日志/可视化 hooks 便于调试控制器行为。 
