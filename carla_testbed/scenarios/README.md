# 模块定位
定义场景生成逻辑（目前支持 follow-stop 场景），负责在 CARLA world 中生成 ego/front 车辆、设置初始位置/速度。场景不直接控制车辆或记录数据，交由 runner/控制器处理。

# 目录与关键文件
- `base.py`：Scenario 抽象基类、场景配置基类。
- `followstop.py`：FollowStopScenario 实现与 FollowStopConfig 配置（城市场景跟随前车并停车）。
- `__init__.py`：导出 FollowStopScenario/FollowStopConfig。

# 对外接口（Public API）
- `FollowStopConfig(town: str, ego_idx: int, front_idx: int)`：场景配置（路点索引）。
- `FollowStopScenario(config)`：
  - `setup(world)`：生成 ego/front 车辆并返回引用。
  - `teardown()`：销毁 actor。
  - `build_map(world)`：缓存 map。

# 数据契约（I/O）
- 输入：CARLA world，配置中指定的 `town`、`ego_idx`、`front_idx`（选定路点）。
- 输出：`ego`、`front` 车辆引用供 runner/control 使用；无文件输出。

# 用法（How to Use）
- 在示例脚本中使用（默认）：
```bash
python examples/run_followstop.py --town Town01 --ego-idx 120 --front-idx 210
```
- 自定义脚本：
```python
from carla_testbed.scenarios import FollowStopScenario, FollowStopConfig
sc = FollowStopScenario(FollowStopConfig(town="Town01", ego_idx=120, front_idx=210))
ego, front = sc.setup(world)
# ... run harness ...
sc.teardown()
```

# 配置（Config）
- CLI：`--town`、`--ego-idx`、`--front-idx`（示例脚本提供）。
- 默认路点：Town01 中的 120/210。

# 常见问题与排错
- 车辆未生成：确认 CARLA world/town 已加载；路点索引是否存在；检查终端是否有 spawn 冲突。
- Teardown 后残留 actor：检查 `scenario.teardown()` 是否被调用；可用 world.get_actors() 验证。
- town 不匹配：确保 runner 调用前 world 与 FollowStopConfig.town 一致。
- 位置重合/碰撞：调整 ego/front_idx 或在 FollowStopConfig 中添加随机偏移（若扩展）。
- 多场景串行：确保每次都调用 teardown 释放 actor。

# 与其他模块的关系
- 上游：入口脚本选择场景/配置。
- 下游：runner 获取 ego/front 供 control/sensors/record 使用。
- 调用路径：examples/run_followstop.py -> FollowStopScenario.setup -> TestHarness.run。

# Roadmap
- 增加更多场景（巡航/变道/障碍物）。
- 路点自动选取/随机种子。
- 支持动态路网/地图加载。 
