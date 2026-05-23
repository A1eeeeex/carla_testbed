# configs/io/ — Transitional Compatibility Configs

Older `python -m carla_testbed run --config ...` profiles, contracts, and map helpers.

This directory remains runnable and useful, but it is no longer the target home for new platform config structure. Prefer:

- `configs/examples/` for compact canonical examples.
- `configs/profiles/` for future reusable profiles.
- `configs/local/` for machine-local overrides.

结构：

- `examples/`：transitional runnable profiles（`followstop_*`、Town01、Apollo diagnostics）。
- `maps/`：地图相关配置片段与资源。

设计原则：

- profile 只描述意图；实际运行逻辑在 `tbio/` 与 `algo/` 中实现。
- profile 应清晰表达算法栈类型、routing 策略、规划开关、录制模式等关键意图。
- 不要在新的 tracked profile 中加入机器绑定绝对路径；使用 `configs/local/` 或环境变量。
