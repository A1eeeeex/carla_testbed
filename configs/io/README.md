# configs/io/

`python -m carla_testbed run --config ...` 使用的运行 profile 目录。

结构：

- `examples/`：可直接运行的 profile（`followstop_*`）。
- `maps/`：地图相关配置片段与资源。

设计原则：

- profile 只描述意图；实际运行逻辑在 `tbio/` 与 `algo/` 中实现。
- profile 应清晰表达算法栈类型、routing 策略、规划开关、录制模式等关键意图。
