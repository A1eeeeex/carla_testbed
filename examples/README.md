# examples/

场景运行与 CARLA 实验脚本目录。

主要脚本：

- `run_followstop.py`：followstop 场景运行入口（兼容旧链路）。

子目录：

- `carla_examples/`：独立的 CARLA 示例与实验脚本。

使用建议：

- 常规运行优先使用 `python -m carla_testbed run --config ...`。
- 需要快速调试底层场景参数时再直接调用 `examples/run_followstop.py`。
