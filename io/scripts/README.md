# io/scripts/ —— Deprecated Legacy Wrappers

Deprecated compatibility script directory. These scripts are kept so historical
automation can still call `io/scripts/*`, but they should not receive new
platform logic.

当前建议：

- 优先使用 `python -m carla_testbed run`，而不是直接调用此目录脚本。

保留本目录的原因：

- 兼容历史自动化流程中对 `io/scripts/*` 的直接调用。
