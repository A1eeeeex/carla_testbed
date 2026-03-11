# algo/adapters/

算法注册表使用的栈适配器目录。

文件说明：

- `dummy.py`：无外部栈后端的本地占位实现。
- `autoware.py`：Autoware 集成适配器。
- `apollo.py`：Apollo 集成适配器。
- `e2e.py`：E2E 占位适配器（当前未实现）。

适配器职责：

1. 解析对应算法栈的配置块。
2. 构建 `tbio` 所需的后端 profile。
3. 通过统一接口暴露 `start/healthcheck/stop` 生命周期能力。
