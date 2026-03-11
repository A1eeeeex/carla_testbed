# configs/io/examples/

可直接运行的 profile 预设目录。

常用文件：

- `followstop_dummy.yaml`：无外部算法栈，基础 harness 自检。
- `followstop_autoware.yaml`：Autoware 集成 profile。
- `followstop_apollo_demo.yaml`：Apollo 演示 profile。
- `followstop_apollo_gt.yaml`：Apollo GT 闭环测试 profile。

运行示例：

```bash
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt.yaml
```

迁移 smoke 推荐顺序：

1. `followstop_dummy.yaml`（先验证宿主机与 CARLA）
2. `followstop_apollo_gt.yaml`（目标闭环链路）
