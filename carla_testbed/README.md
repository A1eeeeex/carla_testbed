# carla_testbed/ — Simulator & Harness Layer

`carla_testbed/` 是 core platform 包：CARLA 连接、场景构建、harness tick、传感器、记录、summary 与基础评测都应该收敛在这里。

当前项目优先目标是 CARLA + Apollo ground-truth MVP closed loop。`carla_testbed/` 不应该直接承载 Apollo/CyberRT 细节；外部栈集成应通过 adapter/backend contract 接入。

边界说明：

- Core platform logic belongs here when it is independent of a specific external stack.
- ROS2 / CyberRT / Apollo launch and bridge details belong in `tbio/`, `algo/adapters/`, or `tools/apollo10_cyber_bridge/`.
- `examples/run_followstop.py` is a legacy/demo runner, not the place for new platform architecture.

目录速览
- `sim/`：CARLA client、tick、spawn 辅助。
- `scenarios/`：场景构建；当前 follow-stop 是 legacy baseline/demo 场景。
- `runner/`：Harness 与录制逻辑。
- `schemas/`：FramePacket / GroundTruthPacket / ControlCommand。
- `sensors/`：事件传感器、rig 挂载与录制。

推荐入口

Planned canonical command:

```bash
python -m carla_testbed run --config configs/examples/follow_stop.yaml
```

Current compatible command:

```bash
python -m carla_testbed run --config configs/io/examples/followstop_dummy.yaml
```

Autoware configs remain legacy experimental compatibility and are not the current priority path.
