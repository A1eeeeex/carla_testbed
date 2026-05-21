# scenarios/

`carla_testbed/scenarios/` 只负责**场景构建与 actor 生命周期**，不负责 CARLA bring-up、桥接启动、评测汇总或录制编排。

## 当前主用场景

- `followstop.py`
  - 跟车/停车主场景
- `town01_route_health.py`
  - Town01 route-health route executor
- `apollo_semantic_suite.py`
  - Apollo 控制语义/标定采集场景
- `calibration_only.py`
  - 开环执行器标定场景

## 职责边界

场景层负责：

- 根据 config 在已就绪的 CARLA world 中生成 ego / lead / route / 辅助 actor
- 提供 setup / teardown
- 维护场景专属 metadata

场景层不负责：

- 启动或重启 CARLA
- 管理 world-ready / reconnect / load_world
- 启动 Apollo / Autoware / ROS2 bridge
- 写 acceptance summary / platform comparison

## 资产与执行分离

- Town01 canonical route corpus 现在默认位于：
  - `/home/ubuntu/carla_testbed/carla_testbed/assets/routes/town01/town01_route_corpus.json`
- `town01_route_health.py` 只消费稳定 corpus/subset，不再承担 canonical 资产位置定义

## 推荐入口

建议通过上层 runner/tool 进入，不直接把场景模块当作主入口：

- `python -m carla_testbed run`
- `tools/run_apollo_mainline.py`
- `tools/run_town01_capability_online_chain.py`
- `tools/run_unified_calibration_pipeline.py`
- `tools/run_town01_demo_showcase.py`

## 历史说明

旧文档里把 `scenarios/` 基本等同于 `followstop`，这已经过时。当前仓库的场景层已经同时服务：

- Town01 route-health
- followstop
- calibration pipeline
- Apollo semantic suite
