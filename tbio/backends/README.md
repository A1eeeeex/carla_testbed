# tbio/backends/

Backends implement stack-specific runtime integration.

Core files:

- `base.py`: backend interface and lifecycle contract.
- `ros2_native.py`: native ROS2 publisher mode.
- `autoware_direct.py`: direct Autoware integration path.
- `cyberrt.py`: Apollo CyberRT integration path (Dreamview, bridge, control bridge).

Selection:

- Selected by `algo.stack` in config (`dummy`, `autoware`, `apollo`).
- Started from `tbio/scripts/run.py`.

Key behavior:

- `start()`: launch required processes and bridges.
- `on_sim_tick()`: optional per-tick hooks (for synced capture).
- `health_check()`: integration readiness.
- `stop()`: ordered shutdown and log snapshot.

When adding a new stack:

1. Implement a new backend inheriting `BackendBase`.
2. Register it in adapter path (`algo/adapters/*`).
3. Add a config profile in `configs/io/examples/`.

