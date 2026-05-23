# examples/

Legacy/demo scripts and small CARLA experiments.

This directory is not the home for new platform architecture. The current platform boundary is:

- `carla_testbed/` for core harness, simulation, sensors, recording, and schemas.
- `tbio/` for transition backend integration.
- `algo/adapters/` and `tools/apollo10_cyber_bridge/` for external stack adapters and Apollo/CyberRT bridge work.

Examples should call platform APIs or legacy compatibility wrappers. They should
not own new config loading, backend contracts, artifact schemas, evaluation
logic, or Apollo/CyberRT bridge semantics.

主要脚本：

- `run_smoke.py`：lightweight no-runtime smoke wrapper around `python -m carla_testbed smoke`.
- `run_follow_stop_baseline.py`：lightweight follow-stop baseline wrapper around the canonical CLI. Default mode is typed dry-run; `--legacy-run` dispatches an existing legacy config.
- `run_followstop.py`：LEGACY demo entrypoint. It remains runnable for compatibility and focused debugging, but new platform logic must not be added here.

子目录：

- `carla_examples/`：独立的 CARLA 示例与实验脚本。
- `legacy/`：legacy examples documentation and migration notes.

使用建议：

- 常规运行优先使用 `python -m carla_testbed run --config ...`。
- Config validation: `python -m carla_testbed config-validate configs/examples/smoke.yaml`。
- No-runtime smoke: `python -m carla_testbed smoke --config configs/examples/smoke.yaml`。
- Typed dry-run: `python -m carla_testbed run --config configs/examples/smoke.yaml --dry-run`。
- Lightweight examples:
  - `python examples/run_smoke.py --help`
  - `python examples/run_follow_stop_baseline.py --help`
- 需要快速调试 legacy follow-stop 场景参数时再直接调用 `examples/run_followstop.py`。
