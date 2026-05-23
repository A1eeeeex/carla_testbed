# Legacy Examples

Legacy examples are kept for compatibility and targeted debugging. They are not
the home for new platform architecture.

Current legacy entry:

- `../run_followstop.py`
  - giant historical follow-stop runner
  - kept runnable for existing configs and demos
  - do not add new config loading, adapter contracts, artifact schemas,
    evaluation logic, or Apollo/CyberRT bridge semantics there

Prefer lightweight examples in `examples/` and canonical commands:

```bash
python examples/run_smoke.py --help
python examples/run_follow_stop_baseline.py --help
python -m carla_testbed smoke --config configs/examples/smoke.yaml
```
