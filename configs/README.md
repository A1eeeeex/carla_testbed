# configs/

Configuration root for runtime profiles and sensor rigs.

Subdirectories:

- `io/`: end-to-end runtime profiles (stack, bridge, routing, recording).
- `rigs/`: sensor rig presets.

Recommended workflow:

1. Start from `configs/io/examples/*.yaml`.
2. Override with `--override key=value` for run-specific tweaks.
3. Keep machine-local values in `.env` or `configs/local.yaml` (ignored by git).

