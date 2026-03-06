# configs/io/

Runtime profiles used by `python -m carla_testbed run --config ...`.

Layout:

- `examples/`: ready-to-run profiles (`followstop_*`).
- `maps/`: map-specific config fragments and assets.

Design:

- Profiles are declarative; runtime behavior is implemented in `tbio/` and `algo/`.
- Keep profile intent explicit: stack type, routing policy, planning toggles, recording mode.

