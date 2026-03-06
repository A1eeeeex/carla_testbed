# configs/io/examples/

Runnable profile presets.

Common files:

- `followstop_dummy.yaml`: no external stack, baseline harness validation.
- `followstop_autoware.yaml`: Autoware integration profile.
- `followstop_apollo_demo.yaml`: Apollo demo profile.
- `followstop_apollo_gt.yaml`: Apollo GT profile for followstop closed-loop testing.

How to run:

```bash
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt.yaml
```

Profile selection for migration smoke:

1. `followstop_dummy.yaml` (sanity check host and CARLA)
2. `followstop_apollo_gt.yaml` (target flow)

