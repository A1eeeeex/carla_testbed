# configs/profiles/

Reusable profile layer for future typed configs.

Profiles should describe scenario/backend intent, not machine-local paths. Examples:

- a CARLA-only dummy smoke profile
- an Apollo/CyberRT MVP profile
- a recording profile
- a calibration profile

The intended load order is documented in `../README.md`. Keep this directory light until the typed config loader is introduced.
