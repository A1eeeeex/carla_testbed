# configs/rigs/

Sensor rig presets for recorder and ROS publishers.

Usage:

- Selected by profile (`rig.name`) or runtime options.
- Loaded by `carla_testbed/config/rig_loader.py`.

Guideline:

- Keep rig files reusable across stacks.
- Add new sensors here instead of embedding sensor definitions in run scripts.

