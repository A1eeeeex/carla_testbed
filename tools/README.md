# tools/

Project utility scripts and integration helpers.

Important subdirectories:

- `apollo10_cyber_bridge/`: ROS2 <-> Apollo CyberRT bridge and support tools.
- `bootstrap_native.sh`: native host bootstrap helper.

Guideline:

- Treat this folder as operational tooling, not business logic.
- Keep long-running runtime code inside `algo/` or `tbio/`.

