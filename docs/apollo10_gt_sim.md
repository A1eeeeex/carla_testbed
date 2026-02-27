# Apollo 10.0 GT Simulation (ROS2 -> CyberRT -> CARLA control)

This document describes the minimum closed loop:

1. testbed publishes GT on ROS2 (`/carla/<ego>/odom`, objects, tf)
2. `tools/apollo10_cyber_bridge/bridge.py` converts ROS2 GT to Apollo Cyber channels
3. Apollo modules output `/apollo/control`
4. bridge maps ControlCommand to ROS2 control topic (`/tb/ego/control_cmd`)
5. `ros2_autoware_to_carla.py` applies ROS2 control to CARLA ego

## 1) Prerequisites

- Apollo 10.0 source tree is available and built.
- `APOLLO_ROOT` points to Apollo 10.0 root.
- ROS2 Humble and CARLA Python API are available in the same shell.
- If Apollo runs in Docker (recommended for 10.0 runtime libs), keep the container running with host network and host IPC:
  - `--network host`
  - `--ipc host`
  - same `ROS_DOMAIN_ID` as host testbed shell

Example:

```bash
export APOLLO_ROOT=/path/to/apollo
export APOLLO_DOCKER_CONTAINER=apollo_dev_x86_64
source /opt/ros/humble/setup.bash
```

If needed, also source Apollo cyber env:

```bash
source "$APOLLO_ROOT/cyber/setup.bash"
```

## 2) Generate Apollo pb2 from local Apollo 10.0 tree

```bash
bash tools/apollo10_cyber_bridge/gen_pb2.sh
```

Generated files are placed at:

- `tools/apollo10_cyber_bridge/pb/modules/common_msgs/.../*_pb2.py`

## 3) Run followstop with Apollo adapter (GT mode)

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt.yaml \
  --run-dir runs/followstop_apollo_gt \
  --override algo.apollo.docker.container="$APOLLO_DOCKER_CONTAINER" \
  --override algo.apollo.apollo_root="$APOLLO_ROOT"
```

Notes:

- `algo.apollo.docker.enabled=true` enables Apollo-container orchestration (auto start container/modules).
- `algo.apollo.docker.bridge_in_container=true` runs `bridge.py` inside container via `docker exec`.
- Recommended in this repo: keep `algo.apollo.docker.bridge_in_container=false` so bridge runs on host (host has ROS2/rclpy).
- If ROS2 is on host, set `algo.apollo.ros2_setup_script` (or leave empty to auto-detect `/opt/ros/<ROS_DISTRO>/setup.bash`).
- Set `algo.apollo.cyber_domain_id` (default `80`) to match Apollo Docker runtime domain.
- If host bridge and container channels are isolated, set non-loopback `algo.apollo.cyber_ip`.
- If container is stopped, backend can auto-start it (`algo.apollo.docker.auto_start_container=true`).
- Backend checks runtime mode at startup and writes `runs/<run>/artifacts/apollo_container_runtime_check.txt`.
- Backend can auto-install missing runtime libs in container (`algo.apollo.docker.auto_install_runtime_deps=true`),
  and writes `apollo_mainboard_runtime_check.log` / `apollo_runtime_deps_install.log`.
- Apollo modules can be auto-started by run script (`algo.apollo.docker.start_modules=true`).
  If `start_modules_cmd` is empty, backend uses a safer default: link packaged `.so` files from `/opt/apollo/neo/lib/modules` into `/apollo/modules`, then start routing/prediction/planning/control launch files directly.
- Optional auto-routing request is supported via config:
  `algo.apollo.routing.enable=true` with `end_ahead_m/resend_sec/max_attempts`.
- Backend stages `bridge.py`, bridge config, and generated pb2 into container under `/tmp/carla_testbed_apollo_bridge/<run>/`.
- `carla_control_bridge` still runs on host and listens ROS2 control topic (`/tb/ego/control_cmd`) to apply CARLA control.

Artifacts:

- `runs/<run>/artifacts/cyber_bridge.out.log`
- `runs/<run>/artifacts/cyber_bridge.err.log`
- `runs/<run>/artifacts/cyber_bridge_stats.json`
- `runs/<run>/artifacts/cyber_bridge_healthcheck.json`
- `runs/<run>/artifacts/cyber_control_bridge.out.log`
- `runs/<run>/artifacts/cyber_control_bridge.err.log`

## 4) Verify channels and recording

Open monitor:

```bash
bash tools/apollo10_cyber_bridge/monitor.sh
```

Expected active channels:

- `/apollo/localization/pose`
- `/apollo/canbus/chassis`
- `/apollo/perception/obstacles`

Record all Cyber channels:

```bash
bash tools/apollo10_cyber_bridge/record_all.sh runs/followstop_apollo_gt 60
```

## 5) Send routing request (optional helper)

```bash
python tools/apollo10_cyber_bridge/send_routing_request.py \
  --apollo-root "$APOLLO_ROOT" \
  --start "0,0,0" \
  --end "30,0,0"
```

## 6) Map and routing notes

Apollo planning/control requires Apollo HDMap assets under:

- `modules/map/data/<map_name>/base_map.bin` / `base_map.xml`
- routing graph assets for that map

For quick validation, use an existing CARLA-to-Apollo map asset repository (for example `Carla_apollo_maps`) and follow that repository's routing graph generation steps before enabling planning/control.
