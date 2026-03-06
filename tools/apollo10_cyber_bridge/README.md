# apollo10_cyber_bridge/

ROS2-to-Apollo bridge used in the followstop Apollo flow.

Main files:

- `bridge.py`: main bridge process.
- `config_example.yaml`: bridge config template.
- `gen_pb2.sh`: generates local Apollo protobuf Python modules.
- `send_routing_request.py`: manual routing request helper.
- `monitor.sh`, `record_all.sh`: runtime monitoring helpers.

Responsibilities:

1. Read ROS2 odometry and obstacle streams.
2. Publish Apollo localization/chassis/perception.
3. Consume Apollo control and map it to CARLA control topic.
4. Auto-routing management and runtime stats/health summaries.

Key runtime outputs (under `runs/<run>/artifacts`):

- `cyber_bridge_stats.json`
- `bridge_health_summary.json`
- `debug_timeseries.csv`
- `control_decode_debug.jsonl`

Config domains:

- `bridge.auto_routing`
- `bridge.traffic_light`
- `bridge.front_obstacle_behavior`
- `bridge.control_mapping`

Migration note:

- This module depends on generated pb2 files under `tools/apollo10_cyber_bridge/pb/`.
- If Apollo version changes, re-run `gen_pb2.sh` and verify imports.

