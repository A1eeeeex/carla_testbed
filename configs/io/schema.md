# io config schema（MODE-2 优先）

顶层：
- run: {seed, ticks, ego_id, map, weather, traffic:{enable,density}}
- scenario: {driver, publish_ros2_native}
- io:
    mode: autoware_direct | dummy
    ros: {domain_id, use_sim_time}
    contract: {sensor_minimal, canon_ros2}
    generate: {sensor_mapping, sensor_kit_calibration, qos_overrides, frames}
- algo:
    stack: autoware | dummy | e2e | apollo
    autoware: {compose, interface, params:{use_sim_time}}
- record: {rosbag:{enable, topics}}
- logging: {level}
- artifacts: 自动生成，包含 dir/sensor_mapping/sensor_kit_calibration/qos_overrides/frames 路径

合并规则：base yaml -> CLI overrides (--override k=v) -> defaults。最终写入 runs/<ts>/effective.yaml。
