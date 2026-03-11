# 控制话题契约

- 控制消息：`ackermann_msgs/msg/AckermannDriveStamped`
- 默认话题：`/tb/ego/control_cmd`
- 坐标/单位：`steering_angle` 单位为 rad，`speed` 单位为 m/s，`acceleration` 单位为 m/s^2
- 桥接行为：将 `steering_angle` 映射为 CARLA `VehicleControl.steer`，并按 `max_steer_angle` 限幅；正值表示左转
- 安全机制：若超时未收到控制（默认 `0.8s`），桥会施加刹车并清零油门
