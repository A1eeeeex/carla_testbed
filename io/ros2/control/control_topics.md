# Control Topics Contract

- Command: `ackermann_msgs/msg/AckermannDriveStamped`
- Default topic: `/tb/ego/control_cmd`
- Frames/units: steering_angle in rad, speed in m/s, acceleration in m/s^2.
- Bridge behaviour: maps steering_angle to CARLA VehicleControl steer after clamping by max_steer_angle; positive steer => left turn.
- Safety: if no command is received within timeout (default 0.8s), bridge applies brake and zero throttle.
