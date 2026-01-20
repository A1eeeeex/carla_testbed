# 模块 README 索引
面向首次上手的开发/算法同学，按模块查看使用说明与排错指南。所有路径相对仓库根。坐标系统一：CARLA world/ego(base_link) x 前、y 右、z 上；相机 optical x 右、y 下、z 前；run 产物写入 `runs/<run_id>/`。

## 模块列表
- [config](../carla_testbed/config/README.md)
- [control](../carla_testbed/control/README.md)
- [control/legacy_followstop](../carla_testbed/control/legacy_followstop/README.md)
- [io](../carla_testbed/io/README.md)
- [record](../carla_testbed/record/README.md)
- [record/sensor_demo](../carla_testbed/record/sensor_demo/README.md)
- [runner](../carla_testbed/runner/README.md)
- [scenarios](../carla_testbed/scenarios/README.md)
- [schemas](../carla_testbed/schemas/README.md)
- [sensors](../carla_testbed/sensors/README.md)
- [sim](../carla_testbed/sim/README.md)
- [utils](../carla_testbed/utils/README.md)

## 模块依赖与数据流（ASCII）
```
scenario -> sim(world/tick) -> sensors(capture) -> runner(config/record) -> record(mp4/csv/json)
                                     \-> control(step -> ControlCommand) -> sim.apply_control
                                     \-> io(ROS2 bridge) -> external nodes/rosbag
schemas 提供数据结构，config 提供 rig/calibration/contract
```

## 三条快速上手路径
1) **只跑仿真并导出原始传感器文件**
```bash
python examples/run_followstop.py --rig fullstack --ticks 500 --record-keep-frames
# 产物：runs/followstop_<ts>/sensors/<id>/，config/calibration.json 等
```
2) **生成传感器 demo mp4（LiDAR+Radar+HUD）**
```bash
python examples/run_followstop.py --rig fullstack --ticks 1000 --record sensor_demo --record-keep-frames
# 产物：runs/followstop_<ts>/video/demo.mp4（frames/ 可选保留）
```
3) **启用 ROS2 bridge 发布 topic 并录 rosbag**
```bash
python examples/run_followstop.py --rig fullstack --enable-ros2-bridge --ticks 500
# 另一个终端（已 source ROS2）：
ros2 bag record /clock /tf_static /sim/ego/camera/front/image_raw /sim/ego/lidar/top/points /sim/ego/imu /sim/ego/gnss
```

如需更多细节，请进入对应模块 README。★ 如果遇到坐标系/时间戳问题，优先检查 config/calibration.json 与 time_sync.json 是否匹配当前 run。 
