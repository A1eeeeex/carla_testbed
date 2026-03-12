# CARLA -> ROS2 GT -> Apollo 10.0 -> ROS2 -> CARLA 当前链路说明

本文只描述仓库当前代码里已经存在的事实，范围限定为当前 GT 闭环主路径：`configs/io/examples/followstop_apollo_gt_baseline.yaml` 对应的 `carla_followstop + Apollo` 运行方式。

本文不包含任何“应该如何改”的推测，只描述代码现在做了什么、数据如何流动、每一步由哪个文件负责。

## 1. 这条链路对应的运行入口

当前推荐入口是：

```bash
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt_baseline.yaml
```

补充入口：

- 冻结基线说明书：`docs/gt_followstop_apollo_baseline.md`
- 三档 profile 差异矩阵：`docs/gt_profile_matrix.md`
- reference line 失效排查：`docs/gt_reference_line_rootcause.md`
- 基线评估工具：`tools/evaluate_gt_baseline.py`
- profile 对比工具：`tools/compare_gt_profiles.py`
- 起点对齐诊断工具：`tools/diagnose_startup_lane_alignment.py`
- reference line 失败诊断工具：`tools/diagnose_reference_line_failure.py`

这条命令实际走的是下面这条调用链：

1. `carla_testbed/__main__.py`
   - 调 `carla_testbed.cli.main()`
2. `carla_testbed/cli.py`
   - 解析 `run` 子命令
   - 调 `tbio/scripts/run.py:main()`
3. `tbio/scripts/run.py`
   - 读取 YAML 配置
   - 合并本地配置和 `--override`
   - 创建 `runs/<run>/`
   - 写 `effective.yaml`
   - 调 `doctor_main(run_dir)` 生成环境检查报告
   - 如果 `scenario.driver == carla_followstop`，启动子进程：
     - `python -m examples.run_followstop --config <effective.yaml> --run-dir <run_dir>`
4. `examples/run_followstop.py`
   - 读取 `effective.yaml`
   - 创建场景、控制器、适配器
   - 在主循环里驱动 CARLA、发布 GT、启动 Apollo bridge、记录产物

关键文件：

- `tbio/scripts/run.py`
- `examples/run_followstop.py`

## 2. 当前这份 Apollo GT 配置实际打开了什么

`configs/io/examples/followstop_apollo_gt_baseline.yaml` 当前关键开关如下：

- `scenario.driver: carla_followstop`
- `scenario.publish_ros2_native: false`
- `scenario.publish_ros2_gt: true`
- `algo.stack: apollo`
- `algo.apollo.control_out_type: direct`
- `runtime.carla.start: true`
- `runtime.carla.extra_args: "--ros2"`

这意味着：

- 当前不是用 CARLA 原生 ROS2 传感器流做输入（相机、激光等原生发布路径是关的）
- 当前是由 testbed 进程自己发布“真值 GT”
- Apollo 是外部算法栈
- Apollo 输出控制后，经 bridge 转成 ROS2，再由另一个 ROS2 节点把控制写回 CARLA

关键文件：

- `configs/io/examples/followstop_apollo_gt_baseline.yaml`

## 3. 场景里 GT 的原始来源是什么

### 3.1 FollowStop 场景如何生成车辆

场景代码在 `carla_testbed/scenarios/followstop.py`。

当前逻辑：

1. 先清理世界里已有的动态 actor：
   - `vehicle.*`
   - `walker.*`
   - `controller.ai.walker`
2. 从 `carla_map.get_spawn_points()` 取出生点列表
3. 生成前车
4. 生成自车

当前默认配置（来自 `configs/io/examples/followstop_apollo_gt_baseline.yaml`）：

- `ego_idx: 120`
- `front_idx: 210`
- `ego_id: hero`
- `front_id: front`

场景对车辆做的事：

- 前车：
  - `role_name = front`
  - `ros_name = front`
  - `set_simulate_physics(True)`
  - `apply_control(throttle=0.0, brake=1.0, hand_brake=True)`
- 自车：
  - `role_name = hero`
  - `ros_name = hero`
  - `set_simulate_physics(True)`

也就是说，GT 的“世界真值源头”就是 CARLA 世界里的这两辆车，加上世界里仍然存在的其他动态 actor（但当前场景已经在生成前清理过一遍）。

关键文件：

- `carla_testbed/scenarios/followstop.py`

### 3.2 CARLA 是否持续推进

这条链路里 GT 发布依赖 CARLA world 每步推进。

当前默认：

- `HarnessConfig.synchronous_mode = True`（`carla_testbed/config/defaults.py`）

主循环里每一步都会：

- `tick_world(world)` -> `world.tick()`

所以当前 GT 时间基准来自 CARLA 同步仿真的每一帧。

关键文件：

- `carla_testbed/config/defaults.py`
- `carla_testbed/runner/harness.py`
- `carla_testbed/sim/tick.py`

## 4. GT 是在哪一层发布出来的

GT 发布器是：

- `carla_testbed/ros2/gt_publisher.py`
- 类：`GroundTruthRos2Publisher`

它不是独立进程；它直接运行在 test harness 所在的 Python 进程里。

初始化位置在：

- `carla_testbed/runner/harness.py`

只有在下面条件成立时才会启用：

- `self.cfg.enable_ros2_gt == True`

当前配置对应的实际参数来自：

- `scenario.publish_ros2_gt: true`
- `scenario.gt.*`

初始化时传入的关键参数包括：

- `ego_id`
- `namespace`
- `invert_tf`
- `calibration_path`
- `publish_tf`
- `publish_odom`
- `publish_objects3d`
- `publish_markers`
- `objects_radius_m`
- `max_objects`
- `odom_hz`
- `tf_hz`
- `objects_hz`
- `markers_hz`
- `qos_reliability`
- `qos_depth`

初始化成功后，harness 会先调用一次：

- `gt_pub.publish_static_tf_from_calibration()`

之后在每个仿真 tick 调：

- `gt_pub.publish_tick(world, ego, extra_actors)`

关键文件：

- `carla_testbed/runner/harness.py`
- `carla_testbed/ros2/gt_publisher.py`

## 5. GT 在 ROS2 里到底发了哪些话题

当前 topic 前缀由这两项组成：

- `namespace`（当前配置是 `/carla`）
- `ego_id`（当前配置是 `hero`）

所以当前前缀是：

- `/carla/hero`

`GroundTruthRos2Publisher` 代码里定义的 topic 集合是：

- `/carla/hero/odom`
- `/tf`
- `/tf_static`
- `/carla/hero/objects3d`
- `/carla/hero/objects_markers`
- `/carla/hero/objects_gt_json`

### 5.1 `/carla/hero/odom`

类型：

- `nav_msgs/msg/Odometry`

发布条件：

- `publish_odom = true`

当前字段来源：

- `header.stamp`：来自 `world.get_snapshot().timestamp.elapsed_seconds`
- `header.frame_id = "odom"`
- `child_frame_id = "base_link"`
- `pose`：来自 `ego.get_transform()`
- `twist.linear`：来自 `ego.get_velocity()`
- `twist.angular`：来自 `ego.get_angular_velocity()`

### 5.2 `/tf`

类型：

- `tf2_msgs/msg/TFMessage`

发布条件：

- `publish_tf = true`

当前内容：

- 动态 TF：`odom -> base_link`

来源：

- 也是 `ego.get_transform()`

### 5.3 `/tf_static`

类型：

- `tf2_msgs/msg/TFMessage`

当前内容：

1. 恒定单位变换：
   - `map -> odom`
2. 从 `calibration.json` 读出的静态外参：
   - `base_link -> 各传感器 frame`

发布时机：

- 初始化后只发一次（`_static_published` 防重复）

### 5.4 `/carla/hero/objects3d`

类型：

- `vision_msgs/msg/Detection3DArray`

发布条件：

- `publish_objects3d = true`
- 且运行环境里 `vision_msgs` 可导入

每个对象的来源：

- 来自 CARLA actor（vehicle / walker）
- 使用：
  - `actor.get_transform()`
  - `actor.bounding_box.extent`

当前填充内容：

- `bbox.center.position`
- `bbox.center.orientation`
- `bbox.size`
- `results[0].hypothesis.class_id`
- `results[0].hypothesis.score = 1.0`
- 如果消息类型支持 `id`，则写入 `actor.id`

### 5.5 `/carla/hero/objects_markers`

类型：

- `visualization_msgs/msg/MarkerArray`

发布条件：

- `publish_markers = true`
- 且运行环境里 `visualization_msgs` 可导入

每个对象会生成一个 `CUBE` marker，使用 actor 的：

- pose
- bbox 尺寸
- 基于类别的颜色

### 5.6 `/carla/hero/objects_gt_json`

类型：

- `std_msgs/msg/String`

发布条件：

- `publish_objects3d = true`
- 但 `objects3d` 发布器不可用（即 `vision_msgs` 不可用）

当前 JSON 结构（代码里直接生成）：

- 顶层：
  - `stamp`
  - `frame_id`
  - `objects`
- 每个对象：
  - `id`
  - `class`
  - `pose`
  - `velocity`
  - `size`

### 5.7 对象是如何筛选的

`GroundTruthRos2Publisher` 不会无条件发布所有 actor。

它会：

1. 从 harness 传入的 `extra_actors` 中筛选：
   - `vehicle.*`
   - `walker.*`
2. 排除 ego 本身
3. 按与 ego 的距离排序
4. 只保留半径内对象：
   - `objects_radius_m`
5. 最多保留：
   - `max_objects`

如果半径内一个都没有：

- 会退回发布“全世界最近的 1 个对象”（前提是 `max_objects > 0`）

关键文件：

- `carla_testbed/ros2/gt_publisher.py`

## 6. GT 坐标是如何从 CARLA 转到 ROS2 的

当前 GT 发布器默认：

- `invert_tf = True`

坐标和姿态转换规则写死在 `GroundTruthRos2Publisher` 里：

- 位置：
  - `x = carla.x`
  - `y = -carla.y`
  - `z = carla.z`
- 姿态：
  - `roll = carla.roll`
  - `pitch = -carla.pitch`
  - `yaw = -carla.yaw`
- 向量（速度、角速度）：
  - `x = carla.x`
  - `y = -carla.y`
  - `z = carla.z`

也就是说，当前 GT 发布到 ROS2 时，会把：

- `y`
- `pitch`
- `yaw`

做符号翻转。

关键文件：

- `carla_testbed/ros2/gt_publisher.py`

## 7. Apollo 这边是如何接收这些 GT 的

Apollo 不是直接订阅 ROS2；中间有一层 bridge：

- `tools/apollo10_cyber_bridge/bridge.py`

这个 bridge 由 `tbio/backends/cyberrt.py` 启动。

### 7.1 `ApolloAdapter` 先做了什么

`ApolloAdapter` 在启动前会生成一份桥接配置：

- 模板：`tools/apollo10_cyber_bridge/config_example.yaml`
- 输出：`runs/<run>/artifacts/apollo_bridge_effective.yaml`

它会把当前 run 的信息填进去，包括：

- ROS2 输入 topic
- ROS2 控制输出 topic
- CyberRT channel
- 自动 routing 配置
- `carla_to_apollo` 刚体变换配置
- `carla_feedback` 配置
- control mapping 配置
- 地图文件路径和 map bounds 文件路径

关键文件：

- `algo/adapters/apollo.py`
- `tools/apollo10_cyber_bridge/config_example.yaml`

### 7.2 `CyberRTBackend` 实际起了哪些进程

`tbio/backends/cyberrt.py` 当前会做这些事：

1. 准备 bridge 配置
2. 启动 Apollo 容器（如果配置允许自动启动）
3. 在容器里启动 Apollo 模块（当前配置 `start_modules: true`）
4. 在宿主机启动：
   - `tools/apollo10_cyber_bridge/bridge.py`
5. 在宿主机再启动：
   - `algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py`

当前 per-run 日志会落到 `runs/<run>/artifacts/`，包括：

- `cyber_bridge.out.log`
- `cyber_bridge.err.log`
- `cyber_control_bridge.out.log`
- `cyber_control_bridge.err.log`
- `apollo_bridge_effective.yaml`

当前还会把 Apollo 核心原生日志的“本轮新增部分”切片保存到：

- `apollo_planning.INFO`
- `apollo_control.INFO`
- `apollo_routing.INFO`
- `apollo_external_command.INFO`

关键文件：

- `tbio/backends/cyberrt.py`

## 8. bridge 在 ROS2 侧实际订阅了什么

`bridge.py` 里会创建一个 `RosCacheNode`，它负责订阅 ROS2 输入并缓存最新消息。

当前订阅的 ROS2 话题是：

- `odom_topic`
- `objects3d_topic`
- `objects_markers_topic`
- `objects_json_topic`

按当前默认映射，分别是：

- `/carla/hero/odom`
- `/carla/hero/objects3d`
- `/carla/hero/objects_markers`
- `/carla/hero/objects_gt_json`

当前代码里这四路消息都只缓存“最新一帧”：

- `latest_odom`
- `latest_objects3d`
- `latest_markers`
- `latest_objects_json`

bridge 每个发布周期都从这些缓存里取快照，再转成 Apollo 的 CyberRT 消息。

关键文件：

- `tools/apollo10_cyber_bridge/bridge.py`

## 9. ROS2 GT 到 Apollo CyberRT 的映射

### 9.1 bridge 最终写到哪些 Apollo channel

bridge 会创建下面这些 writer：

- `/apollo/localization/pose`
- `/apollo/canbus/chassis`
- `/apollo/perception/obstacles`

如果自动 routing 打开，还会额外用到：

- `/apollo/raw_routing_request`

另外还会创建 reader / client：

- reader：`/apollo/control`
- reader：`/apollo/routing_response`
- client：`/apollo/external_command/lane_follow`（当前配置启用）
- client：`/apollo/external_command/action`（当前配置默认关闭）

关键文件：

- `tools/apollo10_cyber_bridge/bridge.py`

### 9.2 `/carla/hero/odom` -> `/apollo/localization/pose`

bridge 中负责这个转换的函数是：

- `_odom_to_loc()`

它做的事：

1. 读取 ROS2 `Odometry`
2. 先经过 `carla_to_apollo` 变换（`SE2`：平移 + 偏航）
3. 如果打开 `auto_calib`，会在收集足够样本后修正这组变换
4. 把姿态转换成 Apollo `LocalizationEstimate`

当前填充内容包括：

- `header.timestamp_sec`
- `header.module_name`
- `header.sequence_num`
- `pose.position`
- `pose.orientation`
- `pose.heading`
- `pose.linear_velocity`
- `pose.linear_velocity_vrf`
- `pose.angular_velocity`
- `pose.angular_velocity_vrf`
- `pose.linear_acceleration`
- `pose.linear_acceleration_vrf`
- `measurement_time`

当前 `measurement_time` 用的是：

- Cyber 当前时间（`_command_now_sec()`）

### 9.3 `Odometry + CARLA 实际控制` -> `/apollo/canbus/chassis`

bridge 中负责这个转换的函数是：

- `_odom_to_chassis()`

当前 `Chassis` 的字段来源分两部分：

1. 速度：
   - 来自 odom 速度模长
2. 控制反馈：
   - 优先来自 `CarlaFeedbackClient.read_control()`
   - 也就是直接调用 `carla.Vehicle.get_control()`

当前会同时写入：

- 实测控制百分比：
  - `throttle_percentage`
  - `brake_percentage`
  - `steering_percentage`
- 期望控制百分比：
  - `throttle_percentage_cmd`
  - `brake_percentage_cmd`
  - `steering_percentage_cmd`

另外还会写：

- `engine_started = True`
- `parking_brake = False`
- 如果字段存在：
  - `driving_mode = COMPLETE_AUTO_DRIVE`
  - `gear_location = GEAR_DRIVE`

### 9.4 障碍物 GT -> `/apollo/perception/obstacles`

bridge 中负责这个转换的函数是：

- `_objects_to_obstacles()`

它的输入优先级是：

1. `objects3d`
2. `objects_json`
3. `objects_markers`

也就是说：

- 如果 `Detection3DArray` 存在且有内容，优先用它
- 否则尝试 JSON
- 再否则才退回 MarkerArray

bridge 会把每个对象转换成 Apollo 的 `PerceptionObstacle`，当前填充：

- `id`
- `position`
- `velocity`
- `theta`
- `length`
- `width`
- `height`
- `type`
- `timestamp`
- `tracking_time`
- `confidence`
- 如果字段存在，还会写 `polygon_point`

当前最多发布：

- `max_obstacles`

### 9.5 当前没有传的真值类型

在这条“GT -> Apollo”链里，代码当前没有做下面这些输入：

- 没有 lane line / lane boundary 真值 topic
- 没有交通灯颜色真值 topic
- 没有相机图像、激光雷达点云、IMU、GNSS 的原生传感器流（当前这份配置里 `publish_ros2_native` 是关的）

也就是说，Apollo 当前收到的“核心 GT 输入”只有：

- 定位（Localization）
- 车辆状态（Chassis）
- 障碍物（PerceptionObstacles）
- 再加上自动 routing / lane-follow 命令

## 10. Apollo 在当前代码里实际启动了哪些算法模块

当前 `CyberRTBackend` 启动的 Apollo 模块是：

- `routing`
- `prediction`
- `planning`
- `control`

对应 DAG：

- `modules/routing/dag/routing.dag`
- `modules/prediction/dag/prediction.dag`
- `modules/planning/planning_component/dag/planning.dag`
- `modules/control/control_component/dag/control.dag`

这不是推测，是 `tbio/backends/cyberrt.py` 里的实际启动路径。

### 10.1 当前 planning 用的是哪条 pipeline

`tbio/backends/cyberrt.py` 会生成一个 lane-follow overlay 配置，位置是：

- `/apollo_workspace/conf_overlay/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt`

当前代码会在这个 overlay 里移除：

- `LaneChangePath`
- `LaneBorrowPath`
- `RuleBasedStopDecider`

所以当前 planning 仍然是 Apollo 的 `lane_follow` 场景，但 task 链是“精简版”。

### 10.2 当前 control 用的是哪条 pipeline

当前 control 读的是 Apollo 自带配置：

- `/opt/apollo/neo/share/modules/control/control_component/conf/pipeline.pb.txt`

当前控制器组合是：

- `LatController`
- `LonController`

这同样来自当前代码实际读取和启动的配置路径。

## 11. 自动 routing 是如何生成任务目标的

当前配置里：

- `algo.apollo.routing.enable: true`

所以 bridge 会自动给 Apollo 发送 route / lane-follow 命令。

### 11.1 发送的 channel

- routing：
  - `/apollo/raw_routing_request`
- lane follow：
  - `/apollo/external_command/lane_follow`

### 11.2 触发条件

bridge 的 `_maybe_send_routing_request()` 会在这些条件满足时才触发：

- 已经收到第一帧 odom
- 超过 `startup_delay_sec`
- 首个 routing 需要时：
  - 至少收到一次 obstacle GT（任一：`objects_json / objects3d / markers`）

### 11.3 起点和终点怎么来的

当前起点：

- 来自当前 odom，经 `carla_to_apollo` 变换后得到 map 坐标
- 再经过：
  - `map_bounds` 裁剪
  - 最近 lane centerline 投影（`_snap_xy_to_lane`）

当前终点：

1. 优先尝试找“前车锚点”
   - 先直接通过 `CarlaFeedbackClient.find_vehicle_by_roles(("front",))`
   - 找场景里 `role_name = front` 的车辆
2. 如果没找到，再从缓存的 GT 对象里找“前方最近的 vehicle”
3. 有锚点时：
   - 以锚点位置为基准，沿 seed heading 往前推 `end_ahead_m`
4. 没有锚点时：
   - 退回 `ego_seed`
5. 终点也会再经过：
   - `map_bounds` 裁剪
   - 最近 lane centerline 投影

也就是说，当前 routing goal 不是写死坐标，而是运行时由 bridge 自动算出来的。

关键文件：

- `tools/apollo10_cyber_bridge/bridge.py`

## 12. Apollo 控制是如何回到 ROS2 的

Apollo 控制输入 channel 是：

- `/apollo/control`

bridge 对它的处理函数是：

- `_on_control_cmd()`

### 12.1 当前从 Apollo `ControlCommand` 读取哪些字段

纵向：

- `throttle`
- `brake`

横向：

按顺序找第一个存在的字段：

1. `steering_target`
2. `steering_percentage`
3. `steering`
4. `steering_rate`

也就是说，当前如果 `steering_target` 存在，就优先用它。

### 12.2 当前如何做归一化

原始 Apollo 百分比会被归一化成 0~1 或 -1~1：

- `raw_throttle = throttle_pct / 100.0`
- `raw_brake = brake_pct / 100.0`
- `raw_steer = steer_pct / 100.0`

### 12.3 当前如何做二次映射

归一化后，还会经过 control mapping：

- `throttle_scale`
- `brake_scale`
- `steer_scale`
- `brake_deadzone`
- `zero_hold_sec`
- `steer_sign`

当前直接控制的核心公式是：

- `throttle = clamp(raw_throttle * throttle_scale, 0, 1)`
- `brake = clamp(raw_brake * brake_scale, 0, 1)`
- `if brake < brake_deadzone: brake = 0`
- `steer_pre = raw_steer * steer_sign * steer_scale`
- `steer = clamp(steer_pre, -1, 1)`

### 12.4 当前发回 ROS2 的消息类型

因为当前配置：

- `control_out_type: direct`

所以 bridge 发布的是：

- `std_msgs/msg/Float32MultiArray`

内容固定是：

- `[throttle, brake, steer]`

发布 topic：

- `/tb/ego/control_cmd`

关键文件：

- `tools/apollo10_cyber_bridge/bridge.py`

## 13. ROS2 控制是如何最终写回 CARLA 的

负责这一步的节点是：

- `algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py`

虽然文件名带 `autoware`，但当前 Apollo 这条链路也在用它。

### 13.1 当前订阅的控制 topic

- `/tb/ego/control_cmd`

### 13.2 当前控制类型

配置是：

- `control_type = direct`

所以它按 `Float32MultiArray` 读取：

- `data[0] -> throttle`
- `data[1] -> brake`
- `data[2] -> steer`

### 13.3 当前写回 CARLA 的方式

它会直接构造：

- `carla.VehicleControl()`

并把这三个值原样写进去：

- `ctrl.throttle = clamp(data[0], 0, 1)`
- `ctrl.brake = clamp(data[1], 0, 1)`
- `ctrl.steer = clamp(data[2], -1, 1)`

然后调用：

- `self.ego.apply_control(ctrl)`

### 13.4 当前 watchdog 行为

如果超过 `timeout_sec` 没收到新控制：

- 会自动对 ego 施加：
  - `throttle=0.0`
  - `brake=1.0`
  - `steer=0.0`

这一步是桥末端的保护逻辑。

关键文件：

- `algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py`

## 14. 这条链路里每一层的“实际数据格式”总结

### 14.1 CARLA 世界中的真值源

- `carla.Vehicle.get_transform()`
- `carla.Vehicle.get_velocity()`
- `carla.Vehicle.get_angular_velocity()`
- `carla.Actor.bounding_box`
- `carla.Vehicle.get_control()`（给 chassis 反馈用）

### 14.2 ROS2 GT 输出

- `nav_msgs/msg/Odometry`
- `tf2_msgs/msg/TFMessage`
- `vision_msgs/msg/Detection3DArray`（可选）
- `visualization_msgs/msg/MarkerArray`（可选）
- `std_msgs/msg/String`（JSON fallback）

### 14.3 Apollo CyberRT 输入

- `LocalizationEstimate`
- `Chassis`
- `PerceptionObstacles`
- `RoutingRequest`
- `LaneFollowCommand`

### 14.4 Apollo CyberRT 输出

- `ControlCommand`

### 14.5 ROS2 控制回写

- `std_msgs/msg/Float32MultiArray`（当前 direct 模式）

### 14.6 CARLA 最终执行

- `carla.VehicleControl`

## 15. 这条链路当前不会做什么

下面这些事当前不在这条 GT 闭环主路径里：

- 不会把 lane line 真值单独发布给 Apollo
- 不会把 traffic light 颜色真值单独发布给 Apollo
- 不会把 CARLA 原生相机/激光/IMU/GNSS 作为 Apollo 输入（当前这份配置已关闭 `publish_ros2_native`）
- 不会让 Apollo 直接控制 CARLA；Apollo 必须经过：
  - CyberRT `ControlCommand`
  - bridge
  - ROS2 `/tb/ego/control_cmd`
  - CARLA control bridge

这些限制不是推测，而是当前代码里没有对应 publisher / subscriber / channel。

## 16. 当前每次 run 在 `runs/<run>/` 里能看到哪些与这条链路相关的产物

当前代码会在 `runs/<run>/` 写出下面这些与 GT / Apollo 相关的关键文件：

- `effective.yaml`
- `artifacts/run_meta.json`
- `artifacts/doctor.txt`
- `artifacts/apollo_bridge_effective.yaml`
- `artifacts/cyber_bridge.out.log`
- `artifacts/cyber_bridge.err.log`
- `artifacts/cyber_control_bridge.out.log`
- `artifacts/cyber_control_bridge.err.log`
- `artifacts/cyber_bridge_stats.json`
- `artifacts/control_decode_debug.jsonl`（开启 raw control dump 时）
- `artifacts/debug_timeseries.csv`
- `artifacts/apollo_planning.INFO`
- `artifacts/apollo_control.INFO`
- `artifacts/apollo_routing.INFO`
- `artifacts/apollo_external_command.INFO`

这些文件对应的正是这条“CARLA -> ROS2 GT -> Apollo -> ROS2 -> CARLA”链路中的各层状态。

## 17. 一句话总结这条链路

当前代码里的 GT 闭环是：

1. `followstop` 场景在 CARLA 里生成前车和自车
2. harness 每帧 `world.tick()`
3. `GroundTruthRos2Publisher` 从 CARLA 世界直接发布：
   - `odom`
   - `tf / tf_static`
   - 障碍物 GT
4. `bridge.py` 订阅这些 ROS2 GT，转换成 Apollo 的：
   - `LocalizationEstimate`
   - `Chassis`
   - `PerceptionObstacles`
   - 同时自动发送 route / lane-follow
5. Apollo 输出 `/apollo/control`
6. `bridge.py` 把它转成 ROS2 `/tb/ego/control_cmd`
7. `carla_control_bridge` 订阅这个 topic，把控制写成 `carla.VehicleControl`
8. CARLA ego 执行控制，进入下一帧

这就是当前代码里真实存在的整条数据链。

## 18. 基线指标评估入口

运行完成后，可直接对单次 run 目录做基线指标提取：

```bash
python tools/evaluate_gt_baseline.py --run-dir runs/<run_name>
```

输出文件：

- `runs/<run_name>/artifacts/gt_baseline_metrics.json`
- `runs/<run_name>/artifacts/gt_baseline_metrics.md`

## 19. 三档 profile 对比入口（含 startalign）

在 `minimal/relaxed/strict` 三次 run 完成后，可直接做汇总对比：

```bash
python tools/compare_gt_profiles.py \
  --run-dir runs/gt_minimal_01 \
  --run-dir runs/gt_startalign_01 \
  --run-dir runs/gt_relaxed_01 \
  --run-dir runs/gt_strict_01
```

## 20. Reference Line 失败诊断入口

当 run 出现：

- `Reference lane is empty`
- `Cannot find waypoint ... s < 0`
- `planning has no trajectory point`

优先执行：

```bash
python tools/diagnose_startup_lane_alignment.py --run-dir runs/<run_name>
python tools/diagnose_reference_line_failure.py --run-dir runs/<run_name>
```

输出：

- `runs/<run>/artifacts/startup_lane_alignment_summary.json`
- `runs/<run>/artifacts/reference_line_rootcause_summary.json`

输出文件：

- `runs/artifacts/gt_profile_compare.json`
- `runs/artifacts/gt_profile_compare.md`
