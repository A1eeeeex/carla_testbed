# Apollo Current Module Logic

这份文档用尽量直白的方式解释：当前 CARLA testbed 里 Apollo 各个模块是怎么配合起来工作的。

先给一句话版本：

> CARLA 提供“真值世界”，bridge 把真值翻译成 Apollo 能读懂的 localization / chassis / obstacle / traffic-light / routing 输入，Apollo 真实运行 routing / planning / control，最后 bridge 再把 Apollo control 转成 CARLA 车辆控制。

这不是完整 Apollo 传感器复现。当前 truth-input MVP 不跑真实 camera / lidar perception，也不证明 Apollo 原生定位模块已经完整复现。

## 当前总流程

```text
CARLA world
  |
  | ego pose / velocity / actors / traffic lights / route goal
  v
CARLA-Testbed bridge
  |
  | publishes Apollo channels
  v
Apollo CyberRT runtime
  |
  | routing -> planning -> control
  v
Apollo /apollo/control
  |
  | control mapping
  v
CARLA ego vehicle actuation
```

更具体一点：

```text
CARLA ego transform  -> /apollo/localization/pose
CARLA ego speed      -> /apollo/canbus/chassis
CARLA actors         -> /apollo/perception/obstacles
CARLA traffic light  -> /apollo/perception/traffic_light
Route goal           -> /apollo/routing_request or lane-follow command

Apollo routing       -> /apollo/routing_response
Apollo planning      -> /apollo/planning
Apollo control       -> /apollo/control
Bridge mapping       -> CARLA VehicleControl
```

## 源码和本地运行证据怎么读

这份说明不是只按概念写的。当前判断主要来自三类证据：

- Apollo 官方源码入口：
  - routing component: `modules/routing/routing_component.cc`
  - routing core: `modules/routing/routing.cc`
  - planning component: `modules/planning/planning_component/planning_component.cc`
  - planning base: `modules/planning/planning_component/planning_base.cc`
  - on-lane planning: `modules/planning/planning_component/on_lane_planning.cc`
  - lane-follow PNC map: `modules/planning/pnc_map/lane_follow_map/lane_follow_map.cc`
  - public road planner: `modules/planning/planners/public_road/public_road_planner.cc`
  - lane-follow scenario/stage: `modules/planning/scenarios/lane_follow/*`
  - control component: `modules/control/control_component/control_component.cc`
  - lateral / longitudinal controller:
    `modules/control/controllers/lat_based_lqr_controller/lat_controller.cc`,
    `modules/control/controllers/lon_based_pid_controller/lon_controller.cc`
- 本机 Apollo 10.0 runtime 配置：
  - `/home/ubuntu/Apollo10.0/application-core/conf_overlay/modules/common/data/global_flagfile.txt`
  - `/home/ubuntu/Apollo10.0/application-core/conf_overlay/modules/planning/planning_component/conf/planning.conf`
  - `/home/ubuntu/Apollo10.0/application-core/conf_overlay/modules/planning/planning_component/conf/public_road_planner_config.pb.txt`
  - `/home/ubuntu/Apollo10.0/application-core/conf_overlay/modules/planning/planning_component/conf/traffic_rule_config.pb.txt`
- 本机最近 Apollo logs：
  - `/home/ubuntu/Apollo10.0/application-core/data/log/routing.log.INFO.*`
  - `/home/ubuntu/Apollo10.0/application-core/data/log/planning.log.INFO.*`
  - `/home/ubuntu/Apollo10.0/application-core/data/log/prediction.log.INFO.*`
  - `/home/ubuntu/Apollo10.0/application-core/data/log/control.log.INFO.*`

一个很关键的本地事实：`/home/ubuntu/Apollo10.0/application-core/.aem/envroot/apollo/modules/*` 主要是运行环境目录，里面大量文件是指向容器或 Apollo runtime 安装路径的 `.so` / config 软链接，不是完整源码树。因此源码级逻辑以 Apollo 官方源码为准，本地 runtime 证据用来确认“这台机器实际加载了哪些插件和配置”。

当前本机可读到的 Apollo 源码树在：

```text
/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src
```

当前本地 runtime 证据显示：

- map dir 当前指向 `modules/map/data/straight_road_for_baguang`；
- routing log 使用 `straight_road_for_baguang/routing_map.bin`；
- planning log 加载了 `LaneFollowMap`、`PublicRoadPlanner`、`LaneFollowScenario`；
- planning overlay 里 `public_road_planner_config.pb.txt` 只配置了 `LANE_FOLLOW`；
- planning overlay 的 `traffic_rule_config.pb.txt` 当前只启用 `BacksideVehicle` 和 `Rerouting`；
- `traffic_rules/traffic_light/conf/default_conf.pb.txt` 里有 `enabled: false`，而且 traffic-light rule 当前没有进入 overlay 的 rule list；
- planning overlay 当前设置 `--default_cruise_speed=22.220`，同时还有 `--planning_upper_speed_limit=20.00`；如果 demo 中又开启了 bridge 侧直道加速 override，要把 Apollo planning 速度、Apollo control 输出、bridge override 三层分开看；
- 当前 overlay 里 `--enable_reference_line_stitching=false`、`--enable_trajectory_stitcher=false`，所以不要用旧结论假设 stitching 一定在帮忙平滑；
- control log 加载了 LQR lateral controller 和 PID longitudinal controller。

这意味着：当前闭环核心是 lane-follow public-road planning，不是完整多场景自然驾驶栈；红绿灯自然交互还需要后续把 Apollo HDMap signal / stop line / traffic light channel / traffic rule 串起来验证，不能因为 Dreamview 能显示地图就认为 traffic-light behavior 已完成。

## 模块速览

| 模块 | 当前作用 | 当前是否真实 Apollo 模块 | 当前输入来自哪里 | 主要证据 |
| --- | --- | --- | --- | --- |
| HDMap / map | 提供 lane、road、signal、reference line 的地图基础。 | 是 | Apollo map files，由 CARLA/RoadRunner 地图转换或 Town01 map 提供。 | `map_contract_guard.json`, `routing_event_debug.jsonl`, `planning_topic_debug.jsonl` |
| localization | 告诉 Apollo ego 车在哪里、朝向哪里、速度状态如何。 | 否，当前由 CARLA GT 替代 | CARLA ego actor transform / velocity。 | `cyber_bridge_stats.json`, `timeseries.csv`, `startup_geometry_debug.jsonl` |
| chassis / canbus | 告诉 Apollo 车辆底盘状态，例如速度、档位、控制反馈。 | 否，当前由 CARLA GT 替代 | CARLA ego speed / applied control。 | `cyber_bridge_stats.json`, `direct_bridge_control_apply.jsonl` |
| perception obstacles | 告诉 Apollo 周围有哪些障碍物。 | 否，当前由 CARLA GT 替代 | CARLA actors。 | `direct_bridge_actor_snapshot.json`, `obstacle_contract_debug.jsonl` |
| traffic light perception | 告诉 Apollo 红绿灯颜色和 signal id。 | 否，当前由 CARLA GT 替代或实验接入 | CARLA traffic light state + Apollo HDMap signal id mapping。 | `traffic_light_contract_report.json`, `cyber_bridge_stats.json` |
| routing | 把目标点或 lane-follow 命令变成 Apollo route。 | 是 | Route goal、HDMap、localization。 | `routing_event_debug.jsonl`, `routing_response_count`, `routing_success_count` |
| prediction | 预测障碍物未来运动。 | 目前不是主要 MVP 证据 | GT obstacle 输入；很多场景可不把 prediction 当核心判据。 | channel stats / Apollo logs |
| planning | 基于 route、地图、车身状态、障碍物生成轨迹。 | 是 | routing response、localization、chassis、obstacles、traffic light。 | `planning_topic_debug.jsonl`, `planning_topic_debug_summary.json`, `route_health.json` |
| control | 把 planning 轨迹变成油门、刹车、方向盘命令。 | 是 | planning trajectory、localization、chassis。 | `apollo_control_raw.jsonl`, `control_handoff_summary.json`, `cyber_bridge_stats.json` |
| control mapping / actuation | 把 Apollo control 命令转成 CARLA 可执行控制。 | 不是 Apollo，是 testbed bridge | `/apollo/control`。 | `direct_bridge_control_apply.jsonl`, `direct_bridge_stats.json`, `carla_vehicle_response.csv` |
| CyberRT | Apollo 模块之间的消息总线。 | 是 | Apollo channels。 | `cyber_bridge_stats.json`, `cyber_channel_list.txt`, Apollo logs |
| Dreamview | 可视化 Apollo 地图、车、轨迹、模块状态。 | 是 | Apollo runtime channels。 | Dreamview screenshot/video, `dreamview_recording_status.json` |

## 一层一层看

### 1. HDMap: Apollo 的“路网说明书”

Apollo 不直接看 CARLA 里的 mesh、道路贴图或路灯模型。Apollo 主要看 HDMap。

HDMap 里需要有：

- road / lane 拓扑；
- lane centerline；
- lane heading / curvature；
- junction；
- signal；
- stop line；
- route 可连接关系。

所以一个 CARLA 自定义地图能 `load_world()`，不代表 Apollo 一定能正常开。还必须有 Apollo 能理解的 HDMap。

当前 `straight_road_for_baguang` 的要点是：

- CARLA package 版加载的是 Unreal/CARLA 地图资产；
- Apollo 使用的是 `data/map_data/straight_road_for_baguang/` 下的 HDMap；
- RoadRunner 导出的 Apollo5 XML 需要转换为 Apollo 10 runtime 可用的 map files；
- lane 方向必须和 CARLA spawn / ego heading 对齐，否则 routing 可能成功但 planning 找不到合理 reference line。

### 2. Localization: 当前不是 Apollo 自己定位

在真实车上，Apollo localization 会融合 GNSS / IMU / LiDAR / map 等信息。

当前 MVP 不做这个。当前逻辑是：

```text
CARLA ego transform -> bridge -> /apollo/localization/pose
```

这意味着 Apollo 看到的 ego pose 是 CARLA 真值，不是 Apollo 原生定位算法算出来的。

这有一个好处：可以先排除传感器定位误差，把问题集中到 map / routing / planning / control / bridge 上。

也有一个限制：不能拿当前结果宣称“完整 Apollo localization 已复现”。

### 3. Chassis / Canbus: 当前由 CARLA 车身状态模拟

Apollo control 和 planning 需要知道车速、档位、底盘反馈。

当前逻辑是：

```text
CARLA ego velocity / applied control -> bridge -> /apollo/canbus/chassis
```

所以 chassis 是真值模拟，不是真车 CAN，也不是 Apollo canbus 模块接真实车辆。

如果 chassis 频率低、timestamp 不单调、速度不合理，planning/control 会被误导。

### 4. Perception Obstacles: 当前用 CARLA actors 代替传感器感知

真实 Apollo perception 会从 camera / lidar / radar 识别障碍物。

当前 MVP 是：

```text
CARLA actors -> bridge -> /apollo/perception/obstacles
```

这叫 ground-truth obstacles。它适合验证 planning/control 是否能处理前车、周围车辆和简单障碍物。

它不适合证明 Apollo camera/lidar perception 能力。

### 5. Traffic Light: 关键不是 actor id，而是 Apollo signal id

红绿灯交互比“看见 CARLA traffic light actor”更复杂。

Apollo planning 需要的是 HDMap 里的 signal id 和 stop line 语义。CARLA actor id 不能直接当 Apollo signal id。

正确链路应该是：

```text
CARLA traffic light actor / landmark
  -> mapping table
  -> Apollo HDMap signal id
  -> /apollo/perception/traffic_light
  -> Apollo planning
```

因此红绿灯场景必须同时检查：

- CARLA traffic light 是否存在；
- Apollo HDMap signal 是否存在；
- stop line 是否存在；
- lane 和 signal overlap 是否对齐；
- `/apollo/perception/traffic_light` 是否按频率发布；
- planning 是否真的对红灯/绿灯作出行为响应。

### 5.5 Prediction: 当前是真模块在跑，但输入仍是 GT obstacles

Prediction 的作用是把 perception obstacles 变成 planning 更容易消费的未来轨迹预测。

当前链路大致是：

```text
CARLA actors
  -> bridge
  -> /apollo/perception/obstacles
  -> Apollo prediction
  -> /apollo/prediction
  -> Apollo planning
```

本地 prediction log 显示 Apollo prediction component 会加载 evaluator / predictor 插件，例如 vehicle / pedestrian / multi-agent 相关插件。因此 prediction 不是完全 mock。

但要注意：它的输入 obstacle 仍然来自 CARLA ground truth，不是 Apollo 原生 camera/lidar/radar perception。因此当前结果不能证明完整 perception + prediction pipeline，只能证明“GT obstacle 输入下，prediction/planning/control 链路能否工作”。

如果 planning 没有输出，prediction 也要检查：

- `/apollo/perception/obstacles` 是否有消息；
- `/apollo/prediction` 是否有消息；
- prediction log 是否报 ego obstacle missing；
- obstacle 坐标系、速度、尺寸是否合理；
- 前车是否被错误过滤或长期隐藏。

### 6. Routing: 把目标变成 Apollo 路线

Routing 模块是真 Apollo 模块。

源码层面可以把它看成四步：

```text
RoutingComponent::Proc(request)
  -> Routing::Process(request, response)
  -> Routing::FillLaneInfoIfMissing(request)
  -> Navigator::SearchRoute(fixed_request, response)
  -> response_writer_->Write(/apollo/routing_response)
```

#### RoutingComponent 做什么

```text
/apollo/raw_routing_request
  -> RoutingComponent::Proc()
  -> response_writer_->Write(/apollo/routing_response)
```

Apollo 10.0 的 `RoutingComponent::Init()` 会创建 `/apollo/routing_response` writer，并创建一个 history writer 周期性重发最近一次 routing response。`RoutingComponent::Proc()` 收到 routing request 后，调用 `routing_.Process()`，成功后填 header 并写出 response。

源码对应关系：

- `RoutingComponent::Init()` 从 component config 读 routing topic，创建 response writer 和 response history writer。
- history timer 会周期性把最近一次 `response_` 写到 history topic，这解释了为什么有时日志里看到 routing response 被重复 materialize。
- `RoutingComponent::Proc()` 本身不做路线搜索，它只负责调用 `routing_.Process()`，然后 `FillHeader()` 和 `Write()`。

这意味着：如果 `/apollo/routing_response` 没有消息，先看 RoutingComponent 是否收到 request；如果收到 request 但 response 为空，再进 `Routing::Process()` / HDMap / routing graph 查。

当前 testbed 里 bridge 的自动 routing 相关配置会涉及三条命令路径：

- `/apollo/raw_routing_request`
- `/apollo/external_command/lane_follow`
- `/apollo/external_command/action`

当前常用配置里通常同时允许 `send_routing_request=true` 和 `send_lane_follow=true`。所以排障时不要只看一个 channel，要同时看：

- routing request 是否发出；
- lane-follow command 是否发出；
- routing response 是否回来；
- planning 是否收到 planning command / lane-follow command；
- routing response 里 road / passage / segment 是否非空。

#### Routing::Process 做什么

```text
RoutingRequest waypoints
  -> 如果 waypoint 缺 lane id，就用 HDMap 投影到附近 lane
  -> 对重叠 lane 生成多个候选 request
  -> 在 routing_map.bin 拓扑图上搜索 route
  -> 选择长度最短的可行 response
```

官方源码里的关键逻辑是 `Routing::FillLaneInfoIfMissing()`：如果 request 里的 waypoint 只有 x/y/z pose、没有 lane id，routing 会在 HDMap 里找附近 lane，把 waypoint 补成 `lane_id + s`。它会先在小半径附近找 lane，找不到再逐步扩大半径；如果附近有多条 lane，还会生成多个候选 request。

然后 `Routing::Process()` 对这些候选 request 调 `Navigator::SearchRoute()`。谁能搜出 route，并且 routing length 最短，谁就是最终 response。

源码里的关键细节：

- `Routing::Init()` 读 `hdmap::RoutingMapFile()`，也就是当前 map 目录下的 `routing_map.bin`，然后拿 `HDMapUtil::BaseMapPtr()` 加载 base map。
- `FillLaneInfoIfMissing()` 只在 waypoint 没有 lane id 时补 lane。它会从半径 `0.3m` 开始找 lane，最多扩大 20 次；找到 lane 后用 `lane->GetProjection(x, y)` 得到 `s/l`，把 waypoint 填成 `lane_id + s`。
- 如果同一个点附近有多条 lane，它会生成多个 fixed request，而不是只认第一条 lane。
- `Process()` 对每个 fixed request 调 `Navigator::SearchRoute()`，最后选择 `measurement.distance()` 最短的那个 response。

这解释了我们自定义地图里“CARLA 在右侧、Apollo 在左侧”那类问题：routing 可能仍然成功，因为目标点能投影到某条 Apollo lane；但如果 CARLA 坐标到 Apollo HDMap 的 Y 轴/左右车道没有对齐，routing 可能投到视觉上另一条 lane 或反向 lane。此时 `routing_success_count > 0` 不能证明定位和 reference line 已对齐，必须继续看 `nearest lane id`、`s/l`、ego heading 与 lane heading。

这对我们很重要：

- routing success 主要证明“目标点能投影到 Apollo HDMap lane，并且 routing topology 连得上”；
- 它不证明 ego 当前 heading 和 route 起点对齐；
- 它不证明 planning 能生成健康 reference line；
- 它不证明 curve 上 matched point / target point 语义健康。

#### Routing 当前最常见失败/误读

- `routing_success_count > 0` 只说明 route materialized；
- 它不等于 planning 已经健康；
- 它也不等于车一定会按 route 正常行驶。

如果 routing 成功但 planning 仍然出现：

- `Reference lane is empty`
- `planning_command not ready`
- `reference_line_count = 0`
- `route_segment_count > 0 but reference_line_missing`

那问题更可能在 planning reference-line / route projection / ego pose alignment，而不是 routing 本身“完全没工作”。

典型证据：

- `routing_event_debug.jsonl`
- `routing_success_count`
- `routing_response_count`
- `goal_validity_debug.jsonl`
- `planning_route_debug.jsonl` 或 `planning_topic_debug.jsonl` 中的 `routing_lane_window_signature`

对当前自定义地图，routing 排障优先看：

1. request waypoint 是否带 `lane_id/s`，还是只带 `x/y/z`。
2. Apollo HDMap 中投影出的 lane id 是否与 CARLA ego 所在道路一致。
3. `s` 是否沿车辆前进方向增加。
4. ego heading 与 lane heading 差是否接近 0，而不是接近 `pi`。
5. routing response 的 `road/passage/segment` 是否覆盖前方 300m follow-stop 目标段。

### 7. Planning: 生成 Apollo 轨迹

Planning 是当前最核心的真实 Apollo 模块之一。

源码层面可以把它看成：

```text
PlanningComponent::Proc(prediction, chassis, localization)
  -> 收集最新 planning command / traffic light / relative map
  -> CheckInput()
  -> OnLanePlanning::RunOnce()
  -> PublicRoadPlanner::Plan()
  -> Scenario::Process()
  -> ADCTrajectory
  -> /apollo/planning
```

#### PlanningComponent 做什么

```text
prediction + chassis + localization 触发一次 planning cycle
latest planning_command / traffic_light 被缓存进 LocalView
CheckInput()
planning_base_->RunOnce(local_view_, &adc_trajectory_pb)
planning_writer_->Write(/apollo/planning)
```

Apollo 10.0 的 planning component 不是简单“收到 routing response 就直接规划”。源码里 `PlanningComponent::Proc()` 的主触发输入是：

- `PredictionObstacles`
- `Chassis`
- `LocalizationEstimate`

它还会通过 reader 缓存其他输入，例如：

- `PlanningCommand`
- `TrafficLightDetection`
- `PadMessage`
- `Stories`
- navigation mode 下的 `MapMsg`

当前本地配置 `--use_navigation_mode=false`，所以会走 `OnLanePlanning`，不是 `NaviPlanning`。

源码里有两个很容易误读的点：

- `PlanningComponent::CheckInput()` 在非 navigation mode 下要求 `local_view_.planning_command` 有 header。也就是说，planning 不是只靠 `/apollo/routing_response` 自动开始；当前 Apollo 10 的 lane-follow command / planning command 链路必须成立。
- `PlanningComponent::Proc()` 会先运行 `CheckRerouting()`，再把 prediction/chassis/localization 和缓存的 planning command/traffic light 组装进 `LocalView`。所以缺 prediction、chassis、localization、planning command 任意一项，都可能表现为 planning empty 或 not ready。

当前 testbed 里要同时确认两条命令链：

```text
raw routing request -> /apollo/routing_response
lane-follow command -> planning_command -> OnLanePlanning
```

这也是为什么只看到 routing success 不够。Planning 真正消费的是 `PlanningCommand` 里的 `lane_follow_command`，而 `LaneFollowMap::IsValid()` 还要求这个 command 里有 road、routing_request，并且 routing_request 至少有两个 waypoint，且 waypoint 都有 `lane_id/s`。

#### OnLanePlanning / PncMap 做什么

`OnLanePlanning` 的核心工作是把当前位置、planning command、HDMap、routing/lane-follow 信息变成 reference line 和 frame。你可以把它理解成 planning 的“准备工作”：

```text
ego localization + HDMap + route command
  -> vehicle state
  -> reference line provider / PncMap / LaneFollowMap
  -> Frame
  -> planner
```

源码调用链更具体是：

```text
OnLanePlanning::RunOnce()
  -> VehicleStateProvider::Update(localization, chassis)
  -> ReferenceLineProvider::UpdateVehicleState(vehicle_state)
  -> 如果 planning command 变了:
       ReferenceLineProvider::Reset()
       ReferenceLineProvider::UpdatePlanningCommand(command)
       planner_->Reset()
  -> TrajectoryStitcher::ComputeStitchingTrajectory()
  -> InitFrame()
       ReferenceLineProvider::GetReferenceLines()
       ReferenceLine::Segment(ego, look_backward, look_forward)
       RouteSegments::Shrink(ego, look_backward, look_forward)
       Frame::Init(reference_lines, segments, future_waypoints)
  -> traffic_decider_.Execute()
  -> planner_->Plan()
```

这里每一步都可能让 planning 变成空轨迹或 fallback：

- `VehicleStateProvider::Update()` 失败：localization/chassis 坐标、朝向、时间戳不可信。
- `ReferenceLineProvider::UpdatePlanningCommand()` 失败：lane-follow command 不完整，或者 `LaneFollowMap` 无法处理。
- `LaneFollowMap::GetNearestPointFromRouting()` 失败：ego 投影不到当前 routing lanes，典型原因就是 map 坐标/左右车道/heading 不一致。
- `ReferenceLineProvider::GetReferenceLines()` 失败：route segments 生成不了 reference line。
- `ReferenceLine::Segment()` 或 `RouteSegments::Shrink()` 失败：ego 附近的 reference line 不能裁剪出可用窗口。
- `traffic_decider` 或 stage task 失败：reference line 被标成不可驾驶，或 path/speed 任务失败。

如果这里失败，常见表现就是：

- reference lane empty；
- reference line not ready；
- planning 有消息但 trajectory point 为 0；
- route segment 有了，但 reference line 没有；
- route/debug 里 lane window 不稳定。

这正是 curve217 / curve213 诊断里要盯的层：它介于 routing 和最终 control 之间，是“route/reference-line/lateral semantics”的核心位置。

#### LaneFollowMap 真正干什么

`LaneFollowMap` 是当前 lane-follow reference line 的核心地图语义层。它不是 routing，也不是 final planner，而是把 routing response 转成 planning 可用的 route segments。

源码逻辑可以拆成：

```text
PlanningCommand.lane_follow_command
  -> LaneFollowMap::UpdatePlanningCommand()
       road/passage/segment -> route_indices_
       routing_request.waypoint -> routing_waypoint_index_
  -> LaneFollowMap::UpdateVehicleState()
       GetNearestPointFromRouting(ego x/y/heading)
       adc_route_index_ / next_routing_waypoint_index_
  -> LaneFollowMap::GetRouteSegments()
       取 ego 附近 backward/forward route window
       生成 RouteSegments
  -> ReferenceLineProvider
       RouteSegments -> ReferenceLine
```

`GetNearestPointFromRouting()` 是当前曲线/左右车道问题最关键的源码点之一。它只在 routing response 的 lane 集合里找当前 ego 的最近点，并且会过滤：

- lane 必须在当前 routing range；
- 如果上一帧 route segment cache 存在，lane 还必须在 cache 里；
- ego 点必须能投影到 lane 的 `s/l`；
- `s` 不能明显超出 lane 长度；
- ego heading 和 lane heading 差不能太大；
- 最终选择 `abs(l)` 最小的 lane waypoint。

所以如果 CARLA ego 视觉上在右侧车道，而 Apollo HDMap 的对应 lane 在左侧，`GetNearestPointFromRouting()` 可能会选错 lane、找不到 lane，或者给出很大的 `l`。这会直接污染 reference line，后续 planning/control 看起来像“方向盘乱打”，但根因可能是 map/route/pose semantics。

本地源码里还可以看到一些 `Stage6` debug 逻辑，例如 `stage6_lane_follow_map_cache_transition_debug.jsonl`。这些是为了定位 lane-follow cache、nearest-point 和 route-segment 状态而加的诊断，不是 Apollo 官方算法能力本身。读结果时要把“官方 planning 逻辑”和“我们加的诊断/guard 逻辑”分开。

#### PublicRoadPlanner 做什么

本地 planning log 显示当前创建的是：

```text
LaneFollowMap
PublicRoadPlanner
LaneFollowScenario
LaneFollowStage
```

对应的 overlay 也确认：

```text
public_road_planner_config.pb.txt:
  scenario {
    name: "LANE_FOLLOW"
    type: "LaneFollowScenario"
  }
```

官方源码里的 `PublicRoadPlanner::Plan()` 很薄：它先让 `ScenarioManager` 根据当前 frame 更新 scenario，然后调用当前 scenario 的 `Process()`。也就是说，真正的规划细节主要藏在：

- scenario；
- stage；
- tasks；
- traffic rules；
- reference line / path / speed deciders。

当前配置只显式启用 `LANE_FOLLOW` scenario，所以现在的主线应理解成“lane-follow public road planner”，不是完整自然驾驶行为集合。

#### LaneFollowScenario / tasks 大致做什么

在 lane-follow 场景下，Apollo 会围绕 reference line 生成路径和速度，再组合成最终轨迹。可以粗略理解为：

```text
reference line
  -> path decision / path bounds / path optimization
  -> speed bounds / speed decision / speed optimization
  -> trajectory stitching / smoothing
  -> ADCTrajectory
```

当前本地 planning flags 里比较重要的项：

- `--enable_reference_line_stitching=false`
- `--enable_trajectory_stitcher=false`
- `--default_cruise_speed=22.220`
- `--planning_upper_speed_limit=20.00`
- `--enable_smoother_failsafe`
- `--use_iterative_anchoring_smoother`
- `--nouse_s_curve_speed_smooth`

这些不是“修 curve 的证据”，只是说明当前 planning 的 path/speed/trajectory 行为会受 reference line stitching、trajectory stitching、smoother 和速度上限影响。

`LaneFollowStage::Process()` 会遍历 frame 里的 reference lines，只保留一个可驾驶 reference line。对每条 reference line，它调用 `PlanOnReferenceLine()`，然后按 pipeline 逐个执行 task。当前 lane-follow pipeline 源码配置为：

```text
LANE_CHANGE_PATH
LANE_FOLLOW_PATH
LANE_BORROW_PATH
FALLBACK_PATH
PATH_DECIDER
RULE_BASED_STOP_DECIDER
SPEED_BOUNDS_PRIORI_DECIDER
SPEED_HEURISTIC_OPTIMIZER
SPEED_DECIDER
SPEED_BOUNDS_FINAL_DECIDER
PIECEWISE_JERK_SPEED
```

可以按“路径”和“速度”两半理解：

- 路径侧：lane change / lane follow / lane borrow / fallback path 负责生成或选择可行 path。
- 决策侧：`PATH_DECIDER`、`RULE_BASED_STOP_DECIDER` 把障碍物、目的地等转成 path/speed 上的约束或 stop decision。
- 速度侧：speed bounds、heuristic optimizer、speed decider、piecewise jerk speed 把前车/停止点/限速等变成速度曲线。
- 最后 `CombinePathAndSpeedProfile()` 把 path 和 speed 合成 `ADCTrajectory`。

这对 follow-stop 很关键：静止前车本质上应该先作为 GT obstacle 进入 prediction/planning，然后在 speed/path decision 里形成 stop/follow 约束，再由 control 跟踪减速。若车子油门刹车异常，要分开看：

1. planning 轨迹里目标速度/停止点是否合理；
2. control 是否根据轨迹输出合理 throttle/brake；
3. bridge 是否把 Apollo throttle/brake 正确映射到 CARLA；
4. CARLA 车辆实际响应是否符合命令。

#### Traffic rule 当前状态

当前 overlay 的 `traffic_rule_config.pb.txt` 只有：

```text
BACKSIDE_VEHICLE
REROUTING
```

而 `traffic_rules/traffic_light/conf/default_conf.pb.txt` 虽然存在，但当前是：

```text
enabled: false
```

所以，现阶段不能说 traffic light behavior 已经由 Apollo planning 真实处理。要让红灯停、绿灯走成立，至少还要证明：

- Apollo HDMap 有正确 signal / stop line / overlap；
- CARLA traffic light id 能映射到 Apollo signal id；
- `/apollo/perception/traffic_light` 持续发布且 timestamp/sequence 健康；
- planning traffic-light rule 已进入 rule list 且 enabled；
- `natural_driving_report.json` 里红灯停车、绿灯通过、红转绿释放指标通过。

#### Planning 输出怎么看

Planning 的输出是 `/apollo/planning` 上的 `ADCTrajectory`。对当前项目来说，不能只看“channel 有消息”。至少要看：

- `trajectory_point_count` 是否大于 0；
- `reference_line_count` 是否大于 0；
- `routing_segment_count` 是否大于 0；
- `trajectory_type` 是否长期 fallback；
- first trajectory point 的 `theta/kappa/v` 是否合理；
- route 上的 lateral error / heading error 是否随 curve 恶化；
- matched point / target point anomaly 是否集中在某个 `route_s`；
- traffic-light 场景中是否有合理停走行为。

典型证据：

- `planning_topic_debug.jsonl`
- `planning_topic_debug_summary.json`
- `route_health.json`
- `curve_segments.csv`
- `apollo_shadow_mode_report.json`

### 8. Control: 从轨迹变成油门/刹车/方向盘

Control 也是真 Apollo 模块。

输入：

```text
/apollo/planning
/apollo/localization/pose
/apollo/canbus/chassis
```

输出：

```text
/apollo/control
```

这个输出还不是 CARLA 车辆控制。它只是 Apollo control command。

源码/运行层面可以把 control 理解成：

```text
/apollo/planning + localization + chassis
  -> ControlComponent
  -> CheckInput() / CheckTimestamp()
  -> lateral controller
  -> longitudinal controller
  -> /apollo/control
```

本地 control log 显示当前加载的是：

- LQR-based lateral controller；
- PID-based longitudinal controller；
- calibration table；
- vehicle param 里的 Lincoln MKZ 几何和转向参数。

源码对应关系：

- `ControlComponent::Init()` 加载 `modules/control/control_component/conf/pipeline.pb.txt`。
- 当前 pipeline 只有两个 controller：`LAT_CONTROLLER`=`LatController`，`LON_CONTROLLER`=`LonController`。
- `ControlComponent::Proc()` 每个 control cycle 拉取最新 chassis、planning trajectory、planning command status、localization、pad message。
- 若 chassis 不是 `COMPLETE_AUTO_DRIVE`，它会走 reset/zero control。
- 若 planning trajectory 为空，源码里会输出 safe hold：throttle=0、steering=0、brake=`soft_estop_brake`。
- 若 planning trajectory 非空，才进入 `ProduceControlCommand()`，再由 `control_task_agent_.ComputeControlCommand()` 调 lateral / longitudinal controller。

当前 `control.conf` 里几个会影响解释的事实：

- `--control_period=0.01`，control 目标周期约 100 Hz。
- `--enable_input_timestamp_check=false`，当前不会因为 timestamp check 直接挡住控制；但这不代表 timestamp 不重要，仍要用 channel health/latency artifact 看。
- `--soft_estop_brake=15.0`，planning 空轨迹或 estop 时会强制刹车。
- `--action=1`，默认 driving action 是 START/进入自动驾驶语义。
- `--use_control_submodules=false`，当前走传统 `ControlComponent -> control_task_agent -> controllers` 路径，而不是 control local-view submodule 输出。

所以当前横向控制不只是 bridge 的方向盘透传。Apollo control 自己会基于 planning trajectory、车辆状态、车辆参数算 steering / throttle / brake。bridge 后面再把 Apollo 的 command 映射到 CARLA。

横向 controller 的核心证据在 `simple_lat_debug`：它会围绕 planning trajectory 计算 lateral error、heading error、preview/matched trajectory point，然后由 LQR/gain scheduler 生成 steering。曲线段如果出现 `matched_point_too_large`、first_high_steer 或 target point jump，优先看 planning trajectory / reference line / matched point，再看 bridge 映射。

纵向 controller 的核心证据在 `simple_lon_debug`：它比较当前 station/speed 与 planning 轨迹上的目标 station/speed，PID 输出 throttle/brake。follow-stop 中如果“油门刹车不正常”，不要先猜 Apollo control 坏了，而是按四层拆：

```text
planning trajectory target v/a/stop point
  -> Apollo ControlCommand throttle/brake
  -> bridge mapped throttle/brake
  -> CARLA applied throttle/brake and speed response
```

如果 planning 轨迹本身没有停止点，control 正常也不会停；如果 Apollo control 有合理 brake 但 CARLA applied brake 没有，问题在 bridge mapping；如果 applied brake 有但车不停，才看 CARLA physics/actuation。

要判断 control 是否可用，需要分开看两件事：

1. Apollo 有没有输出合理 control；
2. bridge 有没有把 control 正确映射到 CARLA。

典型证据：

- `apollo_control_raw.jsonl`
- `control_handoff_summary.json`
- `cyber_bridge_stats.json`
- `direct_bridge_control_apply.jsonl`
- `debug_timeseries.csv` 里的 raw/mapped/applied throttle/brake/steer

### 9. Control Mapping / Actuation: Apollo 命令如何真正作用到 CARLA

这层不是 Apollo 算法本体，而是 testbed bridge。

逻辑是：

```text
Apollo throttle / brake / steer
  -> bridge mapping
  -> CARLA VehicleControl
  -> ego vehicle movement
```

当前要特别注意：

- `steer_scale = 0.25` 仍是 active confounder；
- `actuator_mapping_mode = legacy` 仍在使用；
- 不要直接改 `steer_scale`；
- 不要默认启用 physical mapping；
- calibration 只能作为 control-actuation evidence，不能自动 promotion；任何
  calibration promotion 都必须同时引用 `calibration_report.json` 和
  097/217/031 no-regression gates。

典型证据：

- `direct_bridge_control_apply.jsonl`
- `direct_bridge_stats.json`
- `carla_vehicle_response.csv`
- `calibration_report.json`

### 10. CyberRT: Apollo 的消息交通系统

CyberRT 负责 Apollo channel 的发布、订阅和模块通信。

当前我们关心：

- channel 是否存在；
- message count 是否合理；
- hz 是否合理；
- timestamp 是否单调；
- message gap 是否过大；
- planning/control/localization/chassis 是否同时健康。

典型证据：

- `cyber_bridge_stats.json`
- `cyber_channel_list.txt`
- `apollo_channel_health_report.json`
- Apollo module logs

### 11. Dreamview: 好看的可视化，不是最终判据

Dreamview 是 Apollo 的 UI，可以看到：

- map；
- ego；
- reference line / routing；
- planning trajectory；
- module status；
- console message。

它很适合演示和人工检查。

但要注意：

- Dreamview ready 不等于 planning/control healthy；
- Dreamview 画面正常不等于 route-health pass；
- Dreamview 视频是展示证据，不是 capability promotion 的唯一证据。

当前录制 Dreamview 时，推荐用 Chrome CDP 方式抓页面帧，因为 X11 前台录屏可能被 welcome page、桌面焦点或 Chrome 弹窗污染。

相关工具：

- `tools/open_dreamview_chrome_auto.sh`
- `tools/dreamview_chrome_cdp_auto.py`
- `tools/capture_dreamview_cdp_frames.py`

## 当前 truth-input MVP 中哪些是真 Apollo，哪些不是

| 层 | 当前实现 |
| --- | --- |
| HDMap | Apollo runtime map，来自 Town01 map 或自定义地图转换。 |
| Localization | CARLA GT 替代，不是 Apollo 原生定位。 |
| Chassis | CARLA GT 替代，不是真车 canbus。 |
| Obstacles | CARLA actors GT 替代，不是真 perception。 |
| Traffic light | CARLA GT + Apollo signal id mapping，仍需 contract 验证。 |
| Routing | Real Apollo routing。 |
| Planning | Real Apollo planning。 |
| Control | Real Apollo control。 |
| Control actuation | testbed bridge mapping 到 CARLA。 |
| CyberRT | Real Apollo CyberRT runtime。 |
| Dreamview | Real Apollo UI。 |

## 如何读一次 run 的证据

如果你只想快速判断一条 run 到哪一层了，可以按这个顺序看：

1. `summary.json`
   - 总体成功/失败、退出原因、帧数、安全事件。
2. `map_contract_guard.json`
   - bridge map 和 Apollo runtime map 是否一致。
3. `routing_event_debug.jsonl`
   - routing request 是否发出，routing 是否成功。
4. `planning_topic_debug_summary.json`
   - planning 是否有 non-empty trajectory。
5. `cyber_bridge_stats.json`
   - localization/chassis/planning/control channel 是否有消息。
6. `apollo_control_raw.jsonl`
   - Apollo control 是否输出油门/刹车/方向盘。
7. `direct_bridge_control_apply.jsonl`
   - CARLA 是否真的收到控制。
8. `timeseries.csv`
   - ego speed、lateral error、heading error、raw/mapped/applied control。
9. `route_health.json`
   - route geometry、curve segment、lateral semantics、missing fields。
10. Dreamview video / screenshot
   - 作为可视化辅助，不单独作为结论。

## 常见误读

不要这样解读：

- “routing success 了，所以 Apollo 会正常开。”
- “Dreamview 看到车和线了，所以自然驾驶已完成。”
- “CARLA 车动了，所以 curve tracking 健康。”
- “lateral guard 没触发，所以 bridge 完全无关。”
- “calibration 能直接修 curve。”
- “truth-input MVP 已经证明 Apollo 真实传感器感知和定位链路健康。”

更准确的说法是：

- routing success 只证明 routing materialized；
- planning non-empty 证明 planning 有轨迹输出，但还要看 route-health；
- control output 证明 Apollo control 有输出，但还要看 mapping 和 actuation；
- CARLA 车动证明闭环通了，但还要看 lateral/heading/safety/behavior；
- Dreamview 是展示和辅助诊断，不是最终验收。

## 和当前三条主线的关系

当前三条主线仍然是：

1. Town01 route-health / curve lateral semantics
   - 重点看 route geometry、heading、curvature、matched point、target point、lateral/heading error。
2. `carla_direct` long-window A/B
   - 判断绕过 ROS2 后 transport/materialization 是否更稳。
   - 不能把短窗口 positive 写成 curve solved。
3. control-actuation calibration
   - 解释 throttle/brake/steer response、latency、legacy `steer_scale=0.25`。
   - 不自动改主线配置，不默认启用 physical mapping。

## 相关文档

- `docs/apollo_mvp_bridge.md`
- `docs/apollo_algorithm_inventory.md`
- `docs/apollo_reproduction.md`
- `docs/apollo_town01_truth_natural_driving.md`
- `docs/town01_route_health.md`
- `docs/carla_direct_ab.md`
- `docs/calibration_pipeline.md`
