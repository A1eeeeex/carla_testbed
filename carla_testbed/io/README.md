# 模块定位
负责 CARLA 仿真与外部系统（ROS2 等）间的在线数据桥接与消息构建。提供 `TransportAdapter` 实现，将内部帧包(FramePacket)发布为标准 ROS2 消息，不处理离线 bag。

# 目录与关键文件
- `ros2_adapter.py`：`ROS2BridgeAdapter`，按 contract 自动创建 ROS2 发布器、发布 clock/tf_static/传感器数据。
- `ros2_msg_builders.py`：把内部样本转换为 ROS2 `Image/PointCloud2/Imu/NavSatFix/TF/Clock` 消息。
- `ros2_qos.py`：解析 QoS 契约映射到 `rclpy.qos.QoSProfile`。
- `__init__.py`：导出适配器。

# 对外接口（Public API）
- `ROS2BridgeAdapter(contract_path, calib_path, time_sync_path, node_name="carla_testbed_bridge")`：
  - `start()/stop()`：初始化/关闭 ROS2 节点与发布器。
  - `publish_frame(frame_packet)`：发布 camera/lidar/imu/gnss/radar(占位)/tf_static/clock。
  - `publish_truth(truth_packet)`：发布事件等（可选）。
  - `spin_once(timeout_sec=0.0)`：rclpy 非阻塞 spin。
- `build_*_msg(...)`（ros2_msg_builders.py）：纯函数生成对应 ROS2 消息。
- `qos_from_contract(entry)`（ros2_qos.py）：由契约 dict 生成 QoSProfile。

# 数据契约（I/O Contract）
- 输入：`FramePacket`（见 schemas）+ calibration.json + time_sync.json + io_contract_ros2.yaml。
- 输出：ROS2 话题：
  - `/clock` (`rosgraph_msgs/Clock`，sim_time)
  - `/tf_static` (`tf2_msgs/TFMessage`，base_link->sensor 静态外参)
  - 传感器：如 `/sim/ego/camera/front/image_raw` (`sensor_msgs/Image`)，`/sim/ego/lidar/top/points` (`PointCloud2`)，`/sim/ego/imu` (`Imu`)，`/sim/ego/gnss` (`NavSatFix`)，事件 topic 视契约而定。
- QoS：由 io_contract_ros2.yaml 定义，默认 camera/lidar best_effort，tf_static transient_local 等。

# 用法（How to Use）
- 在跟停示例中启用 ROS2 桥：
```bash
python examples/run_followstop.py --rig fullstack --enable-ros2-bridge --ros2-contract runs/<run>/config/io_contract_ros2.yaml
```
- 单独测试适配器（伪代码）：
```python
from carla_testbed.io import ROS2BridgeAdapter
adapter = ROS2BridgeAdapter("runs/<run>/config/io_contract_ros2.yaml", "runs/<run>/config/calibration.json", "runs/<run>/config/time_sync.json")
adapter.start()
adapter.publish_frame(frame_packet)  # 由 runner 每 tick 调用
adapter.spin_once(0.0)
adapter.stop()
```

# 配置（Config）
- CLI：`--enable-ros2-bridge`、`--ros2-contract`（示例脚本）。
- 文件：`runs/<run>/config/io_contract_ros2.yaml`、`calibration.json`、`time_sync.json`。
- 环境：需要事先 `source` ROS2 环境，`use_sim_time` 需在订阅端设为 true。

# 常见问题与排错
- 无数据/节点不存在：确认已 `source /opt/ros/.../setup.bash`，并启用 `--enable-ros2-bridge`。
- tf_static 不显示：检查 calibration.json 是否包含 base_link->sensor；QoS 是否 transient_local。
- 时间戳错乱：确保 sim_time 使用 snapshot.timestamp.elapsed_seconds；time_sync.json 正确生成。
- PointCloud2 报字段错误：检查 io_contract_ros2.yaml 中字段定义与 builder（x/y/z/intensity）一致。
- Radar 无映射：当前为占位，代码中已注明 mapping 未完成。

# 与其他模块的关系
- 上游：runner 将 FramePacket/GroundTruthPacket 交给适配器；config 提供 contract/calibration/time_sync。
- 下游：外部 ROS2 算法/rviz/rosbag。
- 调用路径：examples/run_followstop.py -> TestHarness -> ROS2BridgeAdapter.publish_frame/publish_truth。

# Roadmap
- 补齐雷达消息映射与订阅回传（轨迹/控制）。
- 增加 QoS/话题前缀可配置化。
- 提供自动 rosbag2 录制辅助脚本。 
