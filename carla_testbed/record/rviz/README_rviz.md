## RViz for CARLA ROS2 Native

- 入口：`RvizLauncher`，由主线 `examples/run_followstop.py` 触发（`--enable-ros2-native --enable-rviz`）。
- 生成的 RViz 配置：`record/rviz/rviz/native_<rig>.rviz`，由 `gen_rviz_from_rig.py` 根据 rig 选择首个启用的 camera/lidar，话题格式 `/carla/<ego>/<sensor>/<suffix>`。
- Docker：`record/rviz/docker/Dockerfile` 基于 `osrf/ros:humble-desktop`，运行时通过 `docker run --net=host` 启动 rviz2，并挂载 `config/fastrtps-profile.xml` 以稳定 DDS。
- Local：`--rviz-mode local` 可直接使用宿主机 `rviz2`（无需开新终端），同样加载生成的 .rviz；需要 `DISPLAY` 和 rviz2 可执行。
- X11 依赖：需要 `DISPLAY` + X11（非 Wayland）；无法满足时不会阻断主线，仅提示跳过 RViz（可自行用本机 rviz2 手动打开生成的 .rviz）。
