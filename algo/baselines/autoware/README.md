# Autoware Direct（Mode-2）

该模式下，Autoware 通过容器内的 `autoware_carla_interface` 与 CARLA 直接通信。

## 工作机制

- `io/backends/autoware_direct.py` 会基于选定 rig + contract，自动生成 `config/autoware_carla_interface` 下的 `sensor_mapping.yaml` 与 `sensor_kit_calibration.yaml`。
- `docker/compose.yaml` 使用 host 网络启动 Autoware，并在启动接口前加载 ROS2 + Autoware 环境。
- ROS2 侧 `/clock` 与感知话题由 Autoware 产生，控制结果由 Autoware 回写到 CARLA。

## 运行方式

```bash
python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml
```

查看日志：

```bash
docker compose -f algo/baselines/autoware/docker/compose.yaml logs -f
```

## 注意事项

- 映射和标定文件会在每次 `run.py` 执行时覆盖。
- 若你使用的是不同 Autoware 版本，请按需调整 `docker/compose.yaml` 中的镜像与启动命令。
- 消费 Autoware 话题的算法节点建议设置 `USE_SIM_TIME=true`。
