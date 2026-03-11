# configs/

运行配置根目录，包含 profile 与传感器 rig。

子目录：

- `io/`：端到端运行 profile（算法栈、桥接、routing、录制等）。
- `rigs/`：传感器 rig 预设。

推荐使用流程：

1. 从 `configs/io/examples/*.yaml` 选择基线 profile。
2. 使用 `--override key=value` 做本轮实验覆盖。
3. 机器私有配置放在 `.env` 或 `configs/local*.yaml`（已被 Git 忽略）。

配置加载层级（从低到高）：

1. `configs/project.yaml`（项目默认，进入 Git）
2. 通过 `--config` 指定的 profile
3. `configs/local.yaml`（可选，本机私有）
4. `configs/local.*.yaml`（可选，本机私有，按文件名字典序叠加）
5. CLI `--override key=value`（最高优先级）

说明：

- `configs/local.example.yaml` 仅为样板，不会被自动加载。
- `paths.*` 支持每台机器各自配置 CARLA/Apollo/map/output 等根路径。
