# carla_testbed/utils

通用运行工具模块，当前主要提供两类能力：

- 环境与本机配置加载（`env.py`）
- run 目录命名与 `runs/latest` 指针维护（`run_naming.py`）

## 关键文件

- `env.py`
  - `resolve_repo_root()`：定位仓库根目录
  - `load_project_config()`：加载 `configs/project.yaml`
  - `load_local_config()`：加载 `configs/local.yaml` + `configs/local.*.yaml`
  - `load_host_config()`：项目默认 + 本地覆盖后的合并结果
  - `resolve_carla_root()`：按优先级解析 CARLA 根目录
- `run_naming.py`
  - 生成规范 run 名称
  - 处理重名后缀
  - 更新 `runs/latest` 与 `runs/LATEST.txt`

## 配置优先级（与 `tbio/scripts/run.py` 一致）

1. `configs/project.yaml`
2. profile（`--config` 指定）
3. `configs/local.yaml`
4. `configs/local.*.yaml`（文件名字典序）
5. CLI `--override`

## 说明

- `configs/local.example.yaml` 仅作为样板，不会被自动加载。
- 机器私有路径（CARLA/Apollo/maps/output）应放在 local 层，不要写入 profile。
