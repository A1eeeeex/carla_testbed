# 双机并行开发工作流（CARLA/Apollo/ROS2）

本文目标：同一个人在两台 PC 上并行开发时，确保代码可合并、环境隔离、两边都可独立运行。

## 1. 分支策略（推荐）

- `main`：稳定分支，只接受“可运行、可回归”的变更。
- `dev`：集成分支，用于合并多个功能分支后统一验证。
- `feat/<topic>`：功能分支，每个任务一个分支。

为什么 `main` 要保持稳定：

- 双机切换时通常直接基于 `main` 拉最新，若 `main` 不稳定，两台机器都会被阻塞。
- 录制/复现实验通常以 `main` 为基线，稳定主干便于定位回归问题。

## 2. 双机轮流开发（同一功能）

### 机器 A 开发结束

```bash
git checkout feat/my-change
git status -sb
python3 scripts/check_local_config.py
git add -A
git commit -m "feat: <message>"
git push -u origin feat/my-change
```

### 机器 B 接续开发

```bash
git fetch origin
git checkout feat/my-change
git pull --rebase origin feat/my-change
python3 scripts/check_local_config.py
```

## 3. 双机并行开发（不同功能）

### 机器 A

```bash
git checkout -b feat/apollo-control-tune main
```

### 机器 B

```bash
git checkout -b feat/hud-refactor main
```

分别完成后，先合到 `dev`，再从 `dev` 合到 `main`：

```bash
git checkout dev
git pull --rebase origin dev
git merge --no-ff feat/apollo-control-tune
git merge --no-ff feat/hud-refactor
git push origin dev
```

验证通过后：

```bash
git checkout main
git pull --rebase origin main
git merge --no-ff dev
git push origin main
```

## 4. 切换机器前后的最小命令清单

### 离开当前机器前

```bash
git status -sb
git add -A
git commit -m "wip: <message>"   # 若不想 commit，可用 git stash push -u
git push
```

### 到另一台机器后

```bash
git fetch origin
git checkout <your-branch>
git pull --rebase
bash scripts/setup_dev_env.sh
python3 scripts/check_local_config.py --strict
```

## 5. 如何避免冲突

- 一次只改一个主题（控制、桥接、文档分开提交）。
- 配置实验优先用 `--override`，稳定后再写入 profile。
- 不在分支里混入 `runs/`、日志、录屏、临时脚本。
- 合并前先 `git pull --rebase`，减少 merge 嵌套提交。

## 6. 明确不能提交的内容

不要提交：

- `runs/`、`dumps/`、`logs/`、`artifacts/`、`outputs/`
- 录屏/截图（`*.mp4/*.png/*.jpg`）
- 感知与仿真大文件（`*.bag/*.pcd/*.db3/*.bin`）
- 模型与权重（`*.onnx/*.pt/*.pth`）
- 本机私有配置（`.env`、`configs/local.yaml`、`configs/local.*.yaml`）

提交前快速检查：

```bash
git status -sb
git ls-files | rg '__pycache__|\.pyc$|^runs/|^dumps/|\.mp4$|\.bag$|\.pcd$|\.onnx$|\.pt$|\.pth$'
```

## 7. 发生冲突时怎么处理

1. 同步远端并 rebase：

```bash
git fetch origin
git rebase origin/<your-branch>
```

2. 编辑冲突文件后继续：

```bash
git add <conflicted-file>
git rebase --continue
```

3. 若冲突过大，先中止并回到可运行状态：

```bash
git rebase --abort
```

建议：优先保留“行为已验证”的版本，再手工把另一边的增量迁入。

## 8. CARLA / Apollo / ROS2 专项注意事项

- 两台机器的 `CARLA_ROOT` / `APOLLO_ROOT` / 地图目录可不同，统一放在 `.env` 与 `configs/local*.yaml`。
- Apollo 地图、CARLA 二进制、Docker 容器状态都不进 Git，只提交配置键位和脚本。
- 录制与日志输出尽量落到 `paths.output_root` 指向的本机目录，避免仓库体积膨胀。
- 每台机器切换后先跑一轮最小验收：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_dummy.yaml \
  --override run.ticks=120
```

再跑 Apollo 闭环：

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt.yaml \
  --override run.ticks=300 \
  --override runtime.carla.start=true
```
