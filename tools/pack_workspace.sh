#!/usr/bin/env bash
set -euo pipefail

# 打包当前工作区（含未提交改动），排除与你的 .gitignore 一致的产物
OUT_DIR="${1:-../dist}"
mkdir -p "$OUT_DIR"
NAME="carla_testbed_ws_$(date +%Y%m%d_%H%M%S)"

tar -czf "${OUT_DIR}/${NAME}.tar.gz" \
  --exclude-vcs \
  --exclude='./.git' \
  --exclude='./.venv' \
  --exclude='./__pycache__' \
  --exclude='./**/__pycache__' \
  --exclude='./*.pyc' \
  --exclude='./runs' \
  --exclude='./logs' \
  --exclude='./*.mp4' \
  --exclude='./*.png' \
  --exclude='./*.pcd' \
  --exclude='./*.bag' \
  --exclude='./carla_testbed_src.tar.gz' \
  --exclude='./.env' \
  --exclude='./configs/local.yaml' \
  --exclude='./configs/local.*.yaml' \
  --exclude='./examples/carla_examples' \
  .

sha256sum "${OUT_DIR}/${NAME}.tar.gz" > "${OUT_DIR}/${NAME}.tar.gz.sha256"
echo "${OUT_DIR}/${NAME}.tar.gz"
