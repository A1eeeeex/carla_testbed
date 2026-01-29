#!/usr/bin/env bash
set -euo pipefail

echo "[env] python: $(python --version 2>/dev/null || echo missing)"
echo "[env] docker: $(docker --version 2>/dev/null || echo missing)"
echo "[env] nvidia-smi:"
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi -L || true
else
  echo "  not found"
fi
if command -v ros2 >/dev/null 2>&1; then
  echo "[env] ROS2 version:"; ros2 --version || true
fi
