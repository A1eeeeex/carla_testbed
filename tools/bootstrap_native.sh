#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -U pip

if [ -f pyproject.toml ]; then
  pip install -e ".[dev]"
elif [ -f requirements.txt ]; then
  pip install -r requirements.txt
fi

# Install CARLA wheel if available
if [ -n "${CARLA_ROOT:-}" ] && [ -d "${CARLA_ROOT}/PythonAPI/carla/dist" ]; then
  wheel=$(ls "${CARLA_ROOT}"/PythonAPI/carla/dist/carla-*.whl 2>/dev/null | sort | tail -n1 || true)
  if [ -n "$wheel" ]; then
    pip install -U "$wheel"
  fi
fi

python -m carla_testbed doctor
