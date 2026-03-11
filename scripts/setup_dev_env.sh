#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "[setup] repo: $REPO_ROOT"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "[setup] created .env from .env.example (please edit paths)"
else
  echo "[setup] keep existing .env"
fi

if [[ ! -f configs/local.yaml ]]; then
  cp configs/local.example.yaml configs/local.yaml
  echo "[setup] created configs/local.yaml from configs/local.example.yaml (please edit paths)"
else
  echo "[setup] keep existing configs/local.yaml"
fi

if [[ "${1:-}" == "--bootstrap" ]]; then
  echo "[setup] running tools/bootstrap_native.sh"
  bash tools/bootstrap_native.sh
fi

echo "[setup] checking local config"
python3 scripts/check_local_config.py || true

cat <<'EOF'
[setup] done
next:
  1) edit .env
  2) edit configs/local.yaml
  3) rerun: python3 scripts/check_local_config.py --strict
EOF
