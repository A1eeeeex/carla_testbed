#!/usr/bin/env bash
set -euo pipefail

APOLLO_ROOT="${APOLLO_ROOT:-}"
if [[ -n "${APOLLO_ROOT}" && -f "${APOLLO_ROOT}/cyber/setup.bash" ]]; then
  # shellcheck disable=SC1090
  source "${APOLLO_ROOT}/cyber/setup.bash"
fi

if command -v cyber_monitor >/dev/null 2>&1; then
  exec cyber_monitor
fi

if command -v cyber_channel >/dev/null 2>&1; then
  echo "[monitor] cyber_monitor not found; fallback to channel list loop"
  while true; do
    date
    cyber_channel list || true
    sleep 2
  done
fi

echo "[monitor] neither cyber_monitor nor cyber_channel found in PATH"
exit 2
