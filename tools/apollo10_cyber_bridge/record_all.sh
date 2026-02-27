#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="${1:-runs/latest_run}"
DURATION_SEC="${2:-0}"
APOLLO_ROOT="${APOLLO_ROOT:-}"

if [[ -n "${APOLLO_ROOT}" && -f "${APOLLO_ROOT}/cyber/setup.bash" ]]; then
  # shellcheck disable=SC1090
  source "${APOLLO_ROOT}/cyber/setup.bash"
fi

OUT_DIR="${RUN_DIR}/artifacts"
mkdir -p "${OUT_DIR}"
OUT_PREFIX="${OUT_DIR}/apollo.record"

CMD=(cyber_recorder record -a -o "${OUT_PREFIX}")
echo "[record_all] ${CMD[*]}"

if [[ "${DURATION_SEC}" != "0" ]]; then
  timeout "${DURATION_SEC}" "${CMD[@]}"
else
  "${CMD[@]}"
fi
