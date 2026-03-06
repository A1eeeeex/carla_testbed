#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DEFAULT="${ROOT}/dist/carla_testbed_migration_${TS}.tar.gz"
OUT_PATH="${1:-${OUT_DEFAULT}}"

mkdir -p "$(dirname "${OUT_PATH}")"

TMP_LIST="$(mktemp)"
trap 'rm -f "${TMP_LIST}"' EXIT

git -C "${ROOT}" ls-files -z > "${TMP_LIST}"

tar -C "${ROOT}" --null --files-from="${TMP_LIST}" -czf "${OUT_PATH}"

echo "[migration] bundle created: ${OUT_PATH}"
