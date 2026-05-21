#!/usr/bin/env bash
set -euo pipefail

# 打包当前工作区（含未提交改动），但严格遵循 git / .gitignore 的可见文件集合。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR_INPUT="${1:-${REPO_ROOT}/dist}"
mkdir -p "${OUT_DIR_INPUT}"
OUT_DIR="$(cd "${OUT_DIR_INPUT}" && pwd)"
NAME="carla_testbed_ws_$(date +%Y%m%d_%H%M%S)"
OUT_BASE="${OUT_DIR}/${NAME}.tar.gz"

TMP_FILE_LIST="$(mktemp)"
TMP_EXISTING_FILE_LIST="$(mktemp)"
cleanup() {
  rm -f "${TMP_FILE_LIST}"
  rm -f "${TMP_EXISTING_FILE_LIST}"
}
trap cleanup EXIT

cd "${REPO_ROOT}"

# `--exclude-standard` 会同时应用 .gitignore、.git/info/exclude 和全局 ignore。
git ls-files --cached --others --exclude-standard -z > "${TMP_FILE_LIST}"

python3 - "${REPO_ROOT}" "${TMP_FILE_LIST}" "${TMP_EXISTING_FILE_LIST}" <<'PY'
from pathlib import Path
import subprocess
import sys

repo_root = Path(sys.argv[1])
src = Path(sys.argv[2])
dst = Path(sys.argv[3])

def is_ignored(rel: str) -> bool:
    proc = subprocess.run(
        ["git", "check-ignore", "--no-index", rel],
        cwd=repo_root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return proc.returncode == 0

data = src.read_bytes().split(b"\0")
with dst.open("wb") as f:
    for raw in data:
        if not raw:
            continue
        rel = raw.decode("utf-8", errors="surrogateescape")
        if (repo_root / rel).exists() and not is_ignored(rel):
            f.write(raw + b"\0")
PY

if [ ! -s "${TMP_EXISTING_FILE_LIST}" ]; then
  echo "no packable files found under git visibility rules" >&2
  exit 1
fi

tar -C "${REPO_ROOT}" --null -czf "${OUT_BASE}" -T "${TMP_EXISTING_FILE_LIST}"
sha256sum "${OUT_BASE}" > "${OUT_BASE}.sha256"
echo "${OUT_BASE}"
