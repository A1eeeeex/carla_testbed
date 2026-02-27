#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/pb"
APOLLO_ROOT="${APOLLO_ROOT:-}"

if [[ -z "${APOLLO_ROOT}" ]]; then
  echo "[gen_pb2] APOLLO_ROOT is required"
  echo "example: export APOLLO_ROOT=/path/to/apollo"
  exit 2
fi

if [[ ! -d "${APOLLO_ROOT}" ]]; then
  echo "[gen_pb2] APOLLO_ROOT does not exist: ${APOLLO_ROOT}"
  exit 2
fi

PROTOC_CMD=()
if command -v protoc >/dev/null 2>&1; then
  PROTOC_CMD=(protoc)
else
  if python3 -m grpc_tools.protoc --version >/dev/null 2>&1; then
    PROTOC_CMD=(python3 -m grpc_tools.protoc)
  else
    echo "[gen_pb2] protoc is required but not found in PATH"
    echo "[gen_pb2] fallback also missing: python3 -m grpc_tools.protoc"
    echo "[gen_pb2] install one of:"
    echo "  - protobuf-compiler"
    echo "  - python package grpcio-tools"
    exit 2
  fi
fi

mkdir -p "${OUT_DIR}"
TMP_PROTO_LIST="$(mktemp)"

python3 - "${APOLLO_ROOT}" "${TMP_PROTO_LIST}" <<'PY'
import collections
import pathlib
import re
import sys

apollo_root = pathlib.Path(sys.argv[1]).resolve()
out_file = pathlib.Path(sys.argv[2]).resolve()

required = [
    "modules/common_msgs/basic_msgs/header.proto",
    "modules/common_msgs/localization_msgs/localization.proto",
    "modules/common_msgs/chassis_msgs/chassis.proto",
    "modules/common_msgs/perception_msgs/perception_obstacle.proto",
    "modules/common_msgs/control_msgs/control_cmd.proto",
    "modules/common_msgs/external_command_msgs/lane_follow_command.proto",
]
optional = [
    "modules/common_msgs/external_command_msgs/command_status.proto",
    "modules/common_msgs/external_command_msgs/action_command.proto",
    "modules/common_msgs/routing_msgs/routing.proto",
    "cyber/proto/role_attributes.proto",
]

missing_required = [p for p in required if not (apollo_root / p).exists()]
if missing_required:
    print("[gen_pb2] missing required proto files:")
    for rel in missing_required:
        print(f"  - {rel}")
    sys.exit(2)

seeds = required + [p for p in optional if (apollo_root / p).exists()]
queue = collections.deque(seeds)
seen = set()
import_re = re.compile(r'^\s*import\s+"([^"]+)";', re.MULTILINE)

while queue:
    rel = queue.popleft()
    if rel in seen:
        continue
    proto_path = apollo_root / rel
    if not proto_path.exists():
        # Imported but missing in local tree: skip with warning.
        print(f"[gen_pb2] warning: imported proto not found, skip: {rel}")
        continue
    seen.add(rel)
    text = proto_path.read_text(encoding="utf-8", errors="ignore")
    for dep in import_re.findall(text):
        if dep.startswith("google/protobuf/"):
            continue
        if dep not in seen:
            queue.append(dep)

out_file.write_text("\n".join(sorted(seen)) + "\n", encoding="utf-8")
print(f"[gen_pb2] proto closure size: {len(seen)}")
PY

if [[ ! -s "${TMP_PROTO_LIST}" ]]; then
  echo "[gen_pb2] empty proto list; abort"
  rm -f "${TMP_PROTO_LIST}"
  exit 2
fi

echo "[gen_pb2] generating python pb2 into ${OUT_DIR}"
mapfile -t PROTOS < "${TMP_PROTO_LIST}"
"${PROTOC_CMD[@]}" -I"${APOLLO_ROOT}" --python_out="${OUT_DIR}" "${PROTOS[@]}"

# Ensure importable regular packages for py versions/environments without namespace package support.
find "${OUT_DIR}" -type d -print0 | while IFS= read -r -d '' d; do
  touch "${d}/__init__.py"
done

echo "[gen_pb2] done"
rm -f "${TMP_PROTO_LIST}"
