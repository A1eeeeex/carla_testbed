#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

find_latest_run() {
  local latest
  latest=$(find runs -mindepth 1 -maxdepth 1 -type d ! -name latest ! -name latest_run | xargs -r ls -td 2>/dev/null | head -n 1 || true)
  if [ -z "${latest:-}" ]; then
    echo "No run directory found under runs/" >&2
    return 1
  fi
  printf '%s\n' "$latest"
}

RUN_DIR="${1:-}"
OUT_DIR="${2:-dist}"

if [ -z "$RUN_DIR" ]; then
  RUN_DIR=$(find_latest_run)
fi

if [ ! -d "$RUN_DIR" ]; then
  echo "Run directory not found: $RUN_DIR" >&2
  exit 1
fi

RUN_DIR=$(python3 - <<'PY' "$RUN_DIR"
from pathlib import Path
import sys
print(Path(sys.argv[1]).resolve())
PY
)

mkdir -p "$OUT_DIR"
OUT_DIR=$(python3 - <<'PY' "$OUT_DIR"
from pathlib import Path
import sys
print(Path(sys.argv[1]).resolve())
PY
)

RUN_BASENAME=$(basename "$RUN_DIR")
STAMP=$(date +%Y%m%d_%H%M%S)
TMP_DIR=$(mktemp -d)
STAGE_ROOT="$TMP_DIR/$RUN_BASENAME"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$STAGE_ROOT"

copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [ -e "$src" ]; then
    mkdir -p "$(dirname "$dst")"
    cp -a "$src" "$dst"
  fi
}

tail_if_exists() {
  local src="$1"
  local dst="$2"
  local lines="$3"
  if [ -f "$src" ]; then
    mkdir -p "$(dirname "$dst")"
    tail -n "$lines" "$src" > "$dst"
  fi
}

copy_if_exists "$RUN_DIR/effective.yaml" "$STAGE_ROOT/effective.yaml"
copy_if_exists "$RUN_DIR/summary.json" "$STAGE_ROOT/summary.json"
copy_if_exists "$RUN_DIR/timeseries.csv" "$STAGE_ROOT/timeseries.csv"
copy_if_exists "$RUN_DIR/config" "$STAGE_ROOT/config"
copy_if_exists "$RUN_DIR/logs" "$STAGE_ROOT/logs"

ART_SRC="$RUN_DIR/artifacts"
ART_DST="$STAGE_ROOT/artifacts"
mkdir -p "$ART_DST"

for name in \
  doctor.txt \
  run_meta.json \
  monitor.json \
  sensor_probe.json \
  sensor_probe.log \
  autoware_control.jsonl \
  autoware_control.log \
  apollo_bridge_effective.yaml \
  apollo_adapter_meta.json \
  apollo_container_runtime_check.txt \
  apollo_log_capture_meta.json \
  apollo_log_snapshot_meta.json \
  apollo_mainboard_runtime_check.log \
  apollo_modules_start.log \
  apollo_modules_status.log \
  apollo_prepare_user.log \
  apollo_docker_libs.txt \
  cyber_bridge.out.log \
  cyber_bridge.err.log \
  cyber_bridge_stats.json \
  cyber_bridge_healthcheck.json \
  cyber_control_bridge.out.log \
  cyber_control_bridge.err.log \
  auto_calib_suggestion.json \
  debug_timeseries.csv \
  dreamview_launch.log \
  dreamview_open.log \
  dreamview_record.log \
  dreamview_record.out.log \
  dreamview_record.err.log \
  dreamview_runtime_deps.log \
  dreamview_stop.log \
  dreamview_url.txt
do
  copy_if_exists "$ART_SRC/$name" "$ART_DST/$name"
done

copy_if_exists "$ART_SRC/apollo_docker_libs" "$ART_DST/apollo_docker_libs"

tail_if_exists "$ART_SRC/apollo_planning.INFO" "$ART_DST/apollo_planning.tail.INFO" 2000
tail_if_exists "$ART_SRC/apollo_control.INFO" "$ART_DST/apollo_control.tail.INFO" 1000
tail_if_exists "$ART_SRC/apollo_routing.INFO" "$ART_DST/apollo_routing.tail.INFO" 1000
tail_if_exists "$ART_SRC/apollo_external_command.INFO" "$ART_DST/apollo_external_command.tail.INFO" 1000
tail_if_exists "$ART_SRC/control_decode_debug.jsonl" "$ART_DST/control_decode_debug.tail.jsonl" 2000

if [ -d "$ART_SRC" ]; then
  find "$ART_SRC" -maxdepth 1 -type f -name 'steer_saturation_snapshot_*.json' -printf '%T@ %p\n' \
    | sort -nr \
    | head -n 10 \
    | cut -d' ' -f2- \
    | while IFS= read -r snap; do
        [ -n "$snap" ] || continue
        copy_if_exists "$snap" "$ART_DST/$(basename "$snap")"
      done
fi

cat > "$STAGE_ROOT/PACKING_NOTES.txt" <<EOF
This package contains the key files from run: $RUN_BASENAME

Included:
- Root run summary: effective.yaml, summary.json, timeseries.csv
- config/ and logs/
- Selected artifacts/ files commonly used for regression and debugging
- Tail snapshots of large Apollo logs instead of the full files
- The latest 10 steer saturation snapshots

Excluded to keep the package small:
- sensors/
- full artifacts/apollo_planning.INFO
- full artifacts/apollo_control.INFO
- full artifacts/apollo_routing.INFO
- full artifacts/apollo_external_command.INFO
- full artifacts/control_decode_debug.jsonl
- raw Dreamview video capture files
EOF

OUT_BASE="$OUT_DIR/${RUN_BASENAME}_key_artifacts_${STAMP}.tar.gz"
tar -czf "$OUT_BASE" -C "$TMP_DIR" "$RUN_BASENAME"
sha256sum "$OUT_BASE" > "$OUT_BASE.sha256"

printf '%s\n' "$OUT_BASE"
