#!/usr/bin/env bash
# Entrypoint for Autoware Mode-2 container (GT planning focus).

set -euo pipefail

# Robust log dir selection
LOG_DIR=${RUN_ARTIFACT_DIR:-/work/runs/latest/artifacts}
if ! mkdir -p "$LOG_DIR" 2>/dev/null; then
  LOG_DIR=/tmp/run_artifacts
  mkdir -p "$LOG_DIR" || LOG_DIR=/tmp
fi
# Keep logging simple inside the Autoware image. Process substitution with tee
# has proven fragile in this container and can exit before any diagnostics are
# written, which makes failed launches opaque.
exec >> "$LOG_DIR/entrypoint.log" 2>&1

log_env_dump() {
  cat > "$LOG_DIR/entrypoint_env.txt" <<EOF
AUTOWARE_LAUNCH_FILE=${AUTOWARE_LAUNCH_FILE}
AUTOWARE_LAUNCH_EXTRA_ARGS=${AUTOWARE_LAUNCH_EXTRA_ARGS}
AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS=${AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS:-}
AUTOWARE_PLANNING_COMMON_PARAM_PATH=${AUTOWARE_PLANNING_COMMON_PARAM_PATH:-}
AUTOWARE_VELOCITY_LIMIT_PROBE_ENABLED=${AUTOWARE_VELOCITY_LIMIT_PROBE_ENABLED:-0}
AUTOWARE_VELOCITY_LIMIT_PROBE_TOPIC=${AUTOWARE_VELOCITY_LIMIT_PROBE_TOPIC:-/planning/scenario_planning/max_velocity_default}
AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS=${AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS:-}
AUTOWARE_VELOCITY_LIMIT_PROBE_PUBLISH_HZ=${AUTOWARE_VELOCITY_LIMIT_PROBE_PUBLISH_HZ:-5.0}
AUTOWARE_VELOCITY_LIMIT_PROBE_DURATION_S=${AUTOWARE_VELOCITY_LIMIT_PROBE_DURATION_S:-30.0}
AUTOWARE_VELOCITY_LIMIT_PROBE_START_DELAY_S=${AUTOWARE_VELOCITY_LIMIT_PROBE_START_DELAY_S:-8.0}
AUTOWARE_VELOCITY_LIMIT_PROBE_SENDER=${AUTOWARE_VELOCITY_LIMIT_PROBE_SENDER:-api}
USE_SIM_TIME=${USE_SIM_TIME:-true}
AUTOWARE_MODE=${AUTOWARE_MODE}
AUTOWARE_PERCEPTION_MODE=${AUTOWARE_PERCEPTION_MODE}
CARLA_MAP=${CARLA_MAP}
CARLA_HOST=${CARLA_HOST}
CARLA_PORT=${CARLA_PORT}
EGO_ROLE_NAME=${EGO_ROLE_NAME}
CONTROL_TOPIC=${CONTROL_TOPIC:-/control/command/control_cmd}
AUTOWARE_CONTROL_BRIDGE_ENABLED=${AUTOWARE_CONTROL_BRIDGE_ENABLED:-0}
AUTOWARE_CONTROL_BRIDGE_TYPE=${AUTOWARE_CONTROL_BRIDGE_TYPE:-autoware_control}
AUTOWARE_CONTROL_BRIDGE_TOPIC=${AUTOWARE_CONTROL_BRIDGE_TOPIC:-${CONTROL_TOPIC:-/control/command/control_cmd}}
AUTOWARE_TRAJECTORY_RELAY_ENABLED=${AUTOWARE_TRAJECTORY_RELAY_ENABLED:-1}
AUTOWARE_TRAJECTORY_RELAY_INPUT=${AUTOWARE_TRAJECTORY_RELAY_INPUT:-/planning/scenario_planning/velocity_smoother/trajectory}
AUTOWARE_TRAJECTORY_RELAY_OUTPUT=${AUTOWARE_TRAJECTORY_RELAY_OUTPUT:-/planning/trajectory}
AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN=${AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN:-0.05}
AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE=${AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE:-open_loop}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN:-0.25}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_BRAKE_GAIN=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_BRAKE_GAIN:-0.25}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_DEADBAND_MPS=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_DEADBAND_MPS:-0.2}
AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_SPEED_MARGIN_MPS=${AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_SPEED_MARGIN_MPS:-0.5}
AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS=${AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS:-0.0}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED:-0}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_MIN_ACCEL_MPS2=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_MIN_ACCEL_MPS2:-0.05}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_SPEED_MARGIN_MPS=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_SPEED_MARGIN_MPS:-0.5}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION:-throttle}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE:-0.0}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE_SPEED_MPS=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE_SPEED_MPS:-5.0}
AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS=${AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS:--1.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC:-0.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_THROTTLE_STEP=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_THROTTLE_STEP:-1.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_BRAKE_STEP=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_BRAKE_STEP:-1.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE=${AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE:-carla}
AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_TOPIC=${AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_TOPIC:-/vehicle/status/velocity_status}
AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_STALE_SEC=${AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_STALE_SEC:-0.5}
LOG_DIR=${LOG_DIR}
RUN_ARTIFACT_DIR=${RUN_ARTIFACT_DIR:-}
EOF
}

log() { echo "[entrypoint][$(date +%H:%M:%S)] $*"; }
sleep_forever() { log "$*"; tail -f /dev/null; }

source_setup_file() {
  # ROS/Autoware setup scripts may read optional env vars before defining them.
  # Keep nounset for our script, but relax it while sourcing upstream setup.
  set +u
  # shellcheck disable=SC1090
  source "$1"
  local rc=$?
  set -u
  return "$rc"
}

# Defaults / env parsing (4.1)
AUTOWARE_MODE=${AUTOWARE_MODE:-gt_planning}
AUTOWARE_PERCEPTION_MODE=${AUTOWARE_PERCEPTION_MODE:-gt}
AUTOWARE_DISABLE_CAMERAS=${AUTOWARE_DISABLE_CAMERAS:-1}
AUTOWARE_TRUTH_OBJECTS_TOPIC=${AUTOWARE_TRUTH_OBJECTS_TOPIC:-/perception/object_recognition/objects}
AUTOWARE_LAUNCH_FILE=${AUTOWARE_LAUNCH_FILE:-autoware.launch.xml}
AUTOWARE_LAUNCH_EXTRA_ARGS=${AUTOWARE_LAUNCH_EXTRA_ARGS:-"launch_sensing:=false launch_perception:=false launch_localization:=false launch_planning:=true launch_control:=true launch_map:=true launch_vehicle:=true"}
AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS=${AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS:-}
AUTOWARE_PLANNING_COMMON_PARAM_PATH=${AUTOWARE_PLANNING_COMMON_PARAM_PATH:-}
AUTOWARE_VELOCITY_LIMIT_PROBE_ENABLED=${AUTOWARE_VELOCITY_LIMIT_PROBE_ENABLED:-0}
AUTOWARE_VELOCITY_LIMIT_PROBE_TOPIC=${AUTOWARE_VELOCITY_LIMIT_PROBE_TOPIC:-/planning/scenario_planning/max_velocity_default}
AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS=${AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS:-}
AUTOWARE_VELOCITY_LIMIT_PROBE_PUBLISH_HZ=${AUTOWARE_VELOCITY_LIMIT_PROBE_PUBLISH_HZ:-5.0}
AUTOWARE_VELOCITY_LIMIT_PROBE_DURATION_S=${AUTOWARE_VELOCITY_LIMIT_PROBE_DURATION_S:-30.0}
AUTOWARE_VELOCITY_LIMIT_PROBE_START_DELAY_S=${AUTOWARE_VELOCITY_LIMIT_PROBE_START_DELAY_S:-8.0}
AUTOWARE_VELOCITY_LIMIT_PROBE_SENDER=${AUTOWARE_VELOCITY_LIMIT_PROBE_SENDER:-api}
EGO_ROLE_NAME=${EGO_ROLE_NAME:-ego}
CARLA_HOST=${CARLA_HOST:-127.0.0.1}
CARLA_PORT=${CARLA_PORT:-2000}
CARLA_MAP=${CARLA_MAP:-Town01}
AUTOWARE_MAP_ROOT=${AUTOWARE_MAP_ROOT:-/autoware_map}
RUN_ARTIFACT_DIR=${RUN_ARTIFACT_DIR:-/work/runs/latest/artifacts}
CONTROL_TOPIC=${CONTROL_TOPIC:-/control/command/control_cmd}
AUTOWARE_CONTROL_BRIDGE_ENABLED=${AUTOWARE_CONTROL_BRIDGE_ENABLED:-0}
AUTOWARE_CONTROL_BRIDGE_TYPE=${AUTOWARE_CONTROL_BRIDGE_TYPE:-autoware_control}
AUTOWARE_CONTROL_BRIDGE_TOPIC=${AUTOWARE_CONTROL_BRIDGE_TOPIC:-${CONTROL_TOPIC}}
AUTOWARE_CONTROL_BRIDGE_MAX_STEER_ANGLE=${AUTOWARE_CONTROL_BRIDGE_MAX_STEER_ANGLE:-0.6}
AUTOWARE_CONTROL_BRIDGE_SPEED_GAIN=${AUTOWARE_CONTROL_BRIDGE_SPEED_GAIN:-10.0}
AUTOWARE_CONTROL_BRIDGE_BRAKE_GAIN=${AUTOWARE_CONTROL_BRIDGE_BRAKE_GAIN:-5.0}
AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN=${AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN:-0.05}
AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE=${AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE:-open_loop}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN:-0.25}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_BRAKE_GAIN=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_BRAKE_GAIN:-0.25}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_DEADBAND_MPS=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_DEADBAND_MPS:-0.2}
AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_SPEED_MARGIN_MPS=${AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_SPEED_MARGIN_MPS:-0.5}
AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS=${AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS:-0.0}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED:-0}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_MIN_ACCEL_MPS2=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_MIN_ACCEL_MPS2:-0.05}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_SPEED_MARGIN_MPS=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_SPEED_MARGIN_MPS:-0.5}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION:-throttle}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE:-0.0}
AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE_SPEED_MPS=${AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE_SPEED_MPS:-5.0}
AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS=${AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS:--1.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC:-0.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_THROTTLE_STEP=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_THROTTLE_STEP:-1.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_BRAKE_STEP=${AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_BRAKE_STEP:-1.0}
AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE=${AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE:-carla}
AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_TOPIC=${AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_TOPIC:-/vehicle/status/velocity_status}
AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_STALE_SEC=${AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_STALE_SEC:-0.5}
AUTOWARE_CONTROL_BRIDGE_TIMEOUT_SEC=${AUTOWARE_CONTROL_BRIDGE_TIMEOUT_SEC:-0.8}
AUTOWARE_CONTROL_BRIDGE_APPLY_HZ=${AUTOWARE_CONTROL_BRIDGE_APPLY_HZ:-20.0}
AUTOWARE_CONTROL_BRIDGE_STEER_SIGN=${AUTOWARE_CONTROL_BRIDGE_STEER_SIGN:--1.0}
AUTOWARE_TRAJECTORY_RELAY_ENABLED=${AUTOWARE_TRAJECTORY_RELAY_ENABLED:-1}
AUTOWARE_TRAJECTORY_RELAY_INPUT=${AUTOWARE_TRAJECTORY_RELAY_INPUT:-/planning/scenario_planning/velocity_smoother/trajectory}
AUTOWARE_TRAJECTORY_RELAY_OUTPUT=${AUTOWARE_TRAJECTORY_RELAY_OUTPUT:-/planning/trajectory}

MAP_LOG="${RUN_ARTIFACT_DIR}/map_staging.log"

# 1) Source ROS/Autoware
if source_setup_file /opt/ros/humble/setup.bash 2>/dev/null; then
  log "sourced /opt/ros/humble/setup.bash"
else
  sleep_forever "ROS humble setup not found"
fi

FOUND_SETUP=""
for CAND in \
  /opt/autoware/setup.bash \
  /opt/autoware/install/setup.bash \
  /opt/Autoware/setup.bash \
  /opt/Autoware/install/setup.bash \
  /autoware/setup.bash \
  /autoware/install/setup.bash \
  /opt/autoware_universe/setup.bash \
  /opt/autoware_universe/install/setup.bash \
  /workspace/install/setup.bash \
  /home/*/install/setup.bash \
  /opt/*/install/setup.bash; do
  if [ -f "$CAND" ]; then
    # shellcheck disable=SC1090
    source_setup_file "$CAND" && FOUND_SETUP="$CAND" && break
  fi
done
if [ -n "$FOUND_SETUP" ]; then
  log "sourced $FOUND_SETUP"
else
  log "Autoware setup.bash not found (continuing, launch may fail)"
fi

apply_planning_common_max_vel_override() {
  local requested="${AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS:-}"
  local report_file="${RUN_ARTIFACT_DIR}/autoware_planning_common_override.json"
  mkdir -p "${RUN_ARTIFACT_DIR}"
  local dyn_requested=0
  for var in \
    AUTOWARE_PLANNING_COMMON_NOMINAL_MAX_ACC_MPS2 \
    AUTOWARE_PLANNING_COMMON_NOMINAL_MIN_ACC_MPS2 \
    AUTOWARE_PLANNING_COMMON_LIMIT_MAX_ACC_MPS2 \
    AUTOWARE_PLANNING_COMMON_LIMIT_MIN_ACC_MPS2; do
    local value="${!var:-}"
    if [ -n "$value" ] && [ "$value" != "null" ] && [ "$value" != "None" ]; then
      dyn_requested=1
      break
    fi
  done
  if { [ -z "$requested" ] || [ "$requested" = "null" ] || [ "$requested" = "None" ]; } && [ "$dyn_requested" = "0" ]; then
    cat >"$report_file" <<EOF
{"enabled": false, "status": "skipped", "reason": "AUTOWARE_PLANNING_COMMON override variables unset"}
EOF
    return
  fi

  local param_path="${AUTOWARE_PLANNING_COMMON_PARAM_PATH:-}"
  if [ -z "$param_path" ]; then
    local prefix=""
    prefix=$(ros2 pkg prefix autoware_launch 2>/dev/null || true)
    local candidates=()
    if [ -n "$prefix" ]; then
      candidates+=("${prefix}/share/autoware_launch/config/planning/scenario_planning/common/common.param.yaml")
    fi
    candidates+=(
      "/opt/autoware/share/autoware_launch/config/planning/scenario_planning/common/common.param.yaml"
      "/opt/autoware/install/autoware_launch/share/autoware_launch/config/planning/scenario_planning/common/common.param.yaml"
      "/autoware/install/autoware_launch/share/autoware_launch/config/planning/scenario_planning/common/common.param.yaml"
      "/workspace/install/autoware_launch/share/autoware_launch/config/planning/scenario_planning/common/common.param.yaml"
    )
    local candidate
    for candidate in "${candidates[@]}"; do
      if [ -f "$candidate" ]; then
        param_path="$candidate"
        break
      fi
    done
  fi

  if [ -z "$param_path" ] || [ ! -f "$param_path" ]; then
    cat >"$report_file" <<EOF
{"enabled": true, "status": "failed", "reason": "common.param.yaml not found"}
EOF
    sleep_forever "Autoware common.param.yaml not found for max_vel override; see ${report_file}"
  fi
  if [ ! -w "$param_path" ]; then
    cat >"$report_file" <<EOF
{"enabled": true, "status": "failed", "reason": "common.param.yaml not writable", "param_path": "${param_path}"}
EOF
    sleep_forever "Autoware common.param.yaml is not writable for max_vel override; see ${report_file}"
  fi

  local before_copy="${RUN_ARTIFACT_DIR}/common.param.before_max_vel_override.yaml"
  local after_copy="${RUN_ARTIFACT_DIR}/common.param.after_max_vel_override.yaml"
  cp "$param_path" "$before_copy"
  if ! python3 - "$param_path" "$report_file" <<'PY'
import json
import os
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
report_path = pathlib.Path(sys.argv[2])

def _raw_env(name):
    raw = os.environ.get(name, "")
    if raw in ("", "null", "None"):
        return None
    return raw

def _parse_float(name, *, positive=False, negative=False):
    raw = _raw_env(name)
    if raw is None:
        return None
    try:
        value = float(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc
    if positive and value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")
    if negative and value >= 0:
        raise ValueError(f"{name} must be < 0, got {value}")
    return value

def _format_yaml_float(value):
    # rclcpp rejects integer-looking YAML for parameters declared as double.
    return f"{float(value):.6f}"

requests = {
    "max_vel": _parse_float("AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS", positive=True),
    "normal.max_acc": _parse_float(
        "AUTOWARE_PLANNING_COMMON_NOMINAL_MAX_ACC_MPS2", positive=True
    ),
    "normal.min_acc": _parse_float(
        "AUTOWARE_PLANNING_COMMON_NOMINAL_MIN_ACC_MPS2", negative=True
    ),
    "limit.max_acc": _parse_float(
        "AUTOWARE_PLANNING_COMMON_LIMIT_MAX_ACC_MPS2", positive=True
    ),
    "limit.min_acc": _parse_float(
        "AUTOWARE_PLANNING_COMMON_LIMIT_MIN_ACC_MPS2", negative=True
    ),
}
requests = {key: value for key, value in requests.items() if value is not None}
if not requests:
    report_path.write_text(
        json.dumps(
            {
                "enabled": False,
                "status": "skipped",
                "reason": "AUTOWARE_PLANNING_COMMON override variables unset",
            },
            sort_keys=True,
        )
    )
    raise SystemExit(0)

text = path.read_text()
originals = {}
missing = []

if "max_vel" in requests:
    pattern = re.compile(r"(^\s*max_vel:\s*)([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)(.*$)", re.MULTILINE)
    match = pattern.search(text)
    if not match:
        missing.append("max_vel")
    else:
        originals["max_vel"] = float(match.group(2))
        text, count = pattern.subn(
            lambda m: f"{m.group(1)}{_format_yaml_float(requests['max_vel'])}{m.group(3)}",
            text,
            count=1,
        )
        if count != 1:
            missing.append("max_vel")

lines = text.splitlines(keepends=True)
section = None
section_indent = None
seen = set()
for index, line in enumerate(lines):
    section_match = re.match(r"^(\s*)(normal|nominal|limit):\s*(?:#.*)?$", line)
    if section_match:
        section = section_match.group(2)
        section_indent = len(section_match.group(1))
        continue
    if section is not None:
        current_indent = len(line) - len(line.lstrip(" "))
        if line.strip() and current_indent <= section_indent:
            section = None
            section_indent = None
        else:
            line_ending = "\n" if line.endswith("\n") else ""
            logical_line = line[:-1] if line_ending else line
            key_match = re.match(
                r"^(\s*)(min_acc|max_acc):\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)(.*)$",
                logical_line,
            )
            if key_match:
                full_key = f"{section}.{key_match.group(2)}"
                if full_key in requests:
                    originals[full_key] = float(key_match.group(3))
                    lines[index] = (
                        f"{key_match.group(1)}{key_match.group(2)}: "
                        f"{_format_yaml_float(requests[full_key])}{key_match.group(4)}{line_ending}"
                    )
                    seen.add(full_key)
text = "".join(lines)

for key in requests:
    if key != "max_vel" and key not in seen:
        missing.append(key)

if missing:
    raise ValueError(f"common.param.yaml keys not found: {', '.join(sorted(set(missing)))}")

path.write_text(text)
report_path.write_text(
    json.dumps(
        {
            "enabled": True,
            "status": "applied",
            "param_path": str(path),
            "original_values": originals,
            "requested_values": requests,
        },
        sort_keys=True,
    )
)
PY
  then
    cat >"$report_file" <<EOF
{"enabled": true, "status": "failed", "reason": "failed to patch common dynamics", "param_path": "${param_path}"}
EOF
    sleep_forever "failed to patch Autoware common.param.yaml max_vel; see ${report_file}"
  fi
  cp "$param_path" "$after_copy"
  python3 - "$report_file" "$before_copy" "$after_copy" <<'PY'
import json
import pathlib
import sys

report_path = pathlib.Path(sys.argv[1])
data = json.loads(report_path.read_text())
data["before_copy"] = sys.argv[2]
data["after_copy"] = sys.argv[3]
report_path.write_text(json.dumps(data, sort_keys=True))
PY
  log "patched Autoware planning common parameters in ${param_path}; report ${report_file}"
}

apply_planning_common_max_vel_override

log "env: MODE=${AUTOWARE_MODE} PERCEPTION=${AUTOWARE_PERCEPTION_MODE} DISABLE_CAMERAS=${AUTOWARE_DISABLE_CAMERAS} CARLA=${CARLA_HOST}:${CARLA_PORT} MAP=${CARLA_MAP} DOMAIN=${ROS_DOMAIN_ID:-?} USE_SIM_TIME=${USE_SIM_TIME:-?} LAUNCH_FILE=${AUTOWARE_LAUNCH_FILE} EXTRA=${AUTOWARE_LAUNCH_EXTRA_ARGS} LOG_DIR=${LOG_DIR}"
log_env_dump

# 2) Ensure CARLA wheel installed (4.2)
ensure_carla_wheel() {
  local pyver wheel_dir wheel
  pyver=$(python3 - <<'PY'
import sys
print(f"{sys.version_info.major}{sys.version_info.minor}")
PY
)
  wheel_dir=${CARLA_WHEEL_DIR:-/carla_wheels}
  wheel=$(ls "$wheel_dir"/carla-0.9.16-cp${pyver}-*.whl 2>/dev/null | head -n1 || true)
  if python3 - <<'PY'
import carla
print("carla import OK")
PY
  then
    log "carla already importable"
    return
  fi
  if [ -z "$wheel" ]; then
    log "CARLA wheel not found for cp${pyver} under $wheel_dir"
    ls -l "$wheel_dir" || true
    sleep_forever "CARLA wheel missing"
  fi
  log "installing CARLA wheel $wheel"
  if ! pip3 install -U "$wheel"; then
    sleep_forever "pip install carla wheel failed"
  fi
}
ensure_carla_wheel

# 3) Map staging (4.3)
stage_map() {
  local src_root="$AUTOWARE_MAP_ROOT"
  [ -d "/autoware_map" ] && src_root="/autoware_map"
  mkdir -p "$(dirname "$MAP_LOG")"
  local stage="/tmp/autoware_map/${CARLA_MAP}"
  mkdir -p "$stage"
  local pc_src="${src_root}/point_cloud_maps/${CARLA_MAP}.pcd"
  local lane_src="${src_root}/vector_maps/lanelet2/${CARLA_MAP}.osm"
  local projector_src=""
  {
    echo "[map] src_root=${src_root}"
    echo "[map] stage=${stage}"
    echo "[map] pc_src=${pc_src}"
    echo "[map] lane_src=${lane_src}"
  } >"$MAP_LOG"
  if [ ! -f "$pc_src" ] || [ ! -f "$lane_src" ]; then
    echo "[map] missing map files for ${CARLA_MAP}" >>"$MAP_LOG"
    find "$src_root" -maxdepth 4 -type f | head -n 50 >>"$MAP_LOG" || true
    sleep_forever "map files missing; see ${MAP_LOG}"
  fi
  ln -sf "$pc_src" "$stage/pointcloud_map.pcd"
  ln -sf "$lane_src" "$stage/lanelet2_map.osm"
  for candidate in \
    "${src_root}/map_projector_info/${CARLA_MAP}.yaml" \
    "${src_root}/${CARLA_MAP}/map_projector_info.yaml" \
    "${src_root}/map_projector_info.yaml"
  do
    if [ -f "$candidate" ]; then
      projector_src="$candidate"
      break
    fi
  done
  if [ -n "$projector_src" ]; then
    ln -sf "$projector_src" "$stage/map_projector_info.yaml"
    echo "[map] projector_src=${projector_src}" >>"$MAP_LOG"
  elif grep -q "local_x" "$lane_src" && grep -q "local_y" "$lane_src"; then
    # CARLA Town maps are authored in local coordinates. Without this file,
    # Autoware infers MGRS from non-zero lat/lon and the route pose no longer
    # lands inside the lanelet geometry used by mission_planner.
    cat >"$stage/map_projector_info.yaml" <<'EOF'
projector_type: Local
EOF
    echo "[map] generated Local map_projector_info.yaml from local_x/local_y lanelet map" >>"$MAP_LOG"
  else
    echo "[map][warn] no map_projector_info.yaml found; Autoware will infer projection from lanelet map" >>"$MAP_LOG"
  fi
  export AUTOWARE_MAP_PATH="$stage"
  echo "[map] staged -> $AUTOWARE_MAP_PATH" >>"$MAP_LOG"
}
stage_map

# 4) Observability (control logger & topic probe retained)
start_observability() {
  local ctrl_topic ctrl_out probe_out probe_raw probe_list probe_max
  ctrl_topic=${CONTROL_TOPIC:-/control/command/control_cmd}
  ctrl_out=${RUN_ARTIFACT_DIR}/autoware_control.jsonl
  probe_out=${RUN_ARTIFACT_DIR}/sensor_probe.json
  probe_raw=${PROBE_TOPICS:-/clock,/tf}
  probe_list=${probe_raw//,/ }
  probe_max=${PROBE_MAX_MSGS:-5}
  mkdir -p "${RUN_ARTIFACT_DIR}"

  nohup python /work/tbio/ros2/tools/control_logger.py --topic "$ctrl_topic" --out "$ctrl_out" --force-anymsg >/tmp/control_logger.out 2>&1 &
  log "control_logger started for $ctrl_topic -> $ctrl_out (pid $!)"

  nohup python /work/tbio/ros2/tools/topic_probe.py --topics $probe_list --max-msgs "$probe_max" --out "$probe_out" >/tmp/topic_probe.out 2>&1 &
  log "topic_probe started for [$probe_list] -> $probe_out (pid $!)"
}
start_observability

start_control_bridge() {
  if [ "${AUTOWARE_CONTROL_BRIDGE_ENABLED}" != "1" ]; then
    log "carla_control_bridge disabled"
    return
  fi
  local log_file=${RUN_ARTIFACT_DIR}/autoware_carla_control_bridge.log
  nohup python3 /work/algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py \
    --carla-host "$CARLA_HOST" \
    --carla-port "$CARLA_PORT" \
    --ego-role-name "$EGO_ROLE_NAME" \
    --control-topic "$AUTOWARE_CONTROL_BRIDGE_TOPIC" \
    --control-type "$AUTOWARE_CONTROL_BRIDGE_TYPE" \
    --max-steer-angle "$AUTOWARE_CONTROL_BRIDGE_MAX_STEER_ANGLE" \
    --timeout-sec "$AUTOWARE_CONTROL_BRIDGE_TIMEOUT_SEC" \
    --speed-gain "$AUTOWARE_CONTROL_BRIDGE_SPEED_GAIN" \
    --brake-gain "$AUTOWARE_CONTROL_BRIDGE_BRAKE_GAIN" \
    --accel-throttle-gain "$AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN" \
    --longitudinal-mode "$AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE" \
    --speed-feedback-gain "$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN" \
    --speed-feedback-brake-gain "$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_BRAKE_GAIN" \
    --speed-feedback-deadband-mps "$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_DEADBAND_MPS" \
    --accel-feedforward-speed-margin-mps "$AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_SPEED_MARGIN_MPS" \
    --accel-feedforward-overspeed-taper-mps "$AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS" \
    $( [ "$AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED" = "1" ] && printf '%s' "--positive-accel-brake-inhibit-enabled" || printf '%s' "--positive-accel-brake-inhibit-disabled" ) \
    --positive-accel-brake-inhibit-min-accel-mps2 "$AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_MIN_ACCEL_MPS2" \
    --positive-accel-brake-inhibit-speed-margin-mps "$AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_SPEED_MARGIN_MPS" \
    --positive-accel-overshoot-action "$AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION" \
    --positive-accel-min-throttle "$AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE" \
    --positive-accel-min-throttle-speed-mps "$AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE_SPEED_MPS" \
    --negative-accel-brake-speed-margin-mps "$AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS" \
    --speed-feedback-transition-coast-sec "$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC" \
    --speed-feedback-max-throttle-step "$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_THROTTLE_STEP" \
    --speed-feedback-max-brake-step "$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_BRAKE_STEP" \
    --speed-source "$AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE" \
    --velocity-status-topic "$AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_TOPIC" \
    --velocity-status-stale-sec "$AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_STALE_SEC" \
    --apply-hz "$AUTOWARE_CONTROL_BRIDGE_APPLY_HZ" \
    --autoware-steer-sign "$AUTOWARE_CONTROL_BRIDGE_STEER_SIGN" \
    >"$log_file" 2>&1 &
  log "carla_control_bridge started for ${AUTOWARE_CONTROL_BRIDGE_TOPIC} -> ${log_file} (pid $!)"
}

# 5) GT bridge (4.4)
start_gt_bridge() {
  local log_file=${RUN_ARTIFACT_DIR}/gt_bridge.log
  nohup python3 /work/algo/baselines/autoware/gt/gt_bridge_node.py \
    --host "$CARLA_HOST" \
    --port "$CARLA_PORT" \
    --ego "$EGO_ROLE_NAME" \
    --objects-topic "$AUTOWARE_TRUTH_OBJECTS_TOPIC" \
    --odom-topic "/localization/kinematic_state" \
    --map-frame "map" \
    --base-frame "base_link" \
    --max-steer-angle "$AUTOWARE_CONTROL_BRIDGE_MAX_STEER_ANGLE" \
    --autoware-steer-sign "$AUTOWARE_CONTROL_BRIDGE_STEER_SIGN" \
    >"$log_file" 2>&1 &
  log "gt_bridge started -> $log_file (pid $!)"
}
# 6) Camera watchdog (4.6)
check_cameras() {
  local out_file=${RUN_ARTIFACT_DIR}/camera_processes.txt
  local found=0
  for _ in {1..10}; do
    local procs
    procs=$(ps aux | grep -E "image_transport/republish|multi_camera_combiner" | grep -v grep || true)
    if [ -n "$procs" ]; then
      found=1
      echo "$procs" >"$out_file"
      pkill -f "/opt/ros/.*/image_transport/republish" || true
      pkill -f "multi_camera_combiner" || true
      sleep 1
    else
      sleep 1
    fi
  done
  ps aux | grep -E "image_transport/republish|multi_camera_combiner" | grep -v grep >"$out_file" || true
  if [ -s "$out_file" ]; then
    log "camera processes remained after kill attempts (see $out_file)"
  else
    : >"$out_file"
  fi
}

# 7) Launch Autoware (4.5)
launch_autoware_gt() {
  local sim_time_arg=()
  IFS=' ' read -r -a EXTRA_ARR <<< "${AUTOWARE_LAUNCH_EXTRA_ARGS}"
  if [[ " ${AUTOWARE_LAUNCH_EXTRA_ARGS} " != *" use_sim_time:="* ]]; then
    sim_time_arg=("use_sim_time:=${USE_SIM_TIME:-true}")
  fi
  log "launching autoware_launch ${AUTOWARE_LAUNCH_FILE} with map_path=${AUTOWARE_MAP_PATH} sim_time=${sim_time_arg[*]:-provided-by-extra-args} extras=${EXTRA_ARR[*]}"
  ros2 launch autoware_launch "${AUTOWARE_LAUNCH_FILE}" \
    map_path:="${AUTOWARE_MAP_PATH}" \
    vehicle_model:=sample_vehicle \
    sensor_model:=carla_sensor_kit \
    "${sim_time_arg[@]}" \
    "${EXTRA_ARR[@]}" \
    >"${RUN_ARTIFACT_DIR}/autoware_launch.stdout" 2>"${RUN_ARTIFACT_DIR}/autoware_launch.stderr" &
  echo $! > /tmp/autoware_launch.pid
  log "autoware_launch pid $!"
}

start_trajectory_relay() {
  if [ "${AUTOWARE_TRAJECTORY_RELAY_ENABLED}" != "1" ]; then
    log "trajectory relay disabled"
    return
  fi
  local log_file=${RUN_ARTIFACT_DIR}/trajectory_relay.log
  nohup ros2 run topic_tools relay \
    --ros-args \
    -r __node:=carla_testbed_trajectory_relay \
    -p input_topic:="${AUTOWARE_TRAJECTORY_RELAY_INPUT}" \
    -p output_topic:="${AUTOWARE_TRAJECTORY_RELAY_OUTPUT}" \
    -p lazy:=false \
    >"$log_file" 2>&1 &
  log "trajectory_relay started ${AUTOWARE_TRAJECTORY_RELAY_INPUT} -> ${AUTOWARE_TRAJECTORY_RELAY_OUTPUT} (pid $!, log $log_file)"
}

start_velocity_limit_probe() {
  if [ "${AUTOWARE_VELOCITY_LIMIT_PROBE_ENABLED}" != "1" ]; then
    log "velocity_limit_probe disabled"
    return
  fi
  if [ -z "${AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS}" ]; then
    log "velocity_limit_probe enabled but AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS is empty"
    return
  fi
  local log_file=${RUN_ARTIFACT_DIR}/velocity_limit_probe.log
  local report_file=${RUN_ARTIFACT_DIR}/velocity_limit_probe_report.json
  nohup python3 /work/tbio/ros2/tools/velocity_limit_publisher.py \
    --topic "${AUTOWARE_VELOCITY_LIMIT_PROBE_TOPIC}" \
    --max-velocity-mps "${AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS}" \
    --publish-hz "${AUTOWARE_VELOCITY_LIMIT_PROBE_PUBLISH_HZ}" \
    --duration-s "${AUTOWARE_VELOCITY_LIMIT_PROBE_DURATION_S}" \
    --start-delay-s "${AUTOWARE_VELOCITY_LIMIT_PROBE_START_DELAY_S}" \
    --sender "${AUTOWARE_VELOCITY_LIMIT_PROBE_SENDER}" \
    --report "$report_file" \
    >"$log_file" 2>&1 &
  log "velocity_limit_probe started ${AUTOWARE_VELOCITY_LIMIT_PROBE_TOPIC}=${AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS}m/s (pid $!, log $log_file)"
}

snapshot_introspection() {
  ros2 topic list > "${RUN_ARTIFACT_DIR}/autoware_topics.txt" 2>&1 || true
  ros2 node list > "${RUN_ARTIFACT_DIR}/autoware_nodes.txt" 2>&1 || true
}

main() {
  if [ "$AUTOWARE_MODE" != "gt_planning" ]; then
    sleep_forever "AUTOWARE_MODE=${AUTOWARE_MODE} not supported by this entrypoint (expected gt_planning)"
  fi

  start_gt_bridge
  start_control_bridge
  launch_autoware_gt
  sleep 5
  start_trajectory_relay
  start_velocity_limit_probe
  snapshot_introspection

  if [ "${AUTOWARE_DISABLE_CAMERAS}" = "1" ]; then
    check_cameras
  fi

  AUTOWARE_PID=$(cat /tmp/autoware_launch.pid)
  set +e
  wait "${AUTOWARE_PID}"
  rc=$?
  set -e
  log "autoware_launch exited with code $rc"
  if [ "${AUTOWARE_DISABLE_CAMERAS}" = "1" ]; then
    check_cameras
  fi
  sleep_forever "autoware_launch exited (code $rc) - keeping container for debugging"
}

main
