#!/usr/bin/env bash
# Entrypoint for Autoware Mode-2 container (GT planning focus).

set -euo pipefail

# Robust log dir selection
LOG_DIR=${RUN_ARTIFACT_DIR:-/work/runs/latest/artifacts}
if ! mkdir -p "$LOG_DIR" 2>/dev/null; then
  LOG_DIR=/tmp/run_artifacts
  mkdir -p "$LOG_DIR" || LOG_DIR=/tmp
fi
exec > >(tee -a "$LOG_DIR/entrypoint.log") 2>&1

log_env_dump() {
  cat > "$LOG_DIR/entrypoint_env.txt" <<EOF
AUTOWARE_LAUNCH_FILE=${AUTOWARE_LAUNCH_FILE}
AUTOWARE_LAUNCH_EXTRA_ARGS=${AUTOWARE_LAUNCH_EXTRA_ARGS}
AUTOWARE_MODE=${AUTOWARE_MODE}
AUTOWARE_PERCEPTION_MODE=${AUTOWARE_PERCEPTION_MODE}
CARLA_MAP=${CARLA_MAP}
CARLA_HOST=${CARLA_HOST}
CARLA_PORT=${CARLA_PORT}
EGO_ROLE_NAME=${EGO_ROLE_NAME}
LOG_DIR=${LOG_DIR}
RUN_ARTIFACT_DIR=${RUN_ARTIFACT_DIR:-}
EOF
}

log() { echo "[entrypoint][$(date +%H:%M:%S)] $*"; }
sleep_forever() { log "$*"; tail -f /dev/null; }

# Defaults / env parsing (4.1)
AUTOWARE_MODE=${AUTOWARE_MODE:-gt_planning}
AUTOWARE_PERCEPTION_MODE=${AUTOWARE_PERCEPTION_MODE:-gt}
AUTOWARE_DISABLE_CAMERAS=${AUTOWARE_DISABLE_CAMERAS:-1}
AUTOWARE_TRUTH_OBJECTS_TOPIC=${AUTOWARE_TRUTH_OBJECTS_TOPIC:-/perception/object_recognition/objects}
AUTOWARE_LAUNCH_FILE=${AUTOWARE_LAUNCH_FILE:-autoware.launch.xml}
AUTOWARE_LAUNCH_EXTRA_ARGS=${AUTOWARE_LAUNCH_EXTRA_ARGS:-"launch_sensing:=false launch_perception:=false launch_localization:=false launch_planning:=true launch_control:=true launch_map:=true launch_vehicle:=true"}
EGO_ROLE_NAME=${EGO_ROLE_NAME:-ego}
CARLA_HOST=${CARLA_HOST:-127.0.0.1}
CARLA_PORT=${CARLA_PORT:-2000}
CARLA_MAP=${CARLA_MAP:-Town01}
AUTOWARE_MAP_ROOT=${AUTOWARE_MAP_ROOT:-/autoware_map}
RUN_ARTIFACT_DIR=${RUN_ARTIFACT_DIR:-/work/runs/latest/artifacts}

MAP_LOG="${RUN_ARTIFACT_DIR}/map_staging.log"

# 1) Source ROS/Autoware
if source /opt/ros/humble/setup.bash 2>/dev/null; then
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
    source "$CAND" && FOUND_SETUP="$CAND" && break
  fi
done
if [ -n "$FOUND_SETUP" ]; then
  log "sourced $FOUND_SETUP"
else
  log "Autoware setup.bash not found (continuing, launch may fail)"
fi

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

  nohup python /work/io/ros2/tools/control_logger.py --topic "$ctrl_topic" --out "$ctrl_out" --force-anymsg >/tmp/control_logger.out 2>&1 &
  log "control_logger started for $ctrl_topic -> $ctrl_out (pid $!)"

  nohup python /work/io/ros2/tools/topic_probe.py --topics $probe_list --max-msgs "$probe_max" --out "$probe_out" >/tmp/topic_probe.out 2>&1 &
  log "topic_probe started for [$probe_list] -> $probe_out (pid $!)"
}
start_observability

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
  IFS=' ' read -r -a EXTRA_ARR <<< "${AUTOWARE_LAUNCH_EXTRA_ARGS}"
  log "launching autoware_launch ${AUTOWARE_LAUNCH_FILE} with map_path=${AUTOWARE_MAP_PATH} extras=${EXTRA_ARR[*]}"
  ros2 launch autoware_launch "${AUTOWARE_LAUNCH_FILE}" \
    map_path:="${AUTOWARE_MAP_PATH}" \
    vehicle_model:=sample_vehicle \
    sensor_model:=carla_sensor_kit \
    "${EXTRA_ARR[@]}" \
    >"${RUN_ARTIFACT_DIR}/autoware_launch.stdout" 2>"${RUN_ARTIFACT_DIR}/autoware_launch.stderr" &
  echo $! > /tmp/autoware_launch.pid
  log "autoware_launch pid $!"
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
  launch_autoware_gt
  sleep 5
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
