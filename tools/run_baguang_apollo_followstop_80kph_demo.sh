#!/usr/bin/env bash
set -euo pipefail

# Baguang RoadRunner map Apollo truth-input follow-stop demo wrapper.
#
# This is intentionally a thin wrapper around examples/run_followstop.py. It
# freezes the currently validated demo-isolation settings:
# - carla_direct transport, no ROS2 GT runtime dependency
# - CARLA waypoint lateral stabilizer for the imported straight map
# - straight-line longitudinal override for 80 kph cruise then stop
#
# These settings are for demo isolation and actuation evidence. They do not
# prove Apollo native lateral/longitudinal planning semantics are solved.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

CARLA16_PYTHON="${CARLA16_PYTHON:-/home/ubuntu/miniconda3/envs/carla16/bin/python}"
APOLLO_ROOT="${APOLLO_ROOT:-/home/ubuntu/Apollo10.0}"
APOLLO_MAP_ROOT="${APOLLO_MAP_ROOT:-${APOLLO_ROOT}/application-core/data/map_data}"
RUN_ID="${RUN_ID:-baguang_apollo_direct_followstop_80kph_demo_$(date +%Y%m%d_%H%M%S)}"
RUN_DIR="${RUN_DIR:-runs/${RUN_ID}}"

bool_cfg() {
  case "${1:-0}" in
    1|true|TRUE|yes|YES|on|ON) printf "true" ;;
    *) printf "false" ;;
  esac
}

RECORD_ARGS=()
if [[ "${RECORD_DEMO:-0}" == "1" ]]; then
  RECORD_ARGS+=(
    --record dual_cam
    --record-dual-cam-third-person-only
    --record-resolution "${RECORD_RESOLUTION:-1280x720}"
    --record-fps "${RECORD_FPS:-20}"
    --record-chase-distance "${RECORD_CHASE_DISTANCE:-14.0}"
    --record-chase-height "${RECORD_CHASE_HEIGHT:-5.0}"
    --record-chase-pitch "${RECORD_CHASE_PITCH:--18.0}"
  )
fi

APOLLO_DISABLE_LANE_INVASION="${APOLLO_DISABLE_LANE_INVASION:-1}"
APOLLO_ENABLE_LATERAL_STABILIZER="${APOLLO_ENABLE_LATERAL_STABILIZER:-1}"
APOLLO_ENABLE_STRAIGHT_ACC_OVERRIDE="${APOLLO_ENABLE_STRAIGHT_ACC_OVERRIDE:-1}"
APOLLO_ENABLE_TERMINAL_STOP_HOLD="${APOLLO_ENABLE_TERMINAL_STOP_HOLD:-1}"
START_CARLA="${START_CARLA:-0}"
CARLA_ROOT="${CARLA_ROOT:-/home/ubuntu/CARLA_0.9.16}"
CARLA_EXTRA_ARGS="${CARLA_EXTRA_ARGS:--RenderOffScreen -ResX=1280 -ResY=720 -quality-level=Low}"
UNSET_SDL_VIDEODRIVER="${UNSET_SDL_VIDEODRIVER:-0}"

if [[ "$(bool_cfg "${UNSET_SDL_VIDEODRIVER}")" == "true" ]]; then
  unset SDL_VIDEODRIVER
fi

CARLA_ARGS=()
if [[ "$(bool_cfg "${START_CARLA}")" == "true" ]]; then
  CARLA_ARGS+=(
    --start-carla
    --carla-root "${CARLA_ROOT}"
    --carla-extra-args "${CARLA_EXTRA_ARGS}"
  )
fi

DREAMVIEW_ARGS=()
RECORD_DREAMVIEW="${RECORD_DREAMVIEW:-${RECORD_DEMO:-0}}"
DREAMVIEW_AUTO_OPEN="${DREAMVIEW_AUTO_OPEN:-${RECORD_DREAMVIEW}}"
if [[ "$(bool_cfg "${RECORD_DREAMVIEW}")" == "true" || "$(bool_cfg "${DREAMVIEW_AUTO_OPEN}")" == "true" ]]; then
  DREAMVIEW_BROWSER_WINDOW_SIZE="${DREAMVIEW_BROWSER_WINDOW_SIZE:-1280,720}"
  DREAMVIEW_BROWSER_WINDOW_POSITION="${DREAMVIEW_BROWSER_WINDOW_POSITION:-0,0}"
  if [[ -z "${DREAMVIEW_BROWSER_CMD:-}" ]]; then
    export DREAMVIEW_CHROME_WIDTH="${DREAMVIEW_CHROME_WIDTH:-${DREAMVIEW_BROWSER_WINDOW_SIZE%,*}}"
    export DREAMVIEW_CHROME_HEIGHT="${DREAMVIEW_CHROME_HEIGHT:-${DREAMVIEW_BROWSER_WINDOW_SIZE#*,}}"
    export DREAMVIEW_CHROME_POS_X="${DREAMVIEW_CHROME_POS_X:-${DREAMVIEW_BROWSER_WINDOW_POSITION%,*}}"
    export DREAMVIEW_CHROME_POS_Y="${DREAMVIEW_CHROME_POS_Y:-${DREAMVIEW_BROWSER_WINDOW_POSITION#*,}}"
    DREAMVIEW_BROWSER_CMD="${ROOT_DIR}/tools/open_dreamview_chrome_auto.sh {url}"
  fi
  DREAMVIEW_CAPTURE_MODE="${DREAMVIEW_CAPTURE_MODE:-tick_snapshot}"
  DREAMVIEW_CAPTURE_REGION="${DREAMVIEW_CAPTURE_REGION:-1280x720+0,0}"
  DREAMVIEW_WINDOW_TITLE="${DREAMVIEW_WINDOW_TITLE:-Dreamview}"
  DREAMVIEW_OPEN_WAIT_PAGE="$(bool_cfg "${DREAMVIEW_OPEN_WAIT_PAGE:-1}")"
  DREAMVIEW_USE_FIXED_REGION="$(bool_cfg "${DREAMVIEW_USE_FIXED_REGION:-1}")"

  DREAMVIEW_ARGS+=(
    --override "algo.apollo.dreamview.enabled=true"
    --override "algo.apollo.dreamview.auto_start=true"
    --override "algo.apollo.dreamview.auto_open=$(bool_cfg "${DREAMVIEW_AUTO_OPEN}")"
    --override "algo.apollo.dreamview.open_wait_page_on_unreachable=${DREAMVIEW_OPEN_WAIT_PAGE}"
    --override "algo.apollo.dreamview.browser_cmd_source=baguang_demo_wrapper"
  )
  if [[ -n "${DREAMVIEW_BROWSER_CMD}" ]]; then
    DREAMVIEW_ARGS+=(--override "algo.apollo.dreamview.browser_cmd=${DREAMVIEW_BROWSER_CMD}")
  fi
  if [[ "$(bool_cfg "${RECORD_DREAMVIEW}")" == "true" ]]; then
    DREAMVIEW_ARGS+=(
      --override "algo.apollo.dreamview.record.enabled=true"
      --override "algo.apollo.dreamview.record.capture_mode=${DREAMVIEW_CAPTURE_MODE}"
      --override "algo.apollo.dreamview.record.mode=window"
      --override "algo.apollo.dreamview.record.window_title=${DREAMVIEW_WINDOW_TITLE}"
      --override "algo.apollo.dreamview.record.fallback_to_screen=true"
      --override "algo.apollo.dreamview.record.use_fixed_region=${DREAMVIEW_USE_FIXED_REGION}"
    )
    if [[ -n "${DREAMVIEW_CAPTURE_REGION}" ]]; then
      if [[ ! "${DREAMVIEW_CAPTURE_REGION}" =~ ^([0-9]+)x([0-9]+)\+(-?[0-9]+),(-?[0-9]+)$ ]]; then
        echo "Invalid DREAMVIEW_CAPTURE_REGION='${DREAMVIEW_CAPTURE_REGION}', expected WIDTHxHEIGHT+X,Y" >&2
        exit 2
      fi
      DREAMVIEW_ARGS+=(
        --override "algo.apollo.dreamview.record.capture_region.width=${BASH_REMATCH[1]}"
        --override "algo.apollo.dreamview.record.capture_region.height=${BASH_REMATCH[2]}"
        --override "algo.apollo.dreamview.record.capture_region.offset_x=${BASH_REMATCH[3]}"
        --override "algo.apollo.dreamview.record.capture_region.offset_y=${BASH_REMATCH[4]}"
      )
    fi
  fi
fi

export CARLA16_PYTHON
export APOLLO_ROOT
export APOLLO_MAP_ROOT

"${CARLA16_PYTHON}" examples/run_followstop.py \
  --config configs/io/examples/followstop_apollo_gt_lateral_enabled_stitcher_v1.yaml \
  --run-dir "${RUN_DIR}" \
  --follow-spectator \
  --follow-spectator-distance "${FOLLOW_SPECTATOR_DISTANCE:-14.0}" \
  --follow-spectator-height "${FOLLOW_SPECTATOR_HEIGHT:-5.0}" \
  --follow-spectator-pitch "${FOLLOW_SPECTATOR_PITCH:--18.0}" \
  "${CARLA_ARGS[@]}" \
  --rig-override "events.lane_invasion=$(if [[ "$(bool_cfg "${APOLLO_DISABLE_LANE_INVASION}")" == "true" ]]; then printf "false"; else printf "true"; fi)" \
  --override run.map=straight_road_for_baguang \
  --override run.ticks="${RUN_TICKS:-750}" \
  --override run.fail_strategy=log_and_continue \
  --override run.post_fail_steps=200 \
  --override scenario.publish_ros2_gt=false \
  --override scenario.ego_idx=0 \
  --override scenario.front_idx=0 \
  --override scenario.auto_align_front_spawn=false \
  --override scenario.front_min_ahead_m=250.0 \
  --override scenario.front_max_ahead_m=350.0 \
  --override scenario.front_pose_offset.x_m=300.0 \
  --override scenario.front_pose_offset.y_m=0.0 \
  --override algo.apollo.transport_mode=carla_direct \
  --override algo.apollo.routing.startup_delay_sec=3.0 \
  --override algo.apollo.routing.scenario_goal_ahead_m=330.0 \
  --override algo.apollo.routing.startup_end_ahead_m=330.0 \
  --override algo.apollo.routing.scenario_goal_min_front_margin_m=20.0 \
  --override algo.apollo.routing.skip_invalid_long_route=false \
  --override algo.apollo.routing.target_speed_mps=22.22 \
  --override algo.apollo.planning.default_cruise_speed_mps=22.22 \
  --override algo.apollo.planning.speed_bounds_decider_lowest_speed_mps=23.61 \
  --override algo.apollo.planning.enable_reference_line_stitching=false \
  --override algo.apollo.planning.enable_trajectory_stitcher=false \
  --override algo.apollo.direct_bridge.require_no_ros2_runtime=true \
  --override algo.apollo.direct_bridge.control_apply_mode=frame_flush_only \
  --override algo.apollo.direct_bridge.stale_world_frame_policy=always_republish \
  --override "algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_enabled=$(bool_cfg "${APOLLO_ENABLE_LATERAL_STABILIZER}")" \
  --override algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_max_abs_steer=0.04 \
  --override algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_k_cte=0.02 \
  --override algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_k_heading=0.35 \
  --override algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_max_cte_m=4.0 \
  --override algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_max_heading_error_deg=20.0 \
  --override algo.apollo.direct_bridge.straight_lane_lateral_stabilizer_max_speed_mps=40.0 \
  --override "algo.apollo.control_mapping.straight_acc_override.enabled=$(bool_cfg "${APOLLO_ENABLE_STRAIGHT_ACC_OVERRIDE}")" \
  --override algo.apollo.control_mapping.straight_acc_override.target_speed_mps=22.22 \
  --override algo.apollo.control_mapping.straight_acc_override.min_cruise_throttle=0.75 \
  --override algo.apollo.control_mapping.straight_acc_override.max_throttle=1.0 \
  --override algo.apollo.control_mapping.straight_acc_override.speed_kp=0.07 \
  --override algo.apollo.control_mapping.straight_acc_override.coast_band_mps=0.4 \
  --override algo.apollo.control_mapping.straight_acc_override.max_brake=1.0 \
  --override algo.apollo.control_mapping.straight_acc_override.brake_kp=0.40 \
  --override algo.apollo.control_mapping.straight_acc_override.slowdown_distance_m=55.0 \
  --override algo.apollo.control_mapping.straight_acc_override.stop_distance_m=11.0 \
  --override algo.apollo.control_mapping.straight_acc_override.full_brake_distance_m=7.0 \
  --override algo.apollo.control_mapping.straight_acc_override.stop_hold_brake=0.70 \
  --override algo.apollo.control_mapping.straight_acc_override.max_e_y_m=1.0 \
  --override algo.apollo.control_mapping.straight_acc_override.max_curvature=0.002 \
  --override algo.apollo.control_mapping.straight_acc_override.max_lateral_gap_m=3.5 \
  --override "algo.apollo.control_mapping.terminal_stop_hold.enabled=$(bool_cfg "${APOLLO_ENABLE_TERMINAL_STOP_HOLD}")" \
  --override algo.apollo.control_mapping.terminal_stop_hold.activate_gap_m=12.0 \
  --override algo.apollo.control_mapping.terminal_stop_hold.release_gap_m=25.0 \
  --override algo.apollo.control_mapping.terminal_stop_hold.activate_speed_mps=2.0 \
  --override algo.apollo.control_mapping.terminal_stop_hold.hold_brake=0.65 \
  --override acceptance.min_speed_mps=20.0 \
  --override acceptance.min_planning_nonempty_trajectory_count=1500 \
  --override acceptance.max_invalid_goal_count=2 \
  --override acceptance.low_speed_creep.max_duration_sec=3.5 \
  --override acceptance.low_speed_creep.require_reached_speed_mps=5.0 \
  "${DREAMVIEW_ARGS[@]}" \
  "${RECORD_ARGS[@]}"
