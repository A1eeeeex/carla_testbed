#!/usr/bin/env python3
"""
Speed comparison test: lane_keep + curve_lane_follow at 20, 40, 60 km/h.

This runner keeps the current Town01 lane-follow lateral fixes frozen. In
particular, it must not re-enable Apollo trajectory stitching: online timing
probes showed stitcher-on runs can create lon_diff replans, short trajectories,
and terminal low-speed fallback behavior that is not part of the current stable
lane-follow profile.
"""
import json
import os
import sys
import subprocess
import time
import yaml
import numpy as np
from pathlib import Path
from datetime import datetime

REPO = Path("/home/ubuntu/carla_testbed")
BASE_CONFIG = REPO / "configs/io/examples/speed_test_base.yaml"
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RUNS_DIR = REPO / "runs" / f"speed_test_{_ts}"
DEFAULT_TICKS = 900
TRAJECTORY_STITCHER_ENABLED = False
ACTUATOR_CALIBRATION_FILE = (
    "artifacts/actuator_calibration_library/"
    "e969a34a5f43fd25/carla_actuator_calibration.json"
)

# Test matrix: (speed_kmh, speed_mps, scenario, route_id, capability)
SPEEDS = [
    ("20kph", 5.56, "lane_keep", "town01_rh_spawn097_goal046", "lane_keep"),
    ("20kph", 5.56, "curve", "town01_rh_spawn217_goal048", "curve_lane_follow"),
    ("40kph", 11.11, "lane_keep", "town01_rh_spawn097_goal046", "lane_keep"),
    ("40kph", 11.11, "curve", "town01_rh_spawn217_goal048", "curve_lane_follow"),
    ("60kph", 16.67, "lane_keep", "town01_rh_spawn097_goal046", "lane_keep"),
    ("60kph", 16.67, "curve", "town01_rh_spawn217_goal048", "curve_lane_follow"),
]

def create_config(speed_label, speed_mps, scenario_type, route_id, capability):
    """Create a speed-specific test config YAML."""
    config = {
        "extends": str(BASE_CONFIG),
        "run": {
            "capability_profile": capability,
            "profile_name": f"speed_test_{speed_label}_{scenario_type}_{route_id}",
            "comparison_label": f"speed_test_{speed_label}_{scenario_type}",
        },
        "scenario": {
            "route_health": {
                "scenario_class": capability,
                "capability_profile": capability,
                "random_seed": 1,
            }
        },
        "algo": {
            "apollo": {
                "bridge": {
                    "localization_time_source": "sim_time",
                    "auto_routing": {"target_speed_mps": speed_mps},
                },
                "routing": {
                    "target_speed_mps": speed_mps,
                },
                "planning": {
                    "default_cruise_speed_mps": speed_mps,
                    "enable_trajectory_stitcher": TRAJECTORY_STITCHER_ENABLED,
                }
            }
        }
    }

    config_dir = RUNS_DIR / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = config_dir / f"{speed_label}_{scenario_type}.yaml"

    with open(cfg_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    return cfg_path


def build_run_command(config_path, label, route_id, capability, speed_mps):
    """Build the route-health command with speed overrides applied last.

    The route-health helper applies capability defaults after loading the config
    file. Keep the speed-test target speed as an explicit CLI override so the
    test matrix cannot silently collapse back to the lane_keep default.
    """
    return [
        sys.executable,
        "tools/run_town01_route_health.py",
        "run",
        "--capability-profile",
        capability,
        "--config",
        str(config_path),
        "--ticks",
        str(DEFAULT_TICKS),
        "--batch-root",
        str(RUNS_DIR / label),
        "--comparison-label",
        label,
        "--route-id",
        route_id,
        "--carla-launch-attempts",
        "1",
        "--carla-world-ready-timeout-sec",
        "180.0",
        "--carla-retry-delay-sec",
        "2.0",
        "--carla-ignore-memory-preflight",
        "--enable-lateral",
        "--no-enable-guard",
        "--override",
        "run.post_fail_steps=0",
        "--override",
        "algo.apollo.control_mapping.actuator_mapping_mode=physical",
        "--override",
        f"algo.apollo.control_mapping.physical.calibration_file={ACTUATOR_CALIBRATION_FILE}",
        "--override",
        "algo.apollo.control_mapping.physical.allow_legacy_fallback=false",
        "--override",
        "algo.apollo.control_mapping.physical.map_steering=true",
        "--override",
        "algo.apollo.control_mapping.physical.map_longitudinal=false",
        "--override",
        "algo.apollo.control_mapping.physical.map_throttle=false",
        "--override",
        "algo.apollo.control_mapping.physical.map_brake=true",
        "--override",
        f"algo.apollo.routing.target_speed_mps={float(speed_mps):.2f}",
        "--override",
        f"algo.apollo.bridge.auto_routing.target_speed_mps={float(speed_mps):.2f}",
        "--override",
        f"algo.apollo.planning.default_cruise_speed_mps={float(speed_mps):.2f}",
        "--override",
        f"algo.apollo.planning.enable_trajectory_stitcher={str(TRAJECTORY_STITCHER_ENABLED).lower()}",
    ]


def run_test(config_path, label, route_id, capability, speed_mps):
    """Run a single test and return the run directory."""
    print(f"\n{'='*70}")
    print(f"  RUNNING: {label}")
    print(f"  Config:  {config_path}")
    print(f"{'='*70}")

    run_subdir = RUNS_DIR / label
    run_subdir.mkdir(parents=True, exist_ok=True)
    cmd = build_run_command(config_path, label, route_id, capability, speed_mps)

    print(f"  Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO),
            capture_output=True,
            text=True,
            timeout=620,
        )
        print(f"  Exit code: {result.returncode}")
        if result.stdout:
            # Print last 20 lines
            lines = result.stdout.strip().split('\n')
            for line in lines[-20:]:
                print(f"  [stdout] {line}")
        if result.returncode != 0 and result.stderr:
            print(f"  [stderr] {result.stderr[:500]}")
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT after 600s")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

    return run_subdir


def extract_metrics(run_dir):
    """Extract key metrics from run artifacts."""
    if run_dir is None:
        return None

    # Find the actual run subdirectory (may be nested)
    summary_files = list(run_dir.glob("**/summary.json"))
    if not summary_files:
        print(f"  No summary.json found in {run_dir}")
        return None

    summary_path = summary_files[0]
    summary = json.loads(summary_path.read_text())
    run_subdir = summary_path.parent

    metrics = {
        "success": summary.get("success"),
        "fail": summary.get("fail"),
        "fail_reason": None,
        "speed_p95": summary.get("speed_abs_p95"),
        "max_speed": None,
        "route_distance_m": None,
        "route_completion_ratio": None,
        "duration_s": summary.get("sim_duration_s"),
    }

    acc = summary.get("acceptance", {})
    checks = acc.get("checks", {})
    metrics["fail_reason"] = acc.get("fail_reason")

    ms = checks.get("max_speed_mps", {})
    if isinstance(ms, dict):
        metrics["max_speed"] = ms.get("actual")

    rd = checks.get("route_distance_achieved_m", {})
    if isinstance(rd, dict):
        metrics["route_distance_m"] = rd.get("actual")

    rc = checks.get("route_completion_ratio", {})
    if isinstance(rc, dict):
        metrics["route_completion_ratio"] = rc.get("actual")

    # Extract from apollo_control_raw.jsonl
    raw_file = run_subdir / "artifacts" / "apollo_control_raw.jsonl"
    if raw_file.exists():
        steering = []
        speed = []
        lat_err = []
        heading = []
        ref_heading = []

        with open(raw_file) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    d = json.loads(line.strip())
                    raw = d.get("apollo_control_raw", {})
                    st = raw.get("steering_target")
                    sp = raw.get("speed")
                    le = raw.get("debug_simple_lat_lateral_error")
                    h = raw.get("debug_simple_lat_heading")
                    rh = raw.get("debug_simple_lat_ref_heading")
                    if st is not None:
                        steering.append(float(st))
                    if sp is not None:
                        speed.append(float(sp))
                    if le is not None:
                        lat_err.append(float(le))
                    if h is not None:
                        heading.append(float(h))
                    if rh is not None:
                        ref_heading.append(float(rh))
                except:
                    pass

        if steering:
            s = np.array(steering)
            metrics["steer_mean"] = float(np.mean(np.abs(s)))
            metrics["steer_maxabs"] = float(np.max(np.abs(s)))
            metrics["steer_nonzero_ratio"] = float(np.mean(np.abs(s) > 0.01))

        if speed:
            sp = np.array(speed)
            metrics["speed_mean"] = float(np.mean(sp))
            metrics["speed_p50"] = float(np.median(sp))
            metrics["speed_max"] = float(np.max(sp))

        if lat_err:
            le = np.array(lat_err)
            metrics["lat_error_mean"] = float(np.mean(np.abs(le)))
            metrics["lat_error_max"] = float(np.max(np.abs(le)))
            metrics["lat_error_p95"] = float(np.percentile(np.abs(le), 95))

        if heading and ref_heading and len(heading) == len(ref_heading):
            h = np.array(heading)
            rh = np.array(ref_heading)
            diff = np.abs(h - rh)
            metrics["heading_drift_max"] = float(np.max(diff))
            metrics["heading_drift_mean"] = float(np.mean(diff))
            metrics["heading_drift_p95"] = float(np.percentile(diff, 95))

    # Extract lane_invasion from events
    events_file = run_subdir / "artifacts" / "events.jsonl"
    if events_file.exists():
        lane_invasion_count = 0
        with open(events_file) as f:
            for line in f:
                if "LANE_INVASION" in line:
                    lane_invasion_count += 1
        metrics["lane_invasion_count"] = lane_invasion_count

    return metrics


def main():
    print("=" * 70)
    print("  SPEED COMPARISON TEST")
    print("  20 / 40 / 60 km/h  x  lane_keep + curve_lane_follow")
    print(f"  Runs dir: {RUNS_DIR}")
    print("=" * 70)

    # Skip CARLA check - testbed CLI manages CARLA connection

    # Create all configs
    configs = []
    for speed_label, speed_mps, scenario_type, route_id, capability in SPEEDS:
        cfg = create_config(speed_label, speed_mps, scenario_type, route_id, capability)
        label = f"{speed_label}_{scenario_type}"
        configs.append((label, cfg, speed_label, speed_mps, scenario_type, route_id, capability))

    print(f"\nCreated {len(configs)} test configs")

    # Run all tests
    results = []
    for label, cfg_path, speed_label, speed_mps, scenario_type, route_id, capability in configs:
        sys.stdout.flush()
        run_dir = run_test(cfg_path, label, route_id, capability, speed_mps)
        metrics = extract_metrics(run_dir)
        if metrics:
            metrics["label"] = label
            metrics["speed_label"] = speed_label
            metrics["speed_mps"] = speed_mps
            metrics["scenario_type"] = scenario_type
            results.append(metrics)
            print(f"\n  >> Result: success={metrics['success']}  "
                  f"speed_p95={metrics.get('speed_p95','?')}  "
                  f"lat_max={metrics.get('lat_error_max','?'):.3f}")
        else:
            results.append({
                "label": label,
                "speed_label": speed_label,
                "speed_mps": speed_mps,
                "scenario_type": scenario_type,
                "success": False,
                "fail_reason": "RUNNER_ERROR",
            })

        # Brief pause between runs
        print("\n  Cooling down for 5s...")
        time.sleep(5)

    # Save results
    results_path = RUNS_DIR / "speed_test_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    # Print comparison table
    print("\n\n" + "=" * 100)
    print("  SPEED TEST COMPARISON REPORT")
    print("=" * 100)

    header = f"{'Test':<25} {'Speed':>6} {'OK':>5} {'v_p95':>6} {'v_mean':>7} {'v_max':>7} {'|steer|':>7} {'lat_max':>7} {'lat_p95':>7} {'hd_drift':>8} {'dist_m':>7}"
    print(header)
    print("-" * 100)

    for r in results:
        ok = "YES" if r.get("success") else "NO"
        label = r.get("label", "?")
        spd = f"{r.get('speed_mps',0):.1f}m/s"
        vp = f"{r.get('speed_p95',0):.1f}" if r.get('speed_p95') else "-"
        vm = f"{r.get('speed_mean',0):.1f}" if r.get('speed_mean') else "-"
        vx = f"{r.get('speed_max',0):.1f}" if r.get('speed_max') else "-"
        st = f"{r.get('steer_maxabs',0):.1f}" if r.get('steer_maxabs') else "-"
        lm = f"{r.get('lat_error_max',0):.3f}" if r.get('lat_error_max') else "-"
        lp = f"{r.get('lat_error_p95',0):.3f}" if r.get('lat_error_p95') else "-"
        hd = f"{r.get('heading_drift_max',0):.3f}" if r.get('heading_drift_max') else "-"
        dm = f"{r.get('route_distance_m',0):.0f}" if r.get('route_distance_m') else "-"

        print(f"{label:<25} {spd:>6} {ok:>5} {vp:>6} {vm:>7} {vx:>7} {st:>7} {lm:>7} {lp:>7} {hd:>8} {dm:>7}")

    print("-" * 100)
    print(f"\nFull results saved to: {results_path}")

    # Per-speed analysis
    print("\n\n--- PER-SPEED ANALYSIS ---")
    for speed_label in ["20kph", "40kph", "60kph"]:
        speed_results = [r for r in results if r.get("speed_label") == speed_label]
        if not speed_results:
            continue

        all_ok = all(r.get("success") for r in speed_results)
        status = "ALL PASS" if all_ok else "SOME FAILED"
        lat_maxs = [r.get("lat_error_max", 0) or 0 for r in speed_results]
        hd_maxs = [r.get("heading_drift_max", 0) or 0 for r in speed_results]

        print(f"\n{speed_label} ({speed_results[0].get('speed_mps',0):.1f} m/s): {status}")
        for r in speed_results:
            print(f"  {r['scenario_type']}: success={r.get('success')}  "
                  f"fail_reason={r.get('fail_reason','-')}  "
                  f"lat_max={r.get('lat_error_max','?'):.3f}  "
                  f"hd_drift={r.get('heading_drift_max','?'):.3f}")


if __name__ == "__main__":
    main()
