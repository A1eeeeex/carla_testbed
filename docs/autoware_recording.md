# Autoware Demo Recording Evidence

Autoware does not have a Dreamview-equivalent web UI in this testbed. The
current demo evidence bundle is:

- CARLA third-person video: `video/dual_cam/demo_third_person.mp4`
- Autoware RViz2 operator-view video: `video/rviz/autoware_rviz.mp4`
- rosbag2 archive: `rosbag2/autoware_demo/`
- Autoware control log: `artifacts/autoware_control.jsonl`
- run diagnostics: `summary.json`, `timeseries.csv`, route/channel/control
  reports, and natural-driving report when available

These recordings are operator evidence. They prove that the run was visible and
archived, not that Autoware passed a natural-driving gate.

## Normalized Evidence Report

Use the offline analyzer:

```bash
python tools/analyze_autoware_evidence.py \
  --run-dir runs/<autoware_run> \
  --out runs/<autoware_run>/analysis/autoware_evidence
```

It writes:

- `autoware_evidence_report.json`
- `autoware_evidence_summary.md`

The report uses schema `autoware_evidence.v1` and records:

- RViz video presence
- rosbag2 presence and lightweight topic inspection
- CARLA third-person video presence
- control log presence
- summary/timeseries presence
- route-health, channel-health, control-health, control-attribution, and
  natural-driving report presence
- `can_compare_with_apollo`

`can_compare_with_apollo=true` requires the same acceptance gate artifacts used
for Apollo comparison, or an explicit `natural_driving_report.json`. RViz,
rosbag, or CARLA video alone is not enough.

## Acceptance Boundary

Autoware and Apollo should be compared through the same acceptance gate, with
`natural_driving_report.json` as the summary artifact:

- `route_health.json`
- channel-health report
- `control_health_report.json`
- `control_attribution_report.json`
- `natural_driving_report.json`
- traffic-light contract/behavior artifacts for traffic-light scenarios

The natural-driving evaluator treats `backend=autoware` with the same route,
channel, and control gates. It additionally requires control-attribution
evidence before an Autoware run can pass the gate. Missing traffic-light
contract evidence keeps traffic-light scenarios at `insufficient_data`.

## CI-Safe Checks

These commands do not start Autoware, ROS2, CARLA, or Apollo:

```bash
python -m pytest tests/test_autoware_evidence.py -q

python tools/analyze_autoware_evidence.py \
  --run-dir tests/fixtures/autoware/demo_recording \
  --out /tmp/autoware_evidence
```

Use real RViz/rosbag/CARLA recordings only as local verification artifacts.
