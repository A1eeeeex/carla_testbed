# Platform RunPlan

`RunPlan` is the new offline planning layer for making CARLA testbed runs
selectable by platform, algorithm, scenario, recording profile, and gate
profile. It is a migration surface, not a new runtime stack yet.

Current scope:

- Compile profile selections into `run_plan.v1`.
- List available platform / algorithm / scenario / recording / gate profiles.
- Compile a suite matrix into per-run `*.plan.resolved.yaml` files.
- Resolve backend facade contracts and recording profile metadata without
  importing runtime stacks.
- Build an evidence bundle and gate summary from existing run artifacts.
- Package small run evidence for review without copying large videos or bags by
  default.
- Keep existing Apollo/Town01 legacy runtime entrypoints unchanged.

Current non-scope:

- It does not start CARLA, Apollo, Autoware, ROS2, or CyberRT.
- It does not replace `tools/run_town01_*` yet.
- It does not make a behavior or natural-driving claim.
- It does not infer missing evidence as success.

## Commands

List profile registry entries:

```bash
python -m carla_testbed list platforms
python -m carla_testbed list algorithms
python -m carla_testbed list scenarios
python -m carla_testbed list recording
python -m carla_testbed list recorders
python -m carla_testbed list gates
python -m carla_testbed list backends
```

Compile one plan:

```bash
python -m carla_testbed plan \
  --platform apollo_cyberrt \
  --algorithm apollo/apollo10_carla_gt \
  --scenario town01/lane_keep_097 \
  --record demo \
  --gate diagnostic \
  --out /tmp/plan.resolved.yaml
```

Compile a suite matrix:

```bash
python -m carla_testbed plan \
  --suite configs/suites/town01_natural_driving.platform.yaml \
  --out /tmp/town01_platform_plans
```

Dry-run a suite matrix and write a manifest plus matrix CSV:

```bash
python -m carla_testbed suite dry-run \
  --suite configs/suites/town01_natural_driving.platform.yaml \
  --out /tmp/town01_platform_suite
```

Preview typed RunPlan dispatch without starting runtime:

```bash
python -m carla_testbed run \
  --plan /tmp/town01_platform_suite/plans/<run_id>.plan.resolved.yaml \
  --plan-only
```

Build evidence and gate reports from an existing run directory:

```bash
python -m carla_testbed analyze \
  --run-dir runs/<run_id> \
  --plan /tmp/town01_platform_suite/plans/<run_id>.plan.resolved.yaml
```

This writes:

- `analysis/evidence_bundle/evidence_bundle.json`
- `analysis/gate/gate_report.json`
- `analysis/gate/gate_summary.md`

Package review evidence without large runtime media by default:

```bash
python -m carla_testbed pack \
  --run-dir runs/<run_id> \
  --out /tmp/<run_id>_evidence.tar.gz
```

## Profile Boundary

Profiles are composable:

- `configs/platforms/` chooses the runtime platform such as `apollo_cyberrt`
  or `autoware_ros2`.
- `configs/algorithms/` chooses the algorithm variant within that platform.
- `configs/scenarios/` describes map, route, actors, requirements, and success
  intent.
- `configs/recording/` chooses neutral and platform-specific recorders.
- `configs/gates/` chooses required analyzers and claim requirements.
- `configs/suites/` expands scenarios x platforms x algorithms x recording x
  gates into multiple RunPlans.

The current platform suite includes representative Town01 lane keeping, curve
diagnostic, junction, and traffic-light scenarios. Curve entries remain
diagnostic until route, localization, HDMap projection, reference-line, matched
point, target point, and control evidence are complete enough for a hard gate.

Recording profiles remain operator evidence. A `demo` profile can request
CARLA video, Dreamview, RViz, or rosbag artifacts, but recording alone is not a
natural-driving pass. Claim boundaries still require the existing evidence
chain: route health, channel health, localization contract, HDMap projection,
reference-line contract, planning materialization, control handoff/control
health, prediction/obstacle/traffic-light evidence when applicable, assist
ledger, and `natural_driving_report.json`.

## Migration Rule

Existing legacy tools remain compatibility entrypoints. New platform work
should compile or consume `RunPlan` first, then dispatch to a backend wrapper.
Do not add new one-off runtime logic to `tools/run_*.py` when the same choice
can be represented as a platform, algorithm, scenario, recording, or gate
profile.

`RunPlan` dispatch is intentionally conservative today:

- `python -m carla_testbed run --plan ... --plan-only` only previews the
  backend contract.
- Real online execution still uses the existing operational runners until a
  backend wrapper owns that runtime path.
- Legacy fallback is compatibility behavior; it is not evidence that the new
  platform dispatch layer is complete.
