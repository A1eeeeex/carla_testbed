# Followstop Migration Playbook

This playbook is for moving the repository to a new host and quickly restoring the followstop Apollo flow.

## 1. Current repository risks to address

- Runtime output directories (`runs/`) are large and host-specific. Do not migrate old run artifacts by default.
- The repo has both modern and compatibility entrypoints (`carla_testbed` vs `io/scripts`). Use the modern entrypoint to avoid confusion.
- Apollo runtime depends on external assets not stored in git:
  - CARLA binary/runtime
  - Apollo environment/container
  - Apollo map files (for current profiles)

## 2. Pre-migration checklist

### 2.1 Code and configuration

- Ensure branch is clean and pushed.
- Preserve these directories:
  - `algo/`
  - `carla_testbed/`
  - `tbio/`
  - `configs/`
  - `tools/apollo10_cyber_bridge/`
  - `examples/`
  - `docs/`
- Exclude by default:
  - `runs/`
  - `.venv/`
  - `dist/`
  - `dumps/`

### 2.2 Host dependencies

- Python `>=3.9`
- `ffmpeg`
- `docker` (for Apollo mode used in current setup)
- CARLA runtime (current team baseline is 0.9.16)

### 2.3 External assets and env

- Apollo map referenced by profile (`configs/io/examples/followstop_apollo_gt.yaml`):
  - `algo.apollo.bridge.map_file`
- Host local env:
  - `.env` (not committed)
  - optional `configs/local.yaml` (not committed)

## 3. Migration package preparation

Use tracked source files only:

```bash
bash tools/prepare_migration_bundle.sh
```

The script writes a `.tar.gz` under `dist/` by default.

Alternative (full git clone) is preferred for collaboration:

```bash
git clone <repo-url>
```

## 4. Post-migration setup on new host

### 4.1 Bootstrap

```bash
bash tools/bootstrap_native.sh
python -m carla_testbed doctor
```

If using conda workflow, create/activate your team env first, then run the same commands.

### 4.2 Local env file

Create `.env` with host-specific values (example):

```dotenv
CARLA_ROOT=/path/to/CARLA_0.9.16
```

### 4.3 Apollo prerequisites

- Verify Docker can start Apollo runtime used by your profile.
- Verify map file path in profile exists on new host.

## 5. Fast validation sequence

### Step A: simulator sanity

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_dummy.yaml \
  --override run.ticks=120
```

Expect:

- run completes
- `runs/<run>/summary.json` exists

### Step B: followstop Apollo flow

```bash
python -m carla_testbed run \
  --config configs/io/examples/followstop_apollo_gt.yaml \
  --override run.ticks=600 \
  --override runtime.carla.start=true
```

Expect key artifacts:

- `runs/<run>/artifacts/cyber_bridge_stats.json`
- `runs/<run>/artifacts/bridge_health_summary.json`
- `runs/<run>/timeseries.csv`

## 6. Quick acceptance criteria after migration

- `routing_request_count >= 1`
- `loc_count > 0` and `chassis_count > 0`
- `video/dual_cam/demo_third_person.mp4` generated if recording is enabled
- vehicle speed rises above zero in `timeseries.csv`

## 7. Collaboration baseline after migration

- Use feature branches; avoid direct edits on shared long-running branch.
- Keep host-specific values out of git (`.env`, `configs/local.yaml`).
- Prefer `--override` for experiment changes, then backport stable settings to profile files.
- Attach `runs/<run>/artifacts/*` in review discussions for behavior regressions.

