# AGENTS.md

This repository builds and evaluates a CARLA–ROS2–Apollo ground-truth simulation pipeline. Treat it as an engineering system with a strict evidence chain, not as a loose collection of scripts.

## Mission
- Primary goal: make Apollo run stably in CARLA Town01 across full routes, not just a single local point fix.
- Secondary goal: improve diagnosability, repeatability, and acceptance evaluation for the end-to-end pipeline.
- Tertiary goal: keep the stack extensible for later sensor simulation and scenario-library integration.

## Current Phase 1 Scope
- Phase 1 is a multi-backend scenario-platform engineering track, not an
  Apollo-only capability claim.
- Apollo remains the reference external AD-stack backend, while
  `carla_builtin` / `simple_acc_route_follower` is the current
  PlanningControlBackend category implementation for scenario playback,
  artifact, and comparison validation.
- Invalid Phase 1 runs such as `backend_not_ready`,
  `missing_required_artifact`, or `missing_target_actor` are setup/evidence
  failures and must not be counted as backend behavior losses.
- A successful `carla_builtin` run is diagnostic scenario evidence only; it is
  not Apollo, Autoware, or natural-driving capability evidence.

## Canonical Documentation Layers
- `AGENTS.md`
  - Repo-wide execution manual, workflow rules, and documentation ownership.
- `reference_pack/reference/`
  - Canonical knowledge base for truths, contracts, failure modes, debug playbooks, and durable decisions.
- `docs/`
  - Tracked operator/onboarding/design docs only.
- `artifacts/` and `runs/`
  - Transient evidence and run outputs. These are inputs to analysis, not the long-term source of truth.

## Read This First
Before changing code, configs, or workflow, read these files in order when relevant:
1. `README.md`
2. `docs/README.md`
3. `reference_pack/reference/00_index.md`
4. `reference_pack/reference/01_system_overview/project_map.md`
5. `reference_pack/reference/01_system_overview/runtime_flow.md`
6. `reference_pack/reference/03_interface_contracts/topic_contracts.md`
7. `reference_pack/reference/03_interface_contracts/control_semantics.md`
8. `reference_pack/reference/04_runtime_and_debug/debug_playbook.md`
9. `reference_pack/reference/05_verified_findings/current_truths.md`
10. `reference_pack/reference/05_verified_findings/to_verify_items.md`
11. `reference_pack/reference/07_experiments_and_acceptance/acceptance_criteria.md`

## Project Ground Truth
- Target stack is Apollo 10.0 + CARLA 0.9.16 + ROS2 Humble on Ubuntu 22.04 unless a file explicitly states otherwise.
- Town01 route-level stability matters more than passing one curated micro-case.
- `runtime_contract.status = aligned` is a prerequisite for using a run as capability evidence.
- Do not assume old Apollo–CARLA bridge repositories are directly compatible; they are reference patterns only.
- Do not silently downgrade architecture assumptions to Apollo 5/6/8-era behavior.

## Working Principles
- Prefer end-to-end root-cause analysis over local patching.
- Prefer minimum necessary changes over broad rewrites.
- Prefer explicit evidence from logs, artifacts, configs, and code over intuition.
- Preserve observability; do not remove diagnostics unless replacing them with something better.
- When uncertain, instrument first, then change behavior.

## Required Workflow For Non-Trivial Tasks
For debugging, routing, planning, control, bridge semantics, startup, runtime contract, or evaluation work:
1. Inspect the relevant `reference_pack/reference/` pages first.
2. Inspect current configs and the latest effective config snapshot.
3. Inspect the newest relevant run artifacts/logs before proposing a fix.
4. State the likely failure chain or behavior chain.
5. Make the smallest coherent code/config/doc change.
6. Define a concrete validation procedure.
7. Update the correct documentation layer before concluding.

## Failure Analysis Order
When ego behavior is wrong, check in this order unless evidence clearly says otherwise:
1. Environment/version mismatch and CARLA `world_ready`
2. Bridge runtime/materialization (`bridge_runtime_preflight`, stats materialization)
3. Goal validity, routing send, and routing success
4. Planning non-empty output, reference line, and route segment validity
5. Planning -> control handoff and control process survival
6. Localization / chassis / time synchronization consistency
7. Control-command semantics and actuation mapping
8. CARLA actuation / physics application
9. Traffic light or scenario side effects

## Documentation Ownership
- Stable truth / interface contract / durable decision:
  - update `reference_pack/reference/`
- Operator guide / onboarding / workflow:
  - update `docs/`
- Raw run analysis / evidence / one-off comparison:
  - keep in `artifacts/` or `runs/`
- Repo-wide rules and workflow:
  - update `AGENTS.md`

## Documentation Hard Rules
- Do not create new long narrative reports in `docs/` or repo root when the content should be a short update to `reference/`.
- Do not leave durable knowledge only in `artifacts/`, chat transcripts, or ignored scratch files.
- If a result is still uncertain or conflicts with other evidence, put it in `reference_pack/reference/05_verified_findings/to_verify_items.md`.
- For Apollo no-interference natural-driving claims, require the documented
  channel-health, localization, HDMap projection, reference-line,
  control-handoff, control-health, link-health, traffic-light/obstacle GT,
  natural-driving, and assist-ledger artifacts before writing a capability
  claim.
- `docs/prompt_usually_used.md` is local scratch and is not part of the tracked project documentation surface.

## Coding Rules
- Do not rename public files, core directories, or config structure unless the task explicitly requires it.
- Avoid introducing new dependencies unless clearly justified.
- Keep scripts runnable from the local repo with clear paths and comments.
- Prefer configuration-backed behavior over hard-coded magic numbers.
- If adding a constant or heuristic, explain why and where it should later be calibrated.
- Keep comments concise and high-value.

## Evidence Standard
Do not claim a problem is solved unless you can point to at least one of:
- successful run artifacts
- log evidence
- metrics satisfying acceptance criteria
- reproducible validation steps

## Acceptance Mindset
Success is not “the code looks better.”
Success is one of:
- route execution becomes measurably more stable
- diagnostics become materially more informative
- bridge/runtime semantics become clearer and easier to validate
- acceptance evaluation becomes more rigorous and repeatable

## Progress Reporting
When summarizing a significant task, prioritize:
1. what is proven
2. what is still suspected
3. what changed since the previous run/baseline
4. what currently blocks route-level stability
5. what the next highest-value validation should be
