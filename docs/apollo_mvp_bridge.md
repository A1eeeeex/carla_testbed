# Apollo MVP Bridge

This document defines the current engineering boundary for the CARLA + Apollo
ground-truth MVP bridge. It is intentionally conservative: the first target is
a debuggable closed-loop integration path, not a full sensor-perception stack.

## MVP Route

The MVP does not run real perception. It uses CARLA/testbed ground truth to
feed Apollo enough state to exercise routing, planning, and control:

1. CARLA world and ego state are converted into core contracts.
2. The Apollo adapter publishes ground-truth localization.
3. The Apollo adapter publishes chassis state.
4. The Apollo adapter may publish ground-truth obstacles and traffic-light
   truth when the runtime path supports it.
5. Routing or drive command is sent through the Apollo adapter/bridge layer.
6. Apollo planning and control outputs are observed.
7. Apollo control is mapped into the core `ControlCommand` contract.
8. The testbed applies the resulting command to CARLA through the normal
   control applicator boundary.

This is the first-stage GT closed loop. It is not evidence that Apollo
perception, prediction, map localization, or a production bridge are complete.

## Adapter Boundary

The canonical backend boundary starts at:

```text
carla_testbed/adapters/base.py
ADStackBackend
```

Apollo-specific code lives under `carla_testbed/adapters/apollo/`:

| File | Role |
| --- | --- |
| `channels.py` | Default Apollo channel names. |
| `config.py` | `ApolloMVPConfig` and mapping/runtime parameters. |
| `control_mapping.py` | Pure dict-to-`ControlCommand` conversion. |
| `mock_backend.py` | CI-safe backend for contract tests. |
| `cyber_backend.py` | Lazy CyberRT backend skeleton for local runtime integration. |
| `publishers.py` | Localization, chassis, and GT obstacle publisher skeletons. |
| `subscribers.py` | Control and planning subscriber skeletons. |
| `time_sync.py` | Core frame/time to Apollo timestamp/sequence conversion. |

Core modules must not import CyberRT or Apollo protobufs. Runtime-specific
protobuf conversion belongs behind adapter implementation code.

## Channel Mapping

`ApolloChannels` currently defaults to:

| Purpose | Channel |
| --- | --- |
| Localization | `/apollo/localization/pose` |
| Chassis | `/apollo/canbus/chassis` |
| Obstacles | `/apollo/perception/obstacles` |
| Traffic light | `/apollo/perception/traffic_light` |
| Routing request | `/apollo/routing_request` |
| Routing response | `/apollo/routing_response` |
| Planning | `/apollo/planning` |
| Control | `/apollo/control` |

These are adapter defaults, not universal Apollo truth. If a local Apollo 10
runtime needs a different command path, the runtime adapter must override the
channel config and record that in run artifacts.

## Timestamp And Frame Convention

`carla_testbed.contracts.frame.FrameStamp` is the core time model:

- `frame_id`: monotonic simulator frame id.
- `sim_time_s`: simulator time in seconds.
- `wall_time_s`: optional host wall-clock timestamp.

`ApolloTimeAdapter` converts this to:

- Apollo `timestamp_sec`: from `sim_time_s`.
- Apollo `sequence_num`: from `frame_id`.

The MVP uses simulator time as the source of truth. If a real runtime path uses
wall time or CyberRT time for a particular channel, that choice must be explicit
in adapter diagnostics and run artifacts.

## Control Mapping

The runtime adapter converts Apollo control protobufs to a plain dictionary
first. Then `map_apollo_control_dict_to_command(raw, cfg)` maps that dictionary
to the core `ControlCommand`.

The mapping supports common fields such as:

- throttle percentage or throttle value
- brake percentage or brake value
- steering target / steering percentage
- reverse and hand-brake flags when present

The configurable mapping fields include:

- `steering_sign`
- `steering_scale`
- `throttle_scale`
- `brake_scale`

The output is validated and clamped:

- throttle in `[0, 1]`
- brake in `[0, 1]`
- steer in `[-1, 1]`

CARLA actuation is a separate step handled by the control applicator. Do not
mix Apollo control parsing, bridge policy, and CARLA `VehicleControl` emission
in one place.

## Mock Backend Vs CyberRT Skeleton

`MockApolloBackend` is for CI and contract testing. It records published frame
contexts, returns queued or fixed `ControlCommand` objects, and reports
diagnostics. Passing mock tests proves only the adapter contract and mapping
behavior.

`ApolloCyberRTBackend` is the placeholder for real CyberRT integration. It is
lazy by design:

- importing `carla_testbed.adapters.apollo.cyber_backend` does not require
  CyberRT;
- `start()` attempts to load the runtime;
- missing runtime raises `ApolloRuntimeUnavailableError` with a clear message.

CI should use mock backend tests and import-only CyberRT skeleton tests. Real
CyberRT publishing/subscribing requires local Apollo validation.

## Current Validation Commands

CI-safe checks:

```bash
python -m pytest tests/unit/adapters/apollo -q
python - <<'PY'
import carla_testbed.adapters.apollo.cyber_backend as cb
print("import ok", cb)
PY
```

General no-runtime test set:

```bash
python -m pytest -m "not carla and not apollo and not integration" -q
```

Local runtime validation is still experimental. Once a real Apollo/CyberRT
doctor check is wired, the intended shape is:

```bash
python -m carla_testbed doctor --check apollo,cyber
```

Until there are concrete run artifacts proving routing, planning, control, and
CARLA actuation, document Apollo/CyberRT support as MVP experimental rather
than fully complete.
