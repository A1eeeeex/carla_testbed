# Apollo MVP Adapter Boundary

Apollo is the current MVP target backend for CARLA ground-truth closed-loop
work.

This namespace contains the runtime-neutral adapter boundary, default channel
names, control mapping helpers, and CI-safe mock backend. The runnable
experimental bridge remains outside this package until promoted behind
`carla_testbed.adapters.base.ADStackBackend`.

Boundary rules:

- Keep Apollo channel names and protobuf conversions inside the adapter/bridge
  implementation.
- Do not import Apollo protobufs from core runner, scenario, sensor, recording,
  evaluation, or contract modules.
- Do not treat old Apollo-CARLA bridge repositories as drop-in compatible; use
  them as structure and mapping references only.
