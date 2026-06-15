# Apollo Town01 HDMap Alignment

This page describes the evidence path for checking whether a Town01 Apollo run
uses the expected HDMap and whether the scenario route, Apollo RoutingResponse,
and Planning reference-line evidence describe the same route.

## Claim Boundary

HDMap alignment is not a driving success claim. It only answers whether the
map/route/reference-line inputs are coherent enough to continue toward
localization, control, perception, and natural-driving gates. Do not use routing
success, Planning non-empty trajectory, or CARLA waypoint projection as a
substitute for this evidence.

## Required Reports

Generate these from a run directory:

```bash
python tools/analyze_apollo_map_identity.py --run-dir <run>
python tools/analyze_apollo_hdmap_projection.py \
  --projection <run>/artifacts/apollo_hdmap_projection.jsonl \
  --out <run>/analysis/apollo_hdmap_projection
python tools/export_apollo_hdmap_projection.py \
  --run-dir <run> \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --include-route-samples \
  --include-start-goal \
  --min-route-s-coverage 300 \
  --analyze
python tools/analyze_apollo_route_contract.py \
  --run-dir <run> \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --out <run>/analysis/apollo_route_contract
python tools/analyze_route_identity.py --run-dir <run> --out <run>/analysis/route_identity
python tools/analyze_apollo_reference_line_contract.py --run-dir <run> \
  --out <run>/analysis/apollo_reference_line_contract
python tools/analyze_apollo_map_route_alignment.py --run-dir <run>
```

The alignment command writes:

- `artifacts/map_identity_report.json`
- `artifacts/lane_equivalence_town01.json`
- `analysis/apollo_map_route_alignment/apollo_map_route_alignment_report.json`
- `analysis/apollo_map_route_alignment/apollo_map_route_alignment_summary.md`

`apollo_map_route_alignment_report.json` is deliberately layered:

- `static_map_identity`: file/config evidence only. It can prove map roots,
selected map names, component existence, and hashes.
- `runtime_projection`: Apollo HDMap API projection evidence. It can prove that
route/localization samples land on specific Apollo lanes with bounded lateral
and heading error.
- `routing_response`: decoded Apollo RoutingResponse evidence. It records
Apollo's actual ordered lane windows and route length.
- `planning_reference_line`: Planning reference-line and route-segment evidence.
It checks whether Planning materialized reference-line data for the same routed
lane windows.

Passing a lower layer never promotes an upper layer. Static map identity can
pass while runtime projection is missing, and routing can produce a valid Apollo
route while still not matching the scenario claim route.

## What Map Is Apollo Actually Using?

`map_identity_report.json` merges evidence from `manifest.json`,
`config.resolved.yaml`, `typed_runtime.effective_legacy.yaml`,
`artifacts/apollo_bridge_effective.yaml`, `artifacts/map_contract_guard.json`,
bridge summaries, Dreamview capture metadata, and the projection exporter
`map_dir`.

For claim-grade map identity:

- CARLA world should be `Town01`.
- Expected Apollo map is `carla_town01`.
- Bridge effective `map_file`, Dreamview selected map, projection exporter
`--map-dir`, and Apollo runtime map root must refer to the same derivation chain.
- `base_map.txt`, `routing_map.txt`, and `sim_map.txt` must all exist.
- Hashes for base/routing/sim maps should be recorded.

Host and container paths can differ if `map_contract_guard.json` proves they are
aliases for the same runtime map root. For example, a host path under
`/home/ubuntu/Apollo10.0/application-core/data/map_data/carla_town01` and a
container path `/apollo/modules/map/data/carla_town01` can be consistent when the
guard records matching component hashes and `same_derivation_chain=true`.

## Third-Party HDMap Failure Modes

A third-party Town01 Apollo HDMap can fail in several different ways:

- `mixed_map_roots`: bridge, Dreamview, exporter, or Apollo runtime point to
different map roots.
- `map_files_missing`: one of `base_map.txt`, `routing_map.txt`, or `sim_map.txt`
is absent.
- `map_hash_mismatch`: base/routing/sim components do not share the expected
derivation chain.
- `projection_missing`: Apollo HDMap API projection was not exported or the
artifact is empty.
- `projection_geometry_bad`: official Apollo projection rows exist but heading or
lateral error is too high.
- `lane_equivalence_missing`: projection geometry is usable, but CARLA
route lane ids and Apollo HDMap lane ids have not been mapped by an ordered
equivalence table.
- `boundary_transition_ambiguous`: failed route samples are concentrated near a
lane-window boundary or junction transition, so the issue is not yet proven to
be a true route mismatch.
- `heading_convention_mismatch`: lane ids may be spatially compatible, but route
trace heading/chord heading and Apollo lane tangent heading disagree beyond the
`0.03 rad` threshold.
- `routing_not_claim_route`: a verified lane equivalence exists and Apollo
RoutingResponse still does not match the claim route.
- `reference_line_missing`: routing aligns, but Planning reference-line evidence
is missing or unproven.

## Lane Namespace Rule

CARLA waypoint/OpenDRIVE lane ids and Apollo HDMap lane ids are different
namespaces. A set overlap is not enough. `lane_equivalence_town01.json` records
ordered signatures for the CARLA claim route and Apollo routing lane window. If
there is no verified equivalence table, the result stays `insufficient_data`
instead of pass.

Verified lane equivalence requires route projection rows with `carla_lane_key`,
`route_s`, `nearest_lane_id`, `projection_l`, and `heading_error_rad` from
`source="apollo_hdmap_api"`. For route-trace samples, the exporter also records
`route_trace_heading_error_rad`, `route_chord_heading_error_rad`, and
`route_heading_source` so the report can separate raw trace heading diagnostics
from the heading evidence used for claim-grade lane equivalence. The route
sample projection should prove each CARLA lane maps to one dominant Apollo lane,
with lateral p95 <= `0.30 m`, claim heading p95 <= `0.03 rad`, and ordered
projected Apollo lane windows matching the decoded RoutingResponse.

`lane_equivalence_town01.json` is pair-level evidence. A pair can be
`verified`, `ambiguous`, `failed`, or `insufficient_data`, with a classification
such as `verified_equivalent`, `boundary_transition_ambiguous`,
`heading_convention_mismatch`, `sampling_density_insufficient`,
`topology_mismatch`, or `routing_not_claim_route`. Lane id equality is not
enough: an exact id match with high heading error still cannot pass. Lane id
difference is also not automatically wrong: a CARLA lane and an Apollo lane can
be verified equivalent if official projection rows prove the same physical lane.
Boundary samples are kept in the artifact and reported as warnings when they
explain raw dominant-ratio or heading spikes. They can only become non-blocking
when the core route samples still prove one dominant Apollo lane, low lateral
error, claim heading below threshold, and ordered RoutingResponse compatibility.
If the core samples fail, keep the result as route/lane identity failure or
`insufficient_data`; do not tune control to mask it.

Do not rewrite scenario route truth from Apollo RoutingResponse, and do not
enable fallback routing to make the report green.

## Latest Run Reading

For the latest 2026-06-15 online evidence family, `map_contract_guard.json`
shows Apollo runtime and bridge map roots resolving to `carla_town01`, with
base/routing/sim map hashes recorded. Re-exporting
`runs/apollo_p0p1_metadata_online_20260615_115705` with
`tools/export_apollo_hdmap_projection.py --include-route-samples
--include-start-goal --min-route-s-coverage 300` produced 143 official
`source="apollo_hdmap_api"` rows, `projection_s_coverage_m=303.78`, and
`analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json`
`status=pass`.

After route chord heading and boundary-core interpretation were added,
`analysis/apollo_route_contract/apollo_route_contract_report.json` for that run
is `status=pass` with `lane_equivalence_status=verified_equivalence` and no
blocking reasons. `117:1 -> 82:1`, `83:1 -> 31:-1`, and exact-match `13:-1`
are all verified-equivalent. The raw boundary/transition issues remain visible
as warnings, for example `83:1` has a boundary sample projecting to `37:1`, and
`13:-1` records raw heading error above threshold while the core claim sample is
within threshold. `apollo_map_route_alignment_report.json` is therefore
`status=warn`, `diagnosis=pass`: route/routing alignment is no longer the
primary blocker, but Planning reference-line evidence still carries warnings.
This is not a natural-driving pass; any later pass claim must still be backed by
`natural_driving_report.json` and the no-interference claim artifacts.

## If You Need To Change Maps Later

Before switching or promoting a new HDMap, capture:

- `map_identity_report.json` with base/routing/sim hashes.
- `apollo_hdmap_projection.jsonl` from `source="apollo_hdmap_api"`.
- `apollo_hdmap_projection_report.json` with heading p95 <= `0.03 rad` and
  lateral p95 <= `0.30 m`.
- `lane_equivalence_town01.json` with ordered lane-window evidence.
- `apollo_route_contract_report.json` showing the claim route is materialized.
- `apollo_reference_line_contract_report.json` proving reference-line evidence
  is available for the same route.

Only after those pass should downstream localization/control/perception
diagnostics be interpreted as driving capability evidence.
