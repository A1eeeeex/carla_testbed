# Apollo Link Alignment Matrix

This page records evidence boundaries for the Town01 Apollo truth-input
transition runtime. It is an operator reference, not a claim that behavior is
already solved.

## Runtime Naming

The typed transition runtime dispatches `examples.run_followstop` through the
current transition backend. Its transport name is
`apollo_cyberrt_gt_over_ros2_transition`, with compatibility layers
`ros2_gt_transition` and `legacy_route_health_transition`.

Use this name when reading manifests or review packs. Do not shorten it to a
pure CyberRT backend unless the run artifacts prove the transition layer was no
longer used.

## Evidence Matrix

| link | artifact | status boundary |
| --- | --- | --- |
| typed config dispatch | `analysis/runtime_claim_boundary/runtime_claim_boundary_report.json` | `pass` means typed runtime did not fall back to legacy dispatch; it does not prove routing, Planning, Control, or natural-driving behavior. |
| CARLA/Apollo runtime | `summary.json`, `manifest.json`, `artifacts/cyber_bridge_stats.json` | A one-tick or `max_ticks=1` run is wiring smoke only. Use the materialization probe before behavior diagnosis. |
| channels | `analysis/channel_stats_normalized/channel_stats_normalized.json` plus `channel_stats.json` compatibility copy | Counter-derived stats prove observed runtime publication. Promotion-grade channel health needs row-level timestamps such as `topic_publish_stats.jsonl`. |
| routing response | `artifacts/routing_response_decoded.jsonl`, `analysis/routing_response_decoded/routing_response_decoded_report.json` | Missing or undecoded routing response keeps route establishment at `insufficient_data`. |
| HDMap projection | `artifacts/apollo_hdmap_projection.jsonl`, `analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json` | Missing file is `artifact_missing`; existing zero-row file is `artifact_empty`; only `source=apollo_hdmap_api` rows can support claim-grade reference-line evidence. |
| Planning materialization | `artifacts/planning_topic_debug.jsonl`, `analysis/planning_materialization/planning_materialization_report.json` | `materialization_status` distinguishes `missing`, `observed_empty`, `observed_reference_line_missing`, and `observed_nonempty`. |
| Control | `artifacts/control_decode_debug.jsonl`, `artifacts/control_apply_trace.jsonl`, `analysis/control_attribution/control_attribution_report.json` | Raw/mapped/applied evidence is required before treating actuation behavior as the primary blocker. |
| natural-driving gate | `analysis/natural_driving/natural_driving_report.json` or batch-level `natural_driving_report.json` | Any natural-driving pass statement must cite `natural_driving_report.json` and the supporting link reports in the same evidence packet. |

## Probe Sequence

Use `configs/io/examples/town01_apollo_route_only_claim_probe.yaml` only when
you need a short routing-contract probe. It is not long enough to evaluate
route materialization if startup delay dominates.

Use `configs/io/examples/town01_apollo_route_materialization_probe.yaml` for
the first online materialization sample. It keeps fallback disabled and runs
`max_ticks=600` at `fixed_dt_s=0.05`, giving a 30 second diagnostic window for
routing response, HDMap projection, Planning materialization, and Control
handoff evidence.

If the materialization probe still produces only empty Planning trajectories,
diagnose in this order: routing response decoded evidence, Apollo HDMap
projection rows, Apollo reference-line contract, localization/chassis freshness,
then Control. Do not tune control mapping to hide missing reference-line or
localization evidence.

## Review Pack Scope

External review packs should include `examples/`, `configs/io/`, and the
transition runtime files whenever the manifest says
`transition_entrypoint=examples.run_followstop`. Without those files, reviewers
cannot inspect the runnable path that produced the artifacts.
