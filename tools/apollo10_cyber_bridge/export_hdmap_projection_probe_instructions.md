# Export Apollo HDMap Projection Probe

This is an operator instruction file, not an executable script. The repository
does not require local Apollo C++ builds in CI, so the CARLA testbed only
consumes `artifacts/apollo_hdmap_projection.jsonl`.

## Goal

Generate one JSONL row per localization sample by using Apollo's HDMap API in
the Apollo container or by replaying a Cyber record and probing localization
poses against the loaded HDMap.

Output path inside a run directory:

```text
artifacts/apollo_hdmap_projection.jsonl
```

## Required Fields

Each row should include:

```json
{
  "timestamp": 0.0,
  "localization_x": 0.0,
  "localization_y": 0.0,
  "localization_heading": 0.0,
  "nearest_lane_id": "lane_id",
  "projection_s": 0.0,
  "projection_l": 0.0,
  "lane_heading_at_s": 0.0,
  "heading_error_rad": 0.0,
  "lateral_error_m": 0.0,
  "road_id": null,
  "junction_id": null,
  "source": "apollo_hdmap_api",
  "map_name": "Town01",
  "map_dir": "/apollo/modules/map/data/town01",
  "status": "ok"
}
```

## Apollo-Side Probe Sketch

Inside the Apollo 10 container:

1. Load the same map directory used by Planning, for example
   `/apollo/modules/map/data/town01`.
2. Read localization samples from a Cyber record or from a saved
   `LocalizationEstimate` debug export.
3. For each sample, call the Apollo HDMap API nearest-lane/projection utility.
4. Compute:
   - `projection_s` and `projection_l`;
   - `lane_heading_at_s`;
   - normalized `heading_error_rad = localization_heading - lane_heading_at_s`;
   - `lateral_error_m`, normally `projection_l` with the exporter convention
     documented.
5. Emit `source="apollo_hdmap_api"` only if the values came from Apollo HDMap,
   not CARLA waypoint APIs or bridge-side approximations.
6. Emit `status="out_of_map"`, `status="no_lane"`, or `status="error"` rather
   than dropping bad samples.

## Consumer

After exporting the file:

```bash
python tools/analyze_apollo_reference_line_contract.py \
  --run-dir runs/<run_id> \
  --out runs/<run_id>/analysis/apollo_reference_line_contract

python tools/analyze_apollo_localization_contract.py \
  --run-dir runs/<run_id> \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --vehicle-reference configs/vehicles/ego_vehicle_reference.verified.yaml \
  --out runs/<run_id>/analysis/localization_contract
```

The analyzers treat missing projection as `insufficient_data`, not failure.
High heading/lateral projection error becomes blocking evidence and should be
investigated as map alignment, lane direction, lane id, or routing snap before
claiming an Apollo behavior limitation.
