#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import (
    _capability_direction_class,
    _capability_geometry_class,
    _capability_pair_coverage,
    _capability_supports_pairs,
    default_corpus_path,
    load_route_corpus,
)


PROFILE_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep",
    "curve_lane_follow_candidate": "curve_lane_follow",
    "junction_traverse_candidate": "junction_traverse",
    "traffic_light_candidate": "traffic_light_actual",
}

FIRST_WAVE_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_first_wave_smoke",
    "curve_lane_follow_candidate": "curve_lane_follow_first_wave_smoke",
    "junction_traverse_candidate": "junction_traverse_first_wave_smoke",
    "traffic_light_candidate": "traffic_light_first_wave_smoke",
}

NEXT_REVIEW_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_next_review_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_next_review_queue",
    "junction_traverse_candidate": "junction_traverse_next_review_queue",
    "traffic_light_candidate": "traffic_light_next_review_queue",
}

CONTRAST_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_contrast_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_contrast_queue",
    "junction_traverse_candidate": "junction_traverse_contrast_queue",
    "traffic_light_candidate": "traffic_light_contrast_queue",
}

REVIEW_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_review_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_review_pack",
    "junction_traverse_candidate": "junction_traverse_review_pack",
    "traffic_light_candidate": "traffic_light_review_pack",
}

REVIEW_PRIORITY_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_review_priority_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_review_priority_queue",
    "junction_traverse_candidate": "junction_traverse_review_priority_queue",
    "traffic_light_candidate": "traffic_light_review_priority_queue",
}

FOCUS_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_focus_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_focus_pack",
    "junction_traverse_candidate": "junction_traverse_focus_pack",
    "traffic_light_candidate": "traffic_light_focus_pack",
}

PROXY_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_proxy_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_proxy_pack",
    "junction_traverse_candidate": "junction_traverse_proxy_pack",
    "traffic_light_candidate": "traffic_light_proxy_pack",
}

SEED_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_seed_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_seed_pack",
    "junction_traverse_candidate": "junction_traverse_seed_pack",
    "traffic_light_candidate": "traffic_light_seed_pack",
}

HISTORY_GAP_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_history_gap_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_history_gap_queue",
    "junction_traverse_candidate": "junction_traverse_history_gap_queue",
    "traffic_light_candidate": "traffic_light_history_gap_queue",
}

PAIR_GAP_FOR_SUBSET = {
    "curve_lane_follow_candidate": "curve_lane_follow_pair_gap_queue",
    "junction_traverse_candidate": "junction_traverse_pair_gap_queue",
    "traffic_light_candidate": "traffic_light_pair_gap_queue",
}

DEFAULT_EXECUTION_SUBSET_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_focus_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_proxy_pack",
    "junction_traverse_candidate": "junction_traverse_proxy_pack",
    "traffic_light_candidate": "traffic_light_proxy_pack",
}

HEURISTIC_NOTES = {
    "lane_keep_candidate": "same-lane same-road proxy, 180m-260m route length, used for stable lane-keeping replay.",
    "curve_lane_follow_candidate": "1 road + 1 lane transition with 20deg-80deg heading change, used as ordinary bend lane-follow proxy.",
    "junction_traverse_candidate": ">=2 road transitions with >=45deg heading change, used as junction traversal proxy.",
    "traffic_light_candidate": "junction traversal proxy with 70deg-110deg heading delta and moderate route length, used as traffic-light-enabled route candidate.",
}


def _route_table(corpus: Dict[str, object], route_ids: List[str], subset_name: str) -> List[str]:
    route_by_id = {
        str(item.get("route_id") or ""): item
        for item in list(corpus.get("routes") or [])
        if isinstance(item, dict) and str(item.get("route_id") or "")
    }
    lines: List[str] = []
    for route_id in route_ids:
        route = route_by_id.get(route_id) or {}
        lines.append(
            "- `{}` | len=`{:.1f}` | road_transitions=`{}` | lane_transitions=`{}` | heading_delta=`{}` | class=`{}` | dir=`{}`".format(
                route_id,
                float(route.get("route_length_m") or 0.0),
                int(route.get("road_transition_count") or 0),
                int(route.get("lane_transition_count") or 0),
                (
                    "{:+.1f}deg".format(float(route.get("goal_heading_delta_deg") or 0.0))
                    if route.get("goal_heading_delta_deg") is not None
                    else "n/a"
                ),
                _capability_geometry_class(route, subset_name),
                _capability_direction_class(route, subset_name),
            )
        )
    return lines


def render_report(corpus: Dict[str, object], corpus_path: Path) -> str:
    subsets = corpus.get("recommended_subsets") or {}
    routes = [item for item in list(corpus.get("routes") or []) if isinstance(item, dict)]
    route_by_id = {
        str(item.get("route_id") or "").strip(): item
        for item in routes
        if str(item.get("route_id") or "").strip()
    }
    lines = [
        "# Town01 Capability Subset Report",
        "",
        f"- generated_at_local: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- corpus_path: `{corpus_path}`",
        "",
        "## Summary",
        "",
    ]
    for subset_name, profile_name in PROFILE_FOR_SUBSET.items():
        supports_pairs = _capability_supports_pairs(subset_name)
        route_ids = list(subsets.get(subset_name) or [])
        smoke_ids = list(subsets.get(FIRST_WAVE_FOR_SUBSET[subset_name]) or [])
        candidate_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in route_ids if route_id in route_by_id],
            subset_name,
        )
        next_review_subset_name = NEXT_REVIEW_FOR_SUBSET[subset_name]
        contrast_subset_name = CONTRAST_FOR_SUBSET[subset_name]
        review_pack_subset_name = REVIEW_PACK_FOR_SUBSET[subset_name]
        review_priority_subset_name = REVIEW_PRIORITY_FOR_SUBSET[subset_name]
        focus_pack_subset_name = FOCUS_PACK_FOR_SUBSET[subset_name]
        proxy_pack_subset_name = PROXY_PACK_FOR_SUBSET[subset_name]
        seed_pack_subset_name = SEED_PACK_FOR_SUBSET[subset_name]
        history_gap_subset_name = HISTORY_GAP_FOR_SUBSET[subset_name]
        default_execution_subset_name = DEFAULT_EXECUTION_SUBSET_FOR_SUBSET[subset_name]
        pair_gap_subset_name = PAIR_GAP_FOR_SUBSET.get(subset_name, "")
        smoke_subset_name = FIRST_WAVE_FOR_SUBSET[subset_name]
        first_wave_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in smoke_ids if route_id in route_by_id],
            subset_name,
        )
        next_review_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(next_review_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        contrast_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(contrast_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        review_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(review_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        review_priority_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(review_priority_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        focus_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(focus_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        proxy_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(proxy_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        seed_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(seed_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        history_gap_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(history_gap_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        pair_gap_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(pair_gap_subset_name) or []) if route_id in route_by_id],
            subset_name,
        ) if pair_gap_subset_name else []
        missing_contrast_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(contrast_pair_coverage)
        )
        missing_review_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_pack_pair_coverage)
        )
        missing_review_priority_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_priority_pair_coverage)
        )
        missing_focus_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(focus_pack_pair_coverage)
        )
        missing_proxy_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(proxy_pack_pair_coverage)
        )
        missing_seed_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(seed_pack_pair_coverage)
        )
        pair_gap_fragment = (
            f" | `{pair_gap_subset_name}`: `{len(subsets.get(pair_gap_subset_name) or [])}` routes"
            if pair_gap_subset_name
            else ""
        )
        review_pack_fragment = f" | `{review_pack_subset_name}`: `{len(subsets.get(review_pack_subset_name) or [])}` routes"
        review_priority_fragment = f" | `{review_priority_subset_name}`: `{len(subsets.get(review_priority_subset_name) or [])}` routes"
        focus_pack_fragment = f" | `{focus_pack_subset_name}`: `{len(subsets.get(focus_pack_subset_name) or [])}` routes"
        proxy_pack_fragment = f" | `{proxy_pack_subset_name}`: `{len(subsets.get(proxy_pack_subset_name) or [])}` routes"
        seed_pack_fragment = f" | `{seed_pack_subset_name}`: `{len(subsets.get(seed_pack_subset_name) or [])}` routes"
        history_gap_fragment = f" | `{history_gap_subset_name}`: `{len(subsets.get(history_gap_subset_name) or [])}` routes"
        lines.append(
            f"- `{subset_name}`: `{len(route_ids)}` routes | `{smoke_subset_name}`: `{len(smoke_ids)}` routes | `{next_review_subset_name}`: `{len(subsets.get(next_review_subset_name) or [])}` routes | `{contrast_subset_name}`: `{len(subsets.get(contrast_subset_name) or [])}` routes{review_pack_fragment}{review_priority_fragment}{focus_pack_fragment}{proxy_pack_fragment}{seed_pack_fragment}{history_gap_fragment}{pair_gap_fragment} | pair_candidates=`{', '.join(candidate_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | contrast_pairs=`{', '.join(contrast_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | review_pack_pairs=`{', '.join(review_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | review_priority_pairs=`{', '.join(review_priority_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | focus_pack_pairs=`{', '.join(focus_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | proxy_pack_pairs=`{', '.join(proxy_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | seed_pack_pairs=`{', '.join(seed_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | history_gap_pairs=`{', '.join(history_gap_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | pair_gap_pairs=`{', '.join(pair_gap_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | missing_contrast_pairs=`{', '.join(missing_contrast_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | missing_review_pack_pairs=`{', '.join(missing_review_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | missing_review_priority_pairs=`{', '.join(missing_review_priority_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | missing_focus_pack_pairs=`{', '.join(missing_focus_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | missing_proxy_pack_pairs=`{', '.join(missing_proxy_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | missing_seed_pack_pairs=`{', '.join(missing_seed_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}` | default_subset=`{default_execution_subset_name}` | runner profile=`{profile_name}` | note={HEURISTIC_NOTES[subset_name]}"
        )
    lines.extend(["", "## Suggested Commands", ""])
    for subset_name, profile_name in PROFILE_FOR_SUBSET.items():
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {DEFAULT_EXECUTION_SUBSET_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {PROXY_PACK_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {SEED_PACK_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {FOCUS_PACK_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {REVIEW_PRIORITY_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {HISTORY_GAP_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {REVIEW_PACK_FOR_SUBSET[subset_name]} --sample-size 6"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {FIRST_WAVE_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {NEXT_REVIEW_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {profile_name} --recommended-subset {CONTRAST_FOR_SUBSET[subset_name]} --sample-size 4"
        )
        lines.append("```")
        pair_gap_subset_name = PAIR_GAP_FOR_SUBSET.get(subset_name, "")
        if pair_gap_subset_name:
            lines.append("```bash")
            lines.append(
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
                f"--capability-profile {profile_name} --recommended-subset {pair_gap_subset_name} --sample-size 4"
            )
            lines.append("```")
    lines.extend(["", "## Current Route IDs", ""])
    for subset_name in PROFILE_FOR_SUBSET:
        supports_pairs = _capability_supports_pairs(subset_name)
        route_ids = list(subsets.get(subset_name) or [])
        candidate_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in route_ids if route_id in route_by_id],
            subset_name,
        )
        next_review_subset_name = NEXT_REVIEW_FOR_SUBSET[subset_name]
        contrast_subset_name = CONTRAST_FOR_SUBSET[subset_name]
        review_pack_subset_name = REVIEW_PACK_FOR_SUBSET[subset_name]
        review_priority_subset_name = REVIEW_PRIORITY_FOR_SUBSET[subset_name]
        pair_gap_subset_name = PAIR_GAP_FOR_SUBSET.get(subset_name, "")
        history_gap_subset_name = HISTORY_GAP_FOR_SUBSET[subset_name]
        proxy_pack_subset_name = PROXY_PACK_FOR_SUBSET[subset_name]
        seed_pack_subset_name = SEED_PACK_FOR_SUBSET[subset_name]
        smoke_subset_name = FIRST_WAVE_FOR_SUBSET[subset_name]
        first_wave_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(smoke_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        next_review_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(next_review_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        contrast_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(contrast_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        review_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(review_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        review_priority_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(review_priority_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        focus_pack_subset_name = FOCUS_PACK_FOR_SUBSET[subset_name]
        focus_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(focus_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        proxy_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(proxy_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        seed_pack_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(seed_pack_subset_name) or []) if route_id in route_by_id],
            subset_name,
        )
        pair_gap_pair_coverage = _capability_pair_coverage(
            [route_by_id[route_id] for route_id in list(subsets.get(pair_gap_subset_name) or []) if route_id in route_by_id],
            subset_name,
        ) if pair_gap_subset_name else []
        missing_contrast_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(contrast_pair_coverage)
        )
        missing_review_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_pack_pair_coverage)
        )
        missing_review_priority_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_priority_pair_coverage)
        )
        missing_focus_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(focus_pack_pair_coverage)
        )
        missing_proxy_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(proxy_pack_pair_coverage)
        )
        missing_seed_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(seed_pack_pair_coverage)
        )
        lines.append(f"### {subset_name}")
        lines.append("")
        if route_ids:
            lines.extend(_route_table(corpus, route_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- candidate_pair_coverage: `{', '.join(candidate_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        next_review_ids = list(subsets.get(next_review_subset_name) or [])
        lines.append(f"### {next_review_subset_name}")
        lines.append("")
        if next_review_ids:
            lines.extend(_route_table(corpus, next_review_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- next_review_pair_coverage: `{', '.join(next_review_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        contrast_subset_name = CONTRAST_FOR_SUBSET[subset_name]
        contrast_ids = list(subsets.get(contrast_subset_name) or [])
        lines.append(f"### {contrast_subset_name}")
        lines.append("")
        if contrast_ids:
            lines.extend(_route_table(corpus, contrast_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- contrast_pair_coverage: `{', '.join(contrast_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append(
            f"- missing_contrast_pair_coverage: `{', '.join(missing_contrast_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        review_pack_ids = list(subsets.get(review_pack_subset_name) or [])
        review_priority_ids = list(subsets.get(review_priority_subset_name) or [])
        focus_pack_ids = list(subsets.get(focus_pack_subset_name) or [])
        proxy_pack_ids = list(subsets.get(proxy_pack_subset_name) or [])
        seed_pack_ids = list(subsets.get(seed_pack_subset_name) or [])
        history_gap_ids = list(subsets.get(history_gap_subset_name) or [])
        lines.append(f"### {review_pack_subset_name}")
        lines.append("")
        if review_pack_ids:
            lines.extend(_route_table(corpus, review_pack_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- review_pack_pair_coverage: `{', '.join(review_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append(
            f"- missing_review_pack_pair_coverage: `{', '.join(missing_review_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        lines.append(f"### {review_priority_subset_name}")
        lines.append("")
        if review_priority_ids:
            lines.extend(_route_table(corpus, review_priority_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- review_priority_pair_coverage: `{', '.join(review_priority_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append(
            f"- missing_review_priority_pair_coverage: `{', '.join(missing_review_priority_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        lines.append(f"### {focus_pack_subset_name}")
        lines.append("")
        if focus_pack_ids:
            lines.extend(_route_table(corpus, focus_pack_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- focus_pack_pair_coverage: `{', '.join(focus_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append(
            f"- missing_focus_pack_pair_coverage: `{', '.join(missing_focus_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        lines.append(f"### {proxy_pack_subset_name}")
        lines.append("")
        if proxy_pack_ids:
            lines.extend(_route_table(corpus, proxy_pack_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- proxy_pack_pair_coverage: `{', '.join(proxy_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append(
            f"- missing_proxy_pack_pair_coverage: `{', '.join(missing_proxy_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        lines.append(f"### {seed_pack_subset_name}")
        lines.append("")
        if seed_pack_ids:
            lines.extend(_route_table(corpus, seed_pack_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- seed_pack_pair_coverage: `{', '.join(seed_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append(
            f"- missing_seed_pack_pair_coverage: `{', '.join(missing_seed_pack_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        lines.append(f"### {history_gap_subset_name}")
        lines.append("")
        if history_gap_ids:
            lines.extend(_route_table(corpus, history_gap_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- history_gap_pair_coverage: `{', '.join(history_gap_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
        if pair_gap_subset_name:
            pair_gap_ids = list(subsets.get(pair_gap_subset_name) or [])
            lines.append(f"### {pair_gap_subset_name}")
            lines.append("")
            if pair_gap_ids:
                lines.extend(_route_table(corpus, pair_gap_ids, subset_name))
            else:
                lines.append("- none")
            lines.append("")
            lines.append(
                f"- pair_gap_pair_coverage: `{', '.join(pair_gap_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
            )
            lines.append("")
        smoke_subset_name = FIRST_WAVE_FOR_SUBSET[subset_name]
        smoke_ids = list(subsets.get(smoke_subset_name) or [])
        lines.append(f"### {smoke_subset_name}")
        lines.append("")
        if smoke_ids:
            lines.extend(_route_table(corpus, smoke_ids, subset_name))
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            f"- first_wave_pair_coverage: `{', '.join(first_wave_pair_coverage) or ('n/a' if not supports_pairs else 'none')}`"
        )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Render Town01 capability subset report from the current route corpus.")
    parser.add_argument("--corpus", default=str(default_corpus_path(REPO_ROOT)))
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    corpus_path = Path(args.corpus).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else (
        REPO_ROOT / "artifacts" / f"town01_capability_subset_report_{datetime.now().strftime('%Y%m%d')}.md"
    )
    corpus = load_route_corpus(corpus_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_report(corpus, corpus_path), encoding="utf-8")
    print(f"[town01-capability-subsets] written: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
