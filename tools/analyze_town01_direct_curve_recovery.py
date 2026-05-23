#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_BASELINE_ROOT = REPO_ROOT / "runs" / "town01_transport_ab_20260522_160253" / "baseline"
DEFAULT_RUNS_ROOT = REPO_ROOT / "runs"
DEFAULT_ROUTE_IDS = ("town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052")
DEFAULT_LABEL_HINT = "direct_random_curve_pair_recovery_420"

from tools.analyze_town01_transport_ab import build_rows, summarize_rows, write_csv, write_markdown


def _collect_summary_paths(root: Path) -> List[Path]:
    resolved = root.expanduser().resolve()
    if resolved.is_file() and resolved.name == "summary.json":
        return [resolved]
    if not resolved.exists():
        return []
    return sorted(resolved.rglob("summary.json"))


def _summary_route_id(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    route_id = str(payload.get("route_id") or "").strip()
    if route_id:
        return route_id
    metadata = payload.get("scenario_metadata")
    if isinstance(metadata, dict):
        return str(metadata.get("route_id") or "").strip()
    return ""


def _candidate_score(root: Path, route_ids: Sequence[str], label_hint: str) -> Tuple[int, float]:
    wanted = {str(route_id).strip() for route_id in route_ids if str(route_id).strip()}
    if not wanted:
        return (0, 0.0)
    summary_paths = _collect_summary_paths(root)
    if label_hint:
        hinted_paths = [path for path in summary_paths if label_hint in str(path)]
        if hinted_paths:
            summary_paths = hinted_paths
    found = {_summary_route_id(path) for path in summary_paths}
    found_count = len(wanted & found)
    try:
        mtime = max((path.stat().st_mtime for path in summary_paths), default=root.stat().st_mtime)
    except Exception:
        mtime = 0.0
    return (found_count, float(mtime))


def find_latest_candidate_root(
    runs_root: Path,
    *,
    route_ids: Sequence[str] = DEFAULT_ROUTE_IDS,
    label_hint: str = DEFAULT_LABEL_HINT,
) -> Path:
    resolved = runs_root.expanduser().resolve()
    candidates = [path for path in resolved.glob("town01_capability_online_chain_*") if path.is_dir()]
    scored = [(path, _candidate_score(path, route_ids, label_hint)) for path in candidates]
    scored = [(path, score) for path, score in scored if score[0] > 0]
    if not scored:
        raise FileNotFoundError(
            f"no candidate run under {resolved} contains route ids {list(route_ids)}"
            + (f" with label hint {label_hint!r}" if label_hint else "")
        )
    return max(scored, key=lambda item: item[1])[0]


def find_latest_candidate_roots(
    runs_root: Path,
    *,
    route_ids: Sequence[str] = DEFAULT_ROUTE_IDS,
    label_hint: str = DEFAULT_LABEL_HINT,
) -> List[Path]:
    resolved = runs_root.expanduser().resolve()
    candidates = [path for path in resolved.glob("town01_capability_online_chain_*") if path.is_dir()]
    route_to_root: Dict[str, Tuple[Path, float]] = {}
    wanted = [str(route_id).strip() for route_id in route_ids if str(route_id).strip()]
    for root in candidates:
        summary_paths = _collect_summary_paths(root)
        if label_hint:
            hinted_paths = [path for path in summary_paths if label_hint in str(path)]
            if hinted_paths:
                summary_paths = hinted_paths
        for summary_path in summary_paths:
            route_id = _summary_route_id(summary_path)
            if route_id not in wanted:
                continue
            try:
                mtime = float(summary_path.stat().st_mtime)
            except Exception:
                mtime = 0.0
            current = route_to_root.get(route_id)
            if current is None or mtime >= current[1]:
                route_to_root[route_id] = (root, mtime)
    roots: List[Path] = []
    seen = set()
    for route_id in wanted:
        item = route_to_root.get(route_id)
        if item is None:
            continue
        root = item[0].resolve()
        if root in seen:
            continue
        seen.add(root)
        roots.append(root)
    if not roots:
        raise FileNotFoundError(
            f"no candidate run under {resolved} contains route ids {list(route_ids)}"
            + (f" with label hint {label_hint!r}" if label_hint else "")
        )
    return roots


def classify_recovery(rows: Sequence[Dict[str, Any]], summary: Dict[str, Any]) -> str:
    verdict_counts = dict(summary.get("verdict_counts") or {})
    route_count = int(summary.get("route_count") or 0)
    missing = int(verdict_counts.get("candidate_missing", 0) or 0)
    baseline_missing = int(verdict_counts.get("baseline_missing", 0) or 0)
    negative = int(summary.get("candidate_negative_count") or 0)
    positive = int(summary.get("candidate_positive_count") or 0)
    inconclusive = int(summary.get("candidate_inconclusive_count") or 0)
    candidate_aligned = int(summary.get("candidate_aligned_count") or 0)
    candidate_control = int(summary.get("candidate_control_consuming_count") or 0)
    if route_count == 0 or baseline_missing:
        return "recovery_unusable_missing_baseline"
    if missing:
        return "recovery_incomplete_candidate_missing"
    if negative:
        return "recovery_negative"
    if positive == route_count:
        return "recovery_positive"
    if inconclusive:
        return "recovery_inconclusive"
    if candidate_aligned == route_count and candidate_control == route_count:
        return "recovery_valid_mixed"
    return "recovery_inconclusive"


def write_recovery_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_recovery_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# Town01 Direct Curve Recovery Assessment",
        "",
        f"- verdict: `{payload['recovery_verdict']}`",
        f"- baseline_root: `{payload['baseline_root']}`",
        f"- candidate_root: `{payload['candidate_root']}`",
        f"- candidate_roots: `{', '.join(payload.get('candidate_roots') or [])}`",
        f"- route_ids: `{', '.join(payload['route_ids'])}`",
        "",
        "## Aggregate",
        "",
        f"- route_count: `{summary['route_count']}`",
        f"- candidate_aligned_count: `{summary['candidate_aligned_count']}`",
        f"- candidate_control_consuming_count: `{summary['candidate_control_consuming_count']}`",
        f"- candidate_positive_count: `{summary['candidate_positive_count']}`",
        f"- candidate_negative_count: `{summary['candidate_negative_count']}`",
        f"- candidate_inconclusive_count: `{summary['candidate_inconclusive_count']}`",
        f"- verdict_counts: `{json.dumps(summary['verdict_counts'], sort_keys=True)}`",
        "",
        "## Route Verdicts",
        "",
        "| route_id | baseline | candidate | delta_m | invalid | verdict |",
        "|---|---:|---:|---:|---|---|",
    ]
    for row in payload["rows"]:
        baseline = row.get("baseline_distance_m")
        candidate = row.get("candidate_distance_m")
        delta = row.get("distance_delta_m")
        baseline_cell = "" if baseline is None else f"{float(baseline):.3f}m"
        candidate_cell = "" if candidate is None else f"{float(candidate):.3f}m"
        delta_cell = "" if delta is None else f"{float(delta):.3f}"
        lines.append(
            "| `{}` | {} | {} | {} | `{}` | `{}` |".format(
                row.get("route_id"),
                baseline_cell,
                candidate_cell,
                delta_cell,
                row.get("candidate_invalid_reason") or "",
                row.get("verdict") or "",
            )
        )
    lines.extend(
        [
            "",
            "## Decision Rule",
            "",
            "- `recovery_positive`: both curve routes are valid and direct is better by the current A/B route verdicts.",
            "- `recovery_negative`: at least one valid curve route is materially worse than baseline.",
            "- `recovery_incomplete_candidate_missing`: one or more candidate curve summaries are still missing.",
            "- `recovery_inconclusive`: evidence exists but remains interrupted or non-decisive.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_recovery_payload(
    *,
    baseline_root: Path,
    candidate_root: Path | Sequence[Path],
    route_ids: Sequence[str] = DEFAULT_ROUTE_IDS,
) -> Dict[str, Any]:
    if isinstance(candidate_root, (list, tuple)):
        candidate_roots = [path.expanduser().resolve() for path in candidate_root]
    else:
        candidate_roots = [candidate_root.expanduser().resolve()]
    rows = build_rows([baseline_root], candidate_roots, route_ids=route_ids)
    summary = summarize_rows(rows)
    return {
        "baseline_root": str(baseline_root.expanduser().resolve()),
        "candidate_root": str(candidate_roots[0]) if len(candidate_roots) == 1 else ",".join(str(path) for path in candidate_roots),
        "candidate_roots": [str(path) for path in candidate_roots],
        "route_ids": [str(route_id) for route_id in route_ids],
        "summary": summary,
        "recovery_verdict": classify_recovery(rows, summary),
        "rows": rows,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a direct curve-pair recovery run against the random A/B baseline."
    )
    parser.add_argument("--baseline-root", type=Path, default=DEFAULT_BASELINE_ROOT)
    parser.add_argument(
        "--candidate-root",
        type=Path,
        action="append",
        default=None,
        help=(
            "Candidate recovery run root. May be repeated, which is useful when curve routes were rerun "
            "as separate batches. If omitted, the latest matching run per route is used."
        ),
    )
    parser.add_argument("--runs-root", type=Path, default=DEFAULT_RUNS_ROOT)
    parser.add_argument("--label-hint", default=DEFAULT_LABEL_HINT)
    parser.add_argument("--route-id", action="append", default=None)
    parser.add_argument("--csv-out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    parser.add_argument("--json-out", type=Path, default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    route_ids = [
        str(route_id).strip()
        for route_id in list(args.route_id if args.route_id is not None else DEFAULT_ROUTE_IDS)
        if str(route_id).strip()
    ]
    candidate_roots = (
        [path.expanduser().resolve() for path in list(args.candidate_root or [])]
        if args.candidate_root is not None
        else find_latest_candidate_roots(args.runs_root, route_ids=route_ids, label_hint=str(args.label_hint or ""))
    )
    out_root = max(candidate_roots, key=lambda path: path.stat().st_mtime if path.exists() else 0.0)
    csv_out = args.csv_out or (out_root / "transport_ab_curve_pair_recovery_summary.csv")
    md_out = args.md_out or (out_root / "transport_ab_curve_pair_recovery_summary.md")
    json_out = args.json_out or (out_root / "transport_ab_curve_pair_recovery_summary.json")

    payload = build_recovery_payload(
        baseline_root=args.baseline_root.expanduser().resolve(),
        candidate_root=candidate_roots,
        route_ids=route_ids,
    )
    write_csv(csv_out, payload["rows"])
    write_markdown(md_out, payload["rows"], payload["summary"])
    write_recovery_json(json_out, payload)
    write_recovery_markdown(md_out.with_name(md_out.stem + "_assessment.md"), payload)
    print(json.dumps({"recovery_verdict": payload["recovery_verdict"], **payload["summary"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
