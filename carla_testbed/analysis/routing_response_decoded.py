from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

ROUTING_RESPONSE_DECODED_SCHEMA_VERSION = "routing_response_decoded.v1"


def decode_routing_response_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a decoded Apollo routing response-like payload.

    The input is already JSON/dict data; this module intentionally does not
    import Apollo protobufs. Runtime bridges may generate this artifact from
    `/apollo/routing_response`, while offline tests can construct the same
    contract directly.
    """

    rows = _lane_segments_from_payload(payload)
    total_length = _first_num(
        payload.get("total_length_m"),
        payload.get("routing_total_length_m"),
        payload.get("response_total_length_m"),
        sum(float(row["length_m"]) for row in rows if row.get("length_m") is not None) if rows else None,
    )
    lane_sequence = _lane_sequence(rows)
    road_segments = payload.get("road_segments") if isinstance(payload.get("road_segments"), list) else []
    status = "pass" if rows and total_length is not None else "insufficient_data"
    missing: list[str] = []
    if not rows:
        missing.append("lane_segments")
    if total_length is None:
        missing.append("total_length_m")
    return {
        "schema_version": ROUTING_RESPONSE_DECODED_SCHEMA_VERSION,
        "status": status,
        "source": payload.get("source") or "decoded_json",
        "map_name": payload.get("map_name"),
        "routing_request": dict(payload.get("routing_request") or {})
        if isinstance(payload.get("routing_request"), Mapping)
        else {},
        "routing_response": dict(payload.get("routing_response") or {})
        if isinstance(payload.get("routing_response"), Mapping)
        else {},
        "road_segments": road_segments,
        "passage_count": _first_int(payload.get("passage_count"), _count_passages(road_segments)),
        "lane_segment_count": len(rows),
        "lane_segments": rows,
        "total_length_m": total_length,
        "lane_sequence_signature": lane_sequence,
        "blocking_reasons": [],
        "warnings": [],
        "missing_fields": missing,
        "claim_boundary": (
            "Decoded routing response proves only that a route response can be "
            "inspected. Natural-driving claims still require route identity, "
            "HDMap projection, Planning materialization, and Control handoff."
        ),
    }


def read_routing_response_decoded(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return decode_routing_response_payload({})
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return decode_routing_response_payload({})
    if file_path.suffix.lower() == ".jsonl":
        rows = _read_jsonl(file_path)
        payload = rows[-1] if rows else {}
    else:
        payload = _read_json(file_path)
    return decode_routing_response_payload(payload if isinstance(payload, Mapping) else {})


def write_routing_response_decoded_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "routing_response_decoded_report.json"
    summary_path = output / "routing_response_decoded_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(routing_response_decoded_summary_md(report), encoding="utf-8")
    return {
        "routing_response_decoded_report": str(json_path),
        "routing_response_decoded_summary": str(summary_path),
    }


def routing_response_decoded_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Routing Response Decoded Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Source: `{report.get('source')}`",
            f"- Total length m: `{report.get('total_length_m')}`",
            f"- Passage count: `{report.get('passage_count')}`",
            f"- Lane segment count: `{report.get('lane_segment_count')}`",
            f"- Lane sequence: `{', '.join(report.get('lane_sequence_signature') or []) or 'none'}`",
            f"- Missing fields: `{', '.join(report.get('missing_fields') or []) or 'none'}`",
            "",
            str(report.get("claim_boundary") or ""),
            "",
        ]
    )


def _lane_segments_from_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    direct = payload.get("lane_segments")
    if isinstance(direct, list):
        return [_normalize_lane_segment(row) for row in direct if isinstance(row, Mapping)]
    rows: list[dict[str, Any]] = []
    roads = payload.get("road_segments")
    if not isinstance(roads, list):
        roads = payload.get("road")
    if not isinstance(roads, list):
        return rows
    for road_index, road in enumerate(roads):
        if not isinstance(road, Mapping):
            continue
        passages = road.get("passages") or road.get("passage") or []
        if not isinstance(passages, list):
            continue
        for passage_index, passage in enumerate(passages):
            if not isinstance(passage, Mapping):
                continue
            segments = passage.get("segments") or passage.get("segment") or []
            if not isinstance(segments, list):
                continue
            for segment_index, segment in enumerate(segments):
                if not isinstance(segment, Mapping):
                    continue
                row = _normalize_lane_segment(segment)
                row.setdefault("road_index", road_index)
                row.setdefault("road_id", road.get("id"))
                row.setdefault("passage_index", passage_index)
                row.setdefault("segment_index", segment_index)
                rows.append(row)
    return rows


def _normalize_lane_segment(row: Mapping[str, Any]) -> dict[str, Any]:
    start_s = _first_num(row.get("start_s"), row.get("start"))
    end_s = _first_num(row.get("end_s"), row.get("end"))
    length = _first_num(row.get("length_m"), row.get("length"))
    if length is None and start_s is not None and end_s is not None:
        length = max(0.0, float(end_s) - float(start_s))
    return {
        "road_index": _first_int(row.get("road_index")),
        "road_id": row.get("road_id"),
        "passage_index": _first_int(row.get("passage_index")),
        "segment_index": _first_int(row.get("segment_index")),
        "lane_id": str(row.get("lane_id") or row.get("id") or "").strip(),
        "start_s": start_s,
        "end_s": end_s,
        "length_m": length,
    }


def _lane_sequence(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id or lane_id in seen:
            continue
        seen.add(lane_id)
        out.append(lane_id)
    return out


def _count_passages(roads: Any) -> int | None:
    if not isinstance(roads, list):
        return None
    count = 0
    for road in roads:
        if not isinstance(road, Mapping):
            continue
        passages = road.get("passages") or road.get("passage") or []
        if isinstance(passages, list):
            count += len(passages)
    return count


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            data = json.loads(line)
            if isinstance(data, dict):
                rows.append(data)
    except (OSError, json.JSONDecodeError):
        return rows
    return rows


def _first_num(*values: Any) -> float | None:
    for value in values:
        try:
            if value is None:
                continue
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_int(*values: Any) -> int | None:
    for value in values:
        try:
            if value is None:
                continue
            return int(value)
        except (TypeError, ValueError):
            continue
    return None
