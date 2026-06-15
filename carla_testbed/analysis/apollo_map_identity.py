from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

MAP_IDENTITY_SCHEMA_VERSION = "map_identity_report.v1"
DEFAULT_EXPECTED_APOLLO_MAP_NAME = "carla_town01"
MAP_COMPONENTS = ("base_map", "routing_map", "sim_map")
MAP_COMPONENT_FILENAMES = {
    "base_map": "base_map.txt",
    "routing_map": "routing_map.txt",
    "sim_map": "sim_map.txt",
}


def analyze_apollo_map_identity_run_dir(
    run_dir: str | Path,
    *,
    expected_apollo_map_name: str = DEFAULT_EXPECTED_APOLLO_MAP_NAME,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    inputs = _resolve_inputs(root)
    payloads = {name: _read_payload(path) for name, path in inputs.items() if name != "projection_jsonl"}
    projection_rows = _read_jsonl(inputs.get("projection_jsonl"))
    return analyze_apollo_map_identity(
        payloads=payloads,
        projection_rows=projection_rows,
        source_paths={name: str(path) if path else None for name, path in inputs.items()},
        expected_apollo_map_name=expected_apollo_map_name,
    )


def analyze_apollo_map_identity(
    *,
    payloads: Mapping[str, Mapping[str, Any]] | None = None,
    projection_rows: Sequence[Mapping[str, Any]] | None = None,
    source_paths: Mapping[str, str | None] | None = None,
    expected_apollo_map_name: str = DEFAULT_EXPECTED_APOLLO_MAP_NAME,
) -> dict[str, Any]:
    payloads = payloads or {}
    source_paths = source_paths or {}
    manifest = payloads.get("manifest", {})
    summary = payloads.get("summary", {})
    map_guard = payloads.get("map_contract_guard", {})
    bridge_health = payloads.get("bridge_health_summary", {})
    bridge_transport = payloads.get("bridge_transport_summary", {})
    cyber_stats = payloads.get("cyber_bridge_stats", {})
    effective_bridge = payloads.get("apollo_bridge_effective", {})
    dreamview = payloads.get("dreamview_capture_manifest", {})
    projection_report = payloads.get("apollo_hdmap_projection_report", {})

    expected_aliases = _accepted_map_aliases(expected_apollo_map_name)
    carla_world = _first_text(
        manifest,
        "carla_world.loaded_map_short_name",
        manifest,
        "carla_world.configured_map_short_name",
        manifest,
        "map",
        summary,
        "carla_world.loaded_map_short_name",
        summary,
        "map",
        default="Town01",
    )

    observed_map_names = _unique(
        [
            carla_world,
            _nested(manifest, "metadata.scenario_metadata.map"),
            _nested(map_guard, "dreamview_selected_map"),
            _nested(bridge_health, "dreamview_selected_map"),
            _nested(cyber_stats, "dreamview_selected_map"),
            _nested(dreamview, "selected_map"),
            *_projection_map_names(projection_rows or [], projection_report),
        ]
    )

    bridge_effective_map_file = _first_text(
        map_guard,
        "effective_bridge_map_file",
        effective_bridge,
        "bridge.map_file",
        cyber_stats,
        "bridge.map_file",
    )
    projection_exporter_map_dir = _first_text(
        projection_report,
        "projection.map_dir",
        projection_report,
        "projection.map_dir_topk.0",
        default=None,
    )
    if projection_exporter_map_dir is None:
        projection_exporter_map_dir = _first_projection_map_dir(projection_rows or [])

    root_candidates = _root_candidates(
        map_guard=map_guard,
        bridge_health=bridge_health,
        bridge_transport=bridge_transport,
        cyber_stats=cyber_stats,
        effective_bridge=effective_bridge,
        bridge_effective_map_file=bridge_effective_map_file,
        projection_exporter_map_dir=projection_exporter_map_dir,
    )
    selected_root = _first_text(
        map_guard,
        "runtime_map_dir",
        map_guard,
        "effective_bridge_map_root",
        cyber_stats,
        "host_container_map_path_mapping.runtime_map_dir_host",
        default=None,
    )
    if selected_root is None and bridge_effective_map_file:
        selected_root = _parent_if_map_file(bridge_effective_map_file)
    if selected_root is None and root_candidates:
        selected_root = root_candidates[0]

    component_info = _component_info(map_guard, selected_root)
    base = component_info["base_map"]
    routing = component_info["routing_map"]
    sim = component_info["sim_map"]

    blocking: list[str] = []
    warnings: list[str] = []

    if not selected_root:
        warnings.append("selected_apollo_map_root_missing")
    if not root_candidates:
        warnings.append("apollo_map_root_candidates_missing")

    if _map_guard_reports_mismatch(map_guard):
        blocking.append("mixed_map_root")
    if not _roots_consistent(root_candidates, expected_aliases, map_guard):
        blocking.append("mixed_map_root")
    if _observed_map_name_mismatch(observed_map_names, expected_aliases):
        blocking.append("mixed_map_root")
    if not all(component_info[name]["exists"] for name in MAP_COMPONENTS):
        blocking.append("map_files_missing")
    if _hash_mismatch(map_guard):
        blocking.append("map_hash_mismatch")

    if projection_exporter_map_dir and selected_root and not _same_map_identity(
        projection_exporter_map_dir,
        selected_root,
        expected_aliases,
        map_guard,
    ):
        blocking.append("mixed_map_root")
    if bridge_effective_map_file and selected_root and not _same_map_identity(
        _parent_if_map_file(bridge_effective_map_file),
        selected_root,
        expected_aliases,
        map_guard,
    ):
        blocking.append("mixed_map_root")

    blocking = sorted(set(blocking))
    warnings = sorted(set(warnings))
    if blocking:
        status = "fail"
    elif not selected_root or not root_candidates:
        status = "insufficient_data"
    else:
        status = "pass" if not warnings else "warn"

    return {
        "schema_version": MAP_IDENTITY_SCHEMA_VERSION,
        "status": status,
        "carla_world": carla_world,
        "expected_apollo_map_name": expected_apollo_map_name,
        "observed_map_names": observed_map_names,
        "apollo_map_root_candidates": root_candidates,
        "selected_apollo_map_root": selected_root,
        "base_map_path": base["path"],
        "routing_map_path": routing["path"],
        "sim_map_path": sim["path"],
        "base_map_exists": base["exists"],
        "routing_map_exists": routing["exists"],
        "sim_map_exists": sim["exists"],
        "base_map_sha256": base["sha256"],
        "routing_map_sha256": routing["sha256"],
        "sim_map_sha256": sim["sha256"],
        "dreamview_selected_map": _first_text(
            map_guard,
            "dreamview_selected_map",
            bridge_health,
            "dreamview_selected_map",
            cyber_stats,
            "dreamview_selected_map",
        ),
        "bridge_effective_map_file": bridge_effective_map_file,
        "projection_exporter_map_dir": projection_exporter_map_dir,
        "map_root_consistent": "mixed_map_root" not in blocking and bool(selected_root),
        "blocking_reasons": blocking,
        "warnings": warnings,
        "source": dict(source_paths),
        "interpretation_boundary": (
            "Map identity verifies observed Apollo map roots and map component hashes. "
            "It does not prove HDMap geometry, routing identity, Planning reference-line "
            "materialization, or closed-loop driving behavior."
        ),
    }


def write_apollo_map_identity_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "map_identity_report.json"
    summary_path = output / "map_identity_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_map_identity_summary_md(report), encoding="utf-8")
    return {"map_identity_report": str(json_path), "map_identity_summary": str(summary_path)}


def apollo_map_identity_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Apollo Map Identity Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- CARLA world: `{report.get('carla_world')}`",
            f"- Expected Apollo map: `{report.get('expected_apollo_map_name')}`",
            f"- Observed map names: `{', '.join(report.get('observed_map_names') or []) or 'none'}`",
            f"- Selected Apollo map root: `{report.get('selected_apollo_map_root')}`",
            f"- Root consistent: `{report.get('map_root_consistent')}`",
            f"- Dreamview selected map: `{report.get('dreamview_selected_map')}`",
            f"- Bridge effective map file: `{report.get('bridge_effective_map_file')}`",
            f"- Projection exporter map dir: `{report.get('projection_exporter_map_dir')}`",
            f"- base_map exists/hash: `{report.get('base_map_exists')}` / `{report.get('base_map_sha256')}`",
            f"- routing_map exists/hash: `{report.get('routing_map_exists')}` / `{report.get('routing_map_sha256')}`",
            f"- sim_map exists/hash: `{report.get('sim_map_exists')}` / `{report.get('sim_map_sha256')}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )


def _resolve_inputs(root: Path) -> dict[str, Path | None]:
    return {
        "manifest": _find_first(root, ["manifest.json"]),
        "summary": _find_first(root, ["summary.json"]),
        "config_resolved": _find_first(root, ["config.resolved.yaml", "config.resolved.yml"]),
        "typed_runtime": _find_first(root, ["typed_runtime.effective_legacy.yaml"]),
        "bridge_transport_summary": _find_first(root, ["artifacts/bridge_transport_summary.json"]),
        "bridge_health_summary": _find_first(root, ["artifacts/bridge_health_summary.json"]),
        "cyber_bridge_stats": _find_first(root, ["artifacts/cyber_bridge_stats.json"]),
        "map_contract_guard": _find_first(root, ["artifacts/map_contract_guard.json"]),
        "goal_validity_report": _find_first(root, ["artifacts/goal_validity_report.json"]),
        "apollo_bridge_effective": _find_first(root, ["artifacts/apollo_bridge_effective.yaml"]),
        "dreamview_capture_manifest": _find_first(root, ["artifacts/dreamview_capture_manifest.json"]),
        "apollo_hdmap_projection_report": _find_first(
            root,
            ["analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json"],
        ),
        "projection_jsonl": _find_first(root, ["artifacts/apollo_hdmap_projection.jsonl"]),
    }


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = root / relative
        if path.exists():
            return path
    return None


def _read_payload(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            return {}
        return dict(payload) if isinstance(payload, Mapping) else {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _root_candidates(
    *,
    map_guard: Mapping[str, Any],
    bridge_health: Mapping[str, Any],
    bridge_transport: Mapping[str, Any],
    cyber_stats: Mapping[str, Any],
    effective_bridge: Mapping[str, Any],
    bridge_effective_map_file: str | None,
    projection_exporter_map_dir: str | None,
) -> list[str]:
    values = [
        _env_map_root_candidate(),
        _nested(map_guard, "runtime_map_dir"),
        _nested(map_guard, "effective_bridge_map_root"),
        _nested(map_guard, "runtime_map_dir_container_actual"),
        _nested(map_guard, "container_runtime_probe.selected_runtime_map_dir"),
        _nested(map_guard, "host_container_map_path_mapping.runtime_map_dir_host"),
        _nested(map_guard, "host_container_map_path_mapping.runtime_map_dir_container_actual"),
        _nested(bridge_health, "apollo_runtime_map_dir"),
        _nested(bridge_transport, "apollo_runtime_map_dir"),
        _nested(cyber_stats, "host_container_map_path_mapping.runtime_map_dir_host"),
        _nested(cyber_stats, "host_container_map_path_mapping.runtime_map_dir_container_actual"),
        _nested(effective_bridge, "bridge.map_root"),
        _parent_if_map_file(bridge_effective_map_file),
        projection_exporter_map_dir,
    ]
    return _unique(_normalize_path(value) for value in values if value)


def _env_map_root_candidate() -> str | None:
    root = os.environ.get("APOLLO_MAP_ROOT")
    if not root:
        return None
    if _map_identity_key(root) == DEFAULT_EXPECTED_APOLLO_MAP_NAME:
        return root
    return str(Path(root) / DEFAULT_EXPECTED_APOLLO_MAP_NAME)


def _component_info(map_guard: Mapping[str, Any], selected_root: str | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for component in MAP_COMPONENTS:
        path = _first_text(
            map_guard,
            f"runtime_component_paths.{component}",
            map_guard,
            f"effective_bridge_component_paths.{component}",
            map_guard,
            f"container_component_paths.{component}",
        )
        if path is None and selected_root:
            path = str(Path(selected_root) / MAP_COMPONENT_FILENAMES[component])
        sha = _first_text(
            map_guard,
            f"hash_summary.{component}.runtime_sha256",
            map_guard,
            f"hash_summary.{component}.effective_bridge_sha256",
            map_guard,
            f"container_component_hashes.{component}",
        )
        exists = False
        if sha:
            exists = True
        elif path:
            exists = Path(path).expanduser().exists() if _looks_like_host_path(path) else False
        if sha is None and path and _looks_like_host_path(path):
            sha = _sha256_if_exists(path)
        result[component] = {"path": path, "exists": bool(exists), "sha256": sha}
    return result


def _projection_map_names(
    projection_rows: Sequence[Mapping[str, Any]],
    projection_report: Mapping[str, Any],
) -> list[str]:
    names = [str(row.get("map_name")) for row in projection_rows if row.get("map_name")]
    names.extend(_topk_values(_nested(projection_report, "projection.map_name_topk")))
    return names


def _first_projection_map_dir(projection_rows: Sequence[Mapping[str, Any]]) -> str | None:
    for row in projection_rows:
        value = row.get("map_dir")
        if value not in {None, ""}:
            return str(value)
    return None


def _topk_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {None, ""}]
    if isinstance(value, str) and value:
        return [value]
    return []


def _map_guard_reports_mismatch(map_guard: Mapping[str, Any]) -> bool:
    return bool(
        map_guard.get("map_contract_invalid") is True
        or map_guard.get("high_risk_mismatch") is True
        or map_guard.get("same_derivation_chain") is False
    )


def _hash_mismatch(map_guard: Mapping[str, Any]) -> bool:
    hash_summary = map_guard.get("hash_summary")
    if not isinstance(hash_summary, Mapping):
        return False
    return any(
        isinstance(value, Mapping) and value.get("hash_equal") is False
        for value in hash_summary.values()
    )


def _roots_consistent(
    roots: Sequence[str],
    expected_aliases: set[str],
    map_guard: Mapping[str, Any],
) -> bool:
    if not roots:
        return False
    if _map_guard_reports_mismatch(map_guard):
        return False
    root_keys = {_map_identity_key(root) for root in roots if root}
    root_keys = {key for key in root_keys if key}
    return bool(root_keys) and root_keys.issubset(expected_aliases)


def _same_map_identity(
    lhs: str | None,
    rhs: str | None,
    expected_aliases: set[str],
    map_guard: Mapping[str, Any],
) -> bool:
    if not lhs or not rhs:
        return False
    if _map_guard_reports_mismatch(map_guard):
        return False
    return _map_identity_key(lhs) == _map_identity_key(rhs) or {
        _map_identity_key(lhs),
        _map_identity_key(rhs),
    }.issubset(expected_aliases)


def _observed_map_name_mismatch(values: Sequence[str], expected_aliases: set[str]) -> bool:
    for value in values:
        key = _map_identity_key(value)
        if key and key not in expected_aliases:
            return True
    return False


def _accepted_map_aliases(expected: str) -> set[str]:
    key = _map_identity_key(expected)
    aliases = {key}
    if key.startswith("carla_"):
        aliases.add(key[len("carla_") :])
    aliases.add("town01")
    aliases.add("carla_town01")
    return {item for item in aliases if item}


def _map_identity_key(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/").rstrip("/")
    if not text:
        return ""
    leaf = text.rsplit("/", 1)[-1]
    if leaf in MAP_COMPONENT_FILENAMES.values():
        leaf = text.rsplit("/", 2)[-2]
    return leaf.lower()


def _parent_if_map_file(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value)
    if text.rsplit("/", 1)[-1] in MAP_COMPONENT_FILENAMES.values():
        return text.rsplit("/", 1)[0]
    return text


def _normalize_path(value: Any) -> str:
    text = os.path.expandvars(str(value or "")).strip()
    if not text:
        return ""
    return str(Path(text).expanduser()) if _looks_like_host_path(text) else text.rstrip("/")


def _looks_like_host_path(path: str) -> bool:
    return path.startswith("/") and not path.startswith("/apollo/") and not path.startswith("/data/")


def _sha256_if_exists(path: str) -> str | None:
    p = Path(path).expanduser()
    if not p.exists() or not p.is_file():
        return None
    digest = hashlib.sha256()
    with p.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _first_text(*parts: Any, default: str | None = None) -> str | None:
    if len(parts) % 2:
        raise ValueError("_first_text expects mapping/path pairs")
    for mapping, path in zip(parts[0::2], parts[1::2]):
        if not isinstance(mapping, Mapping):
            continue
        value = _nested(mapping, str(path))
        if value not in {None, ""}:
            return str(value)
    return default


def _nested(mapping: Mapping[str, Any], path: str) -> Any:
    cursor: Any = mapping
    for part in path.split("."):
        if isinstance(cursor, Mapping):
            cursor = cursor.get(part)
        elif isinstance(cursor, Sequence) and not isinstance(cursor, (str, bytes)):
            try:
                cursor = cursor[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cursor


def _unique(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in {None, ""}:
            continue
        text = str(value)
        if text not in seen:
            seen.add(text)
            result.append(text)
    return result
