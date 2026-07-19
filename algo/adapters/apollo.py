from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import yaml

from algo.adapters.base import Adapter
from carla_testbed.adapters.apollo.vehicle_reference import LOCALIZATION_BACK_OFFSET_AUTO_VALUES
from tbio.backends.cyberrt import CyberRTBackend


_TEXTPROTO_NUMBER = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
_LANE_START_PATTERN = re.compile(r"^(?P<indent>\s*)lane\s*\{")
_LANE_SPEED_LIMIT_PATTERN = re.compile(
    rf"^(?P<prefix>\s*speed_limit\s*:\s*)(?P<value>{_TEXTPROTO_NUMBER})"
)


def _textproto_brace_delta(line: str) -> int:
    """Count structural braces while ignoring quoted text and comments."""

    delta = 0
    in_string = False
    escaped = False
    for char in line:
        if escaped:
            escaped = False
            continue
        if in_string and char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if not in_string and char == "#":
            break
        if not in_string and char == "{":
            delta += 1
        elif not in_string and char == "}":
            delta -= 1
    return delta


def _rewrite_apollo_lane_speed_limits(
    map_text: str,
    *,
    replace_existing_mps: float | None,
    fill_missing: bool,
    missing_default_mps: float,
) -> tuple[str, Dict[str, Any]]:
    """Rewrite only top-level Apollo HDMap lane speed-limit fields."""

    lines = map_text.splitlines(keepends=True)
    rewritten: list[str] = []
    lane_count = 0
    explicit_before = 0
    missing_before = 0
    replaced_count = 0
    inserted_count = 0
    old_values: list[float] = []
    depth = 0
    index = 0

    while index < len(lines):
        line = lines[index]
        lane_match = _LANE_START_PATTERN.match(line) if depth == 0 else None
        if lane_match is None:
            rewritten.append(line)
            depth += _textproto_brace_delta(line)
            index += 1
            continue

        lane_count += 1
        block = [line]
        block_depth = _textproto_brace_delta(line)
        index += 1
        while block_depth > 0 and index < len(lines):
            block_line = lines[index]
            block.append(block_line)
            block_depth += _textproto_brace_delta(block_line)
            index += 1
        if block_depth != 0:
            raise ValueError("unterminated top-level lane block in Apollo HDMap textproto")

        direct_speed_indices: list[int] = []
        local_depth = 0
        for block_index, block_line in enumerate(block):
            depth_before = local_depth
            if depth_before == 1 and _LANE_SPEED_LIMIT_PATTERN.match(block_line):
                direct_speed_indices.append(block_index)
            local_depth += _textproto_brace_delta(block_line)

        if direct_speed_indices:
            explicit_before += len(direct_speed_indices)
            for block_index in direct_speed_indices:
                speed_match = _LANE_SPEED_LIMIT_PATTERN.match(block[block_index])
                if speed_match is None:
                    continue
                old_values.append(float(speed_match.group("value")))
                if replace_existing_mps is not None:
                    value_start, value_end = speed_match.span("value")
                    block[block_index] = (
                        block[block_index][:value_start]
                        + f"{replace_existing_mps:.12f}"
                        + block[block_index][value_end:]
                    )
                    replaced_count += 1
        else:
            missing_before += 1
            if fill_missing:
                closing_line = block[-1]
                newline = "\r\n" if closing_line.endswith("\r\n") else "\n"
                block.insert(
                    len(block) - 1,
                    f"{lane_match.group('indent')}  speed_limit: {missing_default_mps:.12f}{newline}",
                )
                inserted_count += 1

        rewritten.extend(block)

    stats: Dict[str, Any] = {
        "lane_count": lane_count,
        "explicit_speed_limit_count_before": explicit_before,
        "missing_lane_speed_limit_count_before": missing_before,
        "inserted_missing_lane_speed_limit_count": inserted_count,
        "existing_speed_limit_replaced_count": replaced_count,
        "explicit_speed_limit_count_after": explicit_before + inserted_count,
        "missing_lane_speed_limit_count_after": missing_before - inserted_count,
        "old_unique_speed_limits_mps": sorted(set(old_values)),
    }
    return "".join(rewritten), stats


class ApolloAdapter(Adapter):
    def __init__(self):
        self.repo_root = Path(__file__).resolve().parents[2]
        self.backend: CyberRTBackend | None = None

    def _resolve_apollo_root(self, apollo_cfg: Dict[str, Any]) -> Path | None:
        raw = str(apollo_cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT", "")).strip()
        if not raw:
            return None
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        return path.resolve()

    def _apollo_map_dir(self, apollo_root: Path | None, map_name: str) -> Path | None:
        if apollo_root is None or not map_name:
            return None
        map_tokens = [map_name, map_name.lower(), f"carla_{map_name.lower()}"]
        candidates: list[Path] = []
        for token in map_tokens:
            candidates.append(apollo_root / "modules" / "map" / "data" / token)
            candidates.append(apollo_root / "application-core" / "data" / "map_data" / token)
            candidates.append(apollo_root / "application-core" / "data" / "map-data" / token)
            candidates.append((apollo_root / ".." / "share" / "modules" / "map" / "data" / token).resolve())
        app_core_root = None
        parents = list(apollo_root.parents)
        if len(parents) >= 6:
            app_core_root = parents[5]
        if app_core_root is not None:
            for token in map_tokens:
                candidates.append((app_core_root / "data" / "map_data" / token).resolve())
                candidates.append((app_core_root / "data" / "map-data" / token).resolve())
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0] if candidates else None

    def _ensure_map_bounds_file(self, map_dir: Path | None, map_name: str, artifacts: Path) -> Path:
        out = self.repo_root / "configs" / "io" / "maps" / map_name / "map_bounds.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            return out
        if map_dir is None or not map_dir.exists():
            log = {
                "map_dir": str(map_dir) if map_dir is not None else "",
                "output": str(out),
                "returncode": None,
                "stdout": "",
                "stderr": "map_dir_missing",
            }
            (artifacts / "map_bounds_generation.json").write_text(json.dumps(log, indent=2))
            return out
        gen_script = self.repo_root / "tools" / "gen_map_bounds.py"
        if not gen_script.exists():
            return out
        proc = subprocess.run(
            [
                "python3",
                str(gen_script),
                "--map_dir",
                str(map_dir),
                "--output",
                str(out),
            ],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        log = {
            "map_dir": str(map_dir),
            "output": str(out),
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
        (artifacts / "map_bounds_generation.json").write_text(json.dumps(log, indent=2))
        return out

    @staticmethod
    def _sha256_file(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _map_component_paths(root: Path | None) -> Dict[str, str]:
        if root is None:
            return {}
        candidates = {
            "base_map": root / "base_map.txt",
            "routing_map": root / "routing_map.txt",
            "sim_map": root / "sim_map.txt",
        }
        return {name: str(path.resolve()) for name, path in candidates.items() if path.exists()}

    @staticmethod
    def _map_identity_signature(component_paths: Dict[str, str]) -> str:
        if not component_paths:
            return ""
        component_hashes: Dict[str, str] = {}
        for name, raw_path in sorted(component_paths.items()):
            path = Path(raw_path)
            if not path.exists():
                continue
            component_hashes[name] = ApolloAdapter._sha256_file(path)
        return ApolloAdapter._map_identity_signature_from_hashes(component_hashes)

    @staticmethod
    def _map_identity_signature_from_hashes(component_hashes: Dict[str, str]) -> str:
        if not component_hashes:
            return ""
        parts = []
        for name, digest in sorted(component_hashes.items()):
            if not digest:
                continue
            parts.append(f"{name}:{digest}")
        if not parts:
            return ""
        joined = "|".join(parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    @staticmethod
    def _discover_apollo_docker_container(docker_cfg: Dict[str, Any]) -> str:
        explicit = str(docker_cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER", "")).strip()
        if explicit:
            return explicit
        try:
            proc = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--format",
                    "{{.Names}}\t{{.Image}}\t{{.State}}",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            return ""
        if proc.returncode != 0:
            return ""

        running_candidates: list[str] = []
        all_candidates: list[str] = []
        for raw_line in proc.stdout.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t", 2)
            container_name = parts[0].strip()
            image = parts[1].strip().lower() if len(parts) > 1 else ""
            state = parts[2].strip().lower() if len(parts) > 2 else ""
            lowered_name = container_name.lower()
            if lowered_name.startswith("apollo") or "apollo" in lowered_name or "apollo" in image:
                all_candidates.append(container_name)
                if state == "running":
                    running_candidates.append(container_name)
        if len(running_candidates) == 1:
            return running_candidates[0]
        if len(all_candidates) == 1:
            return all_candidates[0]
        return ""

    def _inspect_container_map_context(
        self,
        *,
        docker_cfg: Dict[str, Any],
        requested_map_name: str,
        effective_bridge_root: Path | None,
        runtime_container_guess: str,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "available": False,
            "container_name": "",
            "apollo_root_in_container": str(
                docker_cfg.get("apollo_root_in_container", "/apollo") or "/apollo"
            ).strip(),
            "candidate_dirs": [],
            "selected_runtime_map_dir": "",
            "selected_complete": False,
            "selection_reason": "not_attempted",
            "component_paths": {},
            "component_hashes": {},
            "inspections": [],
            "error": "",
        }
        if not bool(
            docker_cfg.get("enabled") or docker_cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER")
        ):
            payload["selection_reason"] = "docker_disabled"
            return payload

        container_name = self._discover_apollo_docker_container(docker_cfg)
        payload["container_name"] = container_name
        if not container_name:
            payload["selection_reason"] = "container_unresolved"
            return payload

        apollo_root_in_container = str(payload["apollo_root_in_container"] or "/apollo").strip() or "/apollo"
        token_candidates: list[str] = []

        def _add_token(raw: str) -> None:
            text = str(raw or "").strip()
            if not text:
                return
            token_candidates.append(text)

        requested_lower = requested_map_name.lower() if requested_map_name else ""
        _add_token(requested_map_name)
        if requested_lower:
            _add_token(requested_lower)
            _add_token(f"carla_{requested_lower}")
        if effective_bridge_root is not None:
            _add_token(effective_bridge_root.name)
            _add_token(effective_bridge_root.name.lower())

        candidate_dirs: list[str] = []
        seen_dirs: set[str] = set()

        def _add_dir(raw: str) -> None:
            text = str(raw or "").strip()
            if not text or text in seen_dirs:
                return
            seen_dirs.add(text)
            candidate_dirs.append(text)

        _add_dir(runtime_container_guess)
        for token in token_candidates:
            _add_dir(str(PurePosixPath(apollo_root_in_container) / "modules" / "map" / "data" / token))
            _add_dir(
                str(PurePosixPath(apollo_root_in_container) / ".." / "share" / "modules" / "map" / "data" / token)
            )
        payload["candidate_dirs"] = candidate_dirs
        if not candidate_dirs:
            payload["selection_reason"] = "no_candidate_dirs"
            return payload

        inspect_script = """
python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path

candidates = json.loads(os.environ.get("TB_CANDIDATE_MAP_DIRS_JSON", "[]") or "[]")
components = {
    "base_map": "base_map.txt",
    "routing_map": "routing_map.txt",
    "sim_map": "sim_map.txt",
}

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

inspections = []
selected = None
for raw in candidates:
    root = Path(str(raw or "").strip())
    row = {
        "path": str(root),
        "exists": root.exists(),
        "components": {},
        "component_count": 0,
        "complete": False,
    }
    if row["exists"]:
        for component, filename in components.items():
            component_path = root / filename
            exists = component_path.exists()
            row["components"][component] = {
                "path": str(component_path),
                "exists": exists,
                "sha256": sha256(component_path) if exists else "",
            }
        row["component_count"] = sum(
            1 for component in row["components"].values() if bool(component.get("exists"))
        )
        row["complete"] = row["component_count"] == len(components)
        if selected is None and row["complete"]:
            selected = row
    inspections.append(row)

if selected is None:
    for row in inspections:
        if row.get("exists"):
            selected = row
            break

selection_reason = "no_candidate_exists"
if selected is not None:
    selection_reason = (
        "first_complete_candidate" if bool(selected.get("complete")) else "first_existing_candidate"
    )

component_paths = {}
component_hashes = {}
if selected is not None:
    for component, detail in (selected.get("components") or {}).items():
        if bool(detail.get("exists")):
            component_paths[component] = str(detail.get("path") or "")
            component_hashes[component] = str(detail.get("sha256") or "")

print(
    json.dumps(
        {
            "available": selected is not None,
            "container_name": str(os.environ.get("TB_CONTAINER_NAME") or ""),
            "apollo_root_in_container": str(os.environ.get("TB_APOLLO_ROOT_IN_CONTAINER") or ""),
            "candidate_dirs": candidates,
            "selected_runtime_map_dir": str(selected.get("path") or "") if selected is not None else "",
            "selected_complete": bool(selected.get("complete")) if selected is not None else False,
            "selection_reason": selection_reason,
            "component_paths": component_paths,
            "component_hashes": component_hashes,
            "inspections": inspections,
        },
        ensure_ascii=False,
    )
)
PY
""".strip()
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "-e",
                f"TB_CONTAINER_NAME={container_name}",
                "-e",
                f"TB_APOLLO_ROOT_IN_CONTAINER={apollo_root_in_container}",
                "-e",
                f"TB_CANDIDATE_MAP_DIRS_JSON={json.dumps(candidate_dirs, ensure_ascii=False)}",
                container_name,
                "bash",
                "-lc",
                inspect_script,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            payload["selection_reason"] = "docker_exec_failed"
            payload["error"] = proc.stderr.strip()
            return payload
        try:
            inspected = json.loads(proc.stdout.strip())
        except Exception as exc:
            payload["selection_reason"] = "docker_exec_parse_failed"
            payload["error"] = f"{type(exc).__name__}:{exc}"
            return payload
        if isinstance(inspected, dict):
            payload.update(inspected)
        return payload

    def _write_map_contract_guard(
        self,
        *,
        profile: Dict[str, Any],
        bridge: Dict[str, Any],
        artifacts: Path,
        runtime_map_dir: Path | None,
        original_bridge_map_file: str,
        fail_fast_on_high_risk_mismatch: bool,
        apollo_root: Path | None,
    ) -> Dict[str, Any]:
        run_cfg = profile.get("run", {}) or {}
        apollo_cfg = profile.get("algo", {}).get("apollo", {}) or {}
        docker_cfg = apollo_cfg.get("docker", {}) or {}
        requested_map_name = str(run_cfg.get("map", "") or "").strip()
        dreamview_selected_map = requested_map_name or None
        runtime_root = runtime_map_dir.resolve() if runtime_map_dir is not None else None
        effective_bridge_map_file = str(bridge.get("map_file", "") or "").strip()
        effective_bridge_map_path = Path(effective_bridge_map_file).expanduser() if effective_bridge_map_file else None
        if effective_bridge_map_path is not None and not effective_bridge_map_path.is_absolute():
            effective_bridge_map_path = (self.repo_root / effective_bridge_map_path).resolve()
        effective_bridge_root = (
            effective_bridge_map_path.resolve().parent
            if effective_bridge_map_path is not None
            else None
        )
        original_bridge_map_path = Path(original_bridge_map_file).expanduser() if original_bridge_map_file else None
        if original_bridge_map_path is not None and not original_bridge_map_path.is_absolute():
            original_bridge_map_path = (self.repo_root / original_bridge_map_path).resolve()
        original_bridge_root = (
            original_bridge_map_path.resolve().parent
            if original_bridge_map_path is not None
            else None
        )

        runtime_components = self._map_component_paths(runtime_root)
        effective_components = self._map_component_paths(effective_bridge_root)
        original_components = self._map_component_paths(original_bridge_root)
        requested_tokens = (
            {requested_map_name.lower(), f"carla_{requested_map_name.lower()}"} if requested_map_name else set()
        )
        effective_bridge_name = effective_bridge_root.name.lower() if effective_bridge_root is not None else ""
        effective_bridge_map_complete = all(
            component in effective_components for component in ("base_map", "routing_map", "sim_map")
        )
        requested_map_matches_effective_bridge = bool(
            effective_bridge_name and requested_tokens and effective_bridge_name in requested_tokens
        )

        app_core_root = None
        if apollo_root is not None:
            parents = list(apollo_root.parents)
            if len(parents) >= 6:
                app_core_root = parents[5]
        apollo_root_in_container = str(docker_cfg.get("apollo_root_in_container", "/apollo") or "/apollo").strip()
        runtime_container_guess = ""
        if runtime_root is not None and app_core_root is not None:
            try:
                rel = runtime_root.relative_to(app_core_root)
                runtime_container_guess = str((Path(apollo_root_in_container) / ".." / rel).resolve())
            except Exception:
                runtime_container_guess = ""
        container_runtime = self._inspect_container_map_context(
            docker_cfg=docker_cfg,
            requested_map_name=requested_map_name,
            effective_bridge_root=effective_bridge_root,
            runtime_container_guess=runtime_container_guess,
        )
        container_runtime_root = str(container_runtime.get("selected_runtime_map_dir") or "").strip()
        container_components = dict(container_runtime.get("component_paths") or {})
        container_component_hashes = {
            str(name): str(digest or "")
            for name, digest in dict(container_runtime.get("component_hashes") or {}).items()
            if str(digest or "").strip()
        }
        container_map_complete = all(
            component in container_components for component in ("base_map", "routing_map", "sim_map")
        )

        mismatch_reasons: list[str] = []
        if runtime_root is None or not runtime_root.exists():
            mismatch_reasons.append("runtime_map_dir_missing")
        for component in ("base_map", "routing_map", "sim_map"):
            if runtime_root is not None and component not in runtime_components:
                mismatch_reasons.append(f"runtime_{component}_missing")
            if effective_bridge_root is not None and component not in effective_components:
                mismatch_reasons.append(f"effective_bridge_{component}_missing")

        same_path = bool(runtime_root and effective_bridge_root and runtime_root == effective_bridge_root)
        same_version_assumed = same_path
        hash_summary: Dict[str, Dict[str, Any]] = {}
        runtime_signature = self._map_identity_signature(runtime_components)
        if runtime_root is not None and effective_bridge_root is not None:
            comparable = set(runtime_components.keys()) & set(effective_components.keys())
            if comparable:
                same_version_assumed = True
                for component in sorted(comparable):
                    runtime_component = Path(runtime_components[component])
                    effective_component = Path(effective_components[component])
                    runtime_hash = self._sha256_file(runtime_component)
                    effective_hash = self._sha256_file(effective_component)
                    equal_hash = runtime_hash == effective_hash
                    hash_summary[component] = {
                        "runtime_path": str(runtime_component),
                        "effective_bridge_path": str(effective_component),
                        "runtime_sha256": runtime_hash,
                        "effective_bridge_sha256": effective_hash,
                        "hash_equal": equal_hash,
                        "runtime_source": "host",
                    }
                    same_version_assumed = same_version_assumed and equal_hash
            elif not same_path:
                same_version_assumed = False
        elif container_runtime_root and effective_bridge_root is not None:
            mismatch_reasons = [
                reason
                for reason in mismatch_reasons
                if reason != "runtime_map_dir_missing" and not reason.startswith("runtime_")
            ]
            comparable = set(container_components.keys()) & set(effective_components.keys())
            if comparable:
                same_version_assumed = True
                for component in sorted(comparable):
                    effective_component = Path(effective_components[component])
                    container_hash = str(container_component_hashes.get(component) or "")
                    effective_hash = self._sha256_file(effective_component)
                    equal_hash = bool(container_hash) and container_hash == effective_hash
                    hash_summary[component] = {
                        "runtime_path": str(container_components[component]),
                        "effective_bridge_path": str(effective_component),
                        "runtime_sha256": container_hash,
                        "effective_bridge_sha256": effective_hash,
                        "hash_equal": equal_hash,
                        "runtime_source": "container",
                    }
                    same_version_assumed = same_version_assumed and equal_hash
                same_version_assumed = (
                    same_version_assumed and container_map_complete and effective_bridge_map_complete
                )
            else:
                same_version_assumed = False
            runtime_signature = self._map_identity_signature_from_hashes(container_component_hashes)
        same_derivation_chain = bool(same_path or same_version_assumed)
        path_alias_only = bool((not same_path) and same_derivation_chain)

        if not same_path and not path_alias_only:
            mismatch_reasons.append("runtime_map_root_differs_from_effective_bridge_map_root")
        elif path_alias_only:
            mismatch_reasons.append("runtime_map_root_path_alias_only")
        if not same_derivation_chain:
            mismatch_reasons.append("routing_map_base_map_sim_map_not_same_derivation_chain")
        if requested_map_name and runtime_root is not None:
            runtime_name = runtime_root.name.lower()
            tokens = {requested_map_name.lower(), f"carla_{requested_map_name.lower()}"}
            if runtime_name not in tokens:
                mismatch_reasons.append("dreamview_requested_map_differs_from_runtime_map_dir_name")

        effective_signature = self._map_identity_signature(effective_components)
        original_signature = self._map_identity_signature(original_components)
        if path_alias_only and runtime_signature and effective_signature and runtime_signature != effective_signature:
            mismatch_reasons.append("path_alias_claim_but_component_signature_differs")
            same_version_assumed = False
            same_derivation_chain = same_path
            path_alias_only = False

        runtime_unresolved_bridge_explicit = bool(
            runtime_root is None
            and not container_runtime_root
            and effective_bridge_root is not None
            and effective_bridge_map_complete
            and (not requested_tokens or requested_map_matches_effective_bridge)
        )
        if runtime_unresolved_bridge_explicit:
            normalized_reasons: list[str] = []
            for reason in mismatch_reasons:
                if reason == "runtime_map_dir_missing":
                    normalized_reasons.append("runtime_map_dir_unresolved_but_bridge_map_explicit")
                elif reason in {
                    "runtime_map_root_differs_from_effective_bridge_map_root",
                    "routing_map_base_map_sim_map_not_same_derivation_chain",
                    "dreamview_requested_map_differs_from_runtime_map_dir_name",
                }:
                    continue
                else:
                    normalized_reasons.append(reason)
            mismatch_reasons = normalized_reasons

        mismatch_classification = "aligned"
        if path_alias_only:
            mismatch_classification = "path_alias_only"
        elif runtime_unresolved_bridge_explicit:
            mismatch_classification = "runtime_unresolved_bridge_explicit"
        elif mismatch_reasons:
            mismatch_classification = "true_mismatch"

        high_risk = False if runtime_unresolved_bridge_explicit else any(
            reason in {
                "runtime_map_dir_missing",
                "runtime_map_root_differs_from_effective_bridge_map_root",
                "routing_map_base_map_sim_map_not_same_derivation_chain",
                "path_alias_claim_but_component_signature_differs",
            }
            for reason in mismatch_reasons
        )
        map_contract_invalid = bool(high_risk)
        runtime_resolution_mode = (
            "apollo_runtime_map_dir_resolved"
            if runtime_root is not None
            else (
                "container_runtime_map_dir_resolved_host_unresolved"
                if container_runtime_root
                else (
                    "bridge_map_explicit_runtime_unresolved"
                    if runtime_unresolved_bridge_explicit
                    else "runtime_unresolved"
                )
            )
        )

        host_container_map_path_mapping = {
            "apollo_root_host": str(apollo_root) if apollo_root is not None else "",
            "apollo_root_in_container": apollo_root_in_container,
            "application_core_root_host": str(app_core_root) if app_core_root is not None else "",
            "runtime_map_dir_host": str(runtime_root) if runtime_root is not None else "",
            "runtime_map_dir_container_guess": runtime_container_guess,
            "runtime_map_dir_container_actual": container_runtime_root,
            "container_name": str(container_runtime.get("container_name") or ""),
            "runtime_component_source": (
                "host_runtime_dir"
                if runtime_root is not None
                else ("container_runtime_dir" if container_runtime_root else "unresolved")
            ),
            "mapping_confidence": (
                "container_runtime_confirmed"
                if container_runtime_root
                else ("derived_from_application_core_root" if runtime_container_guess else "unknown")
            ),
        }
        payload: Dict[str, Any] = {
            "map_contract_invalid": map_contract_invalid,
            "high_risk_mismatch": high_risk,
            "mismatch_reasons": mismatch_reasons,
            "mismatch_classification": mismatch_classification,
            "dreamview_selected_map": dreamview_selected_map,
            "dreamview_selected_map_source": "profile.run.map",
            "runtime_map_dir": str(runtime_root) if runtime_root is not None else "",
            "runtime_map_dir_container_actual": container_runtime_root,
            "runtime_component_source": host_container_map_path_mapping["runtime_component_source"],
            "effective_bridge_map_file": str(effective_bridge_map_path) if effective_bridge_map_path is not None else "",
            "effective_bridge_map_root": str(effective_bridge_root) if effective_bridge_root is not None else "",
            "original_bridge_map_file": str(original_bridge_map_path) if original_bridge_map_path is not None else "",
            "original_bridge_map_root": str(original_bridge_root) if original_bridge_root is not None else "",
            "runtime_component_paths": runtime_components,
            "container_component_paths": container_components,
            "container_component_hashes": container_component_hashes,
            "container_runtime_map_complete": container_map_complete,
            "container_runtime_probe": container_runtime,
            "effective_bridge_component_paths": effective_components,
            "original_bridge_component_paths": original_components,
            "effective_bridge_map_complete": effective_bridge_map_complete,
            "requested_map_matches_effective_bridge_root": requested_map_matches_effective_bridge,
            "same_path": same_path,
            "same_version_assumed": same_version_assumed,
            "same_derivation_chain": same_derivation_chain,
            "path_alias_only": path_alias_only,
            "runtime_resolution_mode": runtime_resolution_mode,
            "legacy_requested_bridge_map_root_differs": bool(
                original_bridge_root is not None
                and runtime_root is not None
                and original_bridge_root != runtime_root
            ),
            "runtime_map_identity_signature": runtime_signature,
            "effective_bridge_map_identity_signature": effective_signature,
            "original_bridge_map_identity_signature": original_signature,
            "host_container_map_path_mapping": host_container_map_path_mapping,
            "hash_summary": hash_summary,
            "fail_fast_on_high_risk_mismatch": fail_fast_on_high_risk_mismatch,
        }
        (artifacts / "map_contract_guard.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        (artifacts / "stage5_map_contract_guard.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False)
        )
        md_lines = [
            "# Map Contract Guard",
            "",
            f"- dreamview_selected_map: `{dreamview_selected_map}`",
            f"- runtime_map_dir: `{payload['runtime_map_dir']}`",
            f"- runtime_map_dir_container_actual: `{payload['runtime_map_dir_container_actual']}`",
            f"- runtime_component_source: `{payload['runtime_component_source']}`",
            f"- effective_bridge_map_root: `{payload['effective_bridge_map_root']}`",
            f"- original_bridge_map_root: `{payload['original_bridge_map_root']}`",
            f"- runtime_resolution_mode: `{runtime_resolution_mode}`",
            f"- effective_bridge_map_complete: `{effective_bridge_map_complete}`",
            f"- container_runtime_map_complete: `{container_map_complete}`",
            f"- requested_map_matches_effective_bridge_root: `{requested_map_matches_effective_bridge}`",
            f"- same_path: `{same_path}`",
            f"- same_version_assumed: `{same_version_assumed}`",
            f"- same_derivation_chain: `{same_derivation_chain}`",
            f"- mismatch_classification: `{mismatch_classification}`",
            f"- map_contract_invalid: `{map_contract_invalid}`",
            f"- high_risk_mismatch: `{high_risk}`",
            f"- runtime_map_identity_signature: `{runtime_signature}`",
            "",
            "## Mismatch Reasons",
            "",
        ]
        if mismatch_reasons:
            md_lines.extend([f"- `{item}`" for item in mismatch_reasons])
        else:
            md_lines.append("- none")
        md_lines.extend(
            [
                "",
                "## Container Runtime",
                "",
                f"- container_name: `{host_container_map_path_mapping['container_name']}`",
                f"- runtime_map_dir_container_guess: `{host_container_map_path_mapping['runtime_map_dir_container_guess']}`",
                f"- runtime_map_dir_container_actual: `{host_container_map_path_mapping['runtime_map_dir_container_actual']}`",
                f"- mapping_confidence: `{host_container_map_path_mapping['mapping_confidence']}`",
            ]
        )
        if hash_summary:
            md_lines.extend(["", "## Runtime vs Effective Hash Summary", ""])
            for component, detail in sorted(hash_summary.items()):
                md_lines.append(
                    f"- `{component}` runtime=`{detail.get('runtime_sha256')}` "
                    f"effective=`{detail.get('effective_bridge_sha256')}` "
                    f"equal=`{detail.get('hash_equal')}` source=`{detail.get('runtime_source', 'host')}`"
                )
        (artifacts / "map_contract_guard.md").write_text("\n".join(md_lines) + "\n")
        (artifacts / "stage5_map_contract_guard.md").write_text("\n".join(md_lines) + "\n")
        return payload

    def _ensure_bridge_config(self, profile: Dict[str, Any], run_dir: Path) -> Path:
        apollo_cfg = profile.setdefault("algo", {}).setdefault("apollo", {})
        template_path = apollo_cfg.get("bridge_template", "tools/apollo10_cyber_bridge/config_example.yaml")
        template_file = Path(template_path)
        if not template_file.is_absolute():
            template_file = (self.repo_root / template_file).resolve()
        if not template_file.exists():
            raise FileNotFoundError(f"apollo bridge template missing: {template_file}")

        cfg = yaml.safe_load(template_file.read_text()) or {}
        run_cfg = profile.get("run", {}) or {}
        bridge_run_cfg = cfg.setdefault("run", {})
        for key in ("profile_name", "claim_profile", "materialization_probe"):
            if key in run_cfg:
                bridge_run_cfg[key] = run_cfg[key]
        if "claim_profile" not in bridge_run_cfg:
            bridge_run_cfg["claim_profile"] = False
        if "materialization_probe" not in bridge_run_cfg:
            bridge_run_cfg["materialization_probe"] = False
        if isinstance(profile.get("typed_runtime"), dict):
            cfg["typed_runtime"] = dict(profile["typed_runtime"])
        if isinstance(profile.get("reports"), dict):
            cfg["reports"] = dict(profile["reports"])
        artifacts = run_dir / "artifacts"
        io_ros = ((profile.get("io", {}) or {}).get("ros", {}) or {})
        bridge = (cfg.get("bridge", {}) or {})
        ros2_cfg = cfg.setdefault("ros2", {})
        cyber_cfg = cfg.setdefault("cyber", {})
        apollo_root = self._resolve_apollo_root(apollo_cfg)
        map_name = str(run_cfg.get("map", "")).strip()
        map_dir = self._apollo_map_dir(apollo_root, map_name)
        map_contract_cfg = apollo_cfg.get("map_contract", {}) or {}
        align_bridge_map_to_runtime = bool(
            map_contract_cfg.get("align_bridge_map_to_runtime_map", True)
        )
        fail_fast_on_high_risk_mismatch = bool(
            map_contract_cfg.get("fail_fast_on_high_risk_mismatch", False)
        )

        ego_id = str(run_cfg.get("ego_id", "hero"))
        namespace = str(io_ros.get("namespace", "/carla"))
        if not namespace.startswith("/"):
            namespace = "/" + namespace
        namespace = namespace.rstrip("/") or "/carla"
        prefix = f"{namespace}/{ego_id}"

        ros2_cfg["ego_id"] = ego_id
        ros2_cfg["namespace"] = namespace
        ros2_cfg["odom_topic"] = str(ros2_cfg.get("odom_topic") or f"{prefix}/odom")
        ros2_cfg["objects3d_topic"] = str(ros2_cfg.get("objects3d_topic") or f"{prefix}/objects3d")
        ros2_cfg["objects_markers_topic"] = str(
            ros2_cfg.get("objects_markers_topic") or f"{prefix}/objects_markers"
        )
        ros2_cfg["objects_json_topic"] = str(ros2_cfg.get("objects_json_topic") or f"{prefix}/objects_gt_json")
        ros2_cfg["control_out_topic"] = str(
            apollo_cfg.get("control_out_topic")
            or ros2_cfg.get("control_out_topic")
            or "/tb/ego/control_cmd"
        )
        ros2_cfg["control_out_type"] = str(
            apollo_cfg.get("control_out_type")
            or ros2_cfg.get("control_out_type")
            or "direct"
        )

        cyber_cfg["localization_channel"] = str(
            cyber_cfg.get("localization_channel") or "/apollo/localization/pose"
        )
        cyber_cfg["chassis_channel"] = str(cyber_cfg.get("chassis_channel") or "/apollo/canbus/chassis")
        cyber_cfg["obstacles_channel"] = str(
            cyber_cfg.get("obstacles_channel") or "/apollo/perception/obstacles"
        )
        cyber_cfg["control_channel"] = str(cyber_cfg.get("control_channel") or "/apollo/control")
        cyber_cfg["routing_request_channel"] = str(
            cyber_cfg.get("routing_request_channel") or "/apollo/raw_routing_request"
        )
        cyber_cfg["action_channel"] = str(
            cyber_cfg.get("action_channel") or "/apollo/external_command/action"
        )
        cyber_cfg["lane_follow_channel"] = str(
            cyber_cfg.get("lane_follow_channel") or "/apollo/external_command/lane_follow"
        )
        cyber_cfg["routing_response_channel"] = str(
            cyber_cfg.get("routing_response_channel") or "/apollo/routing_response"
        )

        apollo_bridge_cfg = apollo_cfg.get("bridge", {}) or {}
        transport_mode = str(apollo_cfg.get("transport_mode") or "ros2_gt").strip().lower() or "ros2_gt"
        if transport_mode not in {"ros2_gt", "carla_direct"}:
            raise RuntimeError(
                f"unsupported algo.apollo.transport_mode={transport_mode}; expected ros2_gt|carla_direct"
            )
        direct_bridge_cfg = dict(apollo_cfg.get("direct_bridge", {}) or {})
        if "publish_rate_hz" in apollo_bridge_cfg:
            bridge["publish_rate_hz"] = float(apollo_bridge_cfg["publish_rate_hz"])
        if "obstacle_publish_rate_hz" in apollo_bridge_cfg:
            bridge["obstacle_publish_rate_hz"] = float(
                apollo_bridge_cfg["obstacle_publish_rate_hz"]
            )
        if "obstacle_publish_policy" in apollo_bridge_cfg:
            bridge["obstacle_publish_policy"] = str(
                apollo_bridge_cfg["obstacle_publish_policy"]
            )
        if "obstacle_alignment_policy" in apollo_bridge_cfg:
            bridge["obstacle_alignment_policy"] = str(
                apollo_bridge_cfg["obstacle_alignment_policy"]
            )
        if "localization_time_source" in apollo_bridge_cfg:
            bridge["localization_time_source"] = str(apollo_bridge_cfg["localization_time_source"])
        if "obstacle_time_source" in apollo_bridge_cfg:
            bridge["obstacle_time_source"] = str(apollo_bridge_cfg["obstacle_time_source"])
        if "cyber_clock" in apollo_bridge_cfg:
            cyber_clock_cfg = apollo_bridge_cfg["cyber_clock"]
            bridge["cyber_clock"] = (
                dict(cyber_clock_cfg)
                if isinstance(cyber_clock_cfg, dict)
                else cyber_clock_cfg
            )
        if "max_obstacles" in apollo_bridge_cfg:
            bridge["max_obstacles"] = int(apollo_bridge_cfg["max_obstacles"])
        if "radius_m" in apollo_bridge_cfg:
            bridge["radius_m"] = float(apollo_bridge_cfg["radius_m"])
        if "localization_back_offset_m" in apollo_bridge_cfg:
            raw_back_offset = apollo_bridge_cfg["localization_back_offset_m"]
            if str(raw_back_offset).strip().lower() in LOCALIZATION_BACK_OFFSET_AUTO_VALUES:
                bridge["localization_back_offset_m"] = str(raw_back_offset).strip().lower()
            else:
                bridge["localization_back_offset_m"] = float(raw_back_offset)
        if "vehicle_reference_path" in apollo_bridge_cfg:
            bridge["vehicle_reference_path"] = str(apollo_bridge_cfg["vehicle_reference_path"])
        if "debug_pose_print" in apollo_bridge_cfg:
            bridge["debug_pose_print"] = bool(apollo_bridge_cfg["debug_pose_print"])
        if "debug_dump_control_raw" in apollo_bridge_cfg:
            bridge["debug_dump_control_raw"] = bool(apollo_bridge_cfg["debug_dump_control_raw"])
        if "artifact_async_write_enabled" in apollo_bridge_cfg:
            bridge["artifact_async_write_enabled"] = bool(
                apollo_bridge_cfg["artifact_async_write_enabled"]
            )
        for key in (
            "artifact_async_queue_max_rows",
            "artifact_async_queue_soft_limit_rows",
            "artifact_flush_max_pending_rows",
            "stage5_debug_artifact_sample_stride",
            "reference_debug_artifact_sample_stride",
            "control_debug_artifact_sample_stride",
            "claim_evidence_artifact_sample_stride",
        ):
            if key in apollo_bridge_cfg:
                bridge[key] = int(apollo_bridge_cfg[key])
        for key in (
            "artifact_flush_interval_s",
            "artifact_stats_flush_interval_s",
        ):
            if key in apollo_bridge_cfg:
                bridge[key] = float(apollo_bridge_cfg[key])
        if "control_debug_artifact_sample_strides" in apollo_bridge_cfg:
            sample_strides = apollo_bridge_cfg["control_debug_artifact_sample_strides"]
            if not isinstance(sample_strides, dict):
                raise RuntimeError(
                    "algo.apollo.bridge.control_debug_artifact_sample_strides must be a mapping"
                )
            bridge["control_debug_artifact_sample_strides"] = {
                str(artifact_name): int(sample_stride)
                for artifact_name, sample_stride in sample_strides.items()
            }
        if isinstance(apollo_bridge_cfg.get("claim_grade"), dict):
            bridge["claim_grade"] = dict(apollo_bridge_cfg["claim_grade"])
        if isinstance(apollo_bridge_cfg.get("localization_acceleration_filter"), dict):
            bridge["localization_acceleration_filter"] = dict(
                apollo_bridge_cfg["localization_acceleration_filter"]
            )
        if "localization_acceleration_source" in apollo_bridge_cfg:
            bridge["localization_acceleration_source"] = str(
                apollo_bridge_cfg["localization_acceleration_source"]
            )
        if isinstance(
            apollo_bridge_cfg.get("localization_authored_initial_state_transition"),
            dict,
        ):
            bridge["localization_authored_initial_state_transition"] = dict(
                apollo_bridge_cfg["localization_authored_initial_state_transition"]
            )
        bridge["claim_profile"] = bool(bridge_run_cfg.get("claim_profile", False))
        bridge["materialization_probe"] = bool(
            bridge_run_cfg.get("materialization_probe", False)
        )
        if "map_file" in apollo_bridge_cfg:
            bridge["map_file"] = str(apollo_bridge_cfg["map_file"])
        if "map_bounds_file" in apollo_bridge_cfg:
            bridge["map_bounds_file"] = str(apollo_bridge_cfg["map_bounds_file"])
        original_bridge_map_file = str(bridge.get("map_file", "") or "").strip()
        if isinstance(apollo_bridge_cfg.get("front_obstacle_behavior"), dict):
            bridge["front_obstacle_behavior"] = dict(apollo_bridge_cfg["front_obstacle_behavior"])
        bridge["apollo_runtime_map_dir"] = str(map_dir) if map_dir is not None else ""
        bridge["dreamview_selected_map"] = map_name

        selected_map_file = ""
        if map_dir is not None:
            for candidate in ("base_map.txt", "base_map.xml", "sim_map.txt", "sim_map.xml"):
                path = map_dir / candidate
                if path.exists():
                    selected_map_file = str(path)
                    break
        if map_dir is not None and (align_bridge_map_to_runtime or not str(bridge.get("map_file", "")).strip()):
            if selected_map_file:
                bridge["map_file"] = selected_map_file
            elif not str(bridge.get("map_file", "")).strip():
                bridge["map_file"] = str(map_dir / "base_map.txt")
        if not str(bridge.get("map_bounds_file", "")).strip():
            bounds_file = self._ensure_map_bounds_file(map_dir, map_name or "unknown", artifacts)
            bridge["map_bounds_file"] = str(bounds_file)

        auto_routing = bridge.setdefault("auto_routing", {})
        routing_cfg = apollo_cfg.get("routing", {}) or {}
        if "enable" in routing_cfg:
            auto_routing["enabled"] = bool(routing_cfg["enable"])
        if "goal_mode" in routing_cfg:
            auto_routing["goal_mode"] = str(routing_cfg["goal_mode"])
        if "end_ahead_m" in routing_cfg:
            auto_routing["end_ahead_m"] = float(routing_cfg["end_ahead_m"])
        if "min_end_ahead_m" in routing_cfg:
            auto_routing["min_end_ahead_m"] = float(routing_cfg["min_end_ahead_m"])
        if "startup_end_ahead_m" in routing_cfg:
            auto_routing["startup_end_ahead_m"] = float(routing_cfg["startup_end_ahead_m"])
        if "startup_speed_threshold_mps" in routing_cfg:
            auto_routing["startup_speed_threshold_mps"] = float(routing_cfg["startup_speed_threshold_mps"])
        if "startup_hold_sec" in routing_cfg:
            auto_routing["startup_hold_sec"] = float(routing_cfg["startup_hold_sec"])
        if "start_nudge_m" in routing_cfg:
            auto_routing["start_nudge_m"] = float(routing_cfg["start_nudge_m"])
        if "start_nudge_retry_step_m" in routing_cfg:
            auto_routing["start_nudge_retry_step_m"] = float(routing_cfg["start_nudge_retry_step_m"])
        if "start_nudge_min_safe_m" in routing_cfg:
            auto_routing["start_nudge_min_safe_m"] = float(routing_cfg["start_nudge_min_safe_m"])
        if "start_nudge_max_m" in routing_cfg:
            auto_routing["start_nudge_max_m"] = float(routing_cfg["start_nudge_max_m"])
        if "resend_sec" in routing_cfg:
            auto_routing["resend_sec"] = float(routing_cfg["resend_sec"])
        if "max_attempts" in routing_cfg:
            auto_routing["max_attempts"] = int(routing_cfg["max_attempts"])
        if "target_speed_mps" in routing_cfg:
            auto_routing["target_speed_mps"] = float(routing_cfg["target_speed_mps"])
        if "startup_delay_sec" in routing_cfg:
            auto_routing["startup_delay_sec"] = float(routing_cfg["startup_delay_sec"])
        if "startup_apollo_warmup_sec" in routing_cfg:
            auto_routing["startup_apollo_warmup_sec"] = float(
                routing_cfg["startup_apollo_warmup_sec"]
            )
        if "lane_follow_refresh_sec" in routing_cfg:
            auto_routing["lane_follow_refresh_sec"] = float(routing_cfg["lane_follow_refresh_sec"])
        if "lane_follow_no_response_grace_sec" in routing_cfg:
            auto_routing["lane_follow_no_response_grace_sec"] = float(
                routing_cfg["lane_follow_no_response_grace_sec"]
            )
        if "freeze_after_success" in routing_cfg:
            auto_routing["freeze_after_success"] = bool(routing_cfg["freeze_after_success"])
        if "freeze_after_long_route_success_only" in routing_cfg:
            auto_routing["freeze_after_long_route_success_only"] = bool(
                routing_cfg["freeze_after_long_route_success_only"]
            )
        if "use_seed_heading" in routing_cfg:
            auto_routing["use_seed_heading"] = bool(routing_cfg["use_seed_heading"])
        if "use_long_goal_after_move" in routing_cfg:
            auto_routing["use_long_goal_after_move"] = bool(routing_cfg["use_long_goal_after_move"])
        if "defer_long_goal_until_planning_ready" in routing_cfg:
            auto_routing["defer_long_goal_until_planning_ready"] = bool(
                routing_cfg["defer_long_goal_until_planning_ready"]
            )
        if "long_goal_planning_ready_min_nonempty_count" in routing_cfg:
            auto_routing["long_goal_planning_ready_min_nonempty_count"] = int(
                routing_cfg["long_goal_planning_ready_min_nonempty_count"]
            )
        if "defer_long_goal_until_route_debug_ready" in routing_cfg:
            auto_routing["defer_long_goal_until_route_debug_ready"] = bool(
                routing_cfg["defer_long_goal_until_route_debug_ready"]
            )
        if "defer_long_goal_max_wait_sec" in routing_cfg:
            auto_routing["defer_long_goal_max_wait_sec"] = float(
                routing_cfg["defer_long_goal_max_wait_sec"]
            )
        if "clamp_to_map_bounds" in routing_cfg:
            auto_routing["clamp_to_map_bounds"] = bool(routing_cfg["clamp_to_map_bounds"])
        if "map_bounds_margin_m" in routing_cfg:
            auto_routing["map_bounds_margin_m"] = float(routing_cfg["map_bounds_margin_m"])
        if isinstance(routing_cfg.get("fixed_goal_xy"), dict):
            auto_routing["fixed_goal_xy"] = dict(routing_cfg["fixed_goal_xy"])
        auto_routing["scenario_goal_path"] = str(
            routing_cfg.get("scenario_goal_path") or (artifacts / "scenario_goal.json")
        )
        auto_routing["send_lane_follow"] = bool(auto_routing.get("send_lane_follow", False))
        for key in (
            "send_action",
            "send_lane_follow",
            "send_routing_request",
            "auto_enable_lane_follow_fallback",
            "snap_start_to_lane",
            "snap_goal_to_lane",
            "start_nudge_use_lane_heading",
            "snap_allow_untrusted_source",
            "disable_nudge_when_snap_rejected",
        ):
            if key in routing_cfg:
                auto_routing[key] = bool(routing_cfg[key])
        for key in (
            "snap_source_mode",
            "snap_heading_diff_max_deg",
            "snap_heading_diff_hard_reject_deg",
            "lane_heading_nudge_max_heading_diff_deg",
        ):
            if key in routing_cfg:
                auto_routing[key] = (
                    str(routing_cfg[key]) if key == "snap_source_mode" else float(routing_cfg[key])
                )
        if "disable_lane_follow_on_no_response" in routing_cfg:
            auto_routing["disable_lane_follow_on_no_response"] = bool(
                routing_cfg["disable_lane_follow_on_no_response"]
            )
        if "skip_invalid_long_route" in routing_cfg:
            auto_routing["skip_invalid_long_route"] = bool(routing_cfg["skip_invalid_long_route"])
        if "suppress_long_phase_reroute_on_unstable_reference_line" in routing_cfg:
            auto_routing["suppress_long_phase_reroute_on_unstable_reference_line"] = bool(
                routing_cfg["suppress_long_phase_reroute_on_unstable_reference_line"]
            )
        if "wait_for_obstacle_gt_before_initial_routing" in routing_cfg:
            auto_routing["wait_for_obstacle_gt_before_initial_routing"] = bool(
                routing_cfg["wait_for_obstacle_gt_before_initial_routing"]
            )

        traffic_light = bridge.setdefault("traffic_light", {})
        traffic_light_cfg = apollo_cfg.get("traffic_light", {}) or {}
        if "policy" in traffic_light_cfg:
            traffic_light["policy"] = str(traffic_light_cfg["policy"])
        if "publish_hz" in traffic_light_cfg:
            traffic_light["publish_hz"] = float(traffic_light_cfg["publish_hz"])
        if "channel" in traffic_light_cfg:
            traffic_light["channel"] = str(traffic_light_cfg["channel"])
        if "force_ids" in traffic_light_cfg:
            traffic_light["force_ids"] = [str(item) for item in (traffic_light_cfg.get("force_ids") or [])]
        if "ignore_roll_enabled" in traffic_light_cfg:
            traffic_light["ignore_roll_enabled"] = bool(traffic_light_cfg["ignore_roll_enabled"])
        for key in ("ignore_roll_distance_m", "ignore_roll_ahead_m"):
            if key in traffic_light_cfg:
                traffic_light[key] = float(traffic_light_cfg[key])
        if "ignore_roll_max_refresh" in traffic_light_cfg:
            traffic_light["ignore_roll_max_refresh"] = int(traffic_light_cfg["ignore_roll_max_refresh"])
        effective_tl_policy = str(traffic_light.get("policy", "") or "").strip().lower()
        if effective_tl_policy in {"ignore", "force_green"}:
            planning_cfg = apollo_cfg.setdefault("planning", {})
            # In sim debug profiles, when traffic light is ignored or force-green is
            # requested, default to disabling planning traffic-light rule unless user
            # explicitly set otherwise.
            planning_cfg.setdefault("disable_traffic_light_rule", True)

        tf = bridge.setdefault("carla_to_apollo", {})
        apollo_tf = apollo_cfg.get("carla_to_apollo", {}) or {}
        for key in ("tx", "ty", "tz", "yaw_deg"):
            if key in apollo_tf:
                tf[key] = float(apollo_tf[key])
        if "auto_calib" in apollo_tf:
            tf["auto_calib"] = bool(apollo_tf["auto_calib"])
        if "auto_calib_snap_right_angle" in apollo_tf:
            tf["auto_calib_snap_right_angle"] = bool(apollo_tf["auto_calib_snap_right_angle"])
        if "auto_calib_samples" in apollo_tf:
            tf["auto_calib_samples"] = int(apollo_tf["auto_calib_samples"])
        if "auto_calib_dump_file" in apollo_tf:
            tf["auto_calib_dump_file"] = str(apollo_tf["auto_calib_dump_file"])

        runtime_carla = (profile.get("runtime", {}) or {}).get("carla", {}) or {}
        carla_feedback = bridge.setdefault("carla_feedback", {})
        profile_carla_feedback = apollo_bridge_cfg.get("carla_feedback", {}) or {}
        if isinstance(profile_carla_feedback, dict) and "state_source" in profile_carla_feedback:
            carla_feedback["state_source"] = str(profile_carla_feedback["state_source"])
        carla_feedback["enabled"] = bool(carla_feedback.get("enabled", True))
        carla_feedback["host"] = str(runtime_carla.get("host", "127.0.0.1"))
        carla_feedback["port"] = int(runtime_carla.get("port", 2000))
        carla_feedback["ego_role_name"] = ego_id
        direct_bridge_cfg.setdefault("carla_host", str(runtime_carla.get("host", "127.0.0.1")))
        direct_bridge_cfg.setdefault("carla_port", int(runtime_carla.get("port", 2000)))
        direct_bridge_cfg.setdefault("ego_role_name", ego_id)
        direct_bridge_cfg.setdefault("poll_hz", float(bridge.get("publish_rate_hz", 20.0)))
        direct_bridge_cfg.setdefault("obstacle_radius_m", float(bridge.get("radius_m", 120.0)))
        direct_bridge_cfg.setdefault("max_obstacles", int(bridge.get("max_obstacles", 64)))
        direct_bridge_cfg.setdefault("route_command_mode", "cyber_direct")
        direct_bridge_cfg.setdefault("require_no_ros2_runtime", False)

        ctrl_map = bridge.setdefault("control_mapping", {})
        ctrl_cfg = apollo_cfg.get("control_mapping", {}) or {}
        for key in (
            "max_steer_angle",
            "speed_gain",
            "brake_gain",
            "connect_timeout_sec",
            "steer_sign",
            "throttle_scale",
            "brake_scale",
            "steer_scale",
            "brake_deadzone",
            "throttle_brake_min_command",
            "actuator_mapping_mode",
            "steering_percent_normalization",
            "apollo_max_steer_angle_deg",
            "zero_hold_sec",
            "startup_throttle_boost_add",
            "startup_throttle_boost_cap",
            "straight_lane_zero_steer_max_speed_mps",
            "straight_lane_zero_steer_max_e_y_m",
            "straight_lane_zero_steer_max_e_psi_deg",
            "straight_lane_zero_steer_max_curvature",
            "straight_lane_zero_steer_release_max_e_y_m",
            "straight_lane_zero_steer_release_max_e_psi_deg",
            "straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps",
            "low_speed_steer_guard_speed_mps",
            "low_speed_steer_guard_max_abs_steer",
            "low_speed_steer_guard_max_e_y_m",
            "low_speed_steer_guard_max_e_psi_deg",
            "low_speed_sustained_saturation_guard_speed_mps",
            "low_speed_sustained_saturation_guard_raw_threshold",
            "low_speed_sustained_saturation_guard_max_abs_steer",
            "low_speed_sustained_saturation_guard_max_curvature",
            "low_speed_sustained_saturation_guard_max_e_y_m",
            "low_speed_sustained_saturation_guard_max_e_psi_deg",
            "sustained_lateral_guard_raw_threshold",
            "sustained_lateral_guard_max_abs_steer",
            "sustained_lateral_guard_max_e_y_m",
            "sustained_lateral_guard_max_e_psi_deg",
            "sustained_lateral_guard_max_curvature",
            "sustained_lateral_guard_min_speed_mps",
            "sustained_lateral_guard_max_speed_mps",
            "trajectory_contract_lateral_guard_max_abs_steer",
            "trajectory_contract_lateral_guard_max_speed_mps",
            "trajectory_contract_lateral_guard_raw_threshold",
            "trajectory_contract_lateral_guard_max_planning_age_ms",
        ):
            if key in ctrl_cfg:
                ctrl_map[key] = (
                    str(ctrl_cfg[key])
                    if key in {"actuator_mapping_mode", "steering_percent_normalization"}
                    else float(ctrl_cfg[key])
                )
        for key in (
            "auto_apply_steer_sign",
            "startup_throttle_boost_enabled",
            "straight_lane_zero_steer_enabled",
            "straight_lane_zero_steer_latch_enabled",
            "low_speed_steer_guard_enabled",
            "low_speed_sustained_saturation_guard_enabled",
            "low_speed_sustained_saturation_guard_require_lane_inside",
            "sustained_lateral_guard_enabled",
            "trajectory_contract_lateral_guard_enabled",
            "force_zero_steer_output",
            "throttle_brake_mutual_exclusion_enabled",
            "require_valid_planning_before_first_publish",
            "require_exact_planning_match_before_first_publish",
            "require_nonfallback_planning_before_first_publish",
            "require_fixed_scene_handover_before_publish",
        ):
            if key in ctrl_cfg:
                ctrl_map[key] = bool(ctrl_cfg[key])
        if "steer_sign_check_frames" in ctrl_cfg:
            ctrl_map["steer_sign_check_frames"] = int(ctrl_cfg["steer_sign_check_frames"])
        for key in (
            "low_speed_sustained_saturation_guard_trigger_frames",
            "low_speed_sustained_saturation_guard_release_frames",
            "sustained_lateral_guard_trigger_frames",
            "sustained_lateral_guard_release_frames",
            "throttle_brake_hysteresis_frames",
        ):
            if key in ctrl_cfg:
                ctrl_map[key] = int(ctrl_cfg[key])
        if isinstance(ctrl_cfg.get("straight_acc_override"), dict):
            ctrl_map["straight_acc_override"] = dict(ctrl_cfg["straight_acc_override"])

        carla_control_bridge = bridge.setdefault("carla_control_bridge", {})
        control_bridge_cfg = apollo_cfg.get("carla_control_bridge", {}) or {}
        for key in (
            "control_topic",
            "timeout_sec",
            "max_steer_angle",
            "speed_gain",
            "brake_gain",
            "apply_hz",
            "connect_timeout_sec",
            "watchdog_arm_delay_sec",
            "startup_brake_suppression_speed_mps",
            "startup_brake_suppression_max_brake",
            "startup_brake_suppression_min_throttle",
            "startup_brake_suppression_hold_sec",
            "startup_brake_recent_throttle_window_sec",
        ):
            if key in control_bridge_cfg:
                carla_control_bridge[key] = (
                    str(control_bridge_cfg[key]) if key == "control_topic" else float(control_bridge_cfg[key])
                )
        for key in (
            "enabled",
            "sync_to_world_tick",
            "dryrun",
            "watchdog_wait_for_first_msg",
            "startup_brake_suppression_enabled",
        ):
            if key in control_bridge_cfg:
                carla_control_bridge[key] = bool(control_bridge_cfg[key])
        physical_cfg = dict(ctrl_map.get("physical", {}) or {})
        if isinstance(ctrl_cfg.get("physical"), dict):
            physical_cfg.update(dict(ctrl_cfg["physical"]))
        vehicle_param_cfg = apollo_cfg.get("vehicle_param", {}) or {}
        if "apollo_max_steer_angle_deg" not in physical_cfg and "max_steer_angle" in vehicle_param_cfg:
            physical_cfg["apollo_max_steer_angle_deg"] = float(vehicle_param_cfg["max_steer_angle"])
        if "apollo_max_accel_mps2" not in physical_cfg and "max_acceleration" in vehicle_param_cfg:
            physical_cfg["apollo_max_accel_mps2"] = float(vehicle_param_cfg["max_acceleration"])
        if "apollo_max_decel_mps2" not in physical_cfg and "max_deceleration" in vehicle_param_cfg:
            physical_cfg["apollo_max_decel_mps2"] = abs(float(vehicle_param_cfg["max_deceleration"]))
        if physical_cfg:
            ctrl_map["physical"] = physical_cfg

        out_cfg = run_dir / "artifacts" / "apollo_bridge_effective.yaml"
        out_cfg.parent.mkdir(parents=True, exist_ok=True)
        map_guard = self._write_map_contract_guard(
            profile=profile,
            bridge=bridge,
            artifacts=artifacts,
            runtime_map_dir=map_dir,
            original_bridge_map_file=original_bridge_map_file,
            fail_fast_on_high_risk_mismatch=fail_fast_on_high_risk_mismatch,
            apollo_root=apollo_root,
        )
        bridge["map_contract_invalid"] = bool(map_guard.get("map_contract_invalid", False))
        bridge["map_contract_mismatch_reason"] = ";".join(map_guard.get("mismatch_reasons", []) or [])
        bridge["map_contract_same_derivation_chain"] = bool(
            map_guard.get("same_derivation_chain", False)
        )
        bridge["map_contract_mismatch_classification"] = str(
            map_guard.get("mismatch_classification", "") or ""
        )
        bridge["apollo_map_identity_signature"] = str(
            map_guard.get("runtime_map_identity_signature", "") or ""
        )
        bridge["host_container_map_path_mapping"] = dict(
            map_guard.get("host_container_map_path_mapping", {}) or {}
        )
        stage6_cfg = apollo_cfg.get("stage6_reference_line", {}) or {}
        if stage6_cfg:
            cfg.setdefault("algo", {}).setdefault("apollo", {})["stage6_reference_line"] = dict(stage6_cfg)
        cfg.setdefault("algo", {}).setdefault("apollo", {})["transport_mode"] = transport_mode
        cfg.setdefault("algo", {}).setdefault("apollo", {})["direct_bridge"] = dict(direct_bridge_cfg)
        out_cfg.write_text(yaml.safe_dump(cfg, sort_keys=False))
        if bool(map_guard.get("high_risk_mismatch")) and fail_fast_on_high_risk_mismatch:
            raise RuntimeError(
                "map contract guard rejected run due to high-risk mismatch: "
                + ", ".join(map_guard.get("mismatch_reasons", []) or [])
            )
        return out_cfg

    def _resolve_bridge_map_file(self, bridge_cfg: Path) -> Path | None:
        try:
            cfg = yaml.safe_load(bridge_cfg.read_text()) or {}
        except Exception:
            return None
        bridge = (cfg.get("bridge", {}) or {}) if isinstance(cfg, dict) else {}
        raw = str(bridge.get("map_file", "") or "").strip()
        if not raw:
            return None
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        return path.resolve()

    def _patch_apollo_map_speed_limit(self, profile: Dict[str, Any], bridge_cfg: Path, run_dir: Path) -> None:
        apollo_cfg = profile.setdefault("algo", {}).setdefault("apollo", {})
        speed_cfg = apollo_cfg.setdefault("map_speed_limit", {})
        enabled = bool(speed_cfg.get("enabled", False))
        restore_original = bool(speed_cfg.get("restore_original", False))
        fill_missing = bool(speed_cfg.get("fill_missing_lane_speed_limits", False))
        target_mps = float(speed_cfg.get("override_mps", 23.61) or 23.61)
        missing_default_raw = speed_cfg.get("missing_lane_default_mps", "inherit_existing")
        artifacts = run_dir / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        report_path = artifacts / "apollo_map_speed_limit_patch.json"

        map_file_raw = str(speed_cfg.get("map_file", "") or "").strip()
        map_file = Path(map_file_raw).expanduser() if map_file_raw else self._resolve_bridge_map_file(bridge_cfg)
        # When speed patch is disabled and no restore is requested, skip map
        # file probing entirely so host-mode Apollo can still start even if the
        # legacy modules/map_data path is absent on this machine.
        if not enabled and not restore_original and not fill_missing:
            report = {
                "enabled": enabled,
                "restore_original": restore_original,
                "fill_missing_lane_speed_limits": fill_missing,
                "target_speed_limit_mps": target_mps,
                "missing_lane_default_mps_config": missing_default_raw,
                "map_file": str(map_file) if map_file is not None else "",
                "backup_file": "",
                "ok": True,
                "patched": False,
                "reason": "disabled_skip",
            }
            report_path.write_text(json.dumps(report, indent=2))
            return

        if map_file is None:
            report_path.write_text(
                json.dumps(
                    {
                        "enabled": enabled,
                        "restore_original": restore_original,
                        "fill_missing_lane_speed_limits": fill_missing,
                        "ok": False,
                        "reason": "map_file_missing",
                    },
                    indent=2,
                )
            )
            return
        if not map_file.is_absolute():
            map_file = (self.repo_root / map_file).resolve()
        map_file = map_file.resolve()
        backup_path = Path(f"{map_file}.carla_testbed.bak")

        report: Dict[str, Any] = {
            "enabled": enabled,
            "restore_original": restore_original,
            "fill_missing_lane_speed_limits": fill_missing,
            "target_speed_limit_mps": target_mps,
            "missing_lane_default_mps_config": missing_default_raw,
            "map_file": str(map_file),
            "backup_file": str(backup_path),
        }
        if not map_file.exists():
            report["ok"] = False
            report["reason"] = "map_file_not_found"
            report_path.write_text(json.dumps(report, indent=2))
            raise FileNotFoundError(f"Apollo map_file not found for speed patch: {map_file}")

        if restore_original and backup_path.exists():
            shutil.copyfile(backup_path, map_file)
            report["restored_from_backup"] = True
        else:
            report["restored_from_backup"] = False

        if not enabled and not fill_missing:
            report["ok"] = True
            report["patched"] = False
            report_path.write_text(json.dumps(report, indent=2))
            speed_cfg["effective_map_file"] = str(map_file)
            return

        original_text = map_file.read_text()
        if not backup_path.exists():
            backup_path.write_text(original_text)
        _, inspection = _rewrite_apollo_lane_speed_limits(
            original_text,
            replace_existing_mps=None,
            fill_missing=False,
            missing_default_mps=target_mps,
        )
        inherit_missing_default = str(missing_default_raw or "").strip().lower() in {
            "",
            "inherit_existing",
            "inherit_existing_min",
        }
        if inherit_missing_default:
            if enabled:
                missing_default_mps = target_mps
                missing_default_source = "override_mps"
            else:
                positive_existing_limits = [
                    float(value)
                    for value in inspection["old_unique_speed_limits_mps"]
                    if float(value) > 0.0
                ]
                if not positive_existing_limits:
                    report.update(inspection)
                    report["ok"] = False
                    report["reason"] = "cannot_inherit_missing_lane_speed_limit_without_positive_existing_limit"
                    report_path.write_text(json.dumps(report, indent=2))
                    raise RuntimeError(
                        "Cannot fill missing Apollo lane speed limits because the map has no "
                        f"positive explicit lane speed_limit: {map_file}"
                    )
                missing_default_mps = min(positive_existing_limits)
                missing_default_source = "minimum_positive_existing_lane_speed_limit"
        else:
            missing_default_mps = float(missing_default_raw)
            if missing_default_mps <= 0.0:
                raise ValueError("missing_lane_default_mps must be positive")
            missing_default_source = "configured_numeric_value"
        report["missing_lane_default_mps"] = missing_default_mps
        report["missing_lane_default_mps_effective"] = missing_default_mps
        report["missing_lane_default_source"] = missing_default_source
        patched_text, stats = _rewrite_apollo_lane_speed_limits(
            original_text,
            replace_existing_mps=target_mps if enabled else None,
            fill_missing=fill_missing,
            missing_default_mps=missing_default_mps,
        )
        report.update(stats)
        report["map_sha256_before"] = hashlib.sha256(original_text.encode("utf-8")).hexdigest()
        if int(stats["lane_count"]) <= 0:
            report["ok"] = False
            report["reason"] = "lane_block_not_found"
            report_path.write_text(json.dumps(report, indent=2))
            raise RuntimeError(f"No top-level lane block found in Apollo map file: {map_file}")
        if enabled and int(stats["explicit_speed_limit_count_before"]) <= 0 and not fill_missing:
            report["ok"] = False
            report["reason"] = "speed_limit_field_not_found"
            report_path.write_text(json.dumps(report, indent=2))
            raise RuntimeError(f"No lane speed_limit field found in Apollo map file: {map_file}")

        changed = patched_text != original_text
        if changed:
            map_file.write_text(patched_text)
        report["ok"] = True
        report["patched"] = changed
        report["patched_count"] = int(stats["existing_speed_limit_replaced_count"]) + int(
            stats["inserted_missing_lane_speed_limit_count"]
        )
        report["new_speed_limit_mps"] = target_mps if enabled else None
        report["map_sha256_after"] = hashlib.sha256(patched_text.encode("utf-8")).hexdigest()
        report["reason"] = "patched" if changed else "already_complete"
        report_path.write_text(json.dumps(report, indent=2))
        speed_cfg["effective_map_file"] = str(map_file)

    def prepare(self, profile: Dict[str, Any], run_dir):
        run_path = Path(run_dir).resolve()
        run_path.mkdir(parents=True, exist_ok=True)
        artifacts = run_path / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)

        apollo_cfg = profile.setdefault("algo", {}).setdefault("apollo", {})
        apollo_cfg.setdefault("ros2_setup_script", "")
        apollo_cfg.setdefault("cyber_domain_id", 80)
        apollo_cfg.setdefault("cyber_ip", "")
        docker_cfg = apollo_cfg.setdefault("docker", {})
        docker_cfg.setdefault("container", os.environ.get("APOLLO_DOCKER_CONTAINER", ""))
        docker_cfg.setdefault("apollo_root_in_container", "/apollo")
        docker_cfg.setdefault("apollo_distribution_home", "/opt/apollo/neo")
        docker_cfg.setdefault("python_exec", "python3")
        docker_cfg.setdefault("bridge_in_container", False)
        docker_cfg.setdefault("auto_start_container", True)
        docker_cfg.setdefault("auto_install_runtime_deps", True)
        docker_cfg.setdefault("module_exec_user", "1000:1000")
        docker_cfg.setdefault("start_modules", False)
        docker_cfg.setdefault("start_modules_cmd", "")
        docker_cfg.setdefault("modules_status_cmd", "")
        docker_cfg.setdefault("required_modules", ["routing", "prediction", "planning", "control"])
        if "enabled" not in docker_cfg:
            docker_cfg["enabled"] = bool(docker_cfg.get("container"))
        map_speed_cfg = apollo_cfg.setdefault("map_speed_limit", {})
        map_speed_cfg.setdefault("enabled", False)
        map_speed_cfg.setdefault("override_mps", 23.61)
        map_speed_cfg.setdefault("restore_original", False)
        map_speed_cfg.setdefault("fill_missing_lane_speed_limits", False)
        map_speed_cfg.setdefault("missing_lane_default_mps", "inherit_existing")

        bridge_cfg = self._ensure_bridge_config(profile, run_path)
        self._patch_apollo_map_speed_limit(profile, bridge_cfg, run_path)
        apollo_cfg["bridge_config_path"] = str(bridge_cfg)
        apollo_cfg["stats_path"] = str(artifacts / "cyber_bridge_stats.json")
        apollo_cfg.setdefault("pb_root", "tools/apollo10_cyber_bridge/pb")
        apollo_cfg.setdefault("carla_control_bridge", {})
        apollo_cfg.setdefault("transport_mode", "ros2_gt")
        apollo_cfg.setdefault("direct_bridge", {})
        apollo_cfg["carla_control_bridge"].setdefault("enabled", True)

        profile.setdefault("artifacts", {})["dir"] = str(artifacts)
        profile["_apollo_run_dir"] = str(run_path)

        meta = {
            "bridge_config_path": str(bridge_cfg),
            "stats_path": apollo_cfg["stats_path"],
            "pb_root": str(apollo_cfg["pb_root"]),
            "apollo_root": apollo_cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT", ""),
            "transport_mode": str(apollo_cfg.get("transport_mode") or "ros2_gt"),
            "direct_bridge": dict(apollo_cfg.get("direct_bridge") or {}),
            "docker": docker_cfg,
        }
        (artifacts / "apollo_adapter_meta.json").write_text(json.dumps(meta, indent=2))

    def start(self, profile: Dict[str, Any], run_dir):
        self.backend = CyberRTBackend(profile)
        return self.backend.start()

    def healthcheck(self, profile: Dict[str, Any], run_dir) -> bool:
        if self.backend is None:
            self.backend = CyberRTBackend(profile)
        return self.backend.health_check()

    def stop(self, profile: Dict[str, Any], run_dir):
        if self.backend is not None:
            self.backend.stop()
            self.backend = None

    def get_control_topics(self, profile: Dict[str, Any]):
        apollo_cfg = profile.get("algo", {}).get("apollo", {}) or {}
        bridge_cfg_path = apollo_cfg.get("bridge_config_path")
        if not bridge_cfg_path:
            return ["/tb/ego/control_cmd"]
        path = Path(bridge_cfg_path)
        if not path.is_absolute():
            path = (self.repo_root / path).resolve()
        if not path.exists():
            return ["/tb/ego/control_cmd"]
        try:
            cfg = yaml.safe_load(path.read_text()) or {}
            topic = ((cfg.get("ros2", {}) or {}).get("control_out_topic")) or "/tb/ego/control_cmd"
            return [topic]
        except Exception:
            return ["/tb/ego/control_cmd"]
