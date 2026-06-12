from __future__ import annotations

import importlib
import json
import sys
import threading
import types
from pathlib import Path


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))


def _adapter(tmp_path: Path):
    _install_fake_protobuf()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.claim_profile_enabled = True
    adapter.materialization_probe_enabled = True
    adapter.goal_validity_debug_path = tmp_path / "artifacts" / "goal_validity_debug.jsonl"
    adapter.goal_validity_report_path = tmp_path / "artifacts" / "goal_validity_report.json"
    adapter.artifact_async_write_enabled = False
    adapter._jsonl_artifact_handles = {}
    adapter._artifact_write_lock = threading.Lock()
    adapter._artifact_last_flush_sec = {}
    adapter._artifact_pending_rows = {}
    adapter.artifact_flush_interval_s = 0.0
    adapter.artifact_flush_max_pending_rows = 1
    return adapter


def test_claim_profile_blocks_fallback_route_goal_sources(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)

    for source in (
        "startup_short_ahead",
        "invalid_goal_fallback_ahead",
        "scenario_goal_missing_fallback",
        "fixed_goal_missing_fallback",
        "long_ahead_fallback",
    ):
        assert adapter._claim_profile_blocks_route_goal(
            {"goal_source": source, "goal_mode": "scenario_xy"}
        )
    assert adapter._claim_profile_blocks_route_goal(
        {"goal_source": "long_ahead", "goal_mode": "ego_seed_ahead"}
    )
    assert not adapter._claim_profile_blocks_route_goal(
        {"goal_source": "scenario_goal_file", "goal_mode": "scenario_xy"}
    )


def test_goal_validity_report_records_claim_profile_fallback_block(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)

    adapter._record_goal_validity_event(
        {
            "timestamp": 1.0,
            "routing_phase": "long",
            "requested_goal_mode": "scenario_xy",
            "goal_mode": "ego_seed_ahead",
            "goal_source": "scenario_goal_missing_fallback",
            "invalid_goal": True,
            "invalid_goal_reason": "fallback_route_blocked_by_claim_profile",
            "fallback_blocked_by_claim_profile": True,
            "fallback_applied": False,
        }
    )
    for fp in adapter._jsonl_artifact_handles.values():
        fp.flush()
        fp.close()

    report = json.loads(adapter.goal_validity_report_path.read_text(encoding="utf-8"))
    rows = [
        json.loads(line)
        for line in adapter.goal_validity_debug_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert report["status"] == "blocked"
    assert report["fallback_blocked_by_claim_profile"] is True
    assert report["fallback_applied"] is False
    assert rows[-1]["fallback_blocked_by_claim_profile"] is True
