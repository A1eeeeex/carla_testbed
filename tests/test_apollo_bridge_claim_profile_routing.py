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


class _FakeWriter:
    def __init__(self) -> None:
        self.messages = []

    def write(self, msg) -> None:
        self.messages.append(msg)


def _routing_response_adapter(tmp_path: Path):
    adapter = _adapter(tmp_path)
    adapter.cyber_time = types.SimpleNamespace(
        Time=types.SimpleNamespace(now=lambda: types.SimpleNamespace(to_sec=lambda: 123.0))
    )
    adapter.stats = {
        "routing_response_count": 0,
        "raw_routing_response_count": 0,
        "planning_routing_response_count": 0,
        "routing_response_relay_count": 0,
        "routing_response_relay_error_count": 0,
        "routing_success_count": 0,
        "routing_empty_count": 0,
    }
    adapter.routing_response_decoded_path = tmp_path / "artifacts" / "routing_response_decoded.json"
    adapter.routing_response_decoded_jsonl_path = tmp_path / "artifacts" / "routing_response_decoded.jsonl"
    adapter.raw_routing_response_channel = "/apollo/raw_routing_response"
    adapter.routing_response_channel = "/apollo/routing_response"
    adapter.routing_response_writer = _FakeWriter()
    adapter.auto_routing_last_routing_ts = 100.0
    adapter._routing_first_response_ts_sec = None
    adapter._routing_last_response_ts_sec = None
    adapter._routing_first_success_response_ts_sec = None
    adapter._routing_last_success_response_ts_sec = None
    adapter._routing_first_response_after_last_routing_send_ts_sec = None
    adapter._routing_first_response_after_last_routing_send_boundary_ts_sec = None
    adapter._routing_first_success_response_after_last_routing_send_ts_sec = None
    adapter._routing_first_success_response_after_last_routing_send_boundary_ts_sec = None
    adapter.auto_routing_established = False
    adapter.auto_routing_freeze_after_success = False
    adapter.auto_routing_freeze_after_long_route_success_only = False
    adapter._routing_last_request_phase = "long"
    adapter.auto_routing_pending_goal = None
    adapter._routing_freeze_active = False
    adapter._planning_status = lambda: {}
    adapter._write_health_summary = lambda: None
    adapter._write_planning_topic_debug_summary = lambda: None
    return adapter


def _fake_routing_response():
    segment = types.SimpleNamespace(id="lane_1", start_s=1.0, end_s=11.0)
    passage = types.SimpleNamespace(segment=[segment])
    road = types.SimpleNamespace(id="road_1", passage=[passage])
    header = types.SimpleNamespace(timestamp_sec=120.0, sequence_num=7)
    return types.SimpleNamespace(header=header, road=[road])


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


def test_apollo_warmup_readiness_bypass_requires_planning_and_gt_inputs(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.stats = {
        "localization_fresh_publish_count": 5,
        "chassis_fresh_publish_count": 5,
    }
    adapter._planning_msg_count = 4
    adapter.auto_routing_startup_delay_sec = 3.0
    adapter.auto_routing_startup_apollo_warmup_bypass_when_ready = True
    adapter.auto_routing_startup_apollo_warmup_bypass_min_elapsed_sec = 8.0
    adapter.auto_routing_startup_apollo_warmup_ready_min_planning_messages = 3
    adapter.auto_routing_startup_apollo_warmup_ready_accept_preplanning_gt_only = False
    adapter.auto_routing_startup_apollo_warmup_ready_min_gt_fresh_samples = 3
    adapter.routing_writer = object()
    adapter.auto_routing_send_routing = True
    adapter.lane_follow_client = None
    adapter.auto_routing_send_lane_follow = False
    adapter.action_client = None
    adapter.auto_routing_send_action = False
    adapter._lane_follow_disabled_runtime = False

    readiness = adapter._apollo_startup_warmup_readiness(
        startup_delay_elapsed_sec=8.2,
        startup_apollo_warmup_elapsed_sec=11.0,
    )

    assert readiness["ready"] is True
    assert readiness["missing_ready_conditions"] == []
    assert readiness["planning_message_count"] == 4
    assert readiness["readiness_source"] == "planning_reader_and_gt_inputs"
    assert readiness["localization_fresh_publish_count"] == 5
    assert readiness["chassis_fresh_publish_count"] == 5


def test_apollo_warmup_readiness_blocks_when_planning_or_gt_missing(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.stats = {
        "localization_fresh_publish_count": 5,
        "chassis_fresh_publish_count": 0,
    }
    adapter._planning_msg_count = 0
    adapter.auto_routing_startup_delay_sec = 3.0
    adapter.auto_routing_startup_apollo_warmup_bypass_when_ready = True
    adapter.auto_routing_startup_apollo_warmup_bypass_min_elapsed_sec = 8.0
    adapter.auto_routing_startup_apollo_warmup_ready_min_planning_messages = 3
    adapter.auto_routing_startup_apollo_warmup_ready_accept_preplanning_gt_only = False
    adapter.auto_routing_startup_apollo_warmup_ready_min_gt_fresh_samples = 3
    adapter.routing_writer = object()
    adapter.auto_routing_send_routing = True
    adapter.lane_follow_client = None
    adapter.auto_routing_send_lane_follow = False
    adapter.action_client = None
    adapter.auto_routing_send_action = False
    adapter._lane_follow_disabled_runtime = False

    readiness = adapter._apollo_startup_warmup_readiness(
        startup_delay_elapsed_sec=8.2,
        startup_apollo_warmup_elapsed_sec=11.0,
    )

    assert readiness["ready"] is False
    assert "planning_reader_not_observed" in readiness["missing_ready_conditions"]
    assert "chassis_fresh_samples_insufficient" in readiness["missing_ready_conditions"]
    assert "localization_fresh_samples_insufficient" not in readiness["missing_ready_conditions"]


def test_apollo_warmup_readiness_can_use_preplanning_gt_ready_policy(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.stats = {
        "localization_fresh_publish_count": 5,
        "chassis_fresh_publish_count": 5,
    }
    adapter._planning_msg_count = 0
    adapter.auto_routing_startup_delay_sec = 3.0
    adapter.auto_routing_startup_apollo_warmup_bypass_when_ready = True
    adapter.auto_routing_startup_apollo_warmup_bypass_min_elapsed_sec = 8.0
    adapter.auto_routing_startup_apollo_warmup_ready_min_planning_messages = 3
    adapter.auto_routing_startup_apollo_warmup_ready_accept_preplanning_gt_only = True
    adapter.auto_routing_startup_apollo_warmup_ready_min_gt_fresh_samples = 3
    adapter.routing_writer = object()
    adapter.auto_routing_send_routing = True
    adapter.lane_follow_client = None
    adapter.auto_routing_send_lane_follow = False
    adapter.action_client = None
    adapter.auto_routing_send_action = False
    adapter._lane_follow_disabled_runtime = False

    readiness = adapter._apollo_startup_warmup_readiness(
        startup_delay_elapsed_sec=8.2,
        startup_apollo_warmup_elapsed_sec=11.0,
    )

    assert readiness["ready"] is True
    assert readiness["missing_ready_conditions"] == []
    assert readiness["planning_reader_observed"] is False
    assert readiness["accept_preplanning_gt_only"] is True
    assert readiness["readiness_source"] == "preplanning_gt_inputs_and_command_interface"


def test_command_gate_preserves_apollo_warmup_bypass_evidence(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.stats = {"routing_request_count": 0, "control_tx_count": 0}
    adapter._planning_nonempty_count = 0
    adapter._command_gate_state = {
        "evaluation_count": 0,
        "first_eval_ts_sec": None,
        "last_eval_ts_sec": None,
        "first_eligible_ts_sec": None,
        "last_eligible_ts_sec": None,
        "first_ready_to_send_ts_sec": None,
        "last_ready_to_send_ts_sec": None,
        "apollo_warmup_bypassed_by_readiness": False,
        "apollo_warmup_readiness": {},
    }

    adapter._update_command_gate_state(
        ts_sec=10.0,
        phase="startup",
        status="warmup_bypassed_by_readiness",
        eligible=True,
        apollo_warmup_bypassed_by_readiness=True,
        apollo_warmup_readiness={"ready": True},
    )
    adapter._update_command_gate_state(
        ts_sec=10.1,
        phase="long",
        status="ready_to_send",
        eligible=True,
        ready_to_send=True,
        send_routing_now=True,
    )

    assert adapter._command_gate_state["apollo_warmup_bypassed_by_readiness"] is True
    assert adapter._command_gate_state["apollo_warmup_readiness"] == {"ready": True}


def test_scenario_goal_apollo_map_frame_is_not_retransformed(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter.tf = bridge.Transform2D(tx=10.0, ty=20.0, tz=0.0, yaw_deg=0.0)
    adapter.auto_routing_scenario_goal_path = tmp_path / "artifacts" / "scenario_goal.json"
    adapter.auto_routing_scenario_goal_path.parent.mkdir(parents=True, exist_ok=True)
    adapter._scenario_goal_cache = None
    adapter._scenario_goal_mtime_ns = None
    adapter.stats = {}
    adapter.auto_routing_scenario_goal_path.write_text(
        json.dumps(
            {
                "frame": "apollo_map",
                "source": "scenario_metadata_apollo_map_xy",
                "goal": {"x": 154.0, "y": -37.0, "z": 0.0},
                "goal_raw_carla": {"x": 154.0, "y": 37.0, "z": 0.0},
            }
        ),
        encoding="utf-8",
    )

    parsed = adapter._load_scenario_goal_point()

    assert parsed is not None
    assert parsed["frame"] == "apollo_map"
    assert parsed["x"] == 154.0
    assert parsed["y"] == -37.0
    assert parsed["z"] == 0.0


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


def test_startup_disabled_and_long_goal_disabled_uses_claim_routing_phase(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.auto_routing_startup_route_enabled = False
    adapter.auto_routing_use_long_goal_after_move = False

    assert adapter._current_routing_phase(ts_sec=10.0, speed_mps=0.0) == "claim"

    adapter.auto_routing_use_long_goal_after_move = True
    assert adapter._current_routing_phase(ts_sec=10.0, speed_mps=0.0) == "long"


def test_claim_routing_phase_records_claim_route_reason(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.auto_routing_long_routing_sent = False

    request_kind, reroute_reason, trigger = adapter._routing_request_reason(
        phase="claim",
        ignore_roll_active=False,
        routing_skipped_due_to_freeze=False,
        routing_skipped_due_to_invalid_goal=False,
        routing_skipped_due_to_unstable_reference_line=False,
        routing_waiting_for_route_debug_ready=False,
    )

    assert request_kind == "claim_route"
    assert reroute_reason == "claim_initial_route"
    assert trigger == "claim_route_policy"


def test_bridge_path_expands_apollo_map_root_from_existing_candidate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _install_fake_protobuf()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    monkeypatch.delenv("APOLLO_MAP_ROOT", raising=False)
    map_root = tmp_path / "map_data"
    (map_root / "carla_town01").mkdir(parents=True)

    resolved = bridge._bridge_path_text(
        "${APOLLO_MAP_ROOT}/carla_town01/base_map.txt",
        apollo_map_root_candidates=[map_root],
    )

    assert resolved == str(map_root / "carla_town01" / "base_map.txt")


def test_bridge_path_prefers_explicit_apollo_map_root_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _install_fake_protobuf()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    env_root = tmp_path / "env_map_data"
    candidate_root = tmp_path / "candidate_map_data"
    (env_root / "carla_town01").mkdir(parents=True)
    (candidate_root / "carla_town01").mkdir(parents=True)
    monkeypatch.setenv("APOLLO_MAP_ROOT", str(env_root))

    resolved = bridge._bridge_path_text(
        "${APOLLO_MAP_ROOT}/carla_town01/base_map.txt",
        apollo_map_root_candidates=[candidate_root],
    )

    assert resolved == str(env_root / "carla_town01" / "base_map.txt")


def test_initial_claim_route_goal_does_not_require_existing_reference_line(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path)
    adapter._planning_last_route_debug_event = {
        "timestamp": 10.0,
        "reference_line_count": 0,
        "route_segment_count": 0,
        "routing_lane_window_count": 0,
        "reference_line_provider_status": "failed",
        "create_route_segments_status": "failed",
        "lane_follow_map_status": "reference_line_missing",
    }
    adapter.auto_routing_established = False
    adapter.auto_routing_goal_validity_reference_line_stale_sec = 1.0
    adapter.auto_routing_goal_validity_check_enabled = True
    adapter.auto_routing_snap_goal_to_lane = True
    adapter.auto_routing_goal_mode = "scenario_xy"

    validity = adapter._evaluate_goal_validity(
        x0=0.0,
        y0=0.0,
        x1=50.0,
        y1=0.0,
        phase="long",
        goal_meta={"requested_goal_mode": "scenario_xy", "goal_mode": "scenario_xy", "goal_source": "scenario_goal_file"},
        goal_proj={"available": True, "distance_m": 0.5},
        command_ts=10.1,
    )

    assert validity["goal_projection_available"] is True
    assert validity["post_routing_reference_evidence_available"] is False
    assert validity["invalid_goal"] is False
    assert validity["invalid_goal_reason"] == ""


def test_reference_line_unavailable_blocks_after_route_evidence_exists(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter._planning_last_route_debug_event = {
        "timestamp": 10.0,
        "reference_line_count": 0,
        "route_segment_count": 1,
        "routing_lane_window_count": 1,
        "reference_line_provider_status": "failed",
        "create_route_segments_status": "ready",
        "lane_follow_map_status": "reference_line_missing",
    }
    adapter.auto_routing_established = False
    adapter.auto_routing_goal_validity_reference_line_stale_sec = 1.0
    adapter.auto_routing_goal_validity_check_enabled = True
    adapter.auto_routing_snap_goal_to_lane = True
    adapter.auto_routing_goal_mode = "scenario_xy"

    validity = adapter._evaluate_goal_validity(
        x0=0.0,
        y0=0.0,
        x1=50.0,
        y1=0.0,
        phase="long",
        goal_meta={"requested_goal_mode": "scenario_xy", "goal_mode": "scenario_xy", "goal_source": "scenario_goal_file"},
        goal_proj={"available": True, "distance_m": 0.5},
        command_ts=10.1,
    )

    assert validity["post_routing_reference_evidence_available"] is True
    assert validity["invalid_goal"] is True
    assert validity["invalid_goal_reason"] == "reference_line_unavailable"


def test_raw_routing_response_is_decoded_and_relayed_to_planning_channel(tmp_path: Path) -> None:
    adapter = _routing_response_adapter(tmp_path)
    msg = _fake_routing_response()

    adapter._on_raw_routing_response(msg)

    assert adapter.stats["raw_routing_response_count"] == 1
    assert adapter.stats["routing_response_count"] == 1
    assert adapter.stats["routing_success_count"] == 1
    assert adapter.stats["routing_response_relay_count"] == 1
    assert adapter.routing_response_writer.messages == [msg]

    decoded = json.loads(adapter.routing_response_decoded_path.read_text(encoding="utf-8"))
    assert decoded["source"] == "/apollo/raw_routing_response"
    assert decoded["planning_facing_channel"] == "/apollo/routing_response"
    assert decoded["lane_segment_count"] == 1
    assert decoded["lane_window_signature"] == "lane_1@1.0000->11.0000"


def test_materialization_summary_reports_planning_empty_after_routing_success(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.node = types.SimpleNamespace(stats={})
    adapter.stats = {
        "routing_request_count": 1,
        "routing_response_count": 1,
        "routing_success_count": 1,
        "control_rx_count": 0,
        "control_tx_count": 0,
    }
    adapter._command_gate_state = {
        "last_blocking_reason": "routing_phase_already_sent",
        "route_ready": True,
        "lane_follow_ready": False,
    }
    adapter._planning_msg_count = 5
    adapter._planning_nonempty_count = 0
    adapter._latest_speed_mps = 0.0
    adapter._first_odom_ts = 1.0
    adapter.auto_routing_enabled = True
    adapter.transport_mode = "ros2_gt"
    adapter.route_command_mode = "routing_request"
    adapter.route_command_path = "routing"
    adapter.require_no_ros2_runtime = False
    adapter.routing_writer = object()
    adapter.routing_response_writer = object()
    adapter.lane_follow_client = None
    adapter.action_client = None
    adapter.auto_routing_send_routing = True
    adapter.raw_routing_response_channel = "/apollo/raw_routing_response"
    adapter.routing_response_channel = "/apollo/routing_response"
    adapter.auto_routing_relay_raw_routing_response_to_planning = True
    adapter.auto_routing_send_lane_follow = False
    adapter.auto_routing_send_action = False
    adapter._lane_follow_disabled_runtime = False
    adapter._routing_first_response_ts_sec = None
    adapter._routing_first_success_response_ts_sec = None
    adapter._planning_first_msg_ts_sec = 2.0
    adapter._planning_first_nonempty_ts_sec = None

    summary = adapter._command_materialization_summary()

    assert summary["command_path_stage"] == "planning_empty_only"
    assert summary["first_divergence_layer"] == "planning_materialization"
    assert summary["first_divergence_reason"] == "planning_trajectory_empty"


def test_materialization_summary_uses_max_speed_not_only_final_speed(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    adapter.node = types.SimpleNamespace(stats={})
    adapter.stats = {
        "routing_request_count": 1,
        "routing_response_count": 1,
        "routing_success_count": 1,
        "control_rx_count": 12,
        "control_tx_count": 12,
    }
    adapter._command_gate_state = {
        "last_blocking_reason": "routing_phase_already_sent",
        "route_ready": True,
        "lane_follow_ready": True,
    }
    adapter._planning_msg_count = 10
    adapter._planning_nonempty_count = 8
    adapter._latest_speed_mps = 0.1
    adapter._max_speed_mps = 5.3
    adapter._first_odom_ts = 1.0
    adapter._routing_first_response_ts_sec = 2.0
    adapter._routing_first_success_response_ts_sec = 2.0
    adapter._planning_first_msg_ts_sec = 3.0
    adapter._planning_first_nonempty_ts_sec = 3.5
    adapter.auto_routing_enabled = True
    adapter.transport_mode = "ros2_gt"
    adapter.route_command_mode = "routing_request"
    adapter.route_command_path = "routing"
    adapter.require_no_ros2_runtime = False
    adapter.routing_writer = object()
    adapter.routing_response_writer = object()
    adapter.lane_follow_client = object()
    adapter.action_client = None
    adapter.auto_routing_send_routing = True
    adapter.raw_routing_response_channel = "/apollo/raw_routing_response"
    adapter.routing_response_channel = "/apollo/routing_response"
    adapter.auto_routing_relay_raw_routing_response_to_planning = True
    adapter.auto_routing_send_lane_follow = True
    adapter.auto_routing_send_action = False
    adapter._lane_follow_disabled_runtime = False

    summary = adapter._command_materialization_summary()

    assert summary["command_path_stage"] == "materialized"
    assert summary["first_divergence_layer"] == "materialized"
    assert summary["speed_mps_last"] == 0.1
    assert summary["speed_mps_max"] == 5.3
