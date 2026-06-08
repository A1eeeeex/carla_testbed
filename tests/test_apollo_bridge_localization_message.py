from __future__ import annotations

import importlib
import json
import math
import queue
import sys
import types
from pathlib import Path

import pytest

from carla_testbed.adapters.apollo.frame_transform import quaternion_forward_heading
from carla_testbed.adapters.apollo.messages import (
    build_localization_estimate_dict_from_map_state,
    write_localization_estimate_to_pb,
)


class _Header:
    timestamp_sec: float
    module_name: str
    sequence_num: int
    frame_id: str


class _Vector:
    def __init__(self) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = 0.0


class _Orientation:
    def __init__(self) -> None:
        self.qx = 0.0
        self.qy = 0.0
        self.qz = 0.0
        self.qw = 1.0


class _Pose:
    def __init__(self) -> None:
        self.position = _Vector()
        self.orientation = _Orientation()
        self.heading = 0.0
        self.linear_velocity = _Vector()
        self.linear_velocity_vrf = _Vector()
        self.angular_velocity = _Vector()
        self.angular_velocity_vrf = _Vector()
        self.linear_acceleration = _Vector()
        self.linear_acceleration_vrf = _Vector()


class _Localization:
    def __init__(self) -> None:
        self.header = _Header()
        self.pose = _Pose()
        self.measurement_time = 0.0


class _Stamp:
    def __init__(self, sec: int, nanosec: int = 0) -> None:
        self.sec = sec
        self.nanosec = nanosec


class _OdomHeader:
    def __init__(self, timestamp_sec: float, seq: int = 0) -> None:
        sec = int(timestamp_sec)
        self.stamp = _Stamp(sec, int(round((timestamp_sec - sec) * 1_000_000_000)))
        self.seq = seq


class _Odom:
    def __init__(self, timestamp_sec: float, seq: int = 0) -> None:
        self.header = _OdomHeader(timestamp_sec, seq=seq)


def test_canonical_localization_dict_writes_header_frame_and_measurement_time() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=12.5,
        sequence_num=7,
        position={"x": 1.0, "y": 2.0, "z": 0.0},
        heading=0.25,
        linear_velocity={"x": 3.0, "y": 0.0, "z": 0.0},
        angular_velocity={"x": 0.0, "y": 0.0, "z": 0.1},
        module_name="tb_apollo10_gt_bridge",
        frame_id="map",
        heading_source="odom_quaternion_yaw_after_frame_transform",
        vehicle_reference_confidence="verified",
        vehicle_reference_hard_gate_eligible=True,
    )
    loc = _Localization()

    write_localization_estimate_to_pb(loc, payload)

    assert loc.header.timestamp_sec == pytest.approx(12.5)
    assert loc.header.frame_id == "map"
    assert loc.measurement_time == pytest.approx(loc.header.timestamp_sec)
    assert loc.pose.heading == pytest.approx(0.25)


def test_canonical_rfu_quaternion_decodes_to_published_heading() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=1.0,
        sequence_num=1,
        position={"x": 0.0, "y": 0.0, "z": 0.0},
        heading=math.pi / 3.0,
    )
    orientation = payload["pose"]["orientation"]

    decoded = quaternion_forward_heading(orientation)

    assert decoded == pytest.approx(payload["pose"]["heading"], abs=1e-12)
    assert abs(payload["metadata"]["quaternion_heading_diff_rad"]) < 1e-12


def test_heading_source_metadata_is_truthful_for_odom_path() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=1.0,
        sequence_num=1,
        position={"x": 0.0, "y": 0.0, "z": 0.0},
        heading=0.0,
        heading_source="odom_quaternion_yaw_after_frame_transform",
    )

    assert payload["metadata"]["heading_source"] == "odom_quaternion_yaw_after_frame_transform"
    assert payload["metadata"]["orientation_convention"] == "RFU_to_ENU"


def test_assumed_vehicle_reference_is_serialized_as_non_claim_grade() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=1.0,
        sequence_num=1,
        position={"x": 0.0, "y": 0.0, "z": 0.0},
        heading=0.0,
        vehicle_reference_confidence="assumed",
        vehicle_reference_hard_gate_eligible=False,
    )

    assert payload["metadata"]["vehicle_reference_confidence"] == "assumed"
    assert payload["metadata"]["vehicle_reference_hard_gate_eligible"] is False


def test_bridge_fill_header_writes_frame_id_when_supported() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.seq = 0
    header = _Header()

    sequence_num = adapter._fill_header(header, 2.0, "tb_apollo10_gt_bridge", frame_id="map")

    assert sequence_num == 1
    assert header.timestamp_sec == pytest.approx(2.0)
    assert header.module_name == "tb_apollo10_gt_bridge"
    assert header.sequence_num == 1
    assert header.frame_id == "map"


def test_bridge_claim_grade_skips_duplicate_gt_sample() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.claim_grade_enabled = True
    adapter.gt_stale_sample_policy = "skip"
    adapter._last_gt_publish_sample_key = None
    odom = _Odom(10.0, seq=123)

    first, first_key, first_reason = adapter._should_publish_gt_sample(odom)
    second, second_key, second_reason = adapter._should_publish_gt_sample(odom)

    assert first is True
    assert first_reason == "fresh_sample"
    assert first_key == second_key
    assert second is False
    assert second_reason == "stale_sample_skipped"
    assert adapter.stats["gt_stale_sample_duplicate_count"] == 1
    assert adapter.stats["gt_stale_sample_skip_count"] == 1


def test_bridge_artifact_writers_reuse_handles_and_flush(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter._split_csv_headers_written = {}
    adapter.stats = {}

    jsonl_path = tmp_path / "artifact.jsonl"
    csv_path = tmp_path / "artifact.csv"

    adapter._append_jsonl(jsonl_path, {"sample": 1})
    adapter._append_jsonl(jsonl_path, {"sample": 2})
    adapter._write_csv_row(csv_path, {"ts": 1.0, "value": 3})
    adapter._write_csv_row(csv_path, {"ts": 2.0, "value": 4})
    adapter._flush_artifact_buffers(close=False)

    assert len(adapter._jsonl_artifact_handles) == 1
    assert len(adapter._csv_artifact_states) == 1
    assert adapter.stats["artifact_buffering"]["flush_count"] >= 2
    assert jsonl_path.read_text(encoding="utf-8").count("\n") == 2
    assert csv_path.read_text(encoding="utf-8").splitlines() == [
        "ts,value",
        "1.0,3",
        "2.0,4",
    ]

    adapter._flush_artifact_buffers(close=True)


def test_bridge_async_artifact_writer_drains_on_flush(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.artifact_async_write_enabled = True
    adapter._artifact_write_queue = queue.Queue()
    adapter._artifact_writer_thread = None
    adapter._artifact_writer_started = False
    adapter._jsonl_artifact_handles = {}
    adapter._csv_artifact_states = {}
    adapter._artifact_pending_rows = {}
    adapter._artifact_last_flush_sec = {}
    adapter._artifact_write_lock = bridge.threading.Lock()
    adapter._split_csv_headers_written = {}
    adapter.artifact_flush_interval_s = 0.0
    adapter.artifact_flush_max_pending_rows = 0

    jsonl_path = tmp_path / "async.jsonl"
    csv_path = tmp_path / "async.csv"

    adapter._append_jsonl(jsonl_path, {"sample": 1})
    adapter._write_csv_row(csv_path, {"ts": 1.0, "value": 3})

    assert adapter.stats["artifact_buffering"]["async_enqueued_count"] == 2

    adapter._flush_artifact_buffers(close=True)

    assert jsonl_path.read_text(encoding="utf-8").strip() == '{"sample": 1}'
    assert csv_path.read_text(encoding="utf-8").splitlines() == ["ts,value", "1.0,3"]
    assert adapter.stats["artifact_buffering"]["async_written_count"] == 2


def test_bridge_non_close_artifact_flush_does_not_join_async_queue() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    class _JoinTrackingQueue:
        joined = False

        def join(self) -> None:
            self.joined = True
            raise AssertionError("non-close artifact flush must not drain async queue")

        def qsize(self) -> int:
            return 7

    fake_queue = _JoinTrackingQueue()
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter._artifact_write_queue = fake_queue
    adapter._jsonl_artifact_handles = {}
    adapter._csv_artifact_states = {}
    adapter._artifact_write_lock = bridge.threading.Lock()

    adapter._flush_artifact_buffers(close=False)

    assert fake_queue.joined is False
    buffering = adapter.stats["artifact_buffering"]
    assert buffering["async_queue_size"] == 7
    assert buffering["async_queue_size_max"] == 7


def test_bridge_enqueue_artifact_write_records_backpressure(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    class _BackpressureQueue:
        def __init__(self) -> None:
            self.items = []

        def put_nowait(self, item: object) -> None:
            raise queue.Full

        def put(self, item: object, timeout: float | None = None) -> None:
            self.items.append(item)

        def qsize(self) -> int:
            return len(self.items)

    fake_queue = _BackpressureQueue()
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.artifact_async_write_enabled = True
    adapter._artifact_write_queue = fake_queue
    adapter._artifact_writer_started = True

    assert adapter._enqueue_artifact_write("jsonl", tmp_path / "artifact.jsonl", {"sample": 1})

    buffering = adapter.stats["artifact_buffering"]
    assert buffering["async_queue_full_count"] == 1
    assert buffering["async_dropped_count"] == 1
    assert buffering["artifact_backpressure_claim_blocking"] is True
    assert buffering["last_async_drop_kind"] == "jsonl"
    assert fake_queue.items == []


def test_bridge_write_stats_records_diagnostic_write_durations(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.stats_path = tmp_path / "cyber_bridge_stats.json"
    adapter.artifact_stats_flush_interval_s = 0.0
    adapter._last_artifact_stats_flush_sec = 0.0
    adapter.node = types.SimpleNamespace(write_artifacts=lambda: None)
    adapter._write_health_summary = lambda: None
    adapter._write_startup_geometry_summary = lambda: None
    adapter._write_planning_topic_debug_summary = lambda: None

    adapter._write_stats()

    buffering = adapter.stats["artifact_buffering"]
    for key in (
        "health_summary_write_duration_s",
        "startup_geometry_summary_write_duration_s",
        "planning_summary_write_duration_s",
        "node_write_artifacts_duration_s",
        "stats_json_write_duration_s",
        "stats_write_duration_s",
    ):
        assert key in buffering
        assert buffering[key] >= 0.0
        assert buffering[f"{key}_max"] >= buffering[key]
    assert adapter.stats_path.is_file()


def test_bridge_publish_gap_trace_records_skip_reason_and_queue_depth(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {
        "artifact_buffering": {
            "stats_write_duration_s": 0.012,
            "last_writer_write_duration_s": 0.004,
            "writer_write_duration_s_max": 0.007,
            "async_queue_size": 3,
            "async_queue_size_max": 5,
            "async_queue_full_count": 1,
        }
    }
    adapter.artifact_async_write_enabled = False
    adapter.publish_gap_trace_path = tmp_path / "publish_gap_trace.jsonl"
    adapter._latest_world_frame = 123
    adapter._latest_sim_time_sec = 10.0
    adapter._jsonl_artifact_handles = {}
    adapter._csv_artifact_states = {}
    adapter._artifact_pending_rows = {}
    adapter._artifact_last_flush_sec = {}
    adapter._artifact_write_lock = bridge.threading.Lock()
    adapter.artifact_flush_interval_s = 0.0
    adapter.artifact_flush_max_pending_rows = 0
    odom = _Odom(1780600000.0)

    adapter._record_publish_gap_trace(
        snapshot={
            "world_frame": 456,
            "snapshot_wall_time_sec": bridge.time.time() - 0.1,
            "carla_tick_gap_s": 0.05,
            "artifact_payload_build_ms": 2.5,
        },
        odom=odom,
        published_localization=False,
        published_chassis=False,
        skip_reason="stale_sample_skipped",
        loop_start_wall_s=bridge.time.time() - 0.02,
    )
    adapter._flush_artifact_buffers(close=True)

    row = json.loads(adapter.publish_gap_trace_path.read_text(encoding="utf-8").strip())
    assert row["schema_version"] == "publish_gap_trace.v1"
    assert row["world_frame"] == 456
    assert row["skip_reason"] == "stale_sample_skipped"
    assert row["published_localization"] is False
    assert row["snapshot_age_ms"] >= 0.0
    assert row["carla_tick_gap_ms"] == 50.0
    assert row["artifact_payload_build_ms"] == 2.5
    assert row["writer_write_duration_ms"] == 4.0
    assert row["writer_write_duration_ms_max"] == 7.0
    assert row["stats_write_ms"] == 12.0
    assert row["async_queue_depth"] == 3
    assert row["artifact_backpressure"] is True


def test_bridge_debug_timeseries_uses_buffered_csv_writer(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.artifact_async_write_enabled = False
    adapter.debug_csv_path = tmp_path / "debug_timeseries.csv"
    adapter._debug_csv_header_written = False
    adapter._split_csv_headers_written = {}
    adapter._csv_artifact_states = {}
    adapter._jsonl_artifact_handles = {}
    adapter._artifact_pending_rows = {}
    adapter._artifact_last_flush_sec = {}
    adapter._artifact_write_lock = bridge.threading.Lock()
    adapter.artifact_flush_interval_s = 0.0
    adapter.artifact_flush_max_pending_rows = 0

    adapter._write_debug_row({"ts": 1.0, "speed": 2.0})
    adapter._write_debug_row({"ts": 2.0, "speed": 3.0})
    adapter._flush_artifact_buffers(close=True)

    assert adapter.debug_csv_path.read_text(encoding="utf-8").splitlines() == [
        "ts,speed",
        "1.0,2.0",
        "2.0,3.0",
    ]


def test_bridge_publish_loop_timing_records_overruns() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}

    adapter._record_publish_loop_timing(
        start_wall_s=10.0,
        end_wall_s=10.08,
        target_period_s=0.05,
        published_gt=True,
        phase="publish_gt",
    )

    timing = adapter.stats["publish_loop_timing"]
    assert timing["last_phase"] == "publish_gt"
    assert timing["last_duration_s"] == pytest.approx(0.08)
    assert timing["published_gt_iteration_count"] == 1
    assert timing["over_target_period_count"] == 1


def test_bridge_publish_phase_timing_records_stage_overruns() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}

    adapter._record_publish_phase_timing(
        "artifact_trace_writes",
        start_wall_s=1.0,
        end_wall_s=1.07,
        target_period_s=0.05,
    )

    phase = adapter.stats["publish_loop_phase_timing"]["artifact_trace_writes"]
    assert phase["recent_sample_count"] == 1
    assert phase["last_duration_s"] == pytest.approx(0.07)
    assert phase["recent_duration_p95_s"] == pytest.approx(0.07)
    assert phase["over_target_period_count"] == 1


def test_bridge_front_obstacle_status_exposes_actor_probe_boundary() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.front_obstacle_behavior_mode = "normal"
    adapter.front_obstacle_role_names = ["front"]
    adapter.front_obstacle_actor_probe_enabled = False
    adapter.front_obstacle_activate_distance_m = 18.0
    adapter.front_obstacle_release_distance_m = 24.0
    adapter.front_obstacle_min_longitudinal_m = 2.0
    adapter.front_obstacle_max_lateral_m = 3.5
    adapter.front_obstacle_latch_enabled = True
    adapter._front_obstacle_visible = True
    adapter._front_obstacle_suppressed_frames = 0
    adapter._front_obstacle_state_changed_ts = 0.0
    adapter._front_obstacle_last_gap = {}
    adapter.front_obstacle_cache_enabled = True
    adapter.front_obstacle_cache_ttl_sec = 0.5
    adapter._obstacle_cache_hit_count = 0
    adapter._obstacle_cache_last_hit_ts_sec = 0.0

    status = adapter._front_obstacle_behavior_status()

    assert status["mode"] == "normal"
    assert status["actor_probe_enabled"] is False


def test_bridge_extracts_simple_lon_debug_context_for_control_attribution() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    simple_lon_debug = types.SimpleNamespace(
        current_speed=3.2,
        speed_reference=8.0,
        speed_error=4.8,
        acceleration_cmd=0.6,
        acceleration_cmd_closeloop=0.5,
        acceleration_lookup=0.4,
        acceleration_reference=0.7,
        current_acceleration=0.2,
        acceleration_error=0.5,
        path_remain=42.0,
        station_error=-1.0,
        preview_station_error=-0.5,
        throttle_cmd=12.0,
        brake_cmd=0.0,
        is_full_stop=False,
    )
    cmd = types.SimpleNamespace(
        throttle=12.0,
        brake=0.0,
        speed=3.2,
        acceleration=0.6,
        debug=types.SimpleNamespace(simple_lon_debug=simple_lon_debug),
    )

    raw = adapter._extract_raw_control_fields(cmd)

    assert raw["debug_simple_lon_current_speed"] == pytest.approx(3.2)
    assert raw["debug_simple_lon_speed_reference"] == pytest.approx(8.0)
    assert raw["debug_simple_lon_speed_error"] == pytest.approx(4.8)
    assert raw["debug_simple_lon_acceleration_cmd"] == pytest.approx(0.6)
    assert raw["debug_simple_lon_acceleration_cmd_closeloop"] == pytest.approx(0.5)
    assert raw["debug_simple_lon_acceleration_lookup"] == pytest.approx(0.4)
    assert raw["debug_simple_lon_current_acceleration"] == pytest.approx(0.2)
    assert raw["debug_simple_lon_path_remain"] == pytest.approx(42.0)
    assert raw["debug_simple_lon_throttle_cmd"] == pytest.approx(12.0)
    assert raw["debug_simple_lon_is_full_stop"] is False


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)


def _install_fake_carla() -> None:
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))
