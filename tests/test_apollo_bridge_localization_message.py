from __future__ import annotations

import importlib
import json
import math
import queue
import sys
import types
from collections import Counter, deque
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


def test_bridge_converts_objects_json_yaw_degrees_to_apollo_radians() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    transform = bridge.Transform2D(yaw_deg=10.0, heading_offset_deg=-5.0)

    theta = bridge._objects_json_yaw_to_apollo_radians(-179.97, transform)

    assert theta == pytest.approx(math.radians(-174.97))


def test_bridge_obstacle_debug_reads_materialized_apollo_fields() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    obstacle = types.SimpleNamespace(
        id=39,
        position=types.SimpleNamespace(x=252.0, y=5.2, z=0.3),
        theta=-math.pi + 0.001,
        velocity=types.SimpleNamespace(x=-19.43, y=0.01, z=0.0),
    )

    debug = bridge._published_obstacle_debug(obstacle)

    assert debug == {
        "id": 39,
        "position": {"x": 252.0, "y": 5.2, "z": 0.3},
        "theta_rad": pytest.approx(-math.pi + 0.001),
        "velocity": {"x": -19.43, "y": 0.01, "z": 0.0},
    }


def test_bridge_planning_decision_debug_extracts_follow_distance() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    class Tagged(types.SimpleNamespace):
        def __init__(self, oneof_value: str, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.oneof_value = oneof_value

        def WhichOneof(self, _name: str) -> str:
            return self.oneof_value

    follow = types.SimpleNamespace(
        distance_s=-31.5,
        fence_point=types.SimpleNamespace(x=250.0, y=5.2, z=0.0),
        fence_heading=-math.pi,
    )
    object_decision = Tagged("follow", follow=follow)
    obstacle = types.SimpleNamespace(id="48_0", perception_id=48, object_decision=[object_decision])
    main = Tagged("cruise", cruise=types.SimpleNamespace())
    msg = types.SimpleNamespace(
        decision=types.SimpleNamespace(
            main_decision=main,
            object_decision=types.SimpleNamespace(decision=[obstacle]),
        )
    )

    debug = bridge._planning_decision_debug(msg)

    assert debug["main"]["type"] == "cruise"
    assert debug["objects"] == [
        {
            "id": "48_0",
            "perception_id": 48,
            "decisions": [
                {
                    "type": "follow",
                    "distance_s": -31.5,
                    "reason_code": None,
                    "fence_point": {"x": 250.0, "y": 5.2, "z": 0.0},
                    "fence_heading_rad": -math.pi,
                }
            ],
        }
    ]


def test_bridge_planning_st_graph_debug_extracts_follow_boundary() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    points = [types.SimpleNamespace(s=12.0, t=0.0), types.SimpleNamespace(s=36.0, t=4.0)]
    graph = types.SimpleNamespace(
        name="PIECEWISE_JERK_SPEED",
        boundary=[types.SimpleNamespace(name="57_0", type=3, point=points)],
        speed_constraint=types.SimpleNamespace(lower_bound=[0.0, 0.0], upper_bound=[20.0, 18.0]),
        kernel_follow_ref=types.SimpleNamespace(follow_line_s=[8.0, 70.0]),
        speed_limit=[types.SimpleNamespace(v=23.61), types.SimpleNamespace(v=20.0)],
    )
    msg = types.SimpleNamespace(
        debug=types.SimpleNamespace(planning_data=types.SimpleNamespace(st_graph=[graph]))
    )

    debug = bridge._planning_st_graph_debug(msg)

    assert debug == [
        {
            "name": "PIECEWISE_JERK_SPEED",
            "boundaries": [
                {
                    "name": "57_0",
                    "type": 3,
                    "point_count": 2,
                    "s": {"min": 12.0, "max": 36.0},
                    "t": {"min": 0.0, "max": 4.0},
                }
            ],
            "speed_constraint_lower": {"min": 0.0, "max": 0.0},
            "speed_constraint_upper": {"min": 18.0, "max": 20.0},
            "follow_reference_s": {"min": 8.0, "max": 70.0},
            "speed_limit_v": {"min": 20.0, "max": 23.61},
        }
    ]


def test_bridge_resolves_auto_localization_offset_from_vehicle_reference() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    vehicle_reference = bridge.load_vehicle_reference(
        "configs/vehicles/ego_vehicle_reference.verified.yaml"
    )

    offset_m, source, error = bridge._resolve_localization_back_offset_m(
        "auto",
        vehicle_reference=vehicle_reference,
    )

    assert offset_m == pytest.approx(1.4235)
    assert source == "vehicle_reference"
    assert error == ""
    assert bridge._localization_reference_mode(offset_m) == "rear_axle"


def test_bridge_explicit_zero_localization_offset_remains_config_override() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    vehicle_reference = bridge.load_vehicle_reference(
        "configs/vehicles/ego_vehicle_reference.verified.yaml"
    )

    offset_m, source, error = bridge._resolve_localization_back_offset_m(
        0.0,
        vehicle_reference=vehicle_reference,
    )

    assert offset_m == pytest.approx(0.0)
    assert source == "config"
    assert error == ""
    assert bridge._localization_reference_mode(offset_m) == "vehicle_origin"


def test_bridge_auto_localization_offset_without_vehicle_reference_is_not_claim_grade() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    offset_m, source, error = bridge._resolve_localization_back_offset_m(
        "auto",
        vehicle_reference=None,
    )

    assert offset_m == pytest.approx(0.0)
    assert source == "vehicle_reference_unavailable"
    assert "requires a valid vehicle_reference" in error
    assert bridge._localization_reference_mode(offset_m) == "vehicle_origin"


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


def _queued_ros_cache_node(
    bridge: types.ModuleType,
    depth: int,
    *,
    obstacle_alignment_policy: str = "latest_source_not_after_odom",
):
    node = bridge.RosCacheNode.__new__(bridge.RosCacheNode)
    node.lock = __import__("threading").Lock()
    node.latest_odom = None
    node.latest_objects3d = None
    node.latest_markers = None
    node.latest_objects_json = None
    node.rx_counts = {"odom": 0, "objects3d": 0, "markers": 0, "objects_json": 0}
    node._initialize_input_queues(
        depth,
        obstacle_alignment_policy=obstacle_alignment_policy,
    )
    return node


def test_ros_cache_queue_preserves_distinct_odom_samples_and_reports_overflow() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    node = _queued_ros_cache_node(bridge, depth=2)

    node._on_odom(_Odom(10.00, seq=1))
    node._on_odom(_Odom(10.05, seq=2))
    node._on_odom(_Odom(10.10, seq=3))

    first = node.snapshot()
    second = node.snapshot()

    assert first["odom"].header.seq == 2
    assert second["odom"].header.seq == 3
    assert first["ros_input_queue"]["overflow_dropped"] == 1
    assert first["ros_input_queue"]["pending"] == 1
    assert second["ros_input_queue"]["pending"] == 0
    assert second["ros_input_queue"]["dequeued"] == 2


def test_ros_cache_queue_never_pairs_future_obstacle_state_with_older_odom() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    node = _queued_ros_cache_node(bridge, depth=4)

    object_10_00 = json.dumps({"stamp": 10.00, "objects": [{"id": "lead"}]})
    object_10_10 = json.dumps({"stamp": 10.10, "objects": [{"id": "lead"}]})
    node._on_objects_json(types.SimpleNamespace(data=object_10_00))
    node._on_objects_json(types.SimpleNamespace(data=object_10_10))
    node._on_odom(_Odom(10.05, seq=1))
    node._on_odom(_Odom(10.10, seq=2))

    first = node.snapshot()
    second = node.snapshot()

    assert json.loads(first["objects_json"])["stamp"] == pytest.approx(10.00)
    assert json.loads(second["objects_json"])["stamp"] == pytest.approx(10.10)
    assert first["ros_input_queue"]["obstacle_alignment_policy"] == (
        "latest_source_not_after_odom"
    )


def test_ros_cache_queue_waits_for_exact_obstacle_frame_before_dequeuing_odom() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    node = _queued_ros_cache_node(
        bridge,
        depth=4,
        obstacle_alignment_policy="wait_for_exact_source_time",
    )

    object_10_00 = json.dumps({"stamp": 10.00, "objects": [{"id": "lead"}]})
    object_10_05 = json.dumps({"stamp": 10.05, "objects": [{"id": "lead"}]})
    node._on_objects_json(types.SimpleNamespace(data=object_10_00))
    node._on_odom(_Odom(10.05, seq=1))

    waiting = node.snapshot()

    assert waiting["odom"] is None
    assert waiting["odom_queue_pending"] == 1
    assert waiting["ros_input_queue"]["obstacle_alignment_waiting"] is True
    assert waiting["ros_input_queue"]["obstacle_alignment_status"] == "exact_pending"
    assert waiting["ros_input_queue"]["dequeued"] == 0

    node._on_objects_json(types.SimpleNamespace(data=object_10_05))
    aligned = node.snapshot()

    assert aligned["odom"].header.seq == 1
    assert json.loads(aligned["objects_json"])["stamp"] == pytest.approx(10.05)
    assert aligned["ros_input_queue"]["obstacle_alignment_waiting"] is False
    assert aligned["ros_input_queue"]["obstacle_alignment_status"] == "exact_ready"
    assert aligned["ros_input_queue"]["obstacle_alignment_wait_count"] == 1
    assert aligned["ros_input_queue"]["dequeued"] == 1


def test_ros_cache_queue_drops_odom_when_future_source_proves_exact_frame_missing() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    node = _queued_ros_cache_node(
        bridge,
        depth=4,
        obstacle_alignment_policy="wait_for_exact_source_time",
    )

    object_10_10 = json.dumps({"stamp": 10.10, "objects": [{"id": "lead"}]})
    node._on_objects_json(types.SimpleNamespace(data=object_10_10))
    node._on_odom(_Odom(10.05, seq=1))

    dropped = node.snapshot()

    assert dropped["odom"] is None
    assert dropped["ros_input_queue"]["obstacle_alignment_status"] == (
        "exact_missing_future_seen"
    )
    assert dropped["ros_input_queue"]["obstacle_alignment_dropped_odom"] is True
    assert dropped["ros_input_queue"]["obstacle_alignment_missing_exact_count"] == 1
    assert dropped["ros_input_queue"]["obstacle_alignment_dropped_odom_count"] == 1
    assert dropped["ros_input_queue"]["dequeued"] == 0

    node._on_odom(_Odom(10.10, seq=2))
    aligned = node.snapshot()

    assert aligned["odom"].header.seq == 2
    assert json.loads(aligned["objects_json"])["stamp"] == pytest.approx(10.10)


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


def test_indexed_map_projection_matches_exact_projection_across_grid_boundaries() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    segments = bridge._prepare_map_segments(
        [
            (-40.0, 0.0, 10.0, 0.0),
            (10.0, 0.0, 35.0, 8.0),
            (35.0, 8.0, 80.0, 8.0),
            (0.0, 40.0, 0.0, 75.0),
        ]
    )
    index = bridge._build_segment_spatial_index(segments, cell_size_m=25.0)

    for x, y in ((0.0, 2.0), (24.999, 2.0), (25.001, 2.0), (48.0, 7.0), (100.0, 100.0)):
        expected = bridge._nearest_segment_metrics(x, y, segments)
        actual = bridge._nearest_segment_metrics_indexed(
            x,
            y,
            segments,
            index,
            cell_size_m=25.0,
        )
        assert actual is not None
        assert expected is not None
        for field in ("dist", "proj_x", "proj_y", "seg_yaw", "signed_e_y"):
            assert actual[field] == pytest.approx(expected[field], abs=1e-12)


def test_bridge_zero_max_pending_rows_disables_automatic_artifact_flush(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.artifact_async_write_enabled = False
    adapter._artifact_write_queue = None
    adapter._jsonl_artifact_handles = {}
    adapter._csv_artifact_states = {}
    adapter._artifact_pending_rows = {}
    adapter._artifact_last_flush_sec = {}
    adapter._artifact_write_lock = bridge.threading.Lock()
    adapter._split_csv_headers_written = {}
    adapter.artifact_flush_interval_s = 0.0
    adapter.artifact_flush_max_pending_rows = 0

    jsonl_path = tmp_path / "artifact.jsonl"
    adapter._append_jsonl(jsonl_path, {"sample": 1})
    adapter._append_jsonl(jsonl_path, {"sample": 2})

    buffering = adapter.stats.setdefault("artifact_buffering", {})
    assert buffering.get("flush_count", 0) == 0
    assert adapter._artifact_pending_rows[str(jsonl_path)] == 2

    adapter._flush_artifact_buffers(close=True)
    assert adapter.stats["artifact_buffering"]["flush_count"] == 1
    assert jsonl_path.read_text(encoding="utf-8").count("\n") == 2


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


def test_bridge_stage5_debug_artifacts_can_be_sampled_without_backpressure(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.stage5_debug_artifact_sample_stride = 3
    adapter._stage5_debug_artifact_sample_counters = bridge.Counter()
    writes = []
    adapter._append_jsonl = lambda path, payload: writes.append((path, dict(payload)))

    for index in range(1, 8):
        adapter._append_stage5_debug_jsonl(
            "stage5_apollo_lane_follow_map_debug",
            tmp_path / "stage5_apollo_lane_follow_map_debug.jsonl",
            {"index": index},
        )

    assert [payload["index"] for _, payload in writes] == [1, 3, 6]
    buffering = adapter.stats["artifact_buffering"]
    assert buffering["stage5_debug_artifact_sample_stride"] == 3
    assert buffering["stage5_debug_artifact_seen_counts"] == {
        "stage5_apollo_lane_follow_map_debug": 7
    }
    assert buffering["stage5_debug_artifact_written_counts"] == {
        "stage5_apollo_lane_follow_map_debug": 3
    }
    assert buffering["stage5_debug_artifact_sampled_out_counts"] == {
        "stage5_apollo_lane_follow_map_debug": 4
    }
    assert "artifact_backpressure_claim_blocking" not in buffering


def test_bridge_reference_debug_artifacts_can_be_sampled_without_backpressure(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.reference_debug_artifact_sample_stride = 5
    adapter._reference_debug_artifact_sample_counters = bridge.Counter()
    writes = []
    adapter._append_jsonl = lambda path, payload: writes.append((path, dict(payload)))

    for index in range(1, 12):
        adapter._append_reference_debug_jsonl(
            "apollo_route_segment_debug",
            tmp_path / "apollo_route_segment_debug.jsonl",
            {"index": index},
        )

    assert [payload["index"] for _, payload in writes] == [1, 5, 10]
    buffering = adapter.stats["artifact_buffering"]
    assert buffering["reference_debug_artifact_sample_stride"] == 5
    assert buffering["reference_debug_artifact_seen_counts"] == {
        "apollo_route_segment_debug": 11
    }
    assert buffering["reference_debug_artifact_written_counts"] == {
        "apollo_route_segment_debug": 3
    }
    assert buffering["reference_debug_artifact_sampled_out_counts"] == {
        "apollo_route_segment_debug": 8
    }
    assert "artifact_backpressure_claim_blocking" not in buffering


def test_bridge_control_debug_artifacts_can_be_sampled_without_backpressure(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.control_debug_artifact_sample_stride = 4
    adapter._control_debug_artifact_sample_counters = bridge.Counter()
    writes = []
    adapter._append_jsonl = lambda path, payload: writes.append((path, dict(payload)))

    for index in range(1, 10):
        adapter._append_control_debug_jsonl(
            "control_decode_debug",
            tmp_path / "control_decode_debug.jsonl",
            {"index": index},
        )

    assert [payload["index"] for _, payload in writes] == [1, 4, 8]
    buffering = adapter.stats["artifact_buffering"]
    assert buffering["control_debug_artifact_sample_stride"] == 4
    assert buffering["control_debug_artifact_seen_counts"] == {"control_decode_debug": 9}
    assert buffering["control_debug_artifact_written_counts"] == {"control_decode_debug": 3}
    assert buffering["control_debug_artifact_sampled_out_counts"] == {"control_decode_debug": 6}
    assert "artifact_backpressure_claim_blocking" not in buffering


def test_bridge_control_debug_artifact_can_override_global_stride_by_name(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.control_debug_artifact_sample_stride = 10
    adapter.control_debug_artifact_sample_strides = {"apollo_control_raw": 1}
    adapter._control_debug_artifact_sample_counters = bridge.Counter()
    writes = []
    adapter._append_jsonl = lambda path, payload: writes.append((path.name, dict(payload)))

    for index in range(1, 6):
        adapter._append_control_debug_jsonl(
            "apollo_control_raw",
            tmp_path / "apollo_control_raw.jsonl",
            {"index": index},
        )
        adapter._append_control_debug_jsonl(
            "control_decode_debug",
            tmp_path / "control_decode_debug.jsonl",
            {"index": index},
        )

    assert [payload["index"] for name, payload in writes if name == "apollo_control_raw.jsonl"] == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert [payload["index"] for name, payload in writes if name == "control_decode_debug.jsonl"] == [1]
    buffering = adapter.stats["artifact_buffering"]
    assert buffering["control_debug_artifact_sample_strides"] == {"apollo_control_raw": 1}
    assert buffering["control_debug_artifact_effective_strides"] == {
        "apollo_control_raw": 1,
        "control_decode_debug": 10,
    }
    assert buffering["control_debug_artifact_seen_counts"] == {
        "apollo_control_raw": 5,
        "control_decode_debug": 5,
    }
    assert buffering["control_debug_artifact_written_counts"] == {
        "apollo_control_raw": 5,
        "control_decode_debug": 1,
    }
    assert buffering["control_debug_artifact_sampled_out_counts"] == {
        "control_decode_debug": 4
    }


def test_bridge_claim_evidence_artifacts_use_independent_sampling(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.control_debug_artifact_sample_stride = 10
    adapter.claim_evidence_artifact_sample_stride = 1
    adapter._claim_evidence_artifact_sample_counters = bridge.Counter()

    decisions = [
        adapter._should_write_claim_evidence_artifact("apollo_reference_line_contract")
        for _ in range(5)
    ]

    assert decisions == [True, True, True, True, True]
    buffering = adapter.stats["artifact_buffering"]
    assert buffering["claim_evidence_artifact_sample_stride"] == 1
    assert buffering["claim_evidence_artifact_seen_counts"] == {
        "apollo_reference_line_contract": 5
    }
    assert buffering["claim_evidence_artifact_written_counts"] == {
        "apollo_reference_line_contract": 5
    }
    assert buffering["claim_evidence_artifact_sampled_out_counts"] == {}
    assert "control_debug_artifact_seen_counts" not in buffering


def test_bridge_publish_row_missing_optional_float_does_not_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    warnings = []
    monkeypatch.setattr(
        adapter,
        "_warn_bad_value",
        lambda field, value, source: warnings.append((field, value, source)),
    )

    value = adapter._coerce_float(None, float("nan"), "desired_out.optional", "publish_row")

    assert math.isnan(value)
    assert warnings == []


def test_bridge_non_publish_row_missing_float_still_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    warnings = []
    monkeypatch.setattr(
        adapter,
        "_warn_bad_value",
        lambda field, value, source: warnings.append((field, value, source)),
    )

    value = adapter._coerce_float(None, 0.0, "throttle", "apollo.control")

    assert value == pytest.approx(0.0)
    assert warnings == [("throttle", None, "apollo.control")]


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
    assert buffering["last_stats_write_mode"] == "full"
    assert buffering["full_stats_write_count"] == 1
    for key in (
        "health_summary_write_duration_s",
        "startup_geometry_summary_write_duration_s",
        "planning_summary_write_duration_s",
        "node_write_artifacts_duration_s",
        "stats_json_write_duration_s",
        "stats_write_duration_s",
        "full_stats_write_duration_s",
    ):
        assert key in buffering
        assert buffering[key] >= 0.0
        assert buffering[f"{key}_max"] >= buffering[key]
    assert adapter.stats_path.is_file()
    persisted = json.loads(adapter.stats_path.read_text(encoding="utf-8"))
    persisted_buffering = persisted["artifact_buffering"]
    assert persisted_buffering["full_stats_write_count"] == 1
    assert "full_stats_write_duration_s" in persisted_buffering
    assert "full_stats_write_duration_s_max" in persisted_buffering


def test_planning_topic_summary_materializes_last_message_receipt_wall_time() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter._planning_point_counts = [12]
    adapter._planning_parse_fail_count = 0
    adapter._planning_parse_fail_reasons = Counter()
    adapter._planning_last_event = {
        "planning_header_sequence_num": 9,
        "is_replan": True,
        "replan_reason": "matched point drift",
    }
    adapter._planning_recent_events = deque([adapter._planning_last_event])
    adapter._planning_reader_enabled = True
    adapter._planning_reader_enable_reason = "enabled"
    adapter.planning_channel = "/apollo/planning"
    adapter._planning_message_type = "apollo.planning.ADCTrajectory"
    adapter._planning_msg_count = 1
    adapter._planning_nonempty_count = 1
    adapter._planning_empty_count = 0
    adapter._planning_first_nonempty_ts_sec = 10.0
    adapter._planning_last_points = 12
    adapter._planning_last_trajectory_type = "NORMAL"
    adapter._planning_last_msg_ts = 10.0
    adapter._planning_last_msg_wall_time_sec = 1234.5
    adapter._planning_last_distance_to_destination = None
    adapter._planning_stall_consecutive_empty = 0
    adapter._planning_stall_first_empty_ts_sec = None
    adapter._planning_stall_last_reported = 0
    adapter._planning_timing_summary = lambda: {}

    summary = adapter._planning_topic_debug_summary_payload()

    assert summary["last_msg_wall_time_sec"] == pytest.approx(1234.5)
    assert summary["last_planning_header_sequence_num"] == 9
    assert summary["last_trajectory_type"] == "NORMAL"
    assert summary["last_is_replan"] is True
    assert summary["last_replan_reason"] == "matched point drift"


def test_prediction_reader_materializes_compact_runtime_evidence(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter._prediction_msg_count = 0
    adapter._prediction_reader_enabled = True
    adapter._prediction_reader_enable_reason = "enabled"
    adapter.prediction_channel = "/apollo/prediction"
    adapter._prediction_message_type = "apollo.prediction.PredictionObstacles"
    adapter.prediction_topic_debug_path = tmp_path / "prediction_topic_debug.jsonl"
    adapter.stats = {}
    adapter._command_now_sec = lambda: 1000.0
    adapter._timing_snapshot = lambda **_kwargs: {
        "wall_time_sec": 1000.0,
        "sim_time_sec": 35.0,
        "world_frame": 77,
    }
    rows: list[dict[str, object]] = []
    adapter._append_jsonl = lambda _path, row: rows.append(row)
    msg = types.SimpleNamespace(
        header=types.SimpleNamespace(timestamp_sec=1000.0, sequence_num=9),
        prediction_obstacle=[],
    )

    adapter._on_prediction(msg)

    assert adapter._prediction_msg_count == 1
    assert rows[0]["sim_time_sec"] == pytest.approx(35.0)
    assert rows[0]["prediction_header_sequence_num"] == 9
    assert rows[0]["prediction_message_parsed_successfully"] is True
    summary = adapter.stats["prediction_topic_debug"]
    assert summary["total_messages_received"] == 1
    assert summary["last_prediction_obstacle_count"] == 0
    assert summary["artifact_path"].endswith("prediction_topic_debug.jsonl")


def test_bridge_write_stats_lightweight_skips_hot_path_summary_writes(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.stats_path = tmp_path / "cyber_bridge_stats.json"
    adapter.artifact_stats_flush_interval_s = 0.0
    adapter._last_artifact_stats_flush_sec = 0.0
    calls: list[str] = []
    adapter.node = types.SimpleNamespace(write_artifacts=lambda: calls.append("node"))
    adapter._write_health_summary = lambda: calls.append("health")
    adapter._write_startup_geometry_summary = lambda: calls.append("startup")
    adapter._write_planning_topic_debug_summary = lambda: calls.append("planning")

    adapter._write_stats(full=False)

    buffering = adapter.stats["artifact_buffering"]
    assert buffering["last_stats_write_mode"] == "lightweight"
    assert buffering["lightweight_stats_write_count"] == 1
    assert calls == []
    assert "stats_json_write_duration_s" in buffering
    assert "stats_write_duration_s" in buffering
    assert "lightweight_stats_write_duration_s" in buffering
    assert "full_stats_write_duration_s" not in buffering
    assert "health_summary_write_duration_s" not in buffering
    assert adapter.stats_path.is_file()


def test_bridge_json_snapshot_isolated_from_mapping_size_changes(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    class _SizeChangingDict(dict):
        def items(self):
            for index, item in enumerate(super().items()):
                if index == 0:
                    self["callback_update"] = {"count": 2}
                yield item

    payload = _SizeChangingDict({"artifact_buffering": {"count": 1}})
    with pytest.raises(RuntimeError, match="dictionary changed size during iteration"):
        json.dumps(payload, indent=2)

    payload = _SizeChangingDict({"artifact_buffering": {"count": 1}})
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    path = tmp_path / "cyber_bridge_stats.json"
    adapter._write_json_file(path, payload)

    assert json.loads(path.read_text(encoding="utf-8")) == {
        "artifact_buffering": {"count": 1}
    }


def test_bridge_run_loop_uses_configured_periodic_stats_flush() -> None:
    source = Path("tools/apollo10_cyber_bridge/bridge.py").read_text(encoding="utf-8")
    assert "def maybe_write_periodic_stats" in source
    assert "self._write_stats(full=False)" in source
    assert 'getattr(self, "artifact_stats_flush_interval_s", 0.0)' in source
    assert "if interval_s <= 0.0:" in source
    assert "if (now - last_stats_flush) >= 1.0:" not in source


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


def test_bridge_goal_snap_rejects_untrusted_projection(tmp_path: Path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.auto_routing_snap_allow_untrusted_source = False
    adapter._lane_projection_probe = lambda x, y: {
        "available": True,
        "proj_x": x + 1.0,
        "proj_y": y + 1.0,
        "distance_m": 1.4,
        "trusted_lane_centerline": False,
    }

    x, y, probe = adapter._snap_xy_to_lane(10.0, 20.0)

    assert (x, y) == (10.0, 20.0)
    assert probe["accepted"] is False
    assert probe["applied"] is False
    assert probe["reject_reason"] == "untrusted_snap_source"


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


def test_precomputed_map_segment_projection_preserves_exact_metrics() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    raw_segments = [
        (0.0, 0.0, 10.0, 0.0),
        (10.0, 0.0, 10.0, 10.0),
        (100.0, 100.0, 100.0, 100.0),
    ]
    prepared_segments = bridge._prepare_map_segments(raw_segments)

    for point in [(2.0, 3.0), (12.0, 4.0), (50.0, 50.0)]:
        raw = bridge._nearest_segment_metrics(*point, raw_segments)
        prepared = bridge._nearest_segment_metrics(*point, prepared_segments)
        assert prepared is not None
        assert raw is not None
        for key in ("dist", "proj_x", "proj_y", "seg_yaw", "signed_e_y", "curvature"):
            assert prepared[key] == pytest.approx(raw[key])


def test_bridge_mock_clock_publishes_sim_time_in_nanoseconds() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    class FakeClock:
        def __init__(self) -> None:
            self.clock = 0

    class FakeClockPb2:
        Clock = FakeClock

    published: list[FakeClock] = []
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.cyber_clock_enabled = True
    adapter.cyber_clock_pb2 = FakeClockPb2
    adapter.cyber_clock_writer = types.SimpleNamespace(write=published.append)
    adapter.stats = {"cyber_clock": {"publish_count": 0}}
    adapter._publish_cyber_clock(12.345678901)

    assert len(published) == 1
    assert published[0].clock == 12_345_678_901
    assert adapter.stats["cyber_clock"]["publish_count"] == 1
    assert adapter.stats["cyber_clock"]["last_timestamp_sec"] == pytest.approx(12.345678901)


def test_bridge_mock_clock_uses_latest_sim_time_for_diagnostics() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.cyber_clock_enabled = True
    adapter._latest_sim_time_sec = 4.25

    assert adapter._command_now_sec() == pytest.approx(4.25)


def test_bridge_mock_clock_uses_control_sim_time_for_planning_age() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.cyber_clock_enabled = True

    assert adapter._planning_age_reference_time_sec(4.25) == pytest.approx(4.25)


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


def test_bridge_publish_phase_timing_updates_only_current_window_and_retains_others() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}

    adapter._record_publish_phase_timing(
        "state_write",
        start_wall_s=1.0,
        end_wall_s=1.01,
        target_period_s=0.05,
    )
    adapter._record_publish_phase_timing(
        "artifact_trace",
        start_wall_s=2.0,
        end_wall_s=2.02,
        target_period_s=0.05,
    )
    adapter._record_publish_phase_timing(
        "state_write",
        start_wall_s=3.0,
        end_wall_s=3.03,
        target_period_s=0.05,
    )

    phases = adapter.stats["publish_loop_phase_timing"]
    assert set(phases) == {"state_write", "artifact_trace"}
    assert phases["state_write"]["recent_sample_count"] == 2
    assert phases["state_write"]["last_duration_s"] == pytest.approx(0.03)
    assert phases["artifact_trace"]["recent_sample_count"] == 1


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
    adapter.front_obstacle_activation_marker_path = Path("unused.json")
    adapter.front_obstacle_activation_required_status = "pass"
    adapter._front_obstacle_activation_gate_open = True
    adapter._front_obstacle_activation_gate_reason = "not_configured"
    adapter._front_obstacle_activation_gate_opened_ts = 0.0

    status = adapter._front_obstacle_behavior_status()

    assert status["mode"] == "normal"
    assert status["actor_probe_enabled"] is False


def test_bridge_reads_tick_aligned_ego_feedback_without_carla_rpc() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.carla_feedback_state_source = "ros_objects_json"
    adapter.carla_feedback = types.SimpleNamespace(
        read_control=lambda: pytest.fail("CARLA RPC must not be used for JSON state source")
    )
    recorded_vehicle = {}
    adapter._maybe_record_carla_vehicle = lambda payload: recorded_vehicle.update(payload)
    snapshot = {
        "objects_json": json.dumps(
            {
                "stamp": 20.05,
                "ego_state": {
                    "stamp": 20.05,
                    "pose": {"x": 1.0, "y": 2.0, "z": 0.5, "yaw": 15.0},
                    "control": {
                        "throttle": 0.2,
                        "brake": 0.0,
                        "steer": 0.8,
                        "reverse": False,
                        "hand_brake": False,
                        "gear": 4,
                    },
                    "steer_feedback_source": "wheel_angle",
                    "steer_feedback_deg": 3.5,
                    "steer_feedback_pct": 5.0,
                    "speed_mps": 19.4,
                    "forward_accel_mps2": 0.25,
                    "lateral_accel_mps2": -0.1,
                    "yaw_rate_rps": 0.02,
                    "vehicle_characteristics": {"length": 4.8, "width": 1.8},
                },
            }
        )
    }

    measured = adapter._read_measured_control(snapshot)

    assert measured["available"] is True
    assert measured["source"] == "ros_objects_json:wheel_angle"
    assert measured["source_stamp_sec"] == pytest.approx(20.05)
    assert measured["steer"] == pytest.approx(0.05)
    assert measured["steer_feedback_deg"] == pytest.approx(3.5)
    assert measured["forward_accel_mps2"] == pytest.approx(0.25)
    assert adapter.stats["ros_objects_json_ego_state"]["success_count"] == 1
    assert recorded_vehicle == {"length": 4.8, "width": 1.8}


def test_bridge_selects_front_actor_from_aligned_objects_json() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.stats = {}
    adapter.front_obstacle_role_names = ["lead_vehicle", "front"]
    snapshot = {
        "objects_json": json.dumps(
            {
                "stamp": 20.05,
                "objects": [
                    {"id": "4", "role_name": "other"},
                    {
                        "id": "9",
                        "role_name": "lead_vehicle",
                        "type_id": "vehicle.audi.a2",
                        "pose": {"x": 30.0, "y": 0.0, "yaw": 0.0},
                        "velocity": {"x": 19.4, "y": 0.0, "z": 0.0},
                        "size": {"x": 4.2, "y": 1.8, "z": 1.5},
                    },
                ],
            }
        )
    }

    actor = adapter._front_actor_from_objects_json(snapshot)

    assert actor is not None
    assert actor["id"] == "9"
    assert actor["role_name"] == "lead_vehicle"
    assert actor["size"]["x"] == pytest.approx(4.2)


def test_bridge_initial_state_gate_opens_only_for_observed_ready_marker(tmp_path) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    marker = tmp_path / "fixed_scene_obstacle_activation.json"
    adapter.front_obstacle_behavior_mode = "scenario_initial_state_gate"
    adapter.front_obstacle_activation_marker_path = marker
    adapter.front_obstacle_activation_required_status = "pass"
    adapter._front_obstacle_activation_gate_open = False
    adapter._front_obstacle_activation_gate_reason = "activation_marker_missing"
    adapter._front_obstacle_activation_gate_opened_ts = 0.0
    adapter._front_obstacle_visible = False
    adapter._front_obstacle_state_changed_ts = 0.0

    assert adapter._front_obstacle_visible_now() is False
    marker.write_text(
        json.dumps({"status": "pass", "speed_ready": True, "gap_ready": False}),
        encoding="utf-8",
    )
    assert adapter._front_obstacle_visible_now() is False
    assert adapter._front_obstacle_activation_gate_reason == (
        "activation_marker_state_not_ready"
    )

    marker.write_text(
        json.dumps({"status": "pass", "speed_ready": True, "gap_ready": True}),
        encoding="utf-8",
    )
    assert adapter._front_obstacle_visible_now() is True
    assert adapter._front_obstacle_activation_gate_reason == "activation_marker_ready"

    marker.unlink()
    assert adapter._front_obstacle_visible_now() is True


def test_bridge_preserves_objects_json_source_time_for_obstacle_header() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.obstacle_time_source = "source_time"
    snapshot = {
        "objects3d": None,
        "objects_json": json.dumps(
            {
                "stamp": 12.25,
                "objects": [{"id": "7", "pose": {"x": 20.0}}],
            }
        ),
        "markers": None,
    }

    source_time_sec, source_time_base = adapter._obstacle_source_time_from_snapshot(
        snapshot
    )
    obstacle_time_sec, obstacle_time_base = adapter._obstacle_time_from_odom(
        object(),
        localization_header_time_sec=12.35,
        fallback_cyber_time_sec=999.0,
        obstacle_source_time_sec=source_time_sec,
        obstacle_source_time_base=source_time_base,
    )

    assert source_time_sec == pytest.approx(12.25)
    assert source_time_base == "objects_json_source_time"
    assert obstacle_time_sec == pytest.approx(12.25)
    assert obstacle_time_base == "objects_json_source_time"


def test_bridge_front_actor_dimensions_cache_handles_terminal_zero_bbox() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter._front_actor_dimension_cache = {}
    actor = types.SimpleNamespace(
        bounding_box=types.SimpleNamespace(
            extent=types.SimpleNamespace(x=2.45, y=0.92, z=0.75),
        )
    )

    first = adapter._front_actor_dimensions(101, actor)
    actor.bounding_box.extent = types.SimpleNamespace(x=0.0, y=0.0, z=0.0)
    second = adapter._front_actor_dimensions(101, actor)

    assert first["source"] == "carla_actor_bbox"
    assert first["length"] == pytest.approx(4.9)
    assert second["source"] == "cached_carla_actor_bbox"
    assert second["length"] == pytest.approx(first["length"])
    assert second["width"] == pytest.approx(first["width"])
    assert second["height"] == pytest.approx(first["height"])
    assert "front_actor_bbox_non_positive_used_cached_dimensions" in second["warnings"]


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


def test_bridge_extracts_lateral_steer_decomposition_for_control_attribution() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    simple_lat_debug = types.SimpleNamespace(
        steer_angle=12.0,
        steer_angle_feedforward=8.0,
        steer_angle_lateral_contribution=1.0,
        steer_angle_lateral_rate_contribution=2.0,
        steer_angle_heading_contribution=3.0,
        steer_angle_heading_rate_contribution=-2.0,
        steer_angle_feedback=4.0,
    )
    cmd = types.SimpleNamespace(
        steering_target=12.0,
        debug=types.SimpleNamespace(simple_lat_debug=simple_lat_debug),
    )

    raw = adapter._extract_raw_control_fields(cmd)

    assert raw["debug_simple_lat_steer_angle_feedforward"] == pytest.approx(8.0)
    assert raw["debug_simple_lat_steer_angle_feedback"] == pytest.approx(4.0)
    assert raw["debug_simple_lat_steer_angle_lateral_contribution"] == pytest.approx(1.0)
    assert raw["debug_simple_lat_steer_angle_lateral_rate_contribution"] == pytest.approx(2.0)
    assert raw["debug_simple_lat_steer_angle_heading_contribution"] == pytest.approx(3.0)
    assert raw["debug_simple_lat_steer_angle_heading_rate_contribution"] == pytest.approx(-2.0)


class _PresenceProto:
    def __init__(self, *, present: set[str], **fields: object) -> None:
        self._present = set(present)
        for name, value in fields.items():
            setattr(self, name, value)

    def HasField(self, name: str) -> bool:
        return name in self._present


def test_bridge_control_debug_extraction_ignores_unset_protobuf_defaults() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    default_path = _PresenceProto(
        present=set(),
        x=0.0,
        y=0.0,
        theta=0.0,
        kappa=0.0,
        dkappa=0.0,
        s=0.0,
    )
    default_point = _PresenceProto(
        present=set(),
        path_point=default_path,
        relative_time=0.0,
        v=0.0,
    )
    lon_debug = _PresenceProto(
        present=set(),
        current_station=0.0,
        current_matched_point=default_point,
        current_reference_point=default_point,
        preview_reference_point=default_point,
    )
    lat_debug = _PresenceProto(
        present=set(),
        lateral_error=0.0,
        current_target_point=default_point,
        current_reference_point=default_point,
        preview_reference_point=default_point,
    )
    input_debug = _PresenceProto(
        present=set(),
        localization_header=_PresenceProto(present=set(), timestamp_sec=0.0, sequence_num=0),
        canbus_header=_PresenceProto(present=set(), timestamp_sec=0.0, sequence_num=0),
        trajectory_header=_PresenceProto(present=set(), timestamp_sec=0.0, sequence_num=0),
        latest_replan_trajectory_header=_PresenceProto(
            present=set(), timestamp_sec=0.0, sequence_num=0
        ),
    )
    debug = _PresenceProto(
        present={"simple_lon_debug", "simple_lat_debug", "input_debug"},
        simple_lon_debug=lon_debug,
        simple_lat_debug=lat_debug,
        input_debug=input_debug,
    )
    cmd = _PresenceProto(
        present={"debug"},
        debug=debug,
        throttle=0.0,
        steering_target=0.0,
        trajectory_fraction=0.0,
    )

    raw = adapter._extract_raw_control_fields(cmd)

    assert "throttle" not in raw
    assert "steering_target" not in raw
    assert "trajectory_fraction" not in raw
    assert raw["trajectory_fraction_present"] is False
    assert raw["debug_present"] is True
    assert raw["debug_simple_lon_present"] is True
    assert raw["debug_simple_lat_present"] is True
    assert raw["debug_input_present"] is True
    assert "debug_simple_lon_current_station" not in raw
    assert "debug_simple_lat_lateral_error" not in raw
    assert raw["debug_simple_lon_current_matched_point_present"] is False
    assert raw["debug_simple_lat_current_target_point_present"] is False
    assert raw["debug_input_localization_header_present"] is False
    assert raw["debug_input_canbus_header_present"] is False
    assert raw["debug_input_trajectory_header_present"] is False
    assert raw["debug_input_latest_replan_trajectory_header_present"] is False
    assert "debug_simple_lon_current_matched_point_x" not in raw
    assert "debug_simple_lat_current_target_point_x" not in raw


def test_bridge_control_debug_extraction_preserves_input_state_headers() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    input_debug = _PresenceProto(
        present={
            "localization_header",
            "canbus_header",
            "trajectory_header",
            "latest_replan_trajectory_header",
        },
        localization_header=_PresenceProto(
            present={"timestamp_sec", "sequence_num"},
            timestamp_sec=12.30,
            sequence_num=101,
        ),
        canbus_header=_PresenceProto(
            present={"timestamp_sec", "sequence_num"},
            timestamp_sec=12.30,
            sequence_num=202,
        ),
        trajectory_header=_PresenceProto(
            present={"timestamp_sec", "sequence_num"},
            timestamp_sec=12.20,
            sequence_num=303,
        ),
        latest_replan_trajectory_header=_PresenceProto(
            present={"timestamp_sec", "sequence_num"},
            timestamp_sec=11.80,
            sequence_num=299,
        ),
    )
    debug = _PresenceProto(present={"input_debug"}, input_debug=input_debug)
    cmd = _PresenceProto(present={"debug"}, debug=debug)

    raw = adapter._extract_raw_control_fields(cmd)

    assert raw["debug_input_localization_header_present"] is True
    assert raw["debug_input_localization_header_timestamp_sec"] == pytest.approx(12.30)
    assert raw["debug_input_localization_header_sequence_num"] == 101
    assert raw["debug_input_canbus_header_present"] is True
    assert raw["debug_input_canbus_header_timestamp_sec"] == pytest.approx(12.30)
    assert raw["debug_input_canbus_header_sequence_num"] == 202
    assert raw["debug_input_trajectory_header_sequence_num"] == 303
    assert raw["debug_input_latest_replan_trajectory_header_sequence_num"] == 299


def test_bridge_control_debug_extraction_preserves_present_zero_values() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    zero_path = _PresenceProto(
        present={"x", "y", "theta", "kappa", "dkappa", "s"},
        x=0.0,
        y=0.0,
        theta=0.0,
        kappa=0.0,
        dkappa=0.0,
        s=0.0,
    )
    zero_point = _PresenceProto(
        present={"path_point", "relative_time", "v"},
        path_point=zero_path,
        relative_time=0.0,
        v=0.0,
    )
    lon_debug = _PresenceProto(
        present={"current_station", "current_matched_point"},
        current_station=0.0,
        current_matched_point=zero_point,
    )
    lat_debug = _PresenceProto(
        present={"lateral_error", "current_target_point"},
        lateral_error=0.0,
        current_target_point=zero_point,
    )
    debug = _PresenceProto(
        present={"simple_lon_debug", "simple_lat_debug"},
        simple_lon_debug=lon_debug,
        simple_lat_debug=lat_debug,
    )
    cmd = _PresenceProto(
        present={"debug", "throttle", "steering_target", "trajectory_fraction"},
        debug=debug,
        throttle=0.0,
        steering_target=0.0,
        trajectory_fraction=0.0,
    )

    raw = adapter._extract_raw_control_fields(cmd)

    assert raw["throttle"] == pytest.approx(0.0)
    assert raw["steering_target"] == pytest.approx(0.0)
    assert raw["trajectory_fraction"] == pytest.approx(0.0)
    assert raw["trajectory_fraction_present"] is True
    assert raw["debug_simple_lon_current_station"] == pytest.approx(0.0)
    assert raw["debug_simple_lat_lateral_error"] == pytest.approx(0.0)
    assert raw["debug_simple_lon_current_matched_point_present"] is True
    assert raw["debug_simple_lon_current_matched_point_x"] == pytest.approx(0.0)
    assert raw["debug_simple_lon_current_matched_point_s"] == pytest.approx(0.0)
    assert raw["debug_simple_lat_current_target_point_present"] is True
    assert raw["debug_simple_lat_current_target_point_relative_time"] == pytest.approx(0.0)


def test_bridge_obstacle_publish_rate_uses_simulation_time() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.obstacle_publish_policy = "rate_limited"
    adapter.obstacle_publish_rate_hz = 10.0
    adapter._last_obstacle_publish_sim_time = None

    assert adapter._should_publish_obstacles(1.0) is True
    assert adapter._should_publish_obstacles(1.05) is False
    assert adapter._should_publish_obstacles(1.10) is True
    assert adapter._should_publish_obstacles(0.0) is True


def test_bridge_obstacle_source_fresh_policy_publishes_each_source_frame_once() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.obstacle_publish_policy = "source_fresh"
    adapter.obstacle_publish_rate_hz = 10.0
    adapter._last_obstacle_publish_sim_time = None
    adapter._last_obstacle_source_time_sec = None

    assert adapter._should_publish_obstacles(1.00, obstacle_source_time_sec=None) is False
    assert adapter._should_publish_obstacles(1.00, obstacle_source_time_sec=10.0) is True
    assert adapter._should_publish_obstacles(1.05, obstacle_source_time_sec=10.0) is False
    assert adapter._should_publish_obstacles(1.15, obstacle_source_time_sec=10.1) is True
    assert adapter._should_publish_obstacles(1.20, obstacle_source_time_sec=10.1) is False
    assert adapter._should_publish_obstacles(0.00, obstacle_source_time_sec=0.1) is True


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)


def _install_fake_carla() -> None:
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))
