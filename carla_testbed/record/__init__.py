from .artifact_store import (
    EventsWriter,
    RunArtifactStore,
    TimeseriesJsonlWriter,
    build_carla_world_identity,
    build_manifest,
    build_summary,
)
from .route_curve_fields import (
    ROUTE_CURVE_FIELDS_SCHEMA_VERSION,
    ROUTE_CURVE_P0_FIELDS,
    ROUTE_CURVE_P1_FIELDS,
    ROUTE_CURVE_P2_FIELDS,
    build_route_curve_row,
    ensure_route_curve_p0_fields,
)
from .route_curve_context import route_definition_from_metadata
from .timeseries_recorder import TimeseriesRecorder
from .summary_recorder import SummaryRecorder

_LAZY_EXPORTS = {
    "FailFrameCapture": ("carla_testbed.record.fail_capture", "FailFrameCapture"),
    "DemoRecorder": ("carla_testbed.record.video_recorder", "DemoRecorder"),
    "ffmpeg_available": ("carla_testbed.record.video_recorder", "ffmpeg_available"),
    "RecordManager": ("carla_testbed.record.manager", "RecordManager"),
    "RecordOptions": ("carla_testbed.record.manager", "RecordOptions"),
    "RvizLauncher": ("carla_testbed.record.rviz", "RvizLauncher"),
    "Ros2BagRecorder": ("carla_testbed.record.ros2_bag.recorder", "Ros2BagRecorder"),
    "AutowareOperatorViewRecorder": (
        "carla_testbed.record.autoware_operator_view",
        "AutowareOperatorViewRecorder",
    ),
    "AutowareRosbagRecorder": ("carla_testbed.record.autoware_rosbag", "AutowareRosbagRecorder"),
}


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    from importlib import import_module

    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value

__all__ = [
    "EventsWriter",
    "RunArtifactStore",
    "TimeseriesJsonlWriter",
    "TimeseriesRecorder",
    "SummaryRecorder",
    "FailFrameCapture",
    "DemoRecorder",
    "ffmpeg_available",
    "RecordManager",
    "RecordOptions",
    "RvizLauncher",
    "Ros2BagRecorder",
    "AutowareOperatorViewRecorder",
    "AutowareRosbagRecorder",
    "ROUTE_CURVE_FIELDS_SCHEMA_VERSION",
    "ROUTE_CURVE_P0_FIELDS",
    "ROUTE_CURVE_P1_FIELDS",
    "ROUTE_CURVE_P2_FIELDS",
    "build_route_curve_row",
    "ensure_route_curve_p0_fields",
    "route_definition_from_metadata",
    "build_carla_world_identity",
    "build_manifest",
    "build_summary",
]
