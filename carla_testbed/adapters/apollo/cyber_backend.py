from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any

from carla_testbed.adapters.base import BackendDiagnostics, BackendState

from .config import ApolloMVPConfig
from .publishers import ChassisPublisher, GroundTruthObstaclePublisher, LocalizationPublisher
from .subscribers import ControlCommandSubscriber, PlanningTrajectorySubscriber
from .time_sync import ApolloTimeAdapter


class ApolloRuntimeUnavailableError(RuntimeError):
    pass


def _load_cyber_runtime() -> Any:
    errors: list[str] = []
    for module_name in ("cyber", "cyber_py.cyber"):
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            errors.append(f"{module_name}: {exc}")
    raise ApolloRuntimeUnavailableError(
        "Apollo CyberRT runtime is unavailable. Install/source the local Apollo "
        "environment before starting ApolloCyberRTBackend. Tried: " + "; ".join(errors)
    )


@dataclass
class ApolloCyberRTBackend:
    """Lazy CyberRT backend skeleton for the Apollo MVP adapter boundary."""

    config: ApolloMVPConfig = field(default_factory=ApolloMVPConfig)
    cyber_runtime: Any | None = None

    def __post_init__(self) -> None:
        self.config.validate()
        self._prepared = False
        self._running = False
        self._context: Any | None = None
        self._publish_count = 0
        self._control_poll_count = 0
        self._time_adapter = ApolloTimeAdapter(time_source=self.config.time_source)
        self.localization_publisher = LocalizationPublisher(
            channel=self.config.channels.localization,
            time_adapter=self._time_adapter,
        )
        self.chassis_publisher = ChassisPublisher(
            channel=self.config.channels.chassis,
            time_adapter=self._time_adapter,
        )
        self.obstacle_publisher = GroundTruthObstaclePublisher(
            channel=self.config.channels.obstacles,
            time_adapter=self._time_adapter,
        )
        self.control_subscriber = ControlCommandSubscriber(
            channel=self.config.channels.control,
            mapping_config=self.config.control_mapping,
        )
        self.planning_subscriber = PlanningTrajectorySubscriber(channel=self.config.channels.planning)

    @property
    def name(self) -> str:
        return "apollo_cyberrt"

    def prepare(self, context: Any) -> None:
        self._context = context
        self._prepared = True

    def start(self) -> None:
        if not self._prepared:
            self.prepare(context=None)
        if self.cyber_runtime is None:
            self.cyber_runtime = _load_cyber_runtime()
        self._running = True

    def publish_inputs(self, frame_context: Any) -> None:
        if not self._running:
            raise RuntimeError("ApolloCyberRTBackend.publish_inputs called before start")
        stamp = getattr(frame_context, "stamp", None) or getattr(frame_context, "frame_stamp", None)
        if stamp is None:
            stamp = frame_context
        ego_state = getattr(frame_context, "ego_state", None)
        scene_state = getattr(frame_context, "scene_state", None)
        chassis_state = getattr(frame_context, "chassis_state", ego_state)
        if ego_state is not None:
            self.localization_publisher.publish(self.localization_publisher.build_message(ego_state, stamp))
        if chassis_state is not None:
            self.chassis_publisher.publish(self.chassis_publisher.build_message(chassis_state, stamp))
        if self.config.use_ground_truth_obstacles and scene_state is not None:
            self.obstacle_publisher.publish(self.obstacle_publisher.build_message(scene_state, stamp))
        self._publish_count += 1

    def poll_control(self, timeout_s: float | None = None):
        self._control_poll_count += 1
        return self.control_subscriber.poll(
            self.config.control_timeout_s if timeout_s is None else timeout_s
        )

    def collect_diagnostics(self) -> BackendDiagnostics:
        return BackendDiagnostics(
            name=self.name,
            state=BackendState(
                name=self.name,
                running=self._running,
                ready=self._running and self.cyber_runtime is not None,
                metadata={
                    "prepared": self._prepared,
                    "channels": self.config.channels.to_dict(),
                    "time_source": self.config.time_source,
                },
            ),
            counters={
                "publish_inputs_count": self._publish_count,
                "control_poll_count": self._control_poll_count,
                "localization_publish_count": self.localization_publisher.publish_count,
                "chassis_publish_count": self.chassis_publisher.publish_count,
                "obstacle_publish_count": self.obstacle_publisher.publish_count,
                "control_receive_count": self.control_subscriber.receive_count,
            },
        )

    def stop(self) -> None:
        self._running = False
