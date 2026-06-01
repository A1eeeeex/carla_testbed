"""Apollo MVP adapter namespace.

This package contains runtime-neutral Apollo adapter contracts and CI-safe mock
implementations. Real runtime IO remains outside the core contract boundary.
"""

from .channels import ApolloChannels
from .config import ApolloMVPConfig
from .control_mapping import ApolloControlMappingConfig, map_apollo_control_dict_to_command
from .cyber_backend import ApolloCyberRTBackend, ApolloRuntimeUnavailableError
from .mock_backend import MockApolloBackend
from .traffic_light_gt import (
    MockTrafficLightGTPublisher,
    TrafficLightMapping,
    build_traffic_light_gt_message_dict,
    find_traffic_light_mapping,
    iter_traffic_light_mappings,
    load_traffic_light_mappings,
)
from .time_sync import ApolloTimeAdapter

__all__ = [
    "ApolloChannels",
    "ApolloControlMappingConfig",
    "ApolloCyberRTBackend",
    "ApolloMVPConfig",
    "ApolloRuntimeUnavailableError",
    "ApolloTimeAdapter",
    "MockApolloBackend",
    "MockTrafficLightGTPublisher",
    "TrafficLightMapping",
    "build_traffic_light_gt_message_dict",
    "find_traffic_light_mapping",
    "iter_traffic_light_mappings",
    "load_traffic_light_mappings",
    "map_apollo_control_dict_to_command",
]
