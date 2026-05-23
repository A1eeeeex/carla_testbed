"""Apollo MVP adapter namespace.

This package contains runtime-neutral Apollo adapter contracts and CI-safe mock
implementations. Real runtime IO remains outside the core contract boundary.
"""

from .channels import ApolloChannels
from .config import ApolloMVPConfig
from .control_mapping import ApolloControlMappingConfig, map_apollo_control_dict_to_command
from .cyber_backend import ApolloCyberRTBackend, ApolloRuntimeUnavailableError
from .mock_backend import MockApolloBackend
from .time_sync import ApolloTimeAdapter

__all__ = [
    "ApolloChannels",
    "ApolloControlMappingConfig",
    "ApolloCyberRTBackend",
    "ApolloMVPConfig",
    "ApolloRuntimeUnavailableError",
    "ApolloTimeAdapter",
    "MockApolloBackend",
    "map_apollo_control_dict_to_command",
]
