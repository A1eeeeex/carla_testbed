"""Algorithm inventory helpers.

This package is intentionally runtime-light. It must not import CARLA, ROS2,
CyberRT, or Apollo protobuf modules.
"""

from carla_testbed.algorithms.inventory import (
    AlgorithmInventoryError,
    AlgorithmInventoryValidation,
    list_enabled_mvp_modules,
    list_modules,
    load_algorithm_inventory,
    validate_algorithm_inventory,
)
from carla_testbed.algorithms.variant import (
    AlgorithmVariantError,
    AlgorithmVariantValidation,
    load_algorithm_variant,
    validate_algorithm_variant,
)
from carla_testbed.algorithms.reproduction import (
    ReproductionReportError,
    ReproductionReportValidation,
    load_reproduction_report,
    validate_reproduction_report,
)
from carla_testbed.algorithms.reproduction_gate import (
    ReproductionGateError,
    evaluate_reproduction_gate,
    load_reproduction_gate_config,
)
from carla_testbed.algorithms.replay_digest import (
    ReplayDigestComparison,
    ReplayDigestError,
    compare_replay_digest,
    load_replay_digest,
    write_replay_comparison_report,
)
from carla_testbed.algorithms.shadow_mode import (
    ShadowModeError,
    analyze_shadow_mode_timeseries,
    load_shadow_mode_config,
    write_shadow_mode_report,
)
from carla_testbed.algorithms.channel_contract import (
    ChannelContractCheck,
    ChannelContractError,
    check_channel_stats,
    check_channel_stats_file,
    generate_adapter_contract_report,
    load_channel_contract,
)

__all__ = [
    "AlgorithmInventoryError",
    "AlgorithmInventoryValidation",
    "AlgorithmVariantError",
    "AlgorithmVariantValidation",
    "ChannelContractCheck",
    "ChannelContractError",
    "ReproductionReportError",
    "ReproductionReportValidation",
    "ReplayDigestComparison",
    "ReplayDigestError",
    "ReproductionGateError",
    "ShadowModeError",
    "check_channel_stats",
    "check_channel_stats_file",
    "compare_replay_digest",
    "analyze_shadow_mode_timeseries",
    "generate_adapter_contract_report",
    "list_enabled_mvp_modules",
    "list_modules",
    "load_algorithm_inventory",
    "load_algorithm_variant",
    "load_channel_contract",
    "load_reproduction_report",
    "load_reproduction_gate_config",
    "load_replay_digest",
    "load_shadow_mode_config",
    "validate_algorithm_inventory",
    "validate_algorithm_variant",
    "validate_reproduction_report",
    "write_replay_comparison_report",
    "write_shadow_mode_report",
    "evaluate_reproduction_gate",
]
