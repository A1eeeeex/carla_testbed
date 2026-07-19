from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
import xml.etree.ElementTree as ET

import pytest

from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.config.loader import load_config
from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.phase1_pair_runner import _infer_carla_town
from carla_testbed.platform.registry import PlatformRegistry
from carla_testbed.scenario_player import builtin_ego_runner
from carla_testbed.sim import bringup
from carla_testbed.sim.bringup import generate_opendrive_world_with_retry
from tools.build_baguang_extended_opendrive import build_extended_opendrive


REPO_ASSET = Path(
    "configs/io/maps/straight_road_for_baguang_extended_120m/"
    "straight_road_for_baguang_extended_120m.xodr"
)
SCENARIO = Path(
    "configs/scenarios/baguang/lead_decel_accel_70_40_70_20m_extended_opendrive.yaml"
)
APOLLO_PROFILE = Path(
    "configs/io/examples/"
    "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_extended_opendrive_candidate.yaml"
)
DRIVETRAIN_READY_APOLLO_PROFILE = Path(
    "configs/io/examples/"
    "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_"
    "drivetrain_ready_extended_opendrive_candidate.yaml"
)
DRIVETRAIN_READY_PHYSICAL_STEERING_APOLLO_PROFILE = Path(
    "configs/io/examples/"
    "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_"
    "drivetrain_ready_extended_opendrive_physical_steering_candidate.yaml"
)


def test_extended_opendrive_builder_extends_both_ends_without_mutating_source(tmp_path: Path) -> None:
    source = tmp_path / "source.xodr"
    source.write_text(_minimal_xodr(), encoding="utf-8")
    original = source.read_text(encoding="utf-8")
    output = tmp_path / "extended.xodr"

    report = build_extended_opendrive(source, output, extension_each_end_m=120.0)

    root = ET.parse(output).getroot()
    road = root.find("road")
    geometry = root.find("./road/planView/geometry")
    assert road is not None
    assert geometry is not None
    assert float(road.attrib["length"]) == pytest.approx(640.0)
    assert float(geometry.attrib["length"]) == pytest.approx(640.0)
    assert float(geometry.attrib["x"]) == pytest.approx(120.0)
    assert root.find("./road/objects") is None
    assert source.read_text(encoding="utf-8") == original
    assert report["original_road_length_m"] == 400.0
    assert report["generated_road_length_m"] == 640.0
    assert report["removed_static_object_count"] == 1


def test_checked_in_extended_opendrive_asset_has_expected_geometry() -> None:
    root = ET.parse(REPO_ASSET).getroot()
    road = root.find("road")
    geometry = root.find("./road/planView/geometry")

    assert road is not None
    assert geometry is not None
    assert float(road.attrib["length"]) == pytest.approx(639.6800325861155)
    assert float(geometry.attrib["length"]) == pytest.approx(639.6800325861155)
    assert float(geometry.attrib["x"]) > 419.0
    assert root.find("./road/objects") is None


def test_generate_opendrive_world_records_source_hash_and_restores_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "map.xodr"
    source.write_text(_minimal_xodr(), encoding="utf-8")
    world = _World("Carla/Maps/OpenDriveMap")
    client = _GenerateClient(world)
    rows: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setitem(
        sys.modules,
        "carla",
        SimpleNamespace(OpendriveGenerationParameters=_GenerationParameters),
    )

    loaded = generate_opendrive_world_with_retry(
        client,
        source,
        generation_parameters={"vertex_distance": 2.0, "smooth_junctions": True},
        attempts=1,
        timeout_s=45.0,
        restore_timeout_s=30.0,
        attempt_callback=lambda phase, payload: rows.append((phase, payload)),
    )

    assert loaded is world
    assert client.timeout_values == [45.0, 30.0]
    assert client.parameters.values == {"vertex_distance": 2.0, "smooth_junctions": True}
    ok = next(payload for phase, payload in rows if phase == "generate_opendrive_world_attempt_ok")
    assert ok["loaded_map_name"] == "OpenDriveMap"
    assert ok["opendrive_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()


def test_connect_world_validates_generated_map_identity_not_logical_town(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial_world = _World("Carla/Maps/straight_road_for_baguang")
    generated_world = _World("Carla/Maps/OpenDriveMap")
    client = SimpleNamespace()
    monkeypatch.setattr(bringup, "create_client_with_retry", lambda **_kwargs: client)
    monkeypatch.setattr(bringup, "get_world_with_retry", lambda *_args, **_kwargs: initial_world)
    monkeypatch.setattr(
        bringup,
        "generate_opendrive_world_with_retry",
        lambda *_args, **_kwargs: generated_world,
    )
    monkeypatch.setattr(bringup.time, "sleep", lambda _seconds: None)

    result = bringup.connect_world_with_retry(
        host="localhost",
        port=2000,
        target_town="straight_road_for_baguang",
        opendrive_path="custom.xodr",
        opendrive_expected_map_name="OpenDriveMap",
    )

    assert result.world is generated_world
    assert result.final_town == "OpenDriveMap"
    assert result.session_state["target_town"] == "straight_road_for_baguang"
    assert result.session_state["opendrive_expected_map_name"] == "OpenDriveMap"


def test_builtin_runner_materializes_scenario_opendrive_and_writes_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _load_yaml(SCENARIO)
    config = builtin_ego_runner._scenario_opendrive_config(scenario)
    world = _World("Carla/Maps/OpenDriveMap")

    def _generate(client: object, path: Path, **kwargs: object) -> _World:
        callback = kwargs["attempt_callback"]
        callback(
            "generate_opendrive_world_attempt_ok",
            {
                "loaded_map_name": "OpenDriveMap",
                "opendrive_path": str(path),
                "opendrive_sha256": "abc123",
                "opendrive_bytes": 42,
            },
        )
        return world

    monkeypatch.setattr(builtin_ego_runner, "generate_opendrive_world_with_retry", _generate)

    loaded = builtin_ego_runner._load_world_for_builtin_runner(
        object(),
        "OpenDriveMap",
        artifact_dir=tmp_path,
        opendrive_config=config,
    )

    assert loaded is world
    evidence = json.loads((tmp_path / "carla_opendrive_world.json").read_text(encoding="utf-8"))
    assert evidence["status"] == "materialized"
    assert evidence["loaded_map_name"] == "OpenDriveMap"
    assert evidence["opendrive_sha256"] == "abc123"


def test_extended_opendrive_candidate_dispatch_is_explicitly_quarantined() -> None:
    config = load_config(APOLLO_PROFILE)
    apollo = config.backend.params["legacy_algo"]["apollo"]
    runtime = config.backend.params["legacy_runtime"]
    opendrive_runtime = runtime["carla"]["opendrive"]

    assert opendrive_runtime["enabled"] is True
    assert opendrive_runtime["path"] == str(REPO_ASSET)
    assert apollo["routing"]["scenario_goal_ahead_m"] == 630.0
    assert apollo["planning"].get("disable_destination_rule", False) is False
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm=str(APOLLO_PROFILE),
        scenario=str(SCENARIO),
        recording="metrics",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    launch = default_backend_registry().for_plan(plan).build_launch_plan(plan).to_dict()

    assert plan.scenario.map == "OpenDriveMap"
    assert launch["commands"]
    command = launch["commands"][0]
    assert str(APOLLO_PROFILE) in command
    assert not any(str(item).startswith("run.map=") for item in command)
    assert f"runtime.fixed_scene_player.scenario_path={SCENARIO}" in command

    scenario = json.loads(json.dumps(_load_yaml(SCENARIO)))
    opendrive = scenario["carla_world"]["opendrive"]
    assert opendrive["path"] == str(REPO_ASSET)
    assert opendrive["expected_map_name"] == "OpenDriveMap"
    assert _infer_carla_town(SCENARIO) == "OpenDriveMap"


def test_drivetrain_ready_extended_candidate_preserves_handover_and_control_boundaries() -> None:
    config = load_config(DRIVETRAIN_READY_APOLLO_PROFILE)
    apollo = config.backend.params["legacy_algo"]["apollo"]
    runtime = config.backend.params["legacy_runtime"]
    fixed_scene = runtime["fixed_scene_player"]
    opendrive_runtime = runtime["carla"]["opendrive"]

    assert opendrive_runtime["enabled"] is True
    assert opendrive_runtime["path"] == str(REPO_ASSET)
    assert apollo["routing"]["scenario_goal_ahead_m"] == 630.0
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["scene_preroll_drivetrain_gate_enabled"] is True
    assert fixed_scene["scene_preroll_drivetrain_max_abs_acceleration_mps2"] == 2.0
    assert fixed_scene["scene_preroll_ready_hold_ticks"] == 4
    assert apollo["bridge"]["localization_acceleration_source"] == "carla_feedback"
    assert apollo["prediction"]["vehicle_on_lane_caution_mode"] == (
        "cost_move_sequence_current_state"
    )
    assert apollo["control_mapping"]["actuator_mapping_mode"] == "legacy"
    assert "physical" not in apollo["control_mapping"]
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["terminal_stop_hold"]["enabled"] is False
    assert apollo["planning"].get("disable_destination_rule", False) is False

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm=str(DRIVETRAIN_READY_APOLLO_PROFILE),
        scenario=str(SCENARIO),
        recording="metrics",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    assert plan.scenario.map == "OpenDriveMap"


def test_drivetrain_ready_extended_physical_steering_candidate_is_isolated() -> None:
    config = load_config(DRIVETRAIN_READY_PHYSICAL_STEERING_APOLLO_PROFILE)
    apollo = config.backend.params["legacy_algo"]["apollo"]
    runtime = config.backend.params["legacy_runtime"]
    fixed_scene = runtime["fixed_scene_player"]
    mapping = apollo["control_mapping"]
    physical = mapping["physical"]

    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["scene_preroll_drivetrain_gate_enabled"] is True
    assert fixed_scene["scene_preroll_drivetrain_max_abs_acceleration_mps2"] == 2.0
    assert fixed_scene["scene_preroll_ready_hold_ticks"] == 4
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["terminal_stop_hold"]["enabled"] is False
    assert physical["calibration_file"] == (
        "configs/calibration/vehicles/vehicle.lincoln.mkz_2020/"
        "steering_front_wheel_v1.json"
    )
    assert physical["allow_legacy_fallback"] is False
    assert physical["apollo_max_steer_angle_deg"] == 30.0
    assert physical["map_steering"] is True
    assert physical["map_steering_feedback"] is True
    assert physical["map_longitudinal"] is False
    assert physical["map_throttle"] is False
    assert physical["map_brake"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert apollo["planning"].get("disable_destination_rule", False) is False

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm=str(DRIVETRAIN_READY_PHYSICAL_STEERING_APOLLO_PROFILE),
        scenario=str(SCENARIO),
        recording="metrics",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    assert plan.scenario.map == "OpenDriveMap"


def _load_yaml(path: Path) -> dict[str, object]:
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _minimal_xodr() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header east="10" west="-10" north="5" south="-5" />
  <road name="Road 0" length="400" id="0" junction="-1">
    <planView><geometry s="0" x="0" y="0" hdg="3.141592653589793" length="400"><line /></geometry></planView>
    <lanes><laneSection s="0"><center><lane id="0" type="none" /></center></laneSection></lanes>
    <objects><object id="1" name="probe" s="10" t="0" /></objects>
  </road>
</OpenDRIVE>
"""


class _GenerationParameters:
    def __init__(self, **values: object) -> None:
        self.values = values


class _Map:
    def __init__(self, name: str) -> None:
        self.name = name


class _World:
    def __init__(self, name: str) -> None:
        self.map = _Map(name)

    def get_map(self) -> _Map:
        return self.map


class _GenerateClient:
    def __init__(self, world: _World) -> None:
        self.world = world
        self.timeout_values: list[float] = []
        self.parameters: _GenerationParameters | None = None

    def set_timeout(self, value: float) -> None:
        self.timeout_values.append(float(value))

    def generate_opendrive_world(self, xodr: str, parameters: _GenerationParameters) -> _World:
        assert "<OpenDRIVE>" in xodr
        self.parameters = parameters
        return self.world
