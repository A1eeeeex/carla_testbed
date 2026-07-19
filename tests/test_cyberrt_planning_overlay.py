from __future__ import annotations

import json
import shlex
import subprocess
from types import SimpleNamespace

import pytest

from tbio.backends.cyberrt import CyberRTBackend


def _extract_python_overlay_script(shell: str) -> str:
    marker = "python3 -c "
    assert marker in shell
    return shlex.split(shell)[2]


def _planning_ready_backend(*, require_routing_success: bool | str | None = None) -> CyberRTBackend:
    docker_cfg: dict[str, object] = {
        "control_start_gate": "planning_ready",
        "control_planning_ready_min_nonempty_count": 1,
        "control_planning_ready_min_sequence_num": 5,
    }
    if require_routing_success is not None:
        docker_cfg["control_planning_ready_require_routing_success"] = require_routing_success
    return CyberRTBackend({"algo": {"apollo": {"docker": docker_cfg}}})


def _planning_ready_stats(*, routing_success_count: int) -> dict:
    return {
        "routing_request_count": 1,
        "routing_success_count": routing_success_count,
        "chassis_count": 10,
        "planning": {
            "msg_count": 8,
            "nonempty_trajectory_count": 2,
            "last_planning_header_sequence_num": 7,
            "last_trajectory_point_count": 111,
        },
    }


def test_planning_ready_gate_defaults_to_waiting_for_routing_success(tmp_path, monkeypatch) -> None:
    backend = _planning_ready_backend()
    monkeypatch.setattr(
        backend,
        "_read_stats",
        lambda: (True, 0.0, _planning_ready_stats(routing_success_count=0)),
    )

    status, error = backend._docker_probe_planning_ready_before_control(tmp_path)

    wait_log = (tmp_path / "apollo_control_planning_ready_wait.log").read_text(encoding="utf-8")
    assert status == "waiting"
    assert error is None
    assert "require_routing_success=True" in wait_log
    assert "route_established=False" in wait_log
    assert "planning_nonempty_trajectory_count\": 2" in wait_log


def test_planning_ready_gate_can_opt_out_of_routing_success_requirement(tmp_path, monkeypatch) -> None:
    backend = _planning_ready_backend(require_routing_success=False)
    monkeypatch.setattr(
        backend,
        "_read_stats",
        lambda: (True, 0.0, _planning_ready_stats(routing_success_count=0)),
    )

    status, error = backend._docker_probe_planning_ready_before_control(tmp_path)

    wait_log = (tmp_path / "apollo_control_planning_ready_wait.log").read_text(encoding="utf-8")
    assert status == "ready"
    assert error is None
    assert "require_routing_success=False" in wait_log
    assert "route_established=False" in wait_log


def test_planning_ready_gate_starts_after_routing_success_and_nonempty_planning(tmp_path, monkeypatch) -> None:
    backend = _planning_ready_backend()
    monkeypatch.setattr(
        backend,
        "_read_stats",
        lambda: (True, 0.0, _planning_ready_stats(routing_success_count=1)),
    )

    status, error = backend._docker_probe_planning_ready_before_control(tmp_path)

    wait_log = (tmp_path / "apollo_control_planning_ready_wait.log").read_text(encoding="utf-8")
    assert status == "ready"
    assert error is None
    assert "require_routing_success=True" in wait_log
    assert "route_established=True" in wait_log


def test_planning_ready_gate_uses_bridge_health_routing_success_aliases(tmp_path, monkeypatch) -> None:
    backend = _planning_ready_backend()
    monkeypatch.setattr(
        backend,
        "_read_stats",
        lambda: (True, 0.0, _planning_ready_stats(routing_success_count=0)),
    )
    (tmp_path / "bridge_health_summary.json").write_text(
        json.dumps(
            {
                "routing_request_count": 1,
                "routing_response_count": 1,
                "routing_first_success_response_ts_sec": 123.45,
                "planning_nonempty_trajectory_count": 2,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    status, error = backend._docker_probe_planning_ready_before_control(tmp_path)

    wait_log = (tmp_path / "apollo_control_planning_ready_wait.log").read_text(encoding="utf-8")
    assert status == "ready"
    assert error is None
    assert "routing_success_count\": 1" in wait_log
    assert "route_established=True" in wait_log


def test_planning_overlay_can_disable_control_interactive_replan() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {
                        "enable_control_interactive_replan": False,
                    }
                }
            }
        }
    )

    shell = backend._planning_flags_overlay_shell()
    script = _extract_python_overlay_script(shell)

    assert "--enable_control_interactive_replan=false" in shell
    assert "prefixes.append('--enable_control_interactive_replan=')" in script


def test_planning_overlay_default_does_not_touch_control_interactive_replan() -> None:
    shell = CyberRTBackend({})._planning_flags_overlay_shell()

    assert "--enable_control_interactive_replan" not in shell


def test_planning_overlay_replaces_existing_min_stop_distance_obstacle_flag(
    tmp_path,
) -> None:
    src = tmp_path / "planning.conf"
    src.write_text(
        "--flagfile=modules/common/data/global_flagfile.txt\n"
        "--min_stop_distance_obstacle=6.0\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.conf"
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {
                        "min_stop_distance_obstacle_m": 8.0,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._planning_flags_overlay_shell())
    script = script.replace(
        "'/apollo/modules/planning/planning_component/conf/planning.conf'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/planning/planning_component/conf/planning.conf'",
        repr(str(tmp_path / "missing-share.conf")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/planning/planning_component/conf/planning.conf'",
        repr(str(tmp_path / "missing-src.conf")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/planning.conf'); ",
        f"out=Path({str(out)!r}); ",
    )

    subprocess.run(
        ["python3", "-c", script],
        check=True,
        text=True,
        capture_output=True,
    )

    assert out.read_text(encoding="utf-8").splitlines() == [
        "--flagfile=modules/common/data/global_flagfile.txt",
        "--min_stop_distance_obstacle=8.000",
    ]


def test_prediction_overlay_can_disable_interactive_tag_assignment() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "prediction": {
                        "enable_interactive_tag": False,
                    }
                }
            }
        }
    )

    shell = backend._prediction_flags_overlay_shell()
    script = _extract_python_overlay_script(shell)

    assert "--enable_interactive_tag=false" in shell
    assert "--noenable_interactive_tag" in script
    assert "/apollo/modules/prediction/conf/prediction.conf" in shell


def test_prediction_overlay_default_restores_managed_target() -> None:
    shell = CyberRTBackend({})._prediction_flags_overlay_shell()

    assert "python3 -c" not in shell
    assert "prediction.conf.carla_testbed.bak" in shell
    assert "rm -f /apollo_workspace/conf_overlay/modules/prediction/conf/prediction.conf" in shell


def test_prediction_cpu_device_materializes_flag_and_process_isolation() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "docker": {"start_modules": True},
                    "prediction": {"compute_device": "cpu"},
                }
            }
        }
    )

    flags_shell = backend._prediction_flags_overlay_shell()
    launch_shell = backend._docker_start_modules_cmd()

    assert "--use_cuda=false" in flags_shell
    assert "--nouse_cuda" in _extract_python_overlay_script(flags_shell)
    assert "CUDA_VISIBLE_DEVICES='' setsid -f cyber_launch" in launch_shell
    assert '$(basename "$lf")" = "prediction.launch"' in launch_shell


def test_prediction_compute_device_rejects_unknown_value() -> None:
    backend = CyberRTBackend(
        {"algo": {"apollo": {"prediction": {"compute_device": "tpu"}}}}
    )

    with pytest.raises(ValueError, match=r"expected auto\|cpu"):
        backend._prediction_flags_overlay_shell()


def test_managed_module_overlay_restore_script_restores_only_managed_links(
    tmp_path,
) -> None:
    apollo_root = tmp_path / "apollo"
    overlay_root = tmp_path / "conf_overlay"
    managed_target = apollo_root / "modules/prediction/conf/prediction.conf"
    managed_overlay = overlay_root / "modules/prediction/conf/prediction.conf"
    managed_backup = managed_target.with_name(
        managed_target.name + ".carla_testbed.bak"
    )
    managed_target.parent.mkdir(parents=True)
    managed_overlay.parent.mkdir(parents=True)
    managed_overlay.write_text("candidate\n", encoding="utf-8")
    managed_backup.write_text("baseline\n", encoding="utf-8")
    managed_target.symlink_to(managed_overlay)

    originally_missing_target = apollo_root / "modules/common/data/vehicle_param.pb.txt"
    originally_missing_overlay = (
        overlay_root / "modules/common/data/vehicle_param.pb.txt"
    )
    originally_missing_target.parent.mkdir(parents=True)
    originally_missing_overlay.parent.mkdir(parents=True)
    originally_missing_overlay.write_text("vehicle candidate\n", encoding="utf-8")
    originally_missing_target.symlink_to(originally_missing_overlay)

    external_target = apollo_root / "modules/control/conf/control.conf"
    external_source = tmp_path / "external/control.conf"
    external_backup = external_target.with_name(
        external_target.name + ".carla_testbed.bak"
    )
    external_target.parent.mkdir(parents=True)
    external_source.parent.mkdir(parents=True)
    external_source.write_text("external\n", encoding="utf-8")
    external_backup.write_text("must-not-restore\n", encoding="utf-8")
    external_target.symlink_to(external_source)

    script = CyberRTBackend({})._managed_module_overlay_restore_script(
        apollo_root=apollo_root,
        overlay_root=overlay_root,
    )
    completed = subprocess.run(
        ["python3", "-c", script],
        check=True,
        text=True,
        capture_output=True,
    )
    payload = json.loads(completed.stdout)

    assert managed_target.is_file()
    assert not managed_target.is_symlink()
    assert managed_target.read_text(encoding="utf-8") == "baseline\n"
    assert not originally_missing_target.exists()
    assert external_target.is_symlink()
    assert external_target.resolve() == external_source
    assert payload["restored_count"] == 1
    assert payload["removed_count"] == 1
    assert payload["restored_targets"][0]["target"] == str(managed_target)
    assert payload["removed_targets"][0]["target"] == str(
        originally_missing_target
    )


def test_stop_restores_managed_module_overlays(tmp_path, monkeypatch) -> None:
    backend = CyberRTBackend({})
    restore_calls = []
    monkeypatch.setattr(backend, "_artifacts_dir", lambda: tmp_path)
    monkeypatch.setattr(backend, "_snapshot_docker_modules_status", lambda *_args: None)
    monkeypatch.setattr(backend, "_cleanup_stale_control_bridge", lambda *_args: None)
    monkeypatch.setattr(backend, "_cleanup_stale_apollo_bridge", lambda: None)
    monkeypatch.setattr(backend, "_stop_dreamview", lambda *_args: None)
    monkeypatch.setattr(backend, "_docker_restore_control_runtime_overlay", lambda *_args: None)
    monkeypatch.setattr(
        backend,
        "_docker_restore_managed_module_overlays",
        lambda artifacts=None: restore_calls.append(artifacts),
    )
    monkeypatch.setattr(backend, "_snapshot_apollo_logs", lambda *_args: None)
    monkeypatch.setattr(backend, "_snapshot_apollo_bvar_dumps", lambda *_args: None)
    monkeypatch.setattr(backend, "_snapshot_apollo_internal_debug", lambda *_args: None)

    backend.stop()

    assert restore_calls == [tmp_path]


def test_prediction_overlay_replaces_existing_interactive_tag_flags(tmp_path) -> None:
    src = tmp_path / "prediction.conf"
    src.write_text(
        "--flagfile=modules/common/data/global_flagfile.txt\n"
        "--noenable_interactive_tag\n"
        "--enable_interactive_tag=true\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.conf"
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "prediction": {
                        "enable_interactive_tag": False,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._prediction_flags_overlay_shell())
    script = script.replace(
        "'/apollo/modules/prediction/conf/prediction.conf.carla_testbed.bak'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/prediction/conf/prediction.conf'",
        repr(str(tmp_path / "missing-share.conf")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/prediction/conf/prediction.conf'",
        repr(str(tmp_path / "missing-src.conf")),
    )
    script = script.replace(
        "'/apollo/modules/prediction/conf/prediction.conf'",
        repr(str(tmp_path / "missing-target.conf")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/prediction/conf/prediction.conf'); ",
        f"out=Path({str(out)!r}); ",
    )

    completed = subprocess.run(
        ["python3", "-c", script],
        check=True,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0
    assert out.read_text(encoding="utf-8").splitlines() == [
        "--flagfile=modules/common/data/global_flagfile.txt",
        "--enable_interactive_tag=false",
    ]


def test_prediction_obstacle_overlay_selects_current_state_caution_path() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "prediction": {
                        "vehicle_on_lane_caution_mode": (
                            "cost_move_sequence_current_state"
                        ),
                    }
                }
            }
        }
    )

    shell = backend._prediction_obstacle_conf_overlay_shell()
    script = _extract_python_overlay_script(shell)

    assert "COST_EVALUATOR" in script
    assert "MOVE_SEQUENCE_PREDICTOR" in script
    assert "VEHICLE/ON_LANE/CAUTION" in script
    assert "/apollo/modules/prediction/conf/prediction_conf.pb.txt" in shell


def test_prediction_obstacle_overlay_default_restores_managed_target() -> None:
    shell = CyberRTBackend({})._prediction_obstacle_conf_overlay_shell()

    assert "python3 -c" not in shell
    assert "prediction_conf.pb.txt.carla_testbed.bak" in shell
    assert (
        "rm -f /apollo_workspace/conf_overlay/modules/prediction/conf/"
        "prediction_conf.pb.txt" in shell
    )


def test_prediction_obstacle_overlay_rejects_unknown_mode() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "prediction": {
                        "vehicle_on_lane_caution_mode": "unknown",
                    }
                }
            }
        }
    )

    with pytest.raises(ValueError, match="vehicle_on_lane_caution_mode"):
        backend._prediction_obstacle_conf_overlay_shell()


def test_prediction_obstacle_overlay_rewrites_only_vehicle_on_lane_caution(
    tmp_path,
) -> None:
    src = tmp_path / "prediction_conf.pb.txt"
    src.write_text(
        "obstacle_conf {\n"
        "  obstacle_type: VEHICLE\n"
        "  obstacle_status: ON_LANE\n"
        "  priority_type: CAUTION\n"
        "  evaluator_type: VECTORNET_EVALUATOR\n"
        "  predictor_type: EXTRAPOLATION_PREDICTOR\n"
        "}\n"
        "obstacle_conf {\n"
        "  obstacle_type: VEHICLE\n"
        "  obstacle_status: ON_LANE\n"
        "  priority_type: NORMAL\n"
        "  evaluator_type: CRUISE_MLP_EVALUATOR\n"
        "  predictor_type: MOVE_SEQUENCE_PREDICTOR\n"
        "}\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.pb.txt"
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "prediction": {
                        "vehicle_on_lane_caution_mode": (
                            "cost_move_sequence_current_state"
                        ),
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(
        backend._prediction_obstacle_conf_overlay_shell()
    )
    script = script.replace(
        "'/apollo/modules/prediction/conf/prediction_conf.pb.txt.carla_testbed.bak'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/prediction/conf/prediction_conf.pb.txt'",
        repr(str(tmp_path / "missing-share.pb.txt")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/prediction/conf/prediction_conf.pb.txt'",
        repr(str(tmp_path / "missing-src.pb.txt")),
    )
    script = script.replace(
        "'/apollo/modules/prediction/conf/prediction_conf.pb.txt'",
        repr(str(tmp_path / "missing-target.pb.txt")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/prediction/conf/"
        "prediction_conf.pb.txt'); ",
        f"out=Path({str(out)!r}); ",
    )

    completed = subprocess.run(
        ["python3", "-c", script],
        check=True,
        text=True,
        capture_output=True,
    )
    updated = out.read_text(encoding="utf-8")

    assert completed.returncode == 0
    assert updated.count("evaluator_type: COST_EVALUATOR") == 1
    assert updated.count("predictor_type: MOVE_SEQUENCE_PREDICTOR") == 2
    assert "evaluator_type: VECTORNET_EVALUATOR" not in updated
    assert "evaluator_type: CRUISE_MLP_EVALUATOR" in updated


def test_module_launch_applies_prediction_flags_before_obstacle_config() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "docker": {"start_modules": True},
                    "prediction": {
                        "enable_interactive_tag": False,
                        "vehicle_on_lane_caution_mode": (
                            "cost_move_sequence_current_state"
                        ),
                    },
                }
            }
        }
    )

    shell = backend._docker_start_modules_cmd()

    flags_overlay = (
        "/apollo_workspace/conf_overlay/modules/prediction/conf/prediction.conf"
    )
    obstacle_overlay = (
        "/apollo_workspace/conf_overlay/modules/prediction/conf/"
        "prediction_conf.pb.txt"
    )
    assert "--enable_interactive_tag=false" in shell
    assert "COST_EVALUATOR" in shell
    assert "MOVE_SEQUENCE_PREDICTOR" in shell
    assert shell.index(flags_overlay) < shell.index(obstacle_overlay)


def test_mock_clock_overlay_is_explicit_and_preserves_default_boundary() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "bridge": {
                        "cyber_clock": {
                            "enabled": True,
                            "mode": "mock",
                            "channel": "/clock",
                        }
                    }
                }
            }
        }
    )

    shell = backend._docker_cyber_clock_prefix()

    assert "MODE_MOCK" in shell
    assert "/apollo_workspace/conf_overlay/cyber/conf/cyber.pb.conf" in shell
    assert "export CYBER_PATH=/apollo_workspace/conf_overlay/cyber" in shell
    assert "CYBER_CLOCK_SOURCE" in shell
    assert CyberRTBackend({})._docker_cyber_clock_prefix() == ""


def test_planning_overlay_replaces_existing_control_interactive_replan_flag(tmp_path, monkeypatch) -> None:
    src = tmp_path / "planning.conf"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text(
        "--flagfile=modules/common/data/global_flagfile.txt\n"
        "--enable_control_interactive_replan=true\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.conf"

    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {
                        "enable_control_interactive_replan": False,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._planning_flags_overlay_shell())
    script = script.replace(
        "'/apollo/modules/planning/planning_component/conf/planning.conf'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/planning/planning_component/conf/planning.conf'",
        repr(str(tmp_path / "missing-share.conf")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/planning/planning_component/conf/planning.conf'",
        repr(str(tmp_path / "missing-src.conf")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/planning.conf'); ",
        f"out=Path({str(out)!r}); ",
    )

    completed = subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    assert completed.returncode == 0
    assert "--enable_control_interactive_replan=true" not in out.read_text(encoding="utf-8")
    assert "--enable_control_interactive_replan=false" in out.read_text(encoding="utf-8")


def test_destination_rule_overlay_removes_only_destination(tmp_path) -> None:
    src = tmp_path / "traffic_rule_config.pb.txt"
    src.write_text(
        'rule {\n  name: "BACKSIDE_VEHICLE"\n  type: "BacksideVehicle"\n}\n'
        'rule {\n  name: "CROSSWALK"\n  type: "Crosswalk"\n}\n'
        'rule {\n  name: "DESTINATION"\n  type: "Destination"\n}\n'
        'rule {\n  name: "KEEP_CLEAR"\n  type: "KeepClear"\n}\n'
        'rule {\n  name: "REFERENCE_LINE_END"\n  type: "ReferenceLineEnd"\n}\n'
        'rule {\n  name: "REROUTING"\n  type: "Rerouting"\n}\n'
        'rule {\n  name: "STOP_SIGN"\n  type: "StopSign"\n}\n'
        'rule {\n  name: "TRAFFIC_LIGHT"\n  type: "TrafficLight"\n}\n'
        'rule {\n  name: "YIELD_SIGN"\n  type: "YieldSign"\n}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.pb.txt"
    backend = CyberRTBackend(
        {"algo": {"apollo": {"planning": {"disable_destination_rule": True}}}}
    )
    script = _extract_python_overlay_script(backend._traffic_rules_pipeline_overlay_shell())
    script = script.replace(
        "'/apollo/modules/planning/planning_component/conf/traffic_rule_config.pb.txt'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/planning/planning_component/conf/traffic_rule_config.pb.txt'",
        repr(str(tmp_path / "missing-share.pb.txt")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/planning/planning_component/conf/traffic_rule_config.pb.txt'",
        repr(str(tmp_path / "missing-src.pb.txt")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/traffic_rule_config.pb.txt'); ",
        f"out=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert 'name: "DESTINATION"' not in text
    assert 'type: "Destination"' not in text
    assert text.count("rule {") == 8
    for rule_name in (
        "BACKSIDE_VEHICLE",
        "CROSSWALK",
        "KEEP_CLEAR",
        "REFERENCE_LINE_END",
        "REROUTING",
        "STOP_SIGN",
        "TRAFFIC_LIGHT",
        "YIELD_SIGN",
    ):
        assert f'name: "{rule_name}"' in text


def test_destination_rule_overlay_is_default_off() -> None:
    shell = CyberRTBackend({})._traffic_rules_pipeline_overlay_shell()

    assert "python3 -c" not in shell
    assert "traffic_rule_config.pb.txt.carla_testbed.bak" in shell


def test_control_runtime_dag_overlay_sets_timer_interval(tmp_path) -> None:
    src = tmp_path / "control.dag"
    src.write_text(
        "module_config {\n"
        "    timer_components {\n"
        "        class_name : \"ControlComponent\"\n"
        "        config {\n"
        "            interval: 10\n"
        "        }\n"
        "    }\n"
        "}\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.dag"
    backend = CyberRTBackend(
        {"algo": {"apollo": {"control_runtime": {"control_interval_ms": 50}}}}
    )
    script = _extract_python_overlay_script(backend._control_dag_overlay_shell())
    script = script.replace(
        "'/apollo/modules/control/control_component/dag/control.dag'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/control/control_component/dag/control.dag'",
        repr(str(tmp_path / "missing-share.dag")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/control/control_component/dag/control.dag'",
        repr(str(tmp_path / "missing-src.dag")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/control/control_component/dag/control.dag'); ",
        f"out=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "interval: 50" in text
    assert "interval: 10" not in text


def test_control_runtime_flags_overlay_sets_control_period(tmp_path) -> None:
    src = tmp_path / "control.conf"
    src.write_text(
        "--pipeline_file=modules/control/control_component/conf/pipeline.pb.txt\n"
        "--control_period=0.01\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.conf"
    backend = CyberRTBackend(
        {"algo": {"apollo": {"control_runtime": {"control_interval_ms": 50}}}}
    )
    script = _extract_python_overlay_script(backend._control_flags_overlay_shell())
    script = script.replace(
        "'/apollo/modules/control/control_component/conf/control.conf'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/control/control_component/conf/control.conf'",
        repr(str(tmp_path / "missing-share.conf")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/control/control_component/conf/control.conf'",
        repr(str(tmp_path / "missing-src.conf")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/control/control_component/conf/control.conf'); ",
        f"out=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "--control_period=0.01" not in text
    assert "--control_period=0.050" in text


def test_control_runtime_overlays_default_do_not_touch_control_runtime_files() -> None:
    backend = CyberRTBackend({})

    assert "control_interval_ms" not in backend._control_dag_overlay_shell()
    assert "interval:" not in backend._control_dag_overlay_shell()
    assert "--control_period=" not in backend._control_flags_overlay_shell()


def _run_speed_bounds_overlay(tmp_path, planning: dict[str, object]) -> str:
    src = tmp_path / "default_conf.pb.txt"
    src.write_text(
        "total_time: 7.0\n"
        "boundary_buffer: 0.25\n"
        "max_centric_acceleration_limit: 0.8\n"
        "lowest_speed: 0.1\n"
        "enable_nudge_slowdown: true\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.pb.txt"
    backend = CyberRTBackend({"algo": {"apollo": {"planning": planning}}})
    script = _extract_python_overlay_script(backend._speed_bounds_decider_overlay_shell())
    script = script.replace(
        "'/apollo/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt.carla_testbed.bak'",
        repr(str(tmp_path / "missing-backup.pb.txt")),
    )
    script = script.replace(
        "'/apollo/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt'",
        repr(str(tmp_path / "missing-share.pb.txt")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt'",
        repr(str(tmp_path / "missing-src.pb.txt")),
    )
    script = script.replace(
        "p=Path('/apollo_workspace/conf_overlay/modules/planning/tasks/speed_bounds_decider/conf/default_conf.pb.txt'); ",
        f"p=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)
    return out.read_text(encoding="utf-8")


def test_speed_bounds_overlay_replaces_curve_limit_and_preserves_base_config(tmp_path) -> None:
    text = _run_speed_bounds_overlay(
        tmp_path,
        {"speed_bounds_decider_max_centric_acceleration_mps2": 0.4},
    )

    assert "max_centric_acceleration_limit: 0.400" in text
    assert "max_centric_acceleration_limit: 0.8" not in text
    assert text.count("max_centric_acceleration_limit:") == 1
    assert "total_time: 7.0" in text
    assert "boundary_buffer: 0.25" in text
    assert "lowest_speed: 0.1" in text
    assert "enable_nudge_slowdown: true" in text


def test_speed_bounds_overlay_preserves_curve_limit_when_overriding_other_fields(tmp_path) -> None:
    text = _run_speed_bounds_overlay(
        tmp_path,
        {
            "speed_bounds_decider_lowest_speed_mps": 0.2,
            "speed_bounds_decider_enable_nudge_slowdown": False,
        },
    )

    assert "lowest_speed: 0.200" in text
    assert "enable_nudge_slowdown: false" in text
    assert "max_centric_acceleration_limit: 0.8" in text
    assert "total_time: 7.0" in text
    assert "boundary_buffer: 0.25" in text


def test_speed_bounds_overlay_rejects_nonpositive_curve_limit() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {
                        "speed_bounds_decider_max_centric_acceleration_mps2": 0.0,
                    }
                }
            }
        }
    )

    try:
        backend._speed_bounds_decider_overlay_shell()
    except ValueError as exc:
        assert "must be positive" in str(exc)
    else:
        raise AssertionError("nonpositive curve acceleration limit must be rejected")


def test_control_lon_overlay_preserves_base_pid_config(tmp_path) -> None:
    src = tmp_path / "controller_conf.pb.txt"
    src.write_text(
        "ts: 0.01\n"
        "preview_window: 20.0\n"
        "use_speed_itfc: false\n"
        "station_pid_conf {\n"
        "  kp: 0.2\n"
        "}\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.pb.txt"
    backend = CyberRTBackend({"algo": {"apollo": {"control_lon": {"enabled": True}}}})
    script = _extract_python_overlay_script(backend._control_lon_overlay_shell())
    script = script.replace(
        "'/apollo/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt.carla_testbed.bak'",
        repr(str(tmp_path / "missing-backup.pb.txt")),
    )
    script = script.replace(
        "'/apollo/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'",
        repr(str(tmp_path / "missing-share.pb.txt")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'",
        repr(str(tmp_path / "missing-src.pb.txt")),
    )
    script = script.replace(
        "p=Path('/apollo_workspace/conf_overlay/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'); ",
        f"p=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "use_speed_itfc: true" in text
    assert "use_speed_itfc: false" not in text
    assert "ts: 0.01" in text
    assert "preview_window: 20.0" in text
    assert "station_pid_conf" in text
    assert text.count("use_speed_itfc:") == 1


def test_control_lon_overlay_refuses_partial_config_without_positive_ts(tmp_path) -> None:
    src = tmp_path / "controller_conf.pb.txt"
    src.write_text("use_speed_itfc: false\n", encoding="utf-8")
    out = tmp_path / "out.pb.txt"
    backend = CyberRTBackend({"algo": {"apollo": {"control_lon": {"enabled": True}}}})
    script = _extract_python_overlay_script(backend._control_lon_overlay_shell())
    script = script.replace(
        "'/apollo/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt.carla_testbed.bak'",
        repr(str(tmp_path / "missing-backup.pb.txt")),
    )
    script = script.replace(
        "'/apollo/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'",
        repr(str(tmp_path / "missing-share.pb.txt")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'",
        repr(str(tmp_path / "missing-src.pb.txt")),
    )
    script = script.replace(
        "p=Path('/apollo_workspace/conf_overlay/modules/control/controllers/lon_based_pid_controller/conf/controller_conf.pb.txt'); ",
        f"p=Path({str(out)!r}); ",
    )

    completed = subprocess.run(["python3", "-c", script], text=True, capture_output=True)

    assert completed.returncode != 0
    assert "positive ts" in completed.stderr
    assert not out.exists()


def test_control_lqr_overlay_keeps_query_relative_time_configurable(tmp_path) -> None:
    out = tmp_path / "controller_conf.pb.txt"
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "control_lqr": {
                        "enabled": True,
                        "query_relative_time": 0.18,
                        "query_time_nearest_point_only": True,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._control_lqr_overlay_shell())
    script = script.replace(
        "p=Path('/apollo_workspace/conf_overlay/modules/control/controllers/"
        "lat_based_lqr_controller/conf/controller_conf.pb.txt'); ",
        f"p=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "query_relative_time: 0.1800" in text
    assert "query_time_nearest_point_only: true" in text


def test_control_lqr_overlay_materializes_cutoff_frequency(tmp_path) -> None:
    out = tmp_path / "controller_conf.pb.txt"
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "control_lqr": {
                        "enabled": True,
                        "ts": 0.05,
                        "cutoff_freq": 2,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._control_lqr_overlay_shell())
    script = script.replace(
        "p=Path('/apollo_workspace/conf_overlay/modules/control/controllers/"
        "lat_based_lqr_controller/conf/controller_conf.pb.txt'); ",
        f"p=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "ts: 0.0500" in text
    assert "cutoff_freq: 2" in text
    assert "cutoff_freq: 10" not in text


def test_control_lqr_overlay_keeps_lookahead_back_control_configurable(tmp_path) -> None:
    out = tmp_path / "controller_conf.pb.txt"
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "control_lqr": {
                        "enabled": True,
                        "enable_look_ahead_back_control": False,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._control_lqr_overlay_shell())
    script = script.replace(
        "p=Path('/apollo_workspace/conf_overlay/modules/control/controllers/"
        "lat_based_lqr_controller/conf/controller_conf.pb.txt'); ",
        f"p=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    assert "enable_look_ahead_back_control: false" in out.read_text(encoding="utf-8")


def test_deferred_control_survival_keeps_5s_sample_when_control_exits_by_10s(tmp_path, monkeypatch) -> None:
    backend = _planning_ready_backend()
    status_rows = iter(
        [
            "1 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\n",
            "1 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\n",
            "",
        ]
    )
    monkeypatch.setattr("tbio.backends.cyberrt.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        backend,
        "_docker_exec",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=next(status_rows),
            stderr="",
        ),
    )
    monkeypatch.setattr(
        backend,
        "_read_stats",
        lambda: (
            True,
            0.1,
            {
                "planning_nonempty_trajectory_count": 4,
                "planning_messages_received": 5,
                "routing_success_count": 1,
            },
        ),
    )

    survival = backend._docker_probe_deferred_control_survival(
        tmp_path,
        initial_control_running=True,
    )

    assert survival["control_survived_5s"] is True
    assert survival["control_survived_10s"] is False
    assert survival["control_present_at_end"] is False
    assert [sample["control_present"] for sample in survival["samples"]] == [True, True, False]
