from __future__ import annotations

import json

from carla_testbed.analysis.phase1_scenario_catalog import analyze_phase1_scenario_catalog


def test_phase1_catalog_lists_p0_p1_readiness_without_claiming_done() -> None:
    report = analyze_phase1_scenario_catalog(".")

    scenarios = {item["scenario"]: item for item in report["scenarios"]}

    assert scenarios["follow_stop_static"]["case_yaml_status"] == "DONE"
    assert scenarios["follow_stop_static"]["target_actor_status"] == "resolved"
    assert scenarios["follow_stop_static"]["overall_status"] == "PARTIAL"
    assert scenarios["follow_stop_static"]["template_status"] == "DONE"
    assert scenarios["follow_stop_static"]["status"] == scenarios["follow_stop_static"]["overall_status"]
    assert "missing" in scenarios["follow_stop_static"]
    assert "apollo_online_evidence" in scenarios["follow_stop_static"]["missing"]
    assert all("path" in item and "evidence_type" in item for item in scenarios["follow_stop_static"]["evidence"])
    assert scenarios["lead_hard_brake"]["overall_status"] == "NOT_YET"
    assert "scenario_case_yaml" in scenarios["lead_hard_brake"]["missing_items"]
    assert scenarios["cut_in_simple"]["target_actor_status"] == "resolved"
    assert scenarios["cut_in_simple"]["overall_status"] != "DONE"


def test_phase1_catalog_ingests_explicit_evidence_root_without_overclaiming(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_builtin_follow_stop"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "analysis" / "v_t_gap").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "phase1_builtin_follow_stop",
                "scenario_id": "baguang_follow_stop_static_300m",
                "scenario_class": "follow_stop_static",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps({"status": "success", "scenario_id": "baguang_follow_stop_static_300m"}),
        encoding="utf-8",
    )
    (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass"}),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["carla_online_status"] == "DONE"
    assert follow_stop["v_t_gap_readiness"] == "DONE"
    assert follow_stop["comparison_readiness"] == "NOT_YET"
    assert follow_stop["overall_status"] == "PARTIAL"
    assert any(item["evidence_type"] == "CARLA_online" for item in follow_stop["evidence"])
