from __future__ import annotations

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
