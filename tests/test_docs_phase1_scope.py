from __future__ import annotations

from pathlib import Path


def test_agents_and_phase1_brief_declare_multi_backend_scope() -> None:
    agents = Path("AGENTS.md").read_text(encoding="utf-8")
    brief = Path("docs/phase1_engineering_brief.md").read_text(encoding="utf-8")
    combined = agents + "\n" + brief

    assert "Phase 1" in agents
    assert "multi-backend" in combined or "multi-algorithm" in combined
    assert "Apollo-only" in combined
    assert "PlanningControlBackend" in brief
    assert "carla_builtin" in brief


def test_phase1_docs_define_invalid_not_backend_failure() -> None:
    brief = Path("docs/phase1_engineering_brief.md").read_text(encoding="utf-8")

    assert "backend_not_ready" in brief
    assert "missing_required_artifact" in brief
    assert "missing_target_actor" in brief
    assert "invalid run must not count as a backend loss" in brief


def test_readme_recommends_phase1_p0_matrix_before_legacy_town01_suite() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    phase1_index = readme.index("phase1 run-p0-matrix")
    legacy_index = readme.index("town01_natural_driving.platform.yaml")
    assert phase1_index < legacy_index
    assert "not the Phase 1 five-P0 pair matrix" in readme
