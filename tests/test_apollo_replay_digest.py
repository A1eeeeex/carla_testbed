from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from carla_testbed.algorithms.replay_digest import (
    ReplayDigestError,
    compare_replay_digest,
    load_replay_digest,
    write_replay_comparison_report,
)

GOLDEN_PATH = Path("tests/fixtures/apollo/replay_digest_golden.json")
CANDIDATE_PATH = Path("tests/fixtures/apollo/replay_digest_candidate.json")


def test_candidate_within_tolerance_passes() -> None:
    golden = load_replay_digest(GOLDEN_PATH)
    candidate = load_replay_digest(CANDIDATE_PATH)

    comparison = compare_replay_digest(golden, candidate, None)

    assert comparison.status == "pass"
    assert comparison.report["missing_metrics"] == []
    assert comparison.report["tolerance_failures"] == []


def test_missing_control_fails() -> None:
    golden = load_replay_digest(GOLDEN_PATH)
    candidate = load_replay_digest(CANDIDATE_PATH)
    candidate = deepcopy(candidate)
    candidate.pop("control")

    comparison = compare_replay_digest(golden, candidate, None)

    assert comparison.status == "fail"
    assert any(item.startswith("candidate:control must be a mapping") for item in comparison.report["missing_metrics"])


def test_planning_hz_zero_fails() -> None:
    golden = load_replay_digest(GOLDEN_PATH)
    candidate = load_replay_digest(CANDIDATE_PATH)
    candidate = deepcopy(candidate)
    candidate["planning"]["hz"] = 0.0

    comparison = compare_replay_digest(golden, candidate, None)

    assert comparison.status == "fail"
    assert "planning.hz" in comparison.report["tolerance_failures"]
    assert comparison.report["metric_diffs"]["planning.hz"]["status"] == "fail"


def test_steer_abs_p95_huge_diff_fails() -> None:
    golden = load_replay_digest(GOLDEN_PATH)
    candidate = load_replay_digest(CANDIDATE_PATH)
    candidate = deepcopy(candidate)
    candidate["control"]["steer_abs_p95"] = 0.9

    comparison = compare_replay_digest(golden, candidate, None)

    assert comparison.status == "fail"
    assert "control.steer_abs_p95" in comparison.report["tolerance_failures"]


def test_steer_abs_p95_medium_diff_warns() -> None:
    golden = load_replay_digest(GOLDEN_PATH)
    candidate = load_replay_digest(CANDIDATE_PATH)
    candidate = deepcopy(candidate)
    candidate["control"]["steer_abs_p95"] = 0.4

    comparison = compare_replay_digest(golden, candidate, None)

    assert comparison.status == "warn"
    assert comparison.report["metric_diffs"]["control.steer_abs_p95"]["status"] == "warn"


def test_report_writer_creates_output_files(tmp_path: Path) -> None:
    golden = load_replay_digest(GOLDEN_PATH)
    candidate = load_replay_digest(CANDIDATE_PATH)
    comparison = compare_replay_digest(golden, candidate, None)

    outputs = write_replay_comparison_report(
        tmp_path,
        comparison,
        golden_path=GOLDEN_PATH,
        candidate_path=CANDIDATE_PATH,
    )

    assert Path(outputs["report"]).exists()
    assert Path(outputs["golden_digest"]).exists()
    assert Path(outputs["candidate_digest"]).exists()


def test_loader_rejects_bad_digest(tmp_path: Path) -> None:
    path = tmp_path / "bad_digest.json"
    path.write_text('{"schema_version": "wrong"}', encoding="utf-8")

    with pytest.raises(ReplayDigestError):
        load_replay_digest(path)
