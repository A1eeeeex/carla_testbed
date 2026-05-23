from __future__ import annotations

from pathlib import Path

from carla_testbed.core.run_context import RunContext
from carla_testbed.core.time import FrameStamp, is_monotonic


def test_run_context_create_creates_output_dir(tmp_path: Path) -> None:
    output_dir = tmp_path / "runs" / "case"

    ctx = RunContext.create(
        run_id="case",
        output_dir=output_dir,
        config_path="configs/examples/smoke.yaml",
        resolved_config_path=tmp_path / "effective.yaml",
        git_sha="abc123",
        metadata={"profile": "smoke"},
        start_wall_time_s=12.5,
    )

    assert output_dir.is_dir()
    assert ctx.run_id == "case"
    assert ctx.output_dir == output_dir
    assert ctx.config_path == Path("configs/examples/smoke.yaml")
    assert ctx.resolved_config_path == tmp_path / "effective.yaml"
    assert ctx.git_sha == "abc123"
    assert ctx.metadata == {"profile": "smoke"}
    assert ctx.start_wall_time_s == 12.5


def test_frame_stamp_fields_and_monotonic_helper() -> None:
    first = FrameStamp(frame_id=1, sim_time_s=0.05, wall_time_s=1.0)
    second = FrameStamp(frame_id=2, sim_time_s=0.10, wall_time_s=1.1)

    assert first.frame_id == 1
    assert first.sim_time_s == 0.05
    assert first.wall_time_s == 1.0
    assert is_monotonic([first, second])
    assert not is_monotonic([second, first])
