from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_lane_event_contract import (
    analyze_baguang_lane_event_contract,
    parse_xodr_lane_contracts,
    write_baguang_lane_event_contract_report,
)


def _write_xodr(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" standalone="yes"?>
<OpenDRIVE>
  <road name="straight" length="400" id="0" junction="-1">
    <planView><geometry s="0" x="300" y="-5.25" hdg="3.14159" length="400"><line /></geometry></planView>
    <lanes>
      <laneSection s="0">
        <left>
          <lane id="1" type="driving" level="false">
            <width sOffset="0" a="3.75" b="0" c="0" d="0" />
            <roadMark sOffset="0" type="broken" weight="standard" color="white" width="0.1" laneChange="both" />
            <userData><vectorLane travelDir="backward" /></userData>
          </lane>
        </left>
        <center>
          <lane id="0" type="none" level="false">
            <roadMark sOffset="0" type="solid solid" weight="standard" color="yellow" width="0.1" laneChange="none" />
          </lane>
        </center>
        <right>
          <lane id="-1" type="driving" level="false">
            <width sOffset="0" a="3.75" b="0" c="0" d="0" />
            <roadMark sOffset="0" type="broken" weight="standard" color="white" width="0.1" laneChange="both" />
            <userData><vectorLane travelDir="forward" /></userData>
          </lane>
          <lane id="-2" type="driving" level="false">
            <width sOffset="0" a="3.75" b="0" c="0" d="0" />
            <roadMark sOffset="0" type="solid" weight="standard" color="yellow" width="0.1" laneChange="none" />
            <userData><vectorLane travelDir="forward" /></userData>
          </lane>
        </right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
        encoding="utf-8",
    )


def _write_run(
    root: Path,
    *,
    cross_track_error: float,
    heading_error: float = 0.001,
    distance_x: float = 0.9,
    event_type: str = "lane_invasion",
) -> None:
    root.mkdir(parents=True)
    (root / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "fail_reason": "LANE_INVASION",
                "exit_reason": "LANE_INVASION",
                "frames": 2,
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "events.jsonl").write_text(
        json.dumps({"event_type": event_type, "frame": 100, "step": 1, "t": 0.1}) + "\n",
        encoding="utf-8",
    )
    with (root / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame",
                "sim_time",
                "ego_x",
                "ego_y",
                "ego_speed",
                "cross_track_error",
                "heading_error",
                "lane_invasion_count",
                "collision_count",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "frame": 99,
                "sim_time": 0.0,
                "ego_x": 299.7,
                "ego_y": -5.25,
                "ego_speed": 0.0,
                "cross_track_error": 0.0,
                "heading_error": 0.0,
                "lane_invasion_count": 0,
                "collision_count": 0,
            }
        )
        writer.writerow(
            {
                "frame": 100,
                "sim_time": 0.1,
                "ego_x": 299.7 - distance_x,
                "ego_y": -5.25,
                "ego_speed": 1.0,
                "cross_track_error": cross_track_error,
                "heading_error": heading_error,
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        )


def test_parse_xodr_lane_contracts(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    _write_xodr(xodr)

    lanes = parse_xodr_lane_contracts(xodr)
    target = next(lane for lane in lanes if lane.lane_id == -2)

    assert target.lane_type == "driving"
    assert target.width_m == 3.75
    assert target.road_marks[0].mark_type == "solid"
    assert target.travel_dir == "forward"


def test_low_cte_lane_invasion_recommends_quarantine(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert report["status"] == "warn"
    assert report["quarantine_recommended"] is True
    assert report["claim_boundary"]["lane_invasion_event_can_be_used_as_hard_gate"] is False
    assert run_report["reason"] == "lane_invasion_trigger_inconsistent_with_centerline_evidence"
    assert run_report["static_crossing_check"]["trigger_geometrically_implausible"] is True
    assert run_report["static_crossing_check"]["estimated_center_offset_to_cross_mark_m"] == 0.825


def test_high_cte_lane_invasion_is_not_quarantined(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=1.2, distance_x=40.0)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])

    assert report["status"] == "fail"
    assert report["quarantine_recommended"] is False
    assert report["claim_boundary"]["lane_invasion_event_can_be_used_as_hard_gate"] is True


def test_writer_and_cli_create_report_files(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    outputs = write_baguang_lane_event_contract_report(report, out)

    assert Path(outputs["report"]).exists()
    assert Path(outputs["summary"]).exists()
    assert "Claim Boundary" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_lane_event_contract.py",
            "--xodr",
            str(xodr),
            "--run-dir",
            str(run),
            "--out",
            str(cli_out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert '"status": "warn"' in result.stdout
    assert (cli_out / "baguang_lane_event_contract_report.json").exists()
    assert (cli_out / "baguang_lane_event_contract_summary.md").exists()
