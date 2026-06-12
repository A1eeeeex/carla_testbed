from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.routing_response_decoded import decode_routing_response_payload


def test_decodes_nested_routing_response_payload() -> None:
    report = decode_routing_response_payload(
        {
            "source": "/apollo/routing_response",
            "road_segments": [
                {
                    "road_id": "road_15",
                    "passages": [
                        {
                            "segments": [
                                {"lane_id": "15_1_1", "start_s": 10.0, "end_s": 30.0},
                                {"lane_id": "15_1_1", "start_s": 30.0, "end_s": 50.0},
                            ]
                        }
                    ],
                }
            ],
        }
    )

    assert report["status"] == "pass"
    assert report["total_length_m"] == 40.0
    assert report["lane_segment_count"] == 2
    assert report["lane_sequence_signature"] == ["15_1_1"]
    assert report["lane_segments"][0]["lane_window_signature"] == "15_1_1@10.0000->30.0000"
    assert report["lane_window_signature"] == "15_1_1@10.0000->30.0000 | 15_1_1@30.0000->50.0000"
    assert report["unique_lane_signature"] == "15_1_1"


def test_missing_lane_segments_is_insufficient_data() -> None:
    report = decode_routing_response_payload({})

    assert report["status"] == "insufficient_data"
    assert "lane_segments" in report["missing_fields"]
    assert "total_length_m" in report["missing_fields"]


def test_cli_writes_routing_response_decoded_report(tmp_path: Path) -> None:
    input_path = tmp_path / "routing_response_decoded.json"
    input_path.write_text(
        json.dumps({"lane_segments": [{"lane_id": "15_1_1", "start_s": 0.0, "end_s": 10.0}]}),
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_routing_response_decoded.py",
            "--input",
            str(input_path),
            "--out",
            str(out_dir),
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert (out_dir / "routing_response_decoded_report.json").is_file()
    assert (out_dir / "routing_response_decoded_summary.md").is_file()
