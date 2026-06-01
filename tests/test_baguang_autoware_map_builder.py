from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET

from carla_testbed.autoware.baguang_map_builder import (
    _carla_to_autoware_local_xy,
    _lane_point_to_autoware_local_xy,
    build_baguang_autoware_map,
    parse_baguang_xodr,
)
from carla_testbed.autoware.lanelet_speed import analyze_lanelet_speed_metadata


def _xodr(tmp_path: Path) -> Path:
    path = tmp_path / "straight_road_for_baguang.xodr"
    path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <road name="Road 0" length="400.0" id="0" junction="-1">
    <type s="0" type="town"><speed max="40" unit="mph"/></type>
    <planView><geometry s="0" x="300" y="-0.37" hdg="3.141592653589793" length="400"><line/></geometry></planView>
    <lanes>
      <laneSection s="0" singleSide="false">
        <left>
          <lane id="2" type="driving" level="false"><width sOffset="0" a="3.75" b="0" c="0" d="0"/><userData><vectorLane travelDir="backward"/></userData></lane>
          <lane id="1" type="driving" level="false"><width sOffset="0" a="3.75" b="0" c="0" d="0"/><userData><vectorLane travelDir="backward"/></userData></lane>
        </left>
        <center><lane id="0" type="none" level="false"/></center>
        <right>
          <lane id="-1" type="driving" level="false"><width sOffset="0" a="3.75" b="0" c="0" d="0"/><userData><vectorLane travelDir="forward"/></userData></lane>
          <lane id="-2" type="driving" level="false"><width sOffset="0" a="3.75" b="0" c="0" d="0"/><userData><vectorLane travelDir="forward"/></userData></lane>
        </right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
        encoding="utf-8",
    )
    return path


def test_parse_baguang_xodr_extracts_four_driving_lanes(tmp_path: Path) -> None:
    geometry = parse_baguang_xodr(_xodr(tmp_path))

    assert geometry.length_m == 400.0
    assert geometry.source_speed_mph == 40.0
    assert {lane.lane_id for lane in geometry.lanes} == {-2, -1, 1, 2}
    assert {lane.travel_dir for lane in geometry.lanes} == {"forward", "backward"}


def test_baguang_lane_minus_two_matches_runtime_autoware_y_sign(tmp_path: Path) -> None:
    geometry = parse_baguang_xodr(_xodr(tmp_path))
    lane = next(item for item in geometry.lanes if item.lane_id == -2)
    center_t = (lane.inner_t + lane.outer_t) / 2.0

    _, autoware_y = _carla_to_autoware_local_xy(geometry, 0.0, center_t)

    assert autoware_y > 0.0

    _, aligned_y = _lane_point_to_autoware_local_xy(geometry, lane, 0.0, center_t)
    assert abs(aligned_y - 5.25499963760376) < 1e-6


def test_build_baguang_autoware_map_writes_lanelet2_pcd_and_projector(tmp_path: Path) -> None:
    out_root = tmp_path / "maps"

    report = build_baguang_autoware_map(
        xodr_path=_xodr(tmp_path),
        out_root=out_root,
        speed_limit_kph=80.0,
        sample_step_m=100.0,
    )

    osm_path = out_root / "vector_maps" / "lanelet2" / "straight_road_for_baguang.osm"
    pcd_path = out_root / "point_cloud_maps" / "straight_road_for_baguang.pcd"
    projector_path = out_root / "map_projector_info" / "straight_road_for_baguang.yaml"
    assert report["status"] == "pass"
    assert report["lanelet_count"] == 4
    assert report["lane_y_alignment"]["applied"] is True
    assert report["explicit_centerline"]["applied"] is True
    assert osm_path.exists()
    assert pcd_path.exists()
    assert projector_path.read_text(encoding="utf-8") == "projector_type: Local\n"
    assert "output_speed_limit_differs_from_xodr_speed" in " ".join(report["warnings"])

    root = ET.parse(osm_path).getroot()
    assert len(root.findall("relation")) == 4
    for lanelet in root.findall("relation"):
        member_roles = {member.attrib["role"] for member in lanelet.findall("member")}
        assert {"left", "right", "centerline"} <= member_roles
    assert "local_x" in osm_path.read_text(encoding="utf-8")
    assert "POINTS" in pcd_path.read_text(encoding="utf-8")
    lane_minus_two = next(
        rel
        for rel in root.findall("relation")
        if any(tag.attrib.get("k") == "carla_lane_id" and tag.attrib.get("v") == "-2" for tag in rel.findall("tag"))
    )
    way_by_id = {way.attrib["id"]: way for way in root.findall("way")}
    node_by_id = {node.attrib["id"]: node for node in root.findall("node")}
    members = {member.attrib["role"]: member.attrib["ref"] for member in lane_minus_two.findall("member")}
    first_left = node_by_id[way_by_id[members["left"]].findall("nd")[0].attrib["ref"]]
    first_right = node_by_id[way_by_id[members["right"]].findall("nd")[0].attrib["ref"]]
    first_center = node_by_id[way_by_id[members["centerline"]].findall("nd")[0].attrib["ref"]]
    left_y = float(next(tag.attrib["v"] for tag in first_left.findall("tag") if tag.attrib["k"] == "local_y"))
    right_y = float(next(tag.attrib["v"] for tag in first_right.findall("tag") if tag.attrib["k"] == "local_y"))
    center_y = float(next(tag.attrib["v"] for tag in first_center.findall("tag") if tag.attrib["k"] == "local_y"))
    assert min(left_y, right_y) < 5.25 < max(left_y, right_y)
    assert left_y < right_y
    assert abs(center_y - 5.25499963760376) < 1e-6

    speed = analyze_lanelet_speed_metadata(osm_path, requested_max_vel_mps=80 / 3.6)
    assert speed["status"] == "pass"
    assert speed["lanelet2_speed_limit_tag_count"] == 4
