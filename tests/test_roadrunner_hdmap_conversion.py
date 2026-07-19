from pathlib import Path
import xml.etree.ElementTree as ET

from carla_testbed.apollo.roadrunner_hdmap import (
    LaneYLine,
    apply_lane_y_alignment_lines,
    apply_lane_y_shifts,
    convert_roadrunner_apollo_xml_to_metric,
    set_apollo_xml_lane_speed_limits,
    summarize_text_map,
)


def test_roadrunner_apollo_xml_coordinates_are_converted_to_metric(tmp_path: Path) -> None:
    source = tmp_path / "source.xml"
    reference = tmp_path / "reference.xml"
    output = tmp_path / "output.xml"

    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header revMajor="6" revMinor="7" name="" version="1" date="x"
          north="0.001" south="-0.001" east="0.002" west="-0.002"
          vendor="MathWorks">
    <geoReference>+proj=longlat +datum=WGS84 +no_defs</geoReference>
  </header>
  <road id="0" junction="-1">
    <lanes>
      <laneSection>
        <center><lane id="0" uid="0_0_0" type="none"><border><geometry sOffset="0" x="0.0" y="0.0" length="1"><pointSet><point x="0.0" y="0.0"/></pointSet></geometry></border></lane></center>
        <right><lane id="-1" uid="0_0_-1" type="driving" turnType="noTurn" direction="forward"><centerLine><geometry sOffset="0" x="0.002" y="-0.001" length="1"><pointSet><point x="0.002" y="-0.001"/></pointSet></geometry></centerLine><border><geometry sOffset="0" x="0.002" y="-0.001" length="1"><pointSet><point x="0.002" y="-0.001"/></pointSet></geometry></border><sampleAssociates><sampleAssociate sOffset="0" leftWidth="1" rightWidth="1"/></sampleAssociates><leftRoadSampleAssociations><sampleAssociation sOffset="0" width="1"/></leftRoadSampleAssociations><rightRoadSampleAssociations><sampleAssociation sOffset="0" width="1"/></rightRoadSampleAssociations></lane></right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
    )
    reference.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header revMajor="1" revMinor="5" name="" version="1" date="x"
          north="100.0" south="-100.0" east="200.0" west="-200.0"
          vendor="MathWorks"><geoReference>metric</geoReference></header>
</OpenDRIVE>
""",
    )

    scale = convert_roadrunner_apollo_xml_to_metric(source, reference, output, zone_id="31")

    root = ET.parse(output).getroot()
    header = root.find("header")
    assert header is not None
    assert scale.x_scale == 100000.0
    assert scale.y_scale == 100000.0
    assert header.find("projection/utm").attrib["zoneID"] == "31"
    assert "+proj=utm" in header.findtext("geoReference", "")
    point = root.find(".//right/lane/centerLine/geometry/pointSet/point")
    assert point is not None
    assert float(point.attrib["x"]) == 200.0
    assert float(point.attrib["y"]) == -100.0


def test_roadrunner_apollo_xml_can_reverse_driving_lane_points(tmp_path: Path) -> None:
    source = tmp_path / "source.xml"
    reference = tmp_path / "reference.xml"
    output = tmp_path / "output.xml"

    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header revMajor="6" revMinor="7" name="" version="1" date="x"
          north="1.0" south="-1.0" east="1.0" west="-1.0"
          vendor="MathWorks">
    <geoReference>+proj=longlat +datum=WGS84 +no_defs</geoReference>
  </header>
  <road id="0" junction="-1">
    <lanes>
      <laneSection>
        <right>
          <lane id="-1" uid="0_0_-1" type="driving" turnType="noTurn" direction="forward">
            <centerLine>
              <geometry sOffset="0" x="1.0" y="0.0" length="2">
                <pointSet>
                  <point x="1.0" y="0.0"/>
                  <point x="2.0" y="0.0"/>
                  <point x="3.0" y="0.0"/>
                </pointSet>
              </geometry>
            </centerLine>
            <border>
              <geometry sOffset="0" x="1.0" y="1.0" length="2">
                <pointSet>
                  <point x="1.0" y="1.0"/>
                  <point x="2.0" y="1.0"/>
                </pointSet>
              </geometry>
            </border>
          </lane>
        </right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
    )
    reference.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header revMajor="1" revMinor="5" name="" version="1" date="x"
          north="1.0" south="-1.0" east="1.0" west="-1.0"
          vendor="MathWorks"><geoReference>metric</geoReference></header>
</OpenDRIVE>
""",
    )

    convert_roadrunner_apollo_xml_to_metric(
        source,
        reference,
        output,
        reverse_driving_lane_points=True,
    )

    root = ET.parse(output).getroot()
    points = root.findall(".//right/lane/centerLine/geometry/pointSet/point")
    assert [float(point.attrib["x"]) for point in points] == [3.0, 2.0, 1.0]
    geometry = root.find(".//right/lane/centerLine/geometry")
    assert geometry is not None
    assert float(geometry.attrib["x"]) == 3.0


def test_roadrunner_apollo_xml_speed_is_written_in_parser_kph_units(tmp_path: Path) -> None:
    root = ET.fromstring(
        """<OpenDRIVE><road><lanes><laneSection><right>
        <lane id="-1" type="driving"><speed min="17.88" max="17.88"/></lane>
        <lane id="-2" type="driving"/>
        <lane id="0" type="none"/>
        </right></laneSection></lanes></road></OpenDRIVE>"""
    )

    updated = set_apollo_xml_lane_speed_limits(root, 23.61)

    assert updated == 2
    speeds = root.findall(".//lane[@type='driving']/speed")
    assert [float(speed.attrib["max"]) for speed in speeds] == [84.996, 84.996]
    assert root.find(".//lane[@type='none']/speed") is None


def test_roadrunner_apollo_xml_speed_rejects_nonpositive_value() -> None:
    root = ET.fromstring('<OpenDRIVE><lane type="driving"/></OpenDRIVE>')

    try:
        set_apollo_xml_lane_speed_limits(root, 0.0)
    except ValueError as exc:
        assert str(exc) == "lane_speed_limit_mps must be positive"
    else:
        raise AssertionError("expected nonpositive speed limit to be rejected")


def test_text_map_summary_separates_driving_lane_speed_limits(tmp_path: Path) -> None:
    map_path = tmp_path / "base_map.txt"
    map_path.write_text(
        """lane {
  id { id: "driving" }
  type: CITY_DRIVING
  speed_limit: 23.61
}
lane {
  id { id: "shoulder" }
  type: SHOULDER
  speed_limit: 4.967098765432098
}
""",
        encoding="utf-8",
    )

    summary = summarize_text_map(map_path)

    assert summary["speed_limits_mps"] == [4.967098765432098, 23.61]
    assert summary["driving_lane_speed_limits_mps"] == [23.61]


def test_roadrunner_apollo_xml_can_extend_driving_lane_ends(tmp_path: Path) -> None:
    source = tmp_path / "source.xml"
    reference = tmp_path / "reference.xml"
    output = tmp_path / "output.xml"

    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header revMajor="6" revMinor="7" name="" version="1" date="x"
          north="1.0" south="-1.0" east="1.0" west="-1.0"
          vendor="MathWorks">
    <geoReference>+proj=longlat +datum=WGS84 +no_defs</geoReference>
  </header>
  <road id="0" junction="-1">
    <lanes>
      <laneSection>
        <right>
          <lane id="-1" uid="0_0_-1" type="driving" turnType="noTurn" direction="forward">
            <centerLine>
              <geometry sOffset="0" x="0.0" y="0.0" length="10">
                <pointSet>
                  <point x="0.0" y="0.0"/>
                  <point x="10.0" y="0.0"/>
                </pointSet>
              </geometry>
            </centerLine>
          </lane>
        </right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
    )
    reference.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header revMajor="1" revMinor="5" name="" version="1" date="x"
          north="1.0" south="-1.0" east="1.0" west="-1.0"
          vendor="MathWorks"><geoReference>metric</geoReference></header>
</OpenDRIVE>
""",
    )

    convert_roadrunner_apollo_xml_to_metric(
        source,
        reference,
        output,
        extend_driving_lane_ends_m=2.0,
    )

    root = ET.parse(output).getroot()
    points = root.findall(".//right/lane/centerLine/geometry/pointSet/point")
    assert [float(point.attrib["x"]) for point in points] == [-2.0, 0.0, 10.0, 12.0]
    geometry = root.find(".//right/lane/centerLine/geometry")
    assert geometry is not None
    assert float(geometry.attrib["x"]) == -2.0
    assert float(geometry.attrib["length"]) == 14.0


def test_roadrunner_apollo_xml_densifies_long_lane_extensions(tmp_path: Path) -> None:
    source = tmp_path / "source.xml"
    reference = tmp_path / "reference.xml"
    output = tmp_path / "output.xml"

    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE>
  <header north="1.0" south="-1.0" east="1.0" west="-1.0" />
  <road id="0" junction="-1">
    <lanes><laneSection><right>
      <lane id="-1" uid="0_0_-1" type="driving">
        <centerLine><geometry x="0.0" y="0.0" length="10"><pointSet>
          <point x="0.0" y="0.0"/><point x="10.0" y="0.0"/>
        </pointSet></geometry></centerLine>
      </lane>
    </right></laneSection></lanes>
  </road>
</OpenDRIVE>
""",
        encoding="utf-8",
    )
    reference.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<OpenDRIVE><header north="1.0" south="-1.0" east="1.0" west="-1.0" /></OpenDRIVE>
""",
        encoding="utf-8",
    )

    convert_roadrunner_apollo_xml_to_metric(
        source,
        reference,
        output,
        extend_driving_lane_ends_m=120.0,
    )

    points = ET.parse(output).getroot().findall(
        ".//right/lane/centerLine/geometry/pointSet/point"
    )
    xs = [float(point.attrib["x"]) for point in points]
    assert xs[0] == -120.0
    assert xs[-1] == 130.0
    assert max(right - left for left, right in zip(xs, xs[1:])) <= 20.0


def test_apply_lane_y_shifts_only_changes_selected_lane_geometry() -> None:
    root = ET.fromstring(
        """<OpenDRIVE>
  <road id="0">
    <lanes>
      <laneSection>
        <left>
          <lane id="2" uid="0_0_2" type="driving">
            <centerLine>
              <geometry x="1.0" y="-6.0">
                <pointSet><point x="1.0" y="-6.0"/></pointSet>
              </geometry>
            </centerLine>
            <border>
              <geometry x="1.0" y="-7.0">
                <pointSet><point x="1.0" y="-7.0"/></pointSet>
              </geometry>
            </border>
          </lane>
          <lane id="1" uid="0_0_1" type="driving">
            <centerLine>
              <geometry x="1.0" y="-2.0">
                <pointSet><point x="1.0" y="-2.0"/></pointSet>
              </geometry>
            </centerLine>
          </lane>
        </left>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>"""
    )

    applied = apply_lane_y_shifts(root, {"0_0_2": 0.74})

    assert applied == {"0_0_2": 0.74}
    shifted_geometry = root.find(".//lane[@uid='0_0_2']/centerLine/geometry")
    shifted_point = root.find(".//lane[@uid='0_0_2']/centerLine/geometry/pointSet/point")
    shifted_border = root.find(".//lane[@uid='0_0_2']/border/geometry/pointSet/point")
    unchanged_point = root.find(".//lane[@uid='0_0_1']/centerLine/geometry/pointSet/point")
    assert shifted_geometry is not None
    assert shifted_point is not None
    assert shifted_border is not None
    assert unchanged_point is not None
    assert float(shifted_geometry.attrib["y"]) == -5.26
    assert float(shifted_point.attrib["y"]) == -5.26
    assert float(shifted_border.attrib["y"]) == -6.26
    assert float(unchanged_point.attrib["y"]) == -2.0


def test_apply_lane_y_alignment_lines_matches_target_centerline_slope() -> None:
    root = ET.fromstring(
        """<OpenDRIVE>
  <road id="0">
    <lanes>
      <laneSection>
        <left>
          <lane id="2" uid="0_0_2" type="driving">
            <centerLine>
              <geometry x="100.0" y="-6.0">
                <pointSet>
                  <point x="100.0" y="-6.0"/>
                  <point x="0.0" y="-6.2"/>
                </pointSet>
              </geometry>
            </centerLine>
            <border>
              <geometry x="50.0" y="-7.1">
                <pointSet><point x="50.0" y="-7.1"/></pointSet>
              </geometry>
            </border>
          </lane>
        </left>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>"""
    )

    applied = apply_lane_y_alignment_lines(root, {"0_0_2": LaneYLine(100.0, -5.0, 0.0, -4.0)})

    assert "0_0_2" in applied
    points = root.findall(".//lane[@uid='0_0_2']/centerLine/geometry/pointSet/point")
    border_point = root.find(".//lane[@uid='0_0_2']/border/geometry/pointSet/point")
    assert [round(float(point.attrib["y"]), 6) for point in points] == [-5.0, -4.0]
    assert border_point is not None
    # At x=50, source centerline y=-6.1 and target y=-4.5, so border gets +1.6m.
    assert round(float(border_point.attrib["y"]), 6) == -5.5
