from pathlib import Path

from carla_testbed.autoware.lanelet_speed import (
    analyze_lanelet_speed_metadata,
    parse_speed_value,
    write_lanelet_speed_limit_variant,
    write_lanelet_speed_report,
)


def test_parse_speed_value_supports_common_units():
    assert round(parse_speed_value("50").mps, 6) == round(50 / 3.6, 6)
    assert round(parse_speed_value("80 km/h").mps, 6) == round(80 / 3.6, 6)
    assert round(parse_speed_value("22.22 m/s").mps, 6) == 22.22
    assert round(parse_speed_value("30 mph").mps, 6) == round(30 * 0.44704, 6)


def test_lanelet_speed_metadata_detects_missing_speed_tags(tmp_path):
    osm = tmp_path / "Town01.osm"
    osm.write_text(
        """
        <osm>
          <node id="1" lat="0" lon="0"/>
          <way id="10"><nd ref="1"/><tag k="type" v="line_thin"/></way>
        </osm>
        """,
        encoding="utf-8",
    )

    report = analyze_lanelet_speed_metadata(osm, requested_max_vel_mps=22.22)

    assert report["status"] == "warn"
    assert report["missing_speed_metadata"] is True
    assert report["speed_tag_count"] == 0
    assert "speed_metadata_missing" in report["warnings"]
    assert "missing_speed_tags_with_default_urban_speed_candidate" in report["warnings"]


def test_lanelet_speed_metadata_reports_explicit_speed(tmp_path):
    osm = tmp_path / "Town01.osm"
    osm.write_text(
        """
        <osm>
          <way id="10">
            <tag k="type" v="lanelet"/>
            <tag k="speed_limit" v="80 km/h"/>
          </way>
        </osm>
        """,
        encoding="utf-8",
    )

    report = analyze_lanelet_speed_metadata(osm, requested_max_vel_mps=22.22)

    assert report["status"] == "pass"
    assert report["missing_speed_metadata"] is False
    assert report["speed_tag_count"] == 1
    assert report["lanelet2_speed_limit_tag_count"] == 1
    assert round(report["speed_value_stats_mps"]["max"], 6) == round(80 / 3.6, 6)


def test_lanelet_speed_metadata_warns_when_only_maxspeed_is_present(tmp_path):
    osm = tmp_path / "Town01.osm"
    osm.write_text(
        """
        <osm>
          <relation id="1">
            <tag k="type" v="lanelet"/>
            <tag k="subtype" v="road"/>
            <tag k="maxspeed" v="80"/>
          </relation>
        </osm>
        """,
        encoding="utf-8",
    )

    report = analyze_lanelet_speed_metadata(osm, requested_max_vel_mps=22.22)

    assert report["status"] == "warn"
    assert report["speed_tag_count"] == 1
    assert report["lanelet2_speed_limit_tag_count"] == 0
    assert "maxspeed_tag_may_not_affect_lanelet2_german_vehicle_speed_limit" in report["warnings"]


def test_lanelet_speed_metadata_warns_when_map_speed_below_requested(tmp_path):
    osm = tmp_path / "Town01.osm"
    osm.write_text(
        """
        <osm>
          <way id="10">
            <tag k="type" v="lanelet"/>
            <tag k="maxspeed" v="50"/>
          </way>
        </osm>
        """,
        encoding="utf-8",
    )

    report = analyze_lanelet_speed_metadata(osm, requested_max_vel_mps=22.22)

    assert report["status"] == "warn"
    assert "map_speed_below_requested_max_candidate" in report["warnings"]


def test_write_lanelet_speed_limit_variant_patches_road_lanelet_relations(tmp_path):
    src = tmp_path / "Town01.osm"
    dst = tmp_path / "Town01_speed80.osm"
    src.write_text(
        """
        <osm>
          <relation id="1">
            <member type="way" ref="10" role="left"/>
            <member type="way" ref="11" role="right"/>
            <tag k="subtype" v="road"/>
            <tag k="type" v="lanelet"/>
          </relation>
          <relation id="2">
            <tag k="subtype" v="crosswalk"/>
            <tag k="type" v="lanelet"/>
          </relation>
        </osm>
        """,
        encoding="utf-8",
    )

    patch_report = write_lanelet_speed_limit_variant(src, dst, speed_limit="80")
    analysis_report = analyze_lanelet_speed_metadata(dst, requested_max_vel_mps=22.22)
    text = dst.read_text(encoding="utf-8")

    assert patch_report["status"] == "pass"
    assert patch_report["lanelet_relations_seen"] == 1
    assert patch_report["lanelet_relations_modified"] == 1
    assert 'k="speed_limit" v="80"' in text
    assert analysis_report["lanelet2_speed_limit_tag_count"] == 1
    assert analysis_report["status"] == "pass"


def test_lanelet_speed_report_writer_creates_json_and_markdown(tmp_path):
    report = analyze_lanelet_speed_metadata(Path("does-not-exist.osm"))

    paths = write_lanelet_speed_report(report, tmp_path / "out")

    assert Path(paths["json"]).exists()
    assert Path(paths["markdown"]).exists()
    assert "autoware_lanelet_speed_report.v1" in Path(paths["json"]).read_text()
