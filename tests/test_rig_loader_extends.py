from __future__ import annotations

from pathlib import Path

from carla_testbed.config.rig_loader import load_rig_file
from tbio.scripts.run import _load_config_with_extends


def test_load_rig_file_extends_deep_merges_parent(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    base.write_text(
        "\n".join(
            [
                "run:",
                "  ticks: 700",
                "  profile_name: base",
                "algo:",
                "  apollo:",
                "    bridge:",
                "      publish_rate_hz: 20.0",
                "      routing:",
                "        send_lane_follow: true",
                "        send_routing_request: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    child.write_text(
        "\n".join(
            [
                "extends: base.yaml",
                "run:",
                "  profile_name: child",
                "algo:",
                "  apollo:",
                "    bridge:",
                "      routing:",
                "        send_lane_follow: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = load_rig_file(str(child))

    assert cfg["run"]["ticks"] == 700
    assert cfg["run"]["profile_name"] == "child"
    routing = cfg["algo"]["apollo"]["bridge"]["routing"]
    assert routing["send_lane_follow"] is False
    assert routing["send_routing_request"] is True


def test_legacy_online_runner_config_extends_deep_merges_parent(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    base.write_text(
        "\n".join(
            [
                "algo:",
                "  stack: apollo",
                "  apollo:",
                "    routing:",
                "      send_lane_follow: true",
                "      send_routing_request: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    child.write_text(
        "\n".join(
            [
                "extends: base.yaml",
                "algo:",
                "  apollo:",
                "    routing:",
                "      send_lane_follow: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = _load_config_with_extends(child)

    assert cfg["algo"]["stack"] == "apollo"
    assert cfg["algo"]["apollo"]["routing"]["send_lane_follow"] is False
    assert cfg["algo"]["apollo"]["routing"]["send_routing_request"] is True
