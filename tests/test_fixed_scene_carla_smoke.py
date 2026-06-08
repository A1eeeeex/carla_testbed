from __future__ import annotations

import ast
from pathlib import Path

from carla_testbed.scenario_player.carla_smoke import spawn_index_from_ref


def test_spawn_index_from_ref_parses_town_spawn_label() -> None:
    assert spawn_index_from_ref("spawn_097") == 97
    assert spawn_index_from_ref("097") == 97
    assert spawn_index_from_ref(None) == 0
    assert spawn_index_from_ref("hero") == 0


def test_run_fixed_scene_carla_smoke_tool_stays_thin() -> None:
    path = Path("tools/run_fixed_scene_carla_smoke.py")
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)

    assert len(text.splitlines()) < 120
    assert "import carla" not in text
    assert not any(isinstance(node, ast.ClassDef) for node in ast.walk(tree))
    assert "run_fixed_scene_carla_smoke" in text
