from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from algo.adapters.apollo import ApolloAdapter


class ApolloAdapterMapDirTests(unittest.TestCase):
    def test_finds_application_core_map_data_under_apollo_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            apollo_root = Path(tmp) / "Apollo10.0"
            expected = apollo_root / "application-core" / "data" / "map_data" / "straight_road_for_baguang"
            expected.mkdir(parents=True)
            (expected / "base_map.txt").write_text("map {}\n", encoding="utf-8")

            actual = ApolloAdapter()._apollo_map_dir(apollo_root, "straight_road_for_baguang")

            self.assertEqual(expected.resolve(), actual)


if __name__ == "__main__":
    unittest.main()
