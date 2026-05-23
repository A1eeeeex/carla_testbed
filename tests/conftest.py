from __future__ import annotations

from pathlib import Path

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config
    for item in items:
        path = Path(str(item.fspath))
        if "tests/unit" in path.as_posix():
            item.add_marker(pytest.mark.unit)
