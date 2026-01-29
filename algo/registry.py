from __future__ import annotations

from typing import Dict

from algo.adapters.autoware import AutowareAdapter
from algo.adapters.dummy import DummyAdapter
from algo.adapters.e2e import E2EAdapter
from algo.adapters.apollo import ApolloAdapter

_REGISTRY = {
    "autoware": AutowareAdapter,
    "dummy": DummyAdapter,
    "e2e": E2EAdapter,
    "apollo": ApolloAdapter,
}


def get_adapter(name: str):
    key = (name or "").lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown adapter '{name}'")
    return _REGISTRY[key]()
