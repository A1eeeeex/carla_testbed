__all__ = [
    "carla_client",
    "CarlaClientManager",
    "CarlaWorldBringupResult",
    "connect_world_with_retry",
    "create_client_with_retry",
    "configure_synchronous_mode",
    "get_world_with_retry",
    "generate_opendrive_world_with_retry",
    "load_world_with_retry",
    "restore_settings",
    "tick_world",
    "WorldHandle",
    "spawn_with_retry",
]


_LAZY_EXPORTS = {
    "CarlaClientManager": (".carla_client", "CarlaClientManager"),
    "CarlaWorldBringupResult": (".bringup", "CarlaWorldBringupResult"),
    "connect_world_with_retry": (".bringup", "connect_world_with_retry"),
    "create_client_with_retry": (".bringup", "create_client_with_retry"),
    "get_world_with_retry": (".bringup", "get_world_with_retry"),
    "generate_opendrive_world_with_retry": (".bringup", "generate_opendrive_world_with_retry"),
    "load_world_with_retry": (".bringup", "load_world_with_retry"),
    "configure_synchronous_mode": (".tick", "configure_synchronous_mode"),
    "restore_settings": (".tick", "restore_settings"),
    "tick_world": (".tick", "tick_world"),
    "WorldHandle": (".world", "WorldHandle"),
    "spawn_with_retry": (".actors", "spawn_with_retry"),
}

_LAZY_MODULES = {
    "carla_client": ".carla_client",
}


def __getattr__(name: str):
    import importlib

    if name in _LAZY_MODULES:
        module = importlib.import_module(_LAZY_MODULES[name], __name__)
        globals()[name] = module
        return module
    try:
        module_name, attr_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc

    module = importlib.import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
