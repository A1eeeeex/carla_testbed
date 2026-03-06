# algo/adapters/

Stack adapters used by the algorithm registry.

Files:

- `dummy.py`: no external stack backend.
- `autoware.py`: Autoware integration adapter.
- `apollo.py`: Apollo integration adapter.
- `e2e.py`: placeholder adapter (not implemented yet).

Adapter responsibilities:

1. Parse stack-specific config block.
2. Build the backend profile consumed by `tbio`.
3. Expose `start/healthcheck/stop` through a common interface.

