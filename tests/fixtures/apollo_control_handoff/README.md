# Apollo Control Handoff Fixtures

`full_pass/` is the reusable base fixture. `tests/test_apollo_control_handoff.py`
copies it into a temporary directory and mutates it to cover:

- process crash with tcmalloc invalid free
- planning ready but `/apollo/control` missing
- control channel present but bridge receive missing
- bridge receive present but raw decode missing
- mapped command present but CARLA apply missing
- applied control present but vehicle response missing

Keeping one base fixture avoids stale duplicated JSON while preserving each
failure mode as a deterministic test case.
