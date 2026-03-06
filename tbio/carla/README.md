# tbio/carla/

CARLA process startup and launch policy helpers.

Main files:

- `launcher.py`: starts/stops CARLA server process.
- `launch_policy.py`: startup policy options (reuse existing server or spawn new).

Used by:

- `tbio/scripts/run.py` before harness start.

Typical migration concern:

- Ensure host CARLA binary path is valid (`CARLA_ROOT` or config override).
- Match launch args with your environment (`--ros2`, offscreen, GPU).

