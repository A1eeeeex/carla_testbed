# tbio/

`tbio` is the runtime integration layer between the simulator harness and external stacks.

What lives here:

- `backends/`: stack adapters used by `carla_testbed run` (`ros2_native`, `autoware_direct`, `cyberrt`).
- `carla/`: CARLA process launch policy and lifecycle helpers.
- `contract/`: run artifact generation from IO contract schema.
- `ros2/`: ROS2 publishers, probes, topic tools, and control logging.
- `scripts/`: command entrypoints used by `carla_testbed.cli`.

How it fits:

1. `carla_testbed/runner` drives world ticks.
2. A backend from `tbio/backends` is started.
3. Backend bridges runtime data to the selected stack.
4. `tbio/contract` and `carla_testbed/record` write artifacts under `runs/<run>/`.

Start here for stack bring-up:

- `tbio/scripts/run.py`
- `tbio/backends/base.py`

