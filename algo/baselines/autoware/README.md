# Autoware Direct (Mode-2)

Autoware connects directly to CARLA via `autoware_carla_interface` running inside the provided container compose.

## How it works
- `io/backends/autoware_direct.py` generates `sensor_mapping.yaml` and `sensor_kit_calibration.yaml` under `config/autoware_carla_interface` using the selected rig + contract.
- Docker compose (`docker/compose.yaml`) launches Autoware on host networking and sources ROS2 + Autoware before starting the interface.
- ROS2 topics (/clock + sensing) come from Autoware; control is published by Autoware back to CARLA.

## Run
```
python io/scripts/run.py --profile io/contract/profiles/autoware_direct.yaml
```
then tail logs:
```
docker compose -f algo/baselines/autoware/docker/compose.yaml logs -f
```

## Notes
- Generated mapping/calibration files are overwritten on each `run.py` execution.
- Adjust image or launch command inside `docker/compose.yaml` if you use a different Autoware build.
- Set `USE_SIM_TIME=true` in your algorithm nodes when consuming Autoware topics.
