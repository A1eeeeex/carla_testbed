from setuptools import setup, find_packages

setup(
    name="carla_control_bridge",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["rclpy", "ackermann_msgs", "carla"],
    entry_points={
        "console_scripts": [
            "carla_control_bridge = carla_control_bridge.ros2_autoware_to_carla:main",
        ]
    },
)
