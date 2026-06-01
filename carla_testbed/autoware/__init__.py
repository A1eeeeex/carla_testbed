"""Autoware integration helpers.

This package intentionally keeps runtime-specific ROS2/Autoware imports out of
module import time. Helpers here are mostly asset/schema utilities that can run
in CI without a live Autoware or CARLA process.
"""
