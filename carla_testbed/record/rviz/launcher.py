from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional
import copy

from .gen_rviz_from_rig import generate_rviz_from_rig


class RvizLauncher:
    def __init__(
        self,
        rig_path: Path,
        ego_id: str,
        domain_id: int,
        mode: str = "docker",
        docker_image: str = "carla_testbed_rviz:humble",
        camera_suffix: str = "image",
        lidar_suffix: str = "point_cloud",
    ):
        self.rig_path = Path(rig_path)
        self.ego_id = ego_id
        self.domain_id = domain_id
        self.mode = mode
        self.docker_image = docker_image
        self.camera_suffix = camera_suffix
        self.lidar_suffix = lidar_suffix
        self.base_dir = Path(__file__).resolve().parent
        self.rviz_dir = self.base_dir / "rviz"
        self.process: Optional[subprocess.Popen] = None
        self._started = False
        self._xauth_path = Path("/tmp/.docker.xauth")
        stem = self.rig_path.stem
        self.rig_name = stem[:-5] if stem.endswith("_rviz") else stem

    def start(self):
        if self._started:
            return
        print("[RViz] ensure CARLA server started with --ros2; launching RViz for native topics...")
        rviz_cfg_path = self.rviz_dir / f"native_{self.rig_name}.rviz"
        try:
            generate_rviz_from_rig(
                rig_path=self.rig_path,
                ego_id=self.ego_id,
                out_path=rviz_cfg_path,
                camera_image_suffix=self.camera_suffix,
                lidar_cloud_suffix=self.lidar_suffix,
            )
        except Exception as exc:
            print(f"[WARN] failed to generate RViz config: {exc}")
            return
        if self.mode == "local":
            self._start_local(rviz_cfg_path)
            return
        if self.mode != "docker":
            print(f"[WARN] RViz mode {self.mode} unsupported; skip launching.")
            return
        if not shutil.which("docker"):
            print("[WARN] docker not found; skip launching RViz.")
            return
        display = os.environ.get("DISPLAY")
        if not display:
            print("[WARN] DISPLAY not set; cannot forward X11, skipping RViz.")
            return
        if os.environ.get("XDG_SESSION_TYPE") == "wayland":
            print("[WARN] Wayland session detected; X11 forwarding required for RViz docker. Skipping RViz.")
            return
        if not self._prepare_xauth(display):
            print("[WARN] failed to prepare X11 xauth; skip launching RViz.")
            return
        if not self._ensure_docker_image():
            print(f"[WARN] docker image {self.docker_image} unavailable and build failed; try --rviz-mode local if rviz2 is installed.")
            return

        cmd = [
            "docker",
            "run",
            "--rm",
            "--net=host",
            "-e",
            f"DISPLAY={display}",
            "-e",
            f"XAUTHORITY={self._xauth_path}",
            "-e",
            "FASTRTPS_DEFAULT_PROFILES_FILE=/config/fastrtps-profile.xml",
            "-e",
            "RMW_IMPLEMENTATION=rmw_fastrtps_cpp",
            "-e",
            f"ROS_DOMAIN_ID={self.domain_id}",
            "-v",
            "/tmp/.X11-unix:/tmp/.X11-unix:rw",
            "-v",
            f"{self._xauth_path}:{self._xauth_path}:rw",
            "-v",
            f"{self.base_dir / 'config'}:/config:ro",
            "-v",
            f"{self.rviz_dir}:/rviz:rw",
            self.docker_image,
            "ros2",
            "run",
            "rviz2",
            "rviz2",
            "-d",
            f"/rviz/{rviz_cfg_path.name}",
        ]
        try:
            self.process = subprocess.Popen(cmd)
            self._started = True
        except Exception as exc:
            print(f"[WARN] failed to launch RViz docker: {exc}")

    def _start_local(self, rviz_cfg_path: Path):
        if not shutil.which("rviz2"):
            print("[WARN] rviz2 not found in PATH; cannot launch local RViz.")
            return
        display = os.environ.get("DISPLAY")
        if not display:
            print("[WARN] DISPLAY not set; cannot launch local RViz.")
            return
        env = copy.deepcopy(os.environ)
        profile_path = (self.base_dir / "config" / "fastrtps-profile.xml").resolve()
        if profile_path.exists():
            env.setdefault("FASTRTPS_DEFAULT_PROFILES_FILE", str(profile_path))
        env.setdefault("RMW_IMPLEMENTATION", "rmw_fastrtps_cpp")
        env["ROS_DOMAIN_ID"] = str(self.domain_id)
        # Avoid conda/other packages overriding Qt plugin path (cv2 ships its own plugins)
        env.pop("QT_QPA_PLATFORM_PLUGIN_PATH", None)
        env.pop("QT_PLUGIN_PATH", None)
        cmd = ["rviz2", "-d", str(rviz_cfg_path)]
        try:
            self.process = subprocess.Popen(cmd, env=env)
            self._started = True
        except Exception as exc:
            print(f"[WARN] failed to launch local RViz: {exc}")

    def _prepare_xauth(self, display: str) -> bool:
        try:
            nlist = subprocess.run(
                ["xauth", "nlist", display],
                check=False,
                capture_output=True,
                text=True,
            )
            if nlist.returncode != 0 or not nlist.stdout:
                return False
            lines = []
            for line in nlist.stdout.splitlines():
                if not line:
                    continue
                lines.append("ffff" + line[4:] if len(line) > 4 else line)
            fixed = "\n".join(lines)
            merge = subprocess.run(
                ["xauth", "-f", str(self._xauth_path), "nmerge", "-"],
                input=fixed,
                text=True,
                check=False,
                capture_output=True,
            )
            return merge.returncode == 0
        except Exception:
            return False

    def _ensure_docker_image(self) -> bool:
        try:
            res = subprocess.run(["docker", "images", "-q", self.docker_image], capture_output=True, text=True, check=False)
            if res.stdout.strip():
                return True
            print(f"[RViz] building docker image {self.docker_image} ...")
            build = subprocess.run(
                ["docker", "build", "-t", self.docker_image, "-f", str(self.base_dir / "docker" / "Dockerfile"), str(self.base_dir)],
                check=False,
            )
            return build.returncode == 0
        except Exception as exc:
            print(f"[WARN] failed to check/build docker image: {exc}")
            return False

    def stop(self):
        if self.process is None:
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
        except Exception:
            try:
                self.process.kill()
            except Exception:
                pass
        self.process = None
        self._started = False
        if self._xauth_path.exists():
            try:
                self._xauth_path.unlink()
            except Exception:
                pass
