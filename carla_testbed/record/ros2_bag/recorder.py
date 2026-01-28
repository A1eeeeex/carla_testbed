from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import List, Optional


class Ros2BagRecorder:
    def __init__(
        self,
        out_path: Path,
        topics: List[str],
        storage: str = "sqlite3",
        compress: str = "none",
        include_tf: bool = True,
        include_clock: bool = True,
        max_size_mb: Optional[int] = None,
        max_duration_s: Optional[int] = None,
        env: Optional[dict] = None,
        log_path: Optional[Path] = None,
    ):
        self.out_path = Path(out_path)
        self.topics = [t for t in topics if t]
        self.storage = storage
        self.compress = compress
        self.include_tf = include_tf
        self.include_clock = include_clock
        self.max_size_mb = max_size_mb
        self.max_duration_s = max_duration_s
        self.env = env or os.environ.copy()
        self.log_path = log_path
        self.process: Optional[subprocess.Popen] = None
        self.command: List[str] = []

    def build_command(self) -> List[str]:
        cmd = ["ros2", "bag", "record"]
        if self.storage:
            cmd += ["--storage", self.storage]
        if self.compress and self.compress != "none":
            cmd += ["--compression-mode", "file", "--compression-format", self.compress]
        if self.max_size_mb:
            cmd += ["--max-bag-size", str(int(self.max_size_mb) * 1024 * 1024)]
        if self.max_duration_s:
            cmd += ["--max-bag-duration", str(int(self.max_duration_s))]
        if self.include_tf:
            cmd += ["/tf", "/tf_static"]
        if self.include_clock:
            cmd += ["/clock"]
        cmd += self.topics
        cmd += ["-o", str(self.out_path)]
        return cmd

    def start(self):
        if not self.topics:
            raise RuntimeError("No topics specified for rosbag recording.")
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self.command = self.build_command()
        log_handle = None
        if self.log_path:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(self.log_path, "w")
        try:
            self.process = subprocess.Popen(
                self.command,
                stdout=log_handle or subprocess.PIPE,
                stderr=log_handle or subprocess.STDOUT,
                env=self.env,
            )
        except FileNotFoundError:
            if log_handle:
                log_handle.write("ros2 command not found; ensure ROS2 environment is sourced.\n")
                log_handle.flush()
            raise
        except Exception:
            if log_handle:
                log_handle.close()
            raise

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
        finally:
            self.process = None

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None
