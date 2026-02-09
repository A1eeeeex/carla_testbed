from __future__ import annotations

import os
import shlex
import socket
import subprocess
import time
from pathlib import Path
from typing import Dict, Optional

import carla


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


def _client_ok(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        client = carla.Client(host, port)
        client.set_timeout(timeout)
        client.get_server_version()
        return True
    except Exception:
        return False


class CarlaLauncher:
    def __init__(
        self,
        carla_root: Path,
        host: str,
        port: int,
        town: str,
        extra_args: str,
        foreground: bool,
        run_dir: Path,
        env_overrides: Optional[Dict[str, str]] = None,
    ):
        self.carla_root = Path(carla_root)
        self.host = host
        self.port = int(port)
        self.town = town
        self.extra_args = extra_args or ""
        self.foreground = foreground
        self.run_dir = Path(run_dir)
        self.log_dir = self.run_dir / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.proc: Optional[subprocess.Popen] = None
        self.reused = False
        self.env_overrides = env_overrides or {}
        self._server_log = self.log_dir / "carla_server.log"
        self._ue_log = self.carla_root / "CarlaUE4" / "Saved" / "Logs" / "CarlaUE4.log"
        self._ue_tail = self.log_dir / "carlaue4_tail.log"

    def get_logs_paths(self) -> Dict[str, Path]:
        return {
            "server_log": self._server_log,
            "ue_log": self._ue_log,
            "ue_tail": self._ue_tail,
        }

    def _build_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(self.env_overrides)
        has_display = bool(env.get("DISPLAY") or env.get("WAYLAND_DISPLAY"))
        if "SDL_VIDEODRIVER" in self.env_overrides:
            env["SDL_VIDEODRIVER"] = self.env_overrides["SDL_VIDEODRIVER"]
        elif not has_display:
            env["SDL_VIDEODRIVER"] = "offscreen"
        env.setdefault("SDL_AUDIODRIVER", "dummy")
        shim_path = Path(__file__).resolve().parents[1] / "tools" / "libudev_shim.so"
        if shim_path.exists():
            env["LD_PRELOAD"] = f"{shim_path}:{env.get('LD_PRELOAD', '')}"
            print(f"[carla] LD_PRELOAD -> {env['LD_PRELOAD']}")
        udev_lib = Path(__file__).resolve().parents[1] / "tools" / "libudev.so.1"
        if udev_lib.exists():
            env["LD_LIBRARY_PATH"] = f"{udev_lib.parent}:{env.get('LD_LIBRARY_PATH', '')}"
            print(f"[carla] LD_LIBRARY_PATH -> {env['LD_LIBRARY_PATH']}")
        return env

    def _command(self) -> [str]:
        env = os.environ.copy()
        has_display = bool(env.get("DISPLAY") or env.get("WAYLAND_DISPLAY"))
        cmd = [
            "bash",
            str(self.carla_root / "CarlaUE4.sh"),
            f"-carla-rpc-port={self.port}",
            "-stdout",
            "-FullStdOutLogOutput",
            f"-carla-map={self.town}",
        ]
        extras = shlex.split(self.extra_args) if self.extra_args else []
        wants_offscreen = any(x == "-RenderOffScreen" for x in extras)
        if (not has_display) and (not wants_offscreen):
            cmd.append("-RenderOffScreen")
        cmd.extend(extras)
        return cmd

    def start(self) -> None:
        if _client_ok(self.host, self.port):
            self.reused = True
            print(f"[carla] detected running CARLA at {self.host}:{self.port}, reuse")
            return

        if _is_port_open(self.host, self.port):
            print(f"[carla][ERROR] port {self.port} is in use but not CARLA; please change port or stop the process")
            self._print_port_diag()
            raise RuntimeError("port occupied")

        cmd = self._command()
        env = self._build_env()
        self._server_log.parent.mkdir(parents=True, exist_ok=True)

        print(f"[carla] launching: {' '.join(cmd)}")
        print(f"[carla] log -> {self._server_log}")
        print(f"[carla] DISPLAY={env.get('DISPLAY')} WAYLAND_DISPLAY={env.get('WAYLAND_DISPLAY')}")
        print(f"[carla] RenderOffScreen in cmd: {any(x == '-RenderOffScreen' for x in cmd)}")
        if self.foreground:
            tee_cmd = [
                "bash",
                "-lc",
                f"{' '.join(shlex.quote(x) for x in cmd)} 2>&1 | tee -a {shlex.quote(str(self._server_log))}",
            ]
            self.proc = subprocess.Popen(tee_cmd, cwd=self.carla_root, env=env)
        else:
            self._log_handle = open(self._server_log, "a", buffering=1)
            self.proc = subprocess.Popen(cmd, cwd=self.carla_root, env=env, stdout=self._log_handle, stderr=subprocess.STDOUT)
        print(f"[carla] pid={self.proc.pid}")

    def wait_ready(self, timeout_s: float = 180.0, poll_s: float = 1.0) -> bool:
        end = time.time() + timeout_s
        while time.time() < end:
            if self.proc and self.proc.poll() is not None and not self.reused:
                print(f"[carla][ERROR] process exited early code={self.proc.returncode}")
                print(self.diagnose_tail())
                return False
            if _client_ok(self.host, self.port, timeout=1.0):
                return True
            time.sleep(poll_s)
        if not self.reused:
            print(self.diagnose_tail())
        return False

    def stop(self, grace_s: float = 5.0):
        if self.reused or not self.proc:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=grace_s)
            except Exception:
                self.proc.kill()
        if hasattr(self, "_log_handle") and getattr(self, "_log_handle", None):
            try:
                self._log_handle.close()
            except Exception:
                pass

    def _print_port_diag(self):
        try:
            out = subprocess.check_output(["ss", "-ltnp"], text=True)
            filtered = "\n".join([line for line in out.splitlines() if f":{self.port}" in line])
            print("[carla] ss -ltnp | grep port:")
            print(filtered or "(no match)")
        except Exception as exc:  # pragma: no cover
            print(f"[carla] ss failed: {exc}")

        try:
            out = subprocess.check_output(["pgrep", "-af", "CarlaUE4|CarlaUE4-Linux-Shipping"], text=True)
            print("[carla] pgrep CarlaUE4:")
            print(out.strip() or "(no match)")
        except subprocess.CalledProcessError:
            print("[carla] pgrep CarlaUE4: no match")
        except Exception as exc:
            print(f"[carla] pgrep error: {exc}")

    def diagnose_tail(self, n: int = 120) -> str:
        lines = []
        if self._ue_log.exists():
            try:
                data = self._ue_log.read_text(errors="replace").splitlines()
                tail = data[-n:] if len(data) > n else data
                text = "\n".join(tail)
                self._ue_tail.write_text(text)
                lines.append(f"[carla] CarlaUE4.log tail ({len(tail)} lines):")
                lines.append(text)
            except Exception as exc:
                lines.append(f"[carla] failed to read {self._ue_log}: {exc}")
        else:
            lines.append(f"[carla] UE log not found: {self._ue_log}")

        self._print_port_diag()
        return "\n".join(lines)
