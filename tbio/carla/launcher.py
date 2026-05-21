from __future__ import annotations

import os
import sys
import re
import shlex
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


_CARLA_MODULE = None


def _load_carla_module():
    global _CARLA_MODULE
    if _CARLA_MODULE is not None:
        return _CARLA_MODULE
    try:
        import carla as _carla  # type: ignore
    except ModuleNotFoundError as exc:
        carla_root = (
            os.environ.get("CARLA_ROOT")
            or os.environ.get("TB_CARLA_ROOT")
            or "/home/ubuntu/CARLA_0.9.16"
        )
        dist_dir = Path(carla_root) / "PythonAPI" / "carla" / "dist"
        py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
        candidates = sorted(dist_dir.glob(f"carla-*-{py_tag}-*.whl"))
        for candidate in reversed(candidates):
            sys.path.insert(0, str(candidate))
            try:
                import carla as _carla  # type: ignore
                _CARLA_MODULE = _carla
                return _CARLA_MODULE
            except ModuleNotFoundError:
                sys.path.pop(0)
        available = ", ".join(item.name for item in sorted(dist_dir.glob("carla-*.whl")))
        raise ModuleNotFoundError(
            "Failed to import CARLA Python API. "
            f"Current interpreter is cp{sys.version_info.major}{sys.version_info.minor}; "
            f"searched wheels under {dist_dir} and found [{available}]. "
            "Run the launcher under a CARLA-compatible interpreter, for example `conda run -n carla16 ...`, "
            "or provide a matching wheel via CARLA_ROOT."
        ) from exc
    _CARLA_MODULE = _carla
    return _CARLA_MODULE


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


def _client_ok(host: str, port: int, timeout: float = 1.0, require_world: bool = False) -> bool:
    try:
        carla = _load_carla_module()
        client = carla.Client(host, port)
        client.set_timeout(timeout)
        client.get_server_version()
        if require_world:
            world = client.get_world()
            _ = world.get_map().name
        return True
    except Exception:
        return False


def _tail_contains_stream_eof(lines: list[str]) -> bool:
    joined = "\n".join(str(item) for item in lines).lower()
    return "error retrieving stream id" in joined or "failed to read header" in joined


def _tail_contains_request_exit(lines: list[str]) -> bool:
    joined = "\n".join(str(item) for item in lines).lower()
    return "requestexitwithstatus" in joined or "request exit" in joined


def _tail_contains_sig11(lines: list[str]) -> bool:
    joined = "\n".join(str(item) for item in lines).lower()
    return "signal 11" in joined


def _pid_rss_mb(pid: int) -> Optional[float]:
    try:
        for line in Path(f"/proc/{int(pid)}/status").read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.startswith("VmRSS:"):
                continue
            parts = line.split()
            if len(parts) < 2:
                return None
            kb = float(parts[1])
            return round(kb / 1024.0, 3)
    except Exception:
        return None
    return None


def _candidate_carla_ports(port: int) -> list[int]:
    base = int(port)
    return [base, base + 1, base + 2]


def _ss_lines_for_port(port: int) -> list[str]:
    try:
        out = subprocess.check_output(["ss", "-ltnp"], text=True)
    except Exception:
        return []
    return [line for line in out.splitlines() if f":{port}" in line]


def _port_owner_pid(port: int) -> Optional[int]:
    for line in _ss_lines_for_port(port):
        match = re.search(r"pid=(\d+)", line)
        if not match:
            continue
        try:
            return int(match.group(1))
        except Exception:
            return None
    return None


def _pid_cmdline(pid: int) -> str:
    try:
        raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
    except Exception:
        return ""
    return raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()


def _port_likely_owned_by_carla(port: int) -> bool:
    lines = _ss_lines_for_port(port)
    if any("CarlaUE4" in line or "CarlaUE4-Linux" in line for line in lines):
        return True
    pid = _port_owner_pid(port)
    if pid is None:
        return False
    cmdline = _pid_cmdline(pid)
    return "CarlaUE4" in cmdline or "CarlaUE4-Linux-Shipping" in cmdline


def _carla_pid_on_port(port: int) -> Optional[int]:
    pid = _port_owner_pid(port)
    if pid is None:
        return None
    return pid if _port_likely_owned_by_carla(port) else None


def _carla_pids_matching_port_arg(port: int) -> list[int]:
    try:
        out = subprocess.check_output(["pgrep", "-af", "CarlaUE4"], text=True)
    except subprocess.CalledProcessError:
        return []
    except Exception:
        return []
    needle = f"-carla-rpc-port={int(port)}"
    matched: list[int] = []
    for line in out.splitlines():
        if needle not in line:
            continue
        parts = line.strip().split(None, 1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except Exception:
            continue
        matched.append(pid)
    return sorted(set(matched))


def _terminate_pid(pid: int, grace_s: float = 5.0) -> bool:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except Exception:
        return False
    deadline = time.time() + grace_s
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except Exception:
            break
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except Exception:
        return False
    deadline = time.time() + max(1.0, grace_s / 2.0)
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except Exception:
            break
        time.sleep(0.1)
    return False


class CarlaLauncher:
    def __init__(
        self,
        carla_root: Path,
        host: str,
        port: int,
        town: str | None,
        extra_args: str,
        foreground: bool,
        run_dir: Path,
        stop_reused_on_exit: bool = True,
        env_overrides: Optional[Dict[str, str]] = None,
        force_fresh_start: bool = False,
        enable_auto_recovery: bool = True,
        bootstrap_stability_window_s: float = 0.0,
        use_systemd_scope_memory_guard: bool = False,
        memory_high: str = "9G",
        memory_max: str = "10G",
        oom_policy: str = "kill",
    ):
        self.carla_root = Path(carla_root)
        self.host = host
        self.port = int(port)
        self.town = str(town or "").strip()
        self.extra_args = extra_args or ""
        self.foreground = foreground
        self.stop_reused_on_exit = bool(stop_reused_on_exit)
        self.force_fresh_start = bool(force_fresh_start)
        self.enable_auto_recovery = bool(enable_auto_recovery)
        self.bootstrap_stability_window_s = max(0.0, float(bootstrap_stability_window_s or 0.0))
        self.use_systemd_scope_memory_guard = bool(use_systemd_scope_memory_guard)
        self.memory_high = str(memory_high or "").strip() or "9G"
        self.memory_max = str(memory_max or "").strip() or "10G"
        self.oom_policy = str(oom_policy or "").strip() or "kill"
        self.run_dir = Path(run_dir)
        self.log_dir = self.run_dir / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.proc: Optional[subprocess.Popen] = None
        self.reused = False
        self.env_overrides = env_overrides or {}
        self._server_log = self.log_dir / "carla_server.log"
        self._ue_log = self.carla_root / "CarlaUE4" / "Saved" / "Logs" / "CarlaUE4.log"
        self._ue_tail = self.log_dir / "carlaue4_tail.log"
        self._reused_pid: Optional[int] = None
        self._render_offscreen_forced = False
        self._render_offscreen_retry_used = False
        self._lowres_low_quality_recovery_applied = False
        self._lowres_low_quality_retry_used = False
        self._monitorless_windowed_recovery_applied = False
        self._vulkan_oom_windowed_recovery_applied = False
        self._vulkan_oom_windowed_retry_used = False
        self._launch_records: List[Dict[str, Any]] = []
        self._display_probe: Optional[Dict[str, Any]] = None
        self._launch_started_at: Optional[float] = None
        self._systemd_scope_unit: Optional[str] = None

    def _server_tail_flags(self, launch_record: Optional[Dict[str, Any]] = None) -> Dict[str, bool]:
        tail: list[str] = []
        if launch_record:
            raw_tail = launch_record.get("server_log_tail")
            if isinstance(raw_tail, list):
                tail = [str(item) for item in raw_tail]
        if not tail:
            tail = self._tail_file(self._server_log)
        return {
            "stream_eof": _tail_contains_stream_eof(tail),
            "request_exit": _tail_contains_request_exit(tail),
            "sig11": _tail_contains_sig11(tail),
        }

    def _can_use_systemd_scope_memory_guard(self) -> bool:
        if not self.use_systemd_scope_memory_guard:
            return False
        if not shutil.which("systemd-run"):
            return False
        return bool(os.environ.get("XDG_RUNTIME_DIR") or os.environ.get("DBUS_SESSION_BUS_ADDRESS"))

    def _wrap_with_systemd_scope(self, cmd: List[str]) -> List[str]:
        self._systemd_scope_unit = None
        if not self._can_use_systemd_scope_memory_guard():
            return list(cmd)
        unit = f"tb-carla-{self.port}-{int(time.time())}.scope"
        self._systemd_scope_unit = unit
        return [
            "systemd-run",
            "--user",
            "--scope",
            "--quiet",
            "--unit",
            unit,
            "-p",
            f"MemoryHigh={self.memory_high}",
            "-p",
            f"MemoryMax={self.memory_max}",
            "--",
            *cmd,
        ]

    def _actual_carla_pid(self) -> Optional[int]:
        pid = _carla_pid_on_port(self.port)
        if pid is not None:
            return pid
        matched = _carla_pids_matching_port_arg(self.port)
        if matched:
            return matched[0]
        if self.proc is not None and not self._systemd_scope_unit:
            return getattr(self.proc, "pid", None)
        return None

    def _startup_requests_render_offscreen(self) -> bool:
        extras = shlex.split(self.extra_args) if self.extra_args else []
        return any(str(arg) == "-RenderOffScreen" for arg in extras)

    def _monitor_count(self) -> Optional[int]:
        if not self._display_probe:
            return None
        raw_count = self._display_probe.get("monitor_count")
        if isinstance(raw_count, int):
            return raw_count
        return None

    def _monitorless_display(self) -> bool:
        return self._monitor_count() == 0

    def _command_has_lowres_low_quality(self, argv: List[str]) -> bool:
        return (
            "-windowed" in argv
            and any(str(arg).startswith("-ResX=960") for arg in argv)
            and any(str(arg).startswith("-ResY=540") for arg in argv)
            and any(str(arg).startswith("-quality-level=Low") for arg in argv)
        )

    def _command_has_render_offscreen(self, argv: List[str]) -> bool:
        return any(str(arg) == "-RenderOffScreen" for arg in argv)

    def _target_ports(self) -> list[int]:
        return _candidate_carla_ports(self.port)

    def _cleanup_stale_carla_ports(self) -> None:
        # CARLA can leave a listener behind on the RPC/streaming sidecar ports
        # even when the client handshake is no longer healthy. Clean only
        # CARLA-owned listeners in the target triplet before a fresh launch.
        stale_by_pid: Dict[int, list[int]] = {}
        for port in self._target_ports():
            pid = _carla_pid_on_port(port)
            if pid is None:
                continue
            stale_by_pid.setdefault(pid, []).append(port)
        for pid in _carla_pids_matching_port_arg(self.port):
            stale_by_pid.setdefault(pid, [])
        if not stale_by_pid:
            return
        for pid, ports in sorted(stale_by_pid.items()):
            cmdline = _pid_cmdline(pid) or "(cmdline unavailable)"
            port_list = ",".join(str(item) for item in sorted(ports)) or "none"
            print(
                "[carla][WARN] removing stale CARLA process "
                f"pid={pid} ports=[{port_list}] cmd={cmdline}"
            )
            if not _terminate_pid(pid):
                print(
                    "[carla][WARN] failed to fully remove stale CARLA process "
                    f"pid={pid}; launch may still collide on ports [{port_list}]"
                )
        time.sleep(1.0)

    def get_logs_paths(self) -> Dict[str, Path]:
        return {
            "server_log": self._server_log,
            "ue_log": self._ue_log,
            "ue_tail": self._ue_tail,
        }

    def _display_available(self) -> bool:
        return bool(
            self.env_overrides.get("DISPLAY")
            or self.env_overrides.get("WAYLAND_DISPLAY")
            or os.environ.get("DISPLAY")
            or os.environ.get("WAYLAND_DISPLAY")
        )

    def _probe_display_state(self, env: Dict[str, str]) -> Dict[str, Any]:
        if self._display_probe is not None:
            return dict(self._display_probe)
        probe: Dict[str, Any] = {
            "display": env.get("DISPLAY"),
            "wayland_display": env.get("WAYLAND_DISPLAY"),
            "xauthority": env.get("XAUTHORITY"),
        }
        if not (probe["display"] or probe["wayland_display"]):
            probe["available"] = False
            self._display_probe = probe
            return dict(probe)
        probe["available"] = True
        try:
            result = subprocess.run(
                ["bash", "-lc", "xrandr --listmonitors"],
                cwd=str(self.carla_root),
                env=env,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10.0,
            )
            lines = (result.stdout or "").splitlines()
            monitor_count: Optional[int] = None
            if lines:
                match = re.match(r"Monitors:\s+(\d+)", lines[0].strip())
                if match:
                    monitor_count = int(match.group(1))
            probe.update(
                {
                    "xrandr_rc": result.returncode,
                    "xrandr_output_head": lines[:10],
                    "monitor_count": monitor_count,
                }
            )
        except Exception as exc:
            probe["xrandr_error"] = repr(exc)
        self._display_probe = probe
        return dict(probe)

    def _build_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(self.env_overrides)
        # Keep CARLA startup away from the caller's Python/conda runtime.
        # In this repo the launcher is often invoked from the carla16 env,
        # which injects cv2/conda lib paths into LD_LIBRARY_PATH. Those paths
        # are not needed by the Unreal server and can leave the process alive
        # but never bring up the RPC port on this host.
        for key in ("PYTHONHOME", "PYTHONPATH", "VIRTUAL_ENV"):
            env.pop(key, None)
        for key in list(env.keys()):
            if key.startswith("CONDA_"):
                env.pop(key, None)
        ld_path = env.get("LD_LIBRARY_PATH", "")
        if ld_path:
            keep_parts = []
            for part in ld_path.split(":"):
                stripped = part.strip()
                if not stripped:
                    continue
                if "/miniconda" in stripped or "/site-packages/cv2" in stripped:
                    continue
                keep_parts.append(stripped)
            env["LD_LIBRARY_PATH"] = ":".join(keep_parts)
        has_display = bool(env.get("DISPLAY") or env.get("WAYLAND_DISPLAY"))
        display_probe = self._probe_display_state(env) if has_display else None
        if display_probe is not None:
            self._display_probe = dict(display_probe)
        if "SDL_VIDEODRIVER" in self.env_overrides:
            env["SDL_VIDEODRIVER"] = self.env_overrides["SDL_VIDEODRIVER"]
        elif self._render_offscreen_forced:
            env["SDL_VIDEODRIVER"] = "offscreen"
        elif not has_display:
            if self._startup_requests_render_offscreen():
                env.pop("SDL_VIDEODRIVER", None)
            else:
                env["SDL_VIDEODRIVER"] = "offscreen"
        elif not env.get("SDL_VIDEODRIVER"):
            if (display_probe or {}).get("monitor_count") == 0:
                # This host can expose a working X screen while reporting
                # `Monitors: 0`; SDL/UE then exits during LinuxApplication
                # creation unless XRandR is treated as optional.
                env["SDL_VIDEODRIVER"] = "x11"
                env.setdefault("SDL_VIDEO_X11_REQUIRE_XRANDR", "0")
        env.setdefault("SDL_AUDIODRIVER", "dummy")
        env.setdefault("MALLOC_ARENA_MAX", "2")
        shim_path = Path(__file__).resolve().parents[1] / "tools" / "libudev_shim.so"
        if shim_path.exists():
            env["LD_PRELOAD"] = f"{shim_path}:{env.get('LD_PRELOAD', '')}"
            print(f"[carla] LD_PRELOAD -> {env['LD_PRELOAD']}")
        udev_lib = Path(__file__).resolve().parents[1] / "tools" / "libudev.so.1"
        if udev_lib.exists():
            env["LD_LIBRARY_PATH"] = f"{udev_lib.parent}:{env.get('LD_LIBRARY_PATH', '')}"
            print(f"[carla] LD_LIBRARY_PATH -> {env['LD_LIBRARY_PATH']}")
        return env

    def _command(self, env: Optional[Dict[str, str]] = None) -> [str]:
        env = env or self._build_env()
        cmd = [
            "bash",
            str(self.carla_root / "CarlaUE4.sh"),
            f"-carla-rpc-port={self.port}",
            "-nosound",
            "-stdout",
            "-FullStdOutLogOutput",
        ]
        if self.town:
            cmd.append(f"-carla-map={self.town}")
        extras = shlex.split(self.extra_args) if self.extra_args else []
        has_display = bool(env.get("DISPLAY") or env.get("WAYLAND_DISPLAY"))
        # On this host, fresh CARLA can stay alive but fail to become world-ready
        # when forced into RenderOffScreen despite a usable display server.
        # Keep offscreen as the fallback for true headless launches only.
        if (
            (
                self._render_offscreen_forced
                or not has_display
                or (
                    has_display
                    and self._monitorless_display()
                    and not any(
                        arg == "-windowed"
                        or arg == "-fullscreen"
                        or arg.startswith("-ResX=")
                        or arg.startswith("-ResY=")
                        for arg in extras
                    )
                )
            )
            and "-RenderOffScreen" not in extras
        ):
            cmd.append("-RenderOffScreen")
        elif (
            has_display
            and self._lowres_low_quality_recovery_applied
            and not any(arg == "-windowed" or arg == "-fullscreen" for arg in extras)
        ):
            # When a display-path launch hangs before the RPC listener comes up,
            # retry once in a lighter windowed mode before declaring startup dead.
            extras.append("-windowed")
            if not any(arg.startswith("-ResX=") for arg in extras):
                extras.append("-ResX=960")
            if not any(arg.startswith("-ResY=") for arg in extras):
                extras.append("-ResY=540")
            if not any(arg.startswith("-quality-level=") for arg in extras):
                extras.append("-quality-level=Low")
        elif (
            has_display
            and self._vulkan_oom_windowed_recovery_applied
            and not any(arg == "-windowed" or arg == "-fullscreen" or arg.startswith("-ResX=") or arg.startswith("-ResY=") for arg in extras)
        ):
            # On this host a fresh fullscreen Vulkan launch can OOM before the
            # RPC port appears. Retry once in a smaller explicit window before
            # falling back to RenderOffScreen so online validation can still
            # reach world-ready with a usable display path.
            extras.extend(["-windowed", "-ResX=960", "-ResY=540"])
        cmd.extend(extras)
        return cmd

    def _tail_file(self, path: Path, n: int = 80) -> list[str]:
        if not path.exists():
            return []
        try:
            lines = path.read_text(errors="replace").splitlines()
        except Exception:
            return []
        return lines[-n:] if len(lines) > n else lines

    def diagnostics_snapshot(self, *, probe_rpc: bool = True) -> Dict[str, Any]:
        process_pid: Optional[int] = None
        process_alive = False
        if self.proc is not None:
            process_pid = getattr(self.proc, "pid", None)
            try:
                process_alive = self.proc.poll() is None
            except Exception:
                process_alive = False
        rpc_handshake_ready: Optional[bool]
        if probe_rpc:
            rpc_handshake_ready = bool(_client_ok(self.host, self.port, timeout=0.5, require_world=False))
        else:
            rpc_handshake_ready = None
        actual_carla_pid = self._actual_carla_pid()
        launch_elapsed = (
            max(0.0, time.time() - self._launch_started_at)
            if self._launch_started_at is not None
            else None
        )
        launch_record = self._launch_records[-1] if self._launch_records else None
        tail_flags = self._server_tail_flags(launch_record)
        return {
            "reused": bool(self.reused),
            "force_fresh_start": bool(self.force_fresh_start),
            "enable_auto_recovery": bool(self.enable_auto_recovery),
            "bootstrap_stability_window_s": self.bootstrap_stability_window_s,
            "bootstrap_stability_elapsed_s": launch_elapsed,
            "process_survived_stability_window": bool(
                launch_elapsed is not None and launch_elapsed >= self.bootstrap_stability_window_s
            ),
            "process_pid": process_pid,
            "process_alive": bool(process_alive),
            "carla_process_pid": actual_carla_pid,
            "carla_process_rss_mb": _pid_rss_mb(actual_carla_pid) if actual_carla_pid is not None else None,
            "systemd_scope_unit": self._systemd_scope_unit,
            "memory_guard_enabled": bool(self._systemd_scope_unit),
            "render_offscreen_retry_used": bool(self._render_offscreen_retry_used),
            "render_offscreen_forced": bool(self._render_offscreen_forced),
            "lowres_low_quality_recovery_applied": bool(self._lowres_low_quality_recovery_applied),
            "lowres_low_quality_retry_used": bool(self._lowres_low_quality_retry_used),
            "monitorless_windowed_recovery_applied": bool(self._monitorless_windowed_recovery_applied),
            "vulkan_oom_windowed_recovery_applied": bool(self._vulkan_oom_windowed_recovery_applied),
            "vulkan_oom_windowed_retry_used": bool(self._vulkan_oom_windowed_retry_used),
            "target_port_snapshot": [
                {
                    "port": port,
                    "open": bool(_is_port_open(self.host, port)),
                    "ss_lines": _ss_lines_for_port(port),
                }
                for port in self._target_ports()
            ],
            "rpc_handshake_ready": rpc_handshake_ready,
            "launcher_log_contains_end_of_file": tail_flags["stream_eof"],
            "launcher_log_contains_request_exit": tail_flags["request_exit"],
            "launcher_log_contains_sig11": tail_flags["sig11"],
            "latest_server_log_tail": self._tail_file(self._server_log),
            "latest_ue_log_tail": self._tail_file(self._ue_log),
            "display_probe": dict(self._display_probe) if self._display_probe else None,
            "launch_records": list(self._launch_records),
            "logs": {key: str(value) for key, value in self.get_logs_paths().items()},
        }

    def _launch_tail_contains_vulkan_oom(self, launch_record: Optional[Dict[str, Any]] = None) -> bool:
        tail: list[str] = []
        if launch_record:
            raw_tail = launch_record.get("server_log_tail")
            if isinstance(raw_tail, list):
                tail = [str(item) for item in raw_tail]
        if not tail:
            tail = self._tail_file(self._server_log)
        if not tail:
            return False
        joined = "\n".join(tail)
        lowered = joined.lower()
        return "out of memory on vulkan" in lowered or ("vulkan" in lowered and "out of memory" in lowered)

    def _launch_tail_contains_stream_eof(self, launch_record: Optional[Dict[str, Any]] = None) -> bool:
        tail: list[str] = []
        if launch_record:
            raw_tail = launch_record.get("server_log_tail")
            if isinstance(raw_tail, list):
                tail = [str(item) for item in raw_tail]
        if not tail:
            tail = self._tail_file(self._server_log)
        if not tail:
            return False
        joined = "\n".join(tail).lower()
        return "error retrieving stream id" in joined or "failed to read header" in joined

    def _restart_with_lowres_low_quality_recovery(self) -> None:
        print(
            "[carla][WARN] startup hung before RPC listener became reachable; "
            "retry once with low-res low-quality window recovery"
        )
        self._lowres_low_quality_recovery_applied = True
        self._lowres_low_quality_retry_used = True
        self.stop()
        self.start()

    def _hung_startup_retry_after_sec(self, timeout_s: float) -> float:
        return min(15.0, max(1.0, float(timeout_s) * 0.25))

    def start(self) -> None:
        if not self.force_fresh_start and _client_ok(self.host, self.port):
            self.reused = True
            self._reused_pid = _carla_pid_on_port(self.port)
            self._launch_records.append(
                {
                    "mode": "reuse",
                    "host": self.host,
                    "port": self.port,
                    "reused_pid": self._reused_pid,
                    "force_fresh_start": bool(self.force_fresh_start),
                }
            )
            print(f"[carla] detected running CARLA at {self.host}:{self.port}, reuse")
            return

        self._cleanup_stale_carla_ports()

        if not self.force_fresh_start and _client_ok(self.host, self.port):
            self.reused = True
            self._reused_pid = _carla_pid_on_port(self.port)
            print(f"[carla] recovered running CARLA at {self.host}:{self.port} after stale cleanup, reuse")
            return

        for port in self._target_ports():
            if not _is_port_open(self.host, port):
                continue
            if _port_likely_owned_by_carla(port):
                print(
                    f"[carla][ERROR] port {port} is still occupied by CARLA after cleanup; "
                    "refusing a fresh launch to avoid bind races"
                )
            else:
                print(
                    f"[carla][ERROR] port {port} is in use by a non-CARLA process; "
                    "please change port or stop the process"
                )
            self._print_port_diag()
            raise RuntimeError("port occupied")

        env = self._build_env()
        cmd = self._command(env)
        wrapped_cmd = self._wrap_with_systemd_scope(cmd)
        self._server_log.parent.mkdir(parents=True, exist_ok=True)
        launch_record: Dict[str, Any] = {
            "mode": "launch",
            "command": list(cmd),
            "wrapped_command": list(wrapped_cmd),
            "display": env.get("DISPLAY"),
            "wayland_display": env.get("WAYLAND_DISPLAY"),
            "sdl_videodriver": env.get("SDL_VIDEODRIVER"),
            "sdl_video_x11_require_xrandr": env.get("SDL_VIDEO_X11_REQUIRE_XRANDR"),
            "render_offscreen_in_cmd": any(x == "-RenderOffScreen" for x in cmd),
            "render_offscreen_forced": bool(self._render_offscreen_forced),
            "lowres_low_quality_recovery_applied": bool(self._lowres_low_quality_recovery_applied),
            "monitorless_windowed_recovery_applied": bool(self._monitorless_windowed_recovery_applied),
            "vulkan_oom_windowed_recovery_applied": bool(self._vulkan_oom_windowed_recovery_applied),
            "server_log": str(self._server_log),
            "ue_log": str(self._ue_log),
            "display_probe": dict(self._display_probe) if self._display_probe else None,
            "systemd_scope_unit": self._systemd_scope_unit,
            "memory_guard_enabled": bool(self._systemd_scope_unit),
        }
        self._launch_records.append(launch_record)
        self._launch_started_at = time.time()

        print(f"[carla] launching: {' '.join(wrapped_cmd)}")
        print(f"[carla] log -> {self._server_log}")
        print(f"[carla] DISPLAY={env.get('DISPLAY')} WAYLAND_DISPLAY={env.get('WAYLAND_DISPLAY')}")
        print(f"[carla] RenderOffScreen in cmd: {any(x == '-RenderOffScreen' for x in cmd)}")
        if self._systemd_scope_unit:
            print(
                "[carla] memory guard -> "
                f"unit={self._systemd_scope_unit} high={self.memory_high} max={self.memory_max}"
            )
        if self.foreground:
            tee_cmd = [
                "bash",
                "-lc",
                f"{' '.join(shlex.quote(x) for x in wrapped_cmd)} 2>&1 | tee -a {shlex.quote(str(self._server_log))}",
            ]
            self.proc = subprocess.Popen(tee_cmd, cwd=self.carla_root, env=env, start_new_session=True)
        else:
            self._log_handle = open(self._server_log, "w", buffering=1)
            self.proc = subprocess.Popen(
                wrapped_cmd,
                cwd=self.carla_root,
                env=env,
                stdout=self._log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        print(f"[carla] pid={self.proc.pid}")
        launch_record["pid"] = self.proc.pid

    def wait_ready(self, timeout_s: float = 180.0, poll_s: float = 1.0) -> bool:
        hang_retry_after_s = self._hung_startup_retry_after_sec(timeout_s)
        launch_started_at = time.time()
        end = time.time() + timeout_s
        while time.time() < end:
            if self.proc and self.proc.poll() is not None and not self.reused:
                launch_record = self._launch_records[-1] if self._launch_records else None
                current_command = list((launch_record or {}).get("command") or [])
                if self._launch_records:
                    self._launch_records[-1]["exit_code"] = self.proc.returncode
                    self._launch_records[-1]["server_log_tail"] = self._tail_file(self._server_log)
                    self._launch_records[-1]["ue_log_tail"] = self._tail_file(self._ue_log)
                    launch_record = self._launch_records[-1]
                if (
                    self.enable_auto_recovery
                    and
                    self.proc.returncode is not None
                    and not self._vulkan_oom_windowed_retry_used
                    and not self._vulkan_oom_windowed_recovery_applied
                    and not self._render_offscreen_forced
                    and not self._command_has_render_offscreen(current_command)
                    and self._display_available()
                    and self._launch_tail_contains_vulkan_oom(launch_record)
                ):
                    print(
                        "[carla][WARN] Vulkan OOM detected on display path; "
                        "retry once with low-resolution windowed recovery"
                    )
                    self._vulkan_oom_windowed_recovery_applied = True
                    self._vulkan_oom_windowed_retry_used = True
                    if hasattr(self, "_log_handle") and getattr(self, "_log_handle", None):
                        try:
                            self._log_handle.close()
                        except Exception:
                            pass
                        self._log_handle = None
                    self.proc = None
                    self.start()
                    continue
                if (
                    self.enable_auto_recovery
                    and
                    self.proc.returncode is not None
                    and not self._render_offscreen_forced
                    and not self._render_offscreen_retry_used
                    and not self._command_has_render_offscreen(current_command)
                    and self._display_available()
                ):
                    print(
                        "[carla][WARN] process exited early with display path; "
                        "retry once with RenderOffScreen fallback"
                    )
                    self._render_offscreen_forced = True
                    self._render_offscreen_retry_used = True
                    if hasattr(self, "_log_handle") and getattr(self, "_log_handle", None):
                        try:
                            self._log_handle.close()
                        except Exception:
                            pass
                        self._log_handle = None
                    self.proc = None
                    self.start()
                    continue
                print(f"[carla][ERROR] process exited early code={self.proc.returncode}")
                print(self.diagnose_tail())
                return False
            if self.proc and self.proc.poll() is None and not self.reused:
                launch_record = self._launch_records[-1] if self._launch_records else None
                elapsed_s = time.time() - launch_started_at
                any_port_open = any(_is_port_open(self.host, port) for port in self._target_ports())
                tail_flags = self._server_tail_flags(launch_record)
                if (
                    self.bootstrap_stability_window_s > 0.0
                    and elapsed_s < self.bootstrap_stability_window_s
                    and (tail_flags["request_exit"] or tail_flags["sig11"] or tail_flags["stream_eof"])
                ):
                    if self._launch_records:
                        self._launch_records[-1]["early_fail_reason"] = "bootstrap_process_died_before_world_attach"
                        self._launch_records[-1]["elapsed_before_early_fail_sec"] = round(elapsed_s, 3)
                        self._launch_records[-1]["server_log_tail"] = self._tail_file(self._server_log)
                    print(
                        "[carla][WARN] bootstrap stability window saw fatal server tail before world attach; "
                        "fail fast so outer startup can classify this launch",
                    )
                    print(self.diagnose_tail())
                    return False
                if elapsed_s >= hang_retry_after_s:
                    current_command = list((launch_record or {}).get("command") or [])
                    if (
                        self.enable_auto_recovery
                        and
                        not any_port_open
                        and self._display_available()
                        and self._monitorless_display()
                        and self._command_has_lowres_low_quality(current_command)
                        and not self._command_has_render_offscreen(current_command)
                        and not self._render_offscreen_forced
                        and not self._render_offscreen_retry_used
                    ):
                        print(
                            "[carla][WARN] monitorless low-res startup stayed listener-dead; "
                            "retry once with RenderOffScreen fallback"
                        )
                        self._render_offscreen_forced = True
                        self._render_offscreen_retry_used = True
                        if hasattr(self, "_log_handle") and getattr(self, "_log_handle", None):
                            try:
                                self._log_handle.close()
                            except Exception:
                                pass
                            self._log_handle = None
                        self.proc = None
                        self.start()
                        launch_started_at = time.time()
                        continue
                    if (
                        self.enable_auto_recovery
                        and
                        not any_port_open
                        and self._display_available()
                        and not self._command_has_render_offscreen(current_command)
                        and not self._lowres_low_quality_retry_used
                        and not self._render_offscreen_forced
                    ):
                        self._restart_with_lowres_low_quality_recovery()
                        launch_started_at = time.time()
                        continue
                    if self.enable_auto_recovery and not any_port_open:
                        if self._launch_records:
                            self._launch_records[-1]["early_fail_reason"] = "rpc_listener_missing_after_hang"
                            self._launch_records[-1]["elapsed_before_early_fail_sec"] = round(elapsed_s, 3)
                        print(
                            "[carla][WARN] startup remained listener-dead past the recovery window; "
                            "fail fast so outer retry can classify this launch"
                        )
                        print(self.diagnose_tail())
                        return False
                    if self.enable_auto_recovery and self._launch_tail_contains_stream_eof(launch_record):
                        if self._launch_records:
                            self._launch_records[-1]["early_fail_reason"] = "rpc_handshake_dead_with_eof_alive"
                            self._launch_records[-1]["elapsed_before_early_fail_sec"] = round(elapsed_s, 3)
                            self._launch_records[-1]["server_log_tail"] = self._tail_file(self._server_log)
                        print(
                            "[carla][WARN] RPC listeners opened but handshake stayed dead with EOF tail; "
                            "fail fast so outer retry can classify this startup family"
                        )
                        print(self.diagnose_tail())
                        return False
            # Only require a live CARLA RPC handshake here. The main runner has
            # its own get_world/load_world retry path and can correct the town
            # after startup. Requiring get_world() in the launcher can block
            # valid runs before that recovery logic gets a chance to execute.
            if _client_ok(self.host, self.port, timeout=2.0, require_world=False):
                elapsed_s = time.time() - launch_started_at
                if (
                    not self.reused
                    and self.bootstrap_stability_window_s > 0.0
                    and elapsed_s < self.bootstrap_stability_window_s
                ):
                    if self._launch_records:
                        self._launch_records[-1]["rpc_ready_before_bootstrap_window_complete"] = True
                    time.sleep(poll_s)
                    continue
                if self._launch_records:
                    self._launch_records[-1]["ready"] = True
                    self._launch_records[-1]["bootstrap_stability_elapsed_s"] = round(elapsed_s, 3)
                return True
            time.sleep(poll_s)
        print(self.diagnose_tail())
        return False

    def stop(self, grace_s: float = 5.0):
        try:
            if self.reused:
                if not self.stop_reused_on_exit:
                    return
                pid = self._reused_pid or _carla_pid_on_port(self.port)
                if not pid:
                    print("[carla][WARN] reused instance detected but PID is unknown; skip stop")
                    return
                if not _terminate_pid(pid, grace_s=grace_s):
                    print(f"[carla][WARN] failed to fully stop reused CARLA pid={pid}")
                return

            if not self.proc:
                for pid in sorted(set(_carla_pids_matching_port_arg(self.port))):
                    _terminate_pid(pid, grace_s=grace_s)
                target_pid = _carla_pid_on_port(self.port)
                if target_pid:
                    _terminate_pid(target_pid, grace_s=grace_s)
                if self._systemd_scope_unit:
                    subprocess.run(
                        ["systemctl", "--user", "stop", self._systemd_scope_unit],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                return
            if self.proc.poll() is None:
                if self._systemd_scope_unit:
                    subprocess.run(
                        ["systemctl", "--user", "stop", self._systemd_scope_unit],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                try:
                    pgid = os.getpgid(self.proc.pid)
                except Exception:
                    pgid = None
                if pgid is not None:
                    try:
                        os.killpg(pgid, signal.SIGTERM)
                    except Exception:
                        pass
                    deadline = time.time() + grace_s
                    while time.time() < deadline:
                        if self.proc.poll() is not None:
                            break
                        time.sleep(0.1)
                    if self.proc.poll() is None:
                        try:
                            os.killpg(pgid, signal.SIGKILL)
                        except Exception:
                            pass
                else:
                    self.proc.terminate()
                    try:
                        self.proc.wait(timeout=grace_s)
                    except Exception:
                        self.proc.kill()
            # CarlaUE4.sh can leave the actual server behind before or after the
            # RPC triplet binds. Sweep both the current port owner and any
            # CarlaUE4 process that still advertises this RPC port in its args.
            leftover_pids = set(_carla_pids_matching_port_arg(self.port))
            leftover_pid = _carla_pid_on_port(self.port)
            if leftover_pid:
                leftover_pids.add(leftover_pid)
            for pid in sorted(leftover_pids):
                _terminate_pid(pid, grace_s=grace_s)
        finally:
            if self.proc is not None:
                try:
                    self.proc.wait(timeout=0.5)
                except Exception:
                    pass
            self.proc = None
            self.reused = False
            self._reused_pid = None
            self._systemd_scope_unit = None
            self._launch_started_at = None
            if hasattr(self, "_log_handle") and getattr(self, "_log_handle", None):
                try:
                    self._log_handle.close()
                except Exception:
                    pass
                self._log_handle = None

    def _print_port_diag(self):
        try:
            print("[carla] ss -ltnp | target ports:")
            any_match = False
            for port in self._target_ports():
                filtered = "\n".join(_ss_lines_for_port(port))
                if filtered:
                    any_match = True
                print(f"[carla] port {port}:")
                print(filtered or "(no match)")
            if not any_match:
                print("[carla] no listeners on target CARLA ports")
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
        server_tail = self._tail_file(self._server_log, n)
        if server_tail:
            lines.append(f"[carla] carla_server.log tail ({len(server_tail)} lines):")
            lines.append("\n".join(server_tail))
        else:
            lines.append(f"[carla] server log empty or missing: {self._server_log}")
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
