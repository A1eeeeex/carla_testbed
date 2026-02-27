from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .base import Backend


class CyberRTBackend(Backend):
    def __init__(self, profile: Dict[str, Any]):
        super().__init__(profile)
        self.repo_root = Path(__file__).resolve().parents[2]
        self.bridge_proc: Optional[subprocess.Popen] = None
        self.control_proc: Optional[subprocess.Popen] = None
        self._docker_container_name: Optional[str] = None
        self._docker_stage_dir: Optional[str] = None
        self._docker_stats_path: Optional[str] = None
        self._runtime_source_prefix: Optional[str] = None
        self._bridge_out = None
        self._bridge_err = None
        self._control_out = None
        self._control_err = None

    def _apollo_cfg(self) -> Dict[str, Any]:
        return (self.profile.get("algo", {}) or {}).get("apollo", {}) or {}

    def _docker_cfg(self) -> Dict[str, Any]:
        return (self._apollo_cfg().get("docker", {}) or {})

    def _docker_enabled(self) -> bool:
        cfg = self._docker_cfg()
        if "enabled" in cfg:
            return bool(cfg.get("enabled"))
        return bool(cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER"))

    def _docker_container(self) -> str:
        cfg = self._docker_cfg()
        name = str(cfg.get("container") or os.environ.get("APOLLO_DOCKER_CONTAINER", "")).strip()
        if not name:
            raise RuntimeError(
                "Apollo docker mode enabled but container is missing; "
                "set algo.apollo.docker.container or APOLLO_DOCKER_CONTAINER"
            )
        return name

    def _docker_apollo_root(self) -> str:
        cfg = self._docker_cfg()
        return str(cfg.get("apollo_root_in_container") or "/apollo")

    def _docker_apollo_dist(self) -> str:
        cfg = self._docker_cfg()
        return str(cfg.get("apollo_distribution_home") or "/opt/apollo/neo")

    def _docker_python_exec(self) -> str:
        cfg = self._docker_cfg()
        return str(cfg.get("python_exec") or "python3")

    def _docker_bridge_in_container(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("bridge_in_container", False))

    def _docker_require_ipc_host(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("require_ipc_host", True))

    def _docker_require_network_host(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("require_network_host", True))

    def _docker_auto_install_runtime_deps(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("auto_install_runtime_deps", True))

    def _docker_module_exec_user(self) -> str:
        cfg = self._docker_cfg()
        # Keep container module uid aligned with host user to avoid CyberRT shm permission mismatches.
        return str(cfg.get("module_exec_user") or "1000:1000")

    def _host_ros2_setup_script(self) -> Optional[str]:
        apollo_cfg = self._apollo_cfg()
        script = str(apollo_cfg.get("ros2_setup_script") or "").strip()
        if script:
            p = Path(script).expanduser()
            if not p.is_absolute():
                p = (self.repo_root / p).resolve()
            return str(p)
        distro = str(os.environ.get("ROS_DISTRO", "")).strip()
        candidates = []
        if distro:
            candidates.append(Path(f"/opt/ros/{distro}/setup.bash"))
        candidates.append(Path("/opt/ros/humble/setup.bash"))
        for cand in candidates:
            if cand.exists():
                return str(cand)
        return None

    def _cyber_domain_id(self) -> str:
        apollo_cfg = self._apollo_cfg()
        value = str(apollo_cfg.get("cyber_domain_id") or os.environ.get("CYBER_DOMAIN_ID", "")).strip()
        if value:
            return value
        if self._docker_enabled():
            # Apollo Docker setup.sh defaults to 80; keep host bridge aligned.
            return "80"
        return ""

    def _cyber_ip(self) -> str:
        apollo_cfg = self._apollo_cfg()
        return str(apollo_cfg.get("cyber_ip") or os.environ.get("CYBER_IP", "")).strip()

    def _docker_env_script(self) -> str:
        cfg = self._docker_cfg()
        script = cfg.get("apollo_env_script")
        if script:
            return str(script)
        return f"{self._docker_apollo_root()}/cyber/setup.bash"

    def _docker_auto_start_container(self) -> bool:
        cfg = self._docker_cfg()
        if "auto_start_container" in cfg:
            return bool(cfg.get("auto_start_container"))
        return True

    def _docker_start_modules_enabled(self) -> bool:
        cfg = self._docker_cfg()
        return bool(cfg.get("start_modules", False))

    def _docker_start_modules_cmd(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("start_modules_cmd") or "").strip()
        if not raw:
            raw = (
                "set -eo pipefail; "
                "pkill -f 'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' >/dev/null 2>&1 || true; "
                "sleep 1; "
                "overlay_conf_root=/apollo_workspace/conf_overlay; "
                "lf_conf_dir=${overlay_conf_root}/modules/planning/scenarios/lane_follow/conf; "
                "lf_stage_dir=${lf_conf_dir}/lane_follow_stage; "
                "mkdir -p \"$lf_stage_dir\" /apollo_workspace/log /apollo_workspace/dumps >/dev/null 2>&1 || true; "
                "for p in "
                "/apollo/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt "
                "/opt/apollo/neo/share/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt "
                "/opt/apollo/neo/src/modules/planning/scenarios/lane_follow/conf/pipeline.pb.txt; do "
                "[ -f \"$p\" ] && { ln -sf \"$p\" \"$lf_conf_dir/pipeline.pb.txt\" >/dev/null 2>&1 || true; break; }; "
                "done; "
                "declare -A lf_map=( "
                "[lane_change_path.pb.txt]=lane_change_path "
                "[lane_follow_path.pb.txt]=lane_follow_path "
                "[lane_borrow_path.pb.txt]=lane_borrow_path "
                "[fallback_path.pb.txt]=fallback_path "
                "[path_decider.pb.txt]=path_decider "
                "[rule_based_stop_decider.pb.txt]=rule_based_stop_decider "
                "[speed_bounds_priori_decider.pb.txt]=speed_bounds_decider "
                "[speed_heuristic_optimizer.pb.txt]=path_time_heuristic "
                "[speed_decider.pb.txt]=speed_decider "
                "[speed_bounds_final_decider.pb.txt]=speed_bounds_decider "
                "[piecewise_jerk_speed.pb.txt]=piecewise_jerk_speed "
                "); "
                "for dst in \"${!lf_map[@]}\"; do "
                "task=\"${lf_map[$dst]}\"; src=''; "
                "for cand in "
                "\"/apollo/modules/planning/tasks/${task}/conf/default_conf.pb.txt\" "
                "\"/opt/apollo/neo/share/modules/planning/tasks/${task}/conf/default_conf.pb.txt\" "
                "\"/opt/apollo/neo/src/modules/planning/tasks/${task}/conf/default_conf.pb.txt\"; do "
                "[ -f \"$cand\" ] && { src=\"$cand\"; break; }; "
                "done; "
                "[ -f \"$src\" ] && ln -sf \"$src\" \"$lf_stage_dir/$dst\" >/dev/null 2>&1 || true; "
                "done; "
                "launches=(); "
                "[ -f /apollo/modules/routing/launch/routing.launch ] && launches+=(/apollo/modules/routing/launch/routing.launch); "
                "[ -f /apollo/modules/prediction/launch/prediction.launch ] && launches+=(/apollo/modules/prediction/launch/prediction.launch); "
                "[ -f /apollo/modules/planning/planning_component/launch/planning.launch ] && launches+=(/apollo/modules/planning/planning_component/launch/planning.launch); "
                "[ -f /apollo/modules/control/control_component/launch/control.launch ] && launches+=(/apollo/modules/control/control_component/launch/control.launch); "
                "if [ ${#launches[@]} -eq 0 ]; then echo 'no apollo launch files found under /apollo/modules'; exit 2; fi; "
                "mkdir -p /apollo_workspace/log >/dev/null 2>&1 || true; "
                "for lf in \"${launches[@]}\"; do "
                "setsid -f cyber_launch start \"$lf\" >/apollo_workspace/log/$(basename \"$lf\").tb.log 2>&1; "
                "sleep 1; "
                "done; "
                "sleep 1; "
                "pgrep -af 'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' || true"
            )
        return " ".join(raw.split())

    def _docker_required_modules(self) -> list[str]:
        cfg = self._docker_cfg()
        raw = cfg.get("required_modules")
        if isinstance(raw, (list, tuple)):
            out = [str(x).strip().lower() for x in raw if str(x).strip()]
            if out:
                return out
        return ["routing", "prediction", "planning", "control"]

    def _docker_modules_status_cmd(self) -> str:
        cfg = self._docker_cfg()
        raw = str(cfg.get("modules_status_cmd") or "").strip()
        if not raw:
            raw = (
                "pgrep -af "
                "'modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)' "
                "|| true"
            )
        return " ".join(raw.split())

    @staticmethod
    def _safe_run_tag(text: str) -> str:
        return "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in text)

    def _docker_stage_root(self) -> str:
        cfg = self._docker_cfg()
        default = f"/tmp/carla_testbed_apollo_bridge/{self._safe_run_tag(self._run_dir().name)}"
        return str(cfg.get("stage_dir") or default)

    def _docker_exec(
        self,
        shell_cmd: str,
        *,
        user: Optional[str] = None,
        check: bool = True,
        capture_output: bool = False,
        text: bool = True,
    ) -> subprocess.CompletedProcess:
        container = self._docker_container_name or self._docker_container()
        cmd = ["docker", "exec", "-i"]
        if user:
            cmd += ["-u", user]
        cmd += [container, "bash", "-lc", shell_cmd]
        return subprocess.run(cmd, check=check, capture_output=capture_output, text=text)

    def _docker_check_running(self, container: str) -> None:
        try:
            out = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", container], text=True
            ).strip()
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"failed to inspect Apollo container {container}: {exc}") from exc
        if out.lower() != "true" and self._docker_auto_start_container():
            print(f"[cyberrt] starting Apollo container: {container}")
            started = subprocess.run(["docker", "start", container], text=True, capture_output=True)
            if started.returncode != 0:
                detail = (started.stderr or started.stdout or "").strip()
                raise RuntimeError(f"failed to start Apollo container {container}: {detail}")
            out = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", container], text=True
            ).strip()
        if out.lower() != "true":
            raise RuntimeError(f"Apollo container is not running: {container}")

    def _docker_runtime_modes(self, container: str) -> tuple[str, str]:
        ipc_mode = ""
        net_mode = ""
        try:
            ipc_mode = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.HostConfig.IpcMode}}", container],
                text=True,
            ).strip()
            net_mode = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.HostConfig.NetworkMode}}", container],
                text=True,
            ).strip()
        except subprocess.CalledProcessError:
            pass
        return ipc_mode, net_mode

    def _docker_check_runtime_requirements(self, container: str, artifacts: Path) -> None:
        ipc_mode, net_mode = self._docker_runtime_modes(container)
        report = artifacts / "apollo_container_runtime_check.txt"
        report.write_text(f"container={container}\nipc_mode={ipc_mode}\nnetwork_mode={net_mode}\n")
        issues: list[str] = []
        # Host bridge <-> Apollo container CyberRT needs shared IPC/network domains.
        if not self._docker_bridge_in_container() and self._docker_require_ipc_host() and ipc_mode != "host":
            issues.append(f"ipc_mode={ipc_mode} (expected host)")
        if not self._docker_bridge_in_container() and self._docker_require_network_host() and net_mode != "host":
            issues.append(f"network_mode={net_mode} (expected host)")
        if issues:
            raise RuntimeError(
                "Apollo container runtime is incompatible with host bridge: "
                + ", ".join(issues)
                + ". Recreate container with --network host --ipc host."
            )

    def _docker_prepare_user(self, artifacts: Path) -> None:
        cmd = (
            "set -o pipefail; "
            "if [ -x /opt/apollo/aem/docker_start_user.sh ]; then "
            "/opt/apollo/aem/docker_start_user.sh; "
            "fi"
        )
        out = self._docker_exec(cmd, capture_output=True, check=False)
        (artifacts / "apollo_prepare_user.log").write_text(
            f"cmd: {cmd}\nreturncode: {out.returncode}\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )

    def _docker_ensure_runtime_deps(self, artifacts: Path) -> None:
        check_log = artifacts / "apollo_mainboard_runtime_check.log"
        module_user = self._docker_module_exec_user()
        check_cmd = f"{self._docker_modules_prefix()}; mainboard --help"
        first = self._docker_exec(check_cmd, capture_output=True, check=False, user=module_user)
        check_log.write_text(
            f"check_cmd: {check_cmd}\n"
            f"returncode: {first.returncode}\n"
            f"--- stdout ---\n{first.stdout}\n"
            f"--- stderr ---\n{first.stderr}\n"
        )
        if first.returncode == 0:
            return
        if not self._docker_auto_install_runtime_deps():
            raise RuntimeError(
                "Apollo runtime libraries are missing in container and auto_install_runtime_deps=false; "
                f"see {check_log}"
            )

        install_cmd = (
            "set -o pipefail; "
            "mkdir -p /root/.apollo /home/ubuntu /home/ubuntu/.apollo; "
            "touch /home/ubuntu/.bashrc; "
            "DEBIAN_FRONTEND=noninteractive apt-get install -y bvar libgoogle-perftools4"
        )
        install = self._docker_exec(install_cmd, capture_output=True, check=False)
        (artifacts / "apollo_runtime_deps_install.log").write_text(
            f"cmd: {install_cmd}\nreturncode: {install.returncode}\n"
            f"--- stdout ---\n{install.stdout}\n--- stderr ---\n{install.stderr}\n"
        )
        if install.returncode != 0:
            raise RuntimeError(
                "failed to auto-install Apollo runtime deps (bvar/libgoogle-perftools4); "
                f"see {artifacts / 'apollo_runtime_deps_install.log'}"
            )

        second = self._docker_exec(check_cmd, capture_output=True, check=False, user=module_user)
        check_log.write_text(
            check_log.read_text()
            + "\n=== after auto install ===\n"
            + f"returncode: {second.returncode}\n"
            + f"--- stdout ---\n{second.stdout}\n"
            + f"--- stderr ---\n{second.stderr}\n"
        )
        if second.returncode != 0:
            raise RuntimeError(
                "Apollo runtime check still failed after auto-install; "
                f"see {check_log}"
            )

    def _docker_start_modules(self, artifacts: Path) -> None:
        if not self._docker_start_modules_enabled():
            return
        module_user = self._docker_module_exec_user()
        log_path = artifacts / "apollo_modules_start.log"
        status_path = artifacts / "apollo_modules_status.log"
        prefix = self._docker_modules_prefix()
        cmd = self._docker_start_modules_cmd()
        out = self._docker_exec(f"{prefix}; {cmd}", capture_output=True, check=False, user=module_user)
        log_path.write_text(
            f"cmd: {cmd}\nreturncode: {out.returncode}\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}\n"
        )
        if out.returncode != 0:
            raise RuntimeError(f"Apollo modules start failed, see {log_path}")
        status_cmd = self._docker_modules_status_cmd()
        status = self._docker_exec(f"{prefix}; {status_cmd}", capture_output=True, check=False, user=module_user)
        status_path.write_text(
            f"cmd: {status_cmd}\nreturncode: {status.returncode}\n--- stdout ---\n{status.stdout}\n--- stderr ---\n{status.stderr}\n"
        )
        if not status.stdout.strip():
            raise RuntimeError(
                "Apollo modules start command finished but no target module process was found; "
                f"see {status_path}"
            )
        lines = [line.strip() for line in status.stdout.splitlines() if line.strip()]
        pattern_map = {
            "routing": "modules/routing/dag/routing.dag",
            "prediction": "modules/prediction/dag/prediction.dag",
            "planning": "modules/planning/planning_component/dag/planning.dag",
            "control": "modules/control/control_component/dag/control.dag",
        }
        missing = []
        for name in self._docker_required_modules():
            patt = pattern_map.get(name, name)
            if not any(patt in line for line in lines):
                missing.append(name)
        if missing:
            raise RuntimeError(
                "Apollo modules are missing after startup: "
                + ",".join(missing)
                + f" (see {status_path})"
            )

    def _docker_modules_prefix(self) -> str:
        apollo_root = self._docker_apollo_root()
        apollo_dist = self._docker_apollo_dist()
        env_script = self._docker_env_script()
        parts = ["set -o pipefail"]
        parts.append(f"export APOLLO_ROOT={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(apollo_dist)}")
        parts.append('if [ -d /apollo/cyber ]; then export CYBER_PATH=/apollo/cyber; fi')
        parts.append(
            f'if [ -f {shlex.quote(f"{apollo_dist}/setup.sh")} ]; then source {shlex.quote(f"{apollo_dist}/setup.sh")}; fi'
        )
        parts.append(
            f'if [ -f {shlex.quote(env_script)} ]; then source {shlex.quote(env_script)}; fi'
        )
        parts.append(
            'if [ -n "${APOLLO_CONF_PATH:-}" ]; then '
            'export APOLLO_CONF_PATH="/apollo_workspace/conf_overlay:/apollo:/opt/apollo/neo/share:/opt/apollo/neo/src:${APOLLO_CONF_PATH}"; '
            "else export APOLLO_CONF_PATH=/apollo_workspace/conf_overlay:/apollo:/opt/apollo/neo/share:/opt/apollo/neo/src; fi"
        )
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(f"{apollo_dist}/lib"),
                    shlex.quote(f"{apollo_dist}/lib64"),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        # Apollo runtime dependencies are packaged under /opt/apollo/neo/packages/*/lib.
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        parts.append("cd /apollo")
        return "; ".join(parts)

    def _docker_prepare_stage(self, bridge_cfg: Path, bridge_py: Path) -> tuple[str, str, str, str]:
        container = self._docker_container()
        self._docker_check_running(container)
        stage_dir = self._docker_stage_root()
        pb_root = self._pb_root()
        if not pb_root.exists():
            raise RuntimeError(f"pb root not found for docker staging: {pb_root}")

        stage_bridge_dir = f"{stage_dir}/tools/apollo10_cyber_bridge"
        stage_pb_dir = f"{stage_dir}/pb"
        stage_cfg = f"{stage_dir}/apollo_bridge_effective.yaml"
        stage_stats = f"{stage_dir}/cyber_bridge_stats.json"
        self._docker_exec(f"mkdir -p {shlex.quote(stage_bridge_dir)} {shlex.quote(stage_pb_dir)}")
        subprocess.check_call(
            ["docker", "cp", str(bridge_py), f"{container}:{stage_bridge_dir}/bridge.py"]
        )
        subprocess.check_call(
            ["docker", "cp", str(bridge_cfg), f"{container}:{stage_cfg}"]
        )
        subprocess.check_call(
            ["docker", "cp", f"{pb_root}/.", f"{container}:{stage_pb_dir}/"]
        )
        self._docker_container_name = container
        self._docker_stage_dir = stage_dir
        self._docker_stats_path = stage_stats
        return stage_bridge_dir, stage_cfg, stage_pb_dir, stage_stats

    def _docker_sync_host_libs(self, artifacts: Path) -> Optional[Path]:
        if not self._docker_container_name:
            return None
        libs_dir = artifacts / "apollo_docker_libs"
        libs_dir.mkdir(parents=True, exist_ok=True)
        list_cmd = (
            "for p in /usr/local/lib/libbvar.so* /usr/local/lib/libbrpc.so* /usr/local/lib/libbutil.so*; "
            'do [ -e "$p" ] && echo "$p"; done'
        )
        out = self._docker_exec(list_cmd, capture_output=True, check=False)
        paths = [line.strip() for line in out.stdout.splitlines() if line.strip()]
        if not paths:
            return None
        copied: list[str] = []
        for path in paths:
            try:
                subprocess.check_call(["docker", "cp", f"{self._docker_container_name}:{path}", str(libs_dir)])
                copied.append(path)
            except Exception:
                continue
        if not copied:
            return None
        (artifacts / "apollo_docker_libs.txt").write_text("\n".join(copied) + "\n")
        return libs_dir

    def _run_dir(self) -> Path:
        value = self.profile.get("_apollo_run_dir")
        if value:
            return Path(value).resolve()
        return Path(self.profile.get("artifacts", {}).get("dir", "runs/latest/artifacts")).resolve().parent

    def _artifacts_dir(self) -> Path:
        artifacts = self.profile.get("artifacts", {}) or {}
        value = artifacts.get("dir")
        if value:
            return Path(value).resolve()
        path = self._run_dir() / "artifacts"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _apollo_root(self) -> Path:
        cfg = self._apollo_cfg()
        root = cfg.get("apollo_root") or os.environ.get("APOLLO_ROOT")
        if not root:
            raise RuntimeError("APOLLO_ROOT is required (set env APOLLO_ROOT or algo.apollo.apollo_root)")
        raw = Path(root).expanduser().resolve()
        if not raw.exists():
            raise RuntimeError(f"APOLLO_ROOT not found: {raw}")

        def _looks_like_src(p: Path) -> bool:
            return (p / "modules" / "common_msgs").exists() and (p / "cyber").exists()

        candidates = [
            raw,
            raw / "application-core" / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "src",
            raw / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "src",
            raw / "neo" / "src",
            raw / "src",
        ]
        for cand in candidates:
            if cand.exists() and _looks_like_src(cand):
                return cand.resolve()
        raise RuntimeError(
            "APOLLO_ROOT does not look like Apollo source root (missing modules/common_msgs + cyber). "
            f"provided={raw}"
        )

    def _bridge_cfg(self) -> Path:
        cfg = self._apollo_cfg()
        path = cfg.get("bridge_config_path")
        if not path:
            raise RuntimeError("apollo bridge_config_path missing; run adapter.prepare first")
        out = Path(path).expanduser()
        if not out.is_absolute():
            out = (self.repo_root / out).resolve()
        if not out.exists():
            raise RuntimeError(f"apollo bridge config not found: {out}")
        return out

    def _bridge_stats_path(self) -> Path:
        cfg = self._apollo_cfg()
        path = cfg.get("stats_path")
        if path:
            out = Path(path).expanduser()
            if not out.is_absolute():
                out = (self.repo_root / out).resolve()
            return out
        return self._artifacts_dir() / "cyber_bridge_stats.json"

    def _pb_root(self) -> Path:
        cfg = self._apollo_cfg()
        path = cfg.get("pb_root")
        if path:
            out = Path(path).expanduser()
            if not out.is_absolute():
                out = (self.repo_root / out).resolve()
            return out
        return self.repo_root / "tools" / "apollo10_cyber_bridge" / "pb"

    def _required_pb_files(self) -> list[Path]:
        root = self._pb_root()
        return [
            root / "modules/common_msgs/localization_msgs/localization_pb2.py",
            root / "modules/common_msgs/chassis_msgs/chassis_pb2.py",
            root / "modules/common_msgs/perception_msgs/perception_obstacle_pb2.py",
            root / "modules/common_msgs/control_msgs/control_cmd_pb2.py",
            root / "modules/common_msgs/external_command_msgs/lane_follow_command_pb2.py",
        ]

    def _ensure_pb_ready(self) -> None:
        if all(p.exists() for p in self._required_pb_files()):
            return
        script = self.repo_root / "tools" / "apollo10_cyber_bridge" / "gen_pb2.sh"
        if not script.exists():
            raise RuntimeError(f"pb generation script missing: {script}")
        env = os.environ.copy()
        env["APOLLO_ROOT"] = str(self._apollo_root())
        print(f"[cyberrt] generating pb2 via {script}")
        subprocess.check_call(["bash", str(script)], cwd=self.repo_root, env=env)
        missing = [p for p in self._required_pb_files() if not p.exists()]
        if missing:
            raise RuntimeError(f"pb2 generation incomplete, missing: {missing}")

    def _source_prefix(self) -> str:
        apollo_cfg = self._apollo_cfg()
        apollo_root = self._apollo_root()
        apollo_dist = apollo_root.parent if apollo_root.name == "src" else apollo_root
        env_script = apollo_cfg.get("apollo_env_script")
        parts = ["set -eo pipefail"]
        # Apollo setup scripts assume APOLLO_ROOT_DIR may already exist.
        parts.append(f"export APOLLO_ROOT={shlex.quote(str(apollo_root))}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(str(apollo_root))}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(str(apollo_dist))}")
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        ros2_setup = self._host_ros2_setup_script()
        if ros2_setup:
            parts.append(f'source {shlex.quote(ros2_setup)}')
        if env_script:
            script = Path(env_script).expanduser()
            if not script.is_absolute():
                script = (self.repo_root / script).resolve()
            if not script.exists():
                raise RuntimeError(f"apollo_env_script not found: {script}")
            parts.append(f"source {shlex.quote(str(script))}")
        else:
            setup = apollo_root / "cyber" / "setup.bash"
            if setup.exists():
                parts.append(f"source {shlex.quote(str(setup))}")
        pb_root = self._pb_root()
        bridge_root = self.repo_root / "tools" / "apollo10_cyber_bridge"
        py_parts = [
            str(self.repo_root),
            str(bridge_root),
            str(pb_root),
            str(apollo_root / "cyber" / "python"),
            str(apollo_root / "cyber" / "python" / "internal"),
            str(apollo_dist / "lib" / "cyber" / "python"),
            str(apollo_dist / "lib" / "cyber" / "python" / "internal"),
            str(apollo_dist / "lib" / "python3.10" / "site-packages"),
        ]
        py_expr = ":".join(shlex.quote(p) for p in py_parts)
        parts.append(f"export PYTHONPATH={py_expr}:${{PYTHONPATH:-}}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(str(apollo_dist / "lib")),
                    shlex.quote(str(apollo_dist / "lib64")),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        # Apollo dependencies (FastDDS/FastCDR, etc.) are shipped under packages/*/lib.
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        return "; ".join(parts)

    def _docker_source_prefix(self, bridge_root: str, pb_root: str) -> str:
        apollo_root = self._docker_apollo_root()
        apollo_dist = self._docker_apollo_dist()
        env_script = self._docker_env_script()
        parts = ["set -o pipefail"]
        parts.append(f"export APOLLO_ROOT={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_ROOT_DIR={shlex.quote(apollo_root)}")
        parts.append(f"export APOLLO_DISTRIBUTION_HOME={shlex.quote(apollo_dist)}")
        parts.append('if [ -d /apollo/cyber ]; then export CYBER_PATH=/apollo/cyber; fi')
        parts.append(
            f'if [ -f {shlex.quote(f"{apollo_dist}/setup.sh")} ]; then source {shlex.quote(f"{apollo_dist}/setup.sh")}; fi'
        )
        parts.append(
            f'if [ -f {shlex.quote(env_script)} ]; then source {shlex.quote(env_script)}; fi'
        )
        cyber_domain = self._cyber_domain_id()
        cyber_ip = self._cyber_ip()
        if cyber_domain:
            parts.append(f"export CYBER_DOMAIN_ID={shlex.quote(cyber_domain)}")
        if cyber_ip:
            parts.append(f"export CYBER_IP={shlex.quote(cyber_ip)}")
        py_parts = [
            bridge_root,
            pb_root,
            f"{apollo_root}/cyber/python",
            f"{apollo_root}/cyber/python/internal",
            f"{apollo_dist}/lib/cyber/python",
            f"{apollo_dist}/lib/cyber/python/internal",
        ]
        py_expr = ":".join(shlex.quote(p) for p in py_parts)
        parts.append(f"export PYTHONPATH={py_expr}:${{PYTHONPATH:-}}")
        parts.append(
            "export LD_LIBRARY_PATH="
            + ":".join(
                [
                    shlex.quote(f"{apollo_dist}/lib"),
                    shlex.quote(f"{apollo_dist}/lib64"),
                ]
            )
            + ":${LD_LIBRARY_PATH:-}"
        )
        parts.append(
            'for _d in "$APOLLO_DISTRIBUTION_HOME"/packages/*/lib '
            '"$APOLLO_DISTRIBUTION_HOME"/packages/*/*/lib; do '
            '[ -d "$_d" ] && export LD_LIBRARY_PATH="$_d:${LD_LIBRARY_PATH:-}"; '
            "done"
        )
        return "; ".join(parts)

    def _launch(
        self,
        name: str,
        core_cmd: str,
        out_path: Path,
        err_path: Path,
        *,
        source_prefix: Optional[str] = None,
        docker_container: Optional[str] = None,
    ) -> subprocess.Popen:
        prefix = source_prefix if source_prefix is not None else self._source_prefix()
        shell_cmd = f"{prefix}; {core_cmd}" if prefix else core_cmd
        out_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.parent.mkdir(parents=True, exist_ok=True)
        out_fp = out_path.open("w")
        err_fp = err_path.open("w")
        cmd = ["bash", "-lc", shell_cmd]
        cwd = self.repo_root
        if docker_container:
            cmd = ["docker", "exec", "-i", docker_container, "bash", "-lc", shell_cmd]
            cwd = None
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=out_fp,
            stderr=err_fp,
            start_new_session=True,
        )
        if name == "bridge":
            self._bridge_out = out_fp
            self._bridge_err = err_fp
        else:
            self._control_out = out_fp
            self._control_err = err_fp
        return proc

    def start(self) -> bool:
        artifacts = self._artifacts_dir()
        artifacts.mkdir(parents=True, exist_ok=True)
        self._ensure_pb_ready()

        apollo_cfg = self._apollo_cfg()
        bridge_py = self.repo_root / "tools" / "apollo10_cyber_bridge" / "bridge.py"
        if not bridge_py.exists():
            raise RuntimeError(f"bridge.py not found: {bridge_py}")

        run_cfg = self.profile.get("run", {}) or {}
        io_ros = ((self.profile.get("io", {}) or {}).get("ros", {}) or {})
        ego_id = str(run_cfg.get("ego_id", "hero"))
        namespace = str(io_ros.get("namespace", "/carla"))

        bridge_cfg = self._bridge_cfg()
        stats_path = self._bridge_stats_path()
        bridge_yaml = yaml.safe_load(bridge_cfg.read_text()) if bridge_cfg.exists() else {}
        bridge_ros_cfg = (bridge_yaml.get("ros2", {}) if isinstance(bridge_yaml, dict) else {}) or {}
        docker_container: Optional[str] = None
        bridge_in_container = False
        if self._docker_enabled():
            self._docker_container_name = self._docker_container()
            self._docker_check_running(self._docker_container_name)
            self._docker_check_runtime_requirements(self._docker_container_name, artifacts)
            self._docker_prepare_user(artifacts)
            self._docker_ensure_runtime_deps(artifacts)
            self._docker_start_modules(artifacts)
            bridge_in_container = self._docker_bridge_in_container()
            print(
                f"[cyberrt] docker mode enabled, container={self._docker_container_name}, "
                f"bridge_in_container={bridge_in_container}"
            )

        if bridge_in_container:
            stage_bridge_dir, stage_cfg, stage_pb_root, stage_stats = self._docker_prepare_stage(bridge_cfg, bridge_py)
            docker_container = self._docker_container_name
            python_exec = shlex.quote(self._docker_python_exec())
            self._runtime_source_prefix = self._docker_source_prefix(stage_bridge_dir, stage_pb_root)
            bridge_cmd = " ".join(
                [
                    python_exec,
                    shlex.quote(f"{stage_bridge_dir}/bridge.py"),
                    "--config",
                    shlex.quote(stage_cfg),
                    "--stats-path",
                    shlex.quote(stage_stats),
                    "--apollo-root",
                    shlex.quote(self._docker_apollo_root()),
                    "--pb-root",
                    shlex.quote(stage_pb_root),
                    "--ego-id",
                    shlex.quote(ego_id),
                    "--namespace",
                    shlex.quote(namespace),
                ]
            )
        else:
            python_exec = shlex.quote(sys.executable)
            self._runtime_source_prefix = self._source_prefix()
            if self._docker_enabled():
                copied_libs = self._docker_sync_host_libs(artifacts)
                if copied_libs:
                    self._runtime_source_prefix += (
                        f"; export LD_LIBRARY_PATH={shlex.quote(str(copied_libs))}:${{LD_LIBRARY_PATH:-}}"
                    )
            self._docker_stage_dir = None
            self._docker_stats_path = None
            bridge_cmd = " ".join(
                [
                    python_exec,
                    shlex.quote(str(bridge_py)),
                    "--config",
                    shlex.quote(str(bridge_cfg)),
                    "--stats-path",
                    shlex.quote(str(stats_path)),
                    "--apollo-root",
                    shlex.quote(str(self._apollo_root())),
                    "--pb-root",
                    shlex.quote(str(self._pb_root())),
                    "--ego-id",
                    shlex.quote(ego_id),
                    "--namespace",
                    shlex.quote(namespace),
                ]
            )
        self.bridge_proc = self._launch(
            "bridge",
            bridge_cmd,
            artifacts / "cyber_bridge.out.log",
            artifacts / "cyber_bridge.err.log",
            source_prefix=self._runtime_source_prefix,
            docker_container=docker_container,
        )
        print(f"[cyberrt] bridge started pid={self.bridge_proc.pid}")

        control_bridge_cfg = apollo_cfg.get("carla_control_bridge", {}) or {}
        if control_bridge_cfg.get("enabled", True):
            control_py = self.repo_root / "algo" / "nodes" / "control" / "carla_control_bridge" / "ros2_autoware_to_carla.py"
            if control_py.exists():
                control_out_type = str(bridge_ros_cfg.get("control_out_type", "ackermann")).lower()
                if control_out_type not in {"ackermann", "twist"}:
                    print(f"[cyberrt][warn] unsupported control_out_type={control_out_type}; skip built-in control bridge")
                    return self.bridge_proc is not None and self.bridge_proc.poll() is None
                carla_cfg = (self.profile.get("runtime", {}) or {}).get("carla", {}) or {}
                topic = str(control_bridge_cfg.get("control_topic", "/tb/ego/control_cmd"))
                self._cleanup_stale_control_bridge(topic)
                control_cmd = " ".join(
                    [
                        python_exec,
                        shlex.quote(str(control_py)),
                        "--carla-host",
                        shlex.quote(str(carla_cfg.get("host", "127.0.0.1"))),
                        "--carla-port",
                        shlex.quote(str(carla_cfg.get("port", 2000))),
                        "--control-topic",
                        shlex.quote(topic),
                        "--control-type",
                        shlex.quote(control_out_type),
                        "--ego-role-name",
                        shlex.quote(ego_id),
                        "--max-steer-angle",
                        shlex.quote(str(control_bridge_cfg.get("max_steer_angle", 0.6))),
                        "--timeout-sec",
                        shlex.quote(str(control_bridge_cfg.get("timeout_sec", 0.8))),
                        "--speed-gain",
                        shlex.quote(str(control_bridge_cfg.get("speed_gain", 10.0))),
                        "--brake-gain",
                        shlex.quote(str(control_bridge_cfg.get("brake_gain", 5.0))),
                    ]
                )
                self.control_proc = self._launch(
                    "control",
                    control_cmd,
                    artifacts / "cyber_control_bridge.out.log",
                    artifacts / "cyber_control_bridge.err.log",
                    source_prefix=self._runtime_source_prefix if not docker_container else "",
                )
                print(f"[cyberrt] control bridge started pid={self.control_proc.pid}")
            else:
                print(f"[cyberrt][warn] control bridge script not found: {control_py}")

        time.sleep(1.0)
        return self.bridge_proc is not None and self.bridge_proc.poll() is None

    def _read_stats(self) -> tuple[bool, Optional[float], Dict[str, Any]]:
        stats_path = self._bridge_stats_path()
        if self._docker_container_name and self._docker_stats_path:
            cmd = f"cat {shlex.quote(self._docker_stats_path)}"
            try:
                out = self._docker_exec(cmd, capture_output=True).stdout
                stats_path.parent.mkdir(parents=True, exist_ok=True)
                stats_path.write_text(out)
                try:
                    return True, 0.0, json.loads(out)
                except Exception as exc:
                    return True, 0.0, {"_parse_error": str(exc)}
            except Exception:
                return False, None, {}
        if not stats_path.exists():
            return False, None, {}
        age = time.time() - stats_path.stat().st_mtime
        try:
            return True, age, json.loads(stats_path.read_text())
        except Exception as exc:
            return True, age, {"_parse_error": str(exc)}

    def _maybe_capture_channels(self, artifacts: Path) -> Dict[str, Any]:
        cfg = self._apollo_cfg()
        hc_cfg = cfg.get("healthcheck", {}) or {}
        if not hc_cfg.get("probe_channels", False):
            return {"enabled": False}
        prefix = self._runtime_source_prefix or self._source_prefix()
        cmd = f"{prefix}; timeout 5 cyber_channel list" if prefix else "timeout 5 cyber_channel list"
        out_path = artifacts / "cyber_channel_list.txt"
        try:
            if self._docker_container_name:
                out = self._docker_exec(cmd, capture_output=True).stdout
            else:
                out = subprocess.check_output(["bash", "-lc", cmd], cwd=self.repo_root, text=True)
            out_path.write_text(out)
            return {"enabled": True, "ok": True, "channels_count": len([x for x in out.splitlines() if x.strip()])}
        except Exception as exc:
            out_path.write_text(f"probe failed: {exc}\n")
            return {"enabled": True, "ok": False, "error": str(exc)}

    def health_check(self) -> bool:
        artifacts = self._artifacts_dir()
        stats_path = self._bridge_stats_path()
        stats_exists, stats_age, stats = self._read_stats()
        status: Dict[str, Any] = {
            "bridge_running": self.bridge_proc is not None and self.bridge_proc.poll() is None,
            "control_running": self.control_proc is not None and self.control_proc.poll() is None,
            "stats_path": str(stats_path),
            "stats_exists": stats_exists,
            "checked_at_sec": time.time(),
        }
        status["stats_age_sec"] = stats_age
        status["stats"] = stats
        status["channels_probe"] = self._maybe_capture_channels(artifacts)
        stat_obj = status.get("stats", {}) or {}
        loc_count = int(stat_obj.get("loc_count", 0) or 0)
        chassis_count = int(stat_obj.get("chassis_count", 0) or 0)
        obstacles_count = int(stat_obj.get("obstacles_count", 0) or 0)
        recent = bool(status["stats_exists"] and (status["stats_age_sec"] is not None) and status["stats_age_sec"] <= 2.5)
        status["recent_updates"] = recent
        status["has_publish"] = (loc_count + chassis_count + obstacles_count) > 0
        ok = bool(status["bridge_running"] and status["recent_updates"])
        (artifacts / "cyber_bridge_healthcheck.json").write_text(json.dumps(status, indent=2))
        return ok

    @staticmethod
    def _terminate(proc: Optional[subprocess.Popen], timeout_s: float = 8.0) -> None:
        if proc is None or proc.poll() is not None:
            return
        try:
            pgid = os.getpgid(proc.pid)
        except Exception:
            pgid = None
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGINT)
            else:
                proc.send_signal(signal.SIGINT)
            proc.wait(timeout=timeout_s)
            return
        except Exception:
            pass
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                proc.terminate()
            proc.wait(timeout=3.0)
            return
        except Exception:
            pass
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                proc.kill()
        except Exception:
            pass

    def stop(self) -> None:
        apollo_cfg = self._apollo_cfg()
        control_bridge_cfg = apollo_cfg.get("carla_control_bridge", {}) or {}
        topic = str(control_bridge_cfg.get("control_topic", "/tb/ego/control_cmd"))
        self._terminate(self.control_proc)
        self._terminate(self.bridge_proc)
        self._cleanup_stale_control_bridge(topic)
        if self._docker_container_name and self._docker_start_modules_enabled():
            try:
                patt = (
                    "modules/(routing/dag/routing.dag|prediction/dag/prediction.dag|"
                    "planning/planning_component/dag/planning.dag|control/control_component/dag/control.dag)"
                )
                self._docker_exec(
                    f"pkill -f '{patt}' >/dev/null 2>&1 || true; "
                    f"sleep 1; pkill -9 -f '{patt}' >/dev/null 2>&1 || true",
                    check=False,
                )
            except Exception:
                pass
        if self._docker_container_name and self._docker_stage_dir:
            try:
                pattern = shlex.quote(f"{self._docker_stage_dir}/tools/apollo10_cyber_bridge/bridge.py")
                self._docker_exec(f"pkill -f {pattern} >/dev/null 2>&1 || true", check=False)
            except Exception:
                pass
        self.control_proc = None
        self.bridge_proc = None
        self._docker_stage_dir = None
        self._docker_stats_path = None
        self._docker_container_name = None
        self._runtime_source_prefix = None
        for fp_attr in ("_bridge_out", "_bridge_err", "_control_out", "_control_err"):
            fp = getattr(self, fp_attr, None)
            if fp is not None:
                try:
                    fp.close()
                except Exception:
                    pass
                setattr(self, fp_attr, None)

    def _cleanup_stale_control_bridge(self, topic: str) -> None:
        pattern = (
            "algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py "
            f"--control-topic {topic} --control-type "
        )
        try:
            subprocess.run(["pkill", "-f", pattern], check=False)
        except Exception:
            pass
