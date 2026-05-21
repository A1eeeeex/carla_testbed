#!/usr/bin/env python3
"""Run a minimal Apollo control runtime smoke inside the Apollo container.

This tool is intentionally narrow: it starts only the control component DAG
inside the target container and captures whether the process crashes early.
It is useful for comparing the current live Cyber runtime against a legacy
`libcyber.so` backup without burning a full Town01 online run.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import textwrap
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


DEFAULT_CONTAINER = "apollo_neo_dev_10.0.0_pkg"
DEFAULT_BACKUP = "/opt/apollo/neo/lib/cyber/libcyber.so.bak_20260401_080826"


@dataclass
class SmokeResult:
    variant: str
    container: str
    timeout_sec: float
    legacy_libcyber_enabled: bool
    control_runtime_overlay_enabled: bool
    control_runtime_source_dir: str
    returncode: int
    shell_stdout: str
    shell_stderr: str
    log_tail: str
    crash_detected: bool
    crash_reason: str
    runner: str


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--container",
        default=DEFAULT_CONTAINER,
        help="Docker container name running Apollo.",
    )
    parser.add_argument(
        "--mode",
        choices=("current", "legacy", "both"),
        default="both",
        help="Which runtime variant to probe.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=6.0,
        help="How long to let the control DAG run before timeout.",
    )
    parser.add_argument(
        "--runner",
        choices=("mainboard", "gdb"),
        default="mainboard",
        help="How to run the control smoke. 'gdb' collects a backtrace on crash.",
    )
    parser.add_argument(
        "--legacy-libcyber-backup-path",
        default=DEFAULT_BACKUP,
        help="Backup libcyber.so path inside the container for the legacy probe.",
    )
    parser.add_argument(
        "--control-runtime-source-dir",
        default="",
        help=(
            "Optional backup control-runtime directory inside the container. When set, "
            "all .so files under this directory are copied into a temporary overlay and "
            "prepended to LD_LIBRARY_PATH before running the smoke."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional directory to write JSON/text artifacts.",
    )
    return parser


def _variant_modes(mode: str) -> list[tuple[str, bool]]:
    if mode == "current":
        return [("current", False)]
    if mode == "legacy":
        return [("legacy", True)]
    return [("current", False), ("legacy", True)]


def _shell_script(
    *,
    timeout_sec: float,
    legacy: bool,
    legacy_backup_path: str,
    control_runtime_source_dir: str,
    runner: str,
    variant: str,
) -> str:
    process_group = "control_smoke_" + "".join(ch if ch.isalnum() else "_" for ch in variant)
    legacy_setup = ""
    overlay_setup = ""
    ld_prefix_parts = ["/opt/apollo/neo/lib/cyber"]
    if legacy:
        legacy_setup = textwrap.dedent(
            f"""
            mkdir -p /tmp/tb_legacy_libcyber
            cp -f {legacy_backup_path} /tmp/tb_legacy_libcyber/libcyber.so
            """
        ).strip()
        ld_prefix_parts.insert(0, "/tmp/tb_legacy_libcyber")
    if control_runtime_source_dir:
        overlay_setup = textwrap.dedent(
            f"""
            overlay_dir=/tmp/tb_control_runtime_{variant}
            rm -rf "$overlay_dir"
            mkdir -p "$overlay_dir"
            while IFS= read -r -d '' item; do
              cp -f "$item" "$overlay_dir/$(basename "$item")"
            done < <(find {control_runtime_source_dir} -type f -name '*.so' -print0)
            """
        ).strip()
        ld_prefix_parts.insert(0, "$overlay_dir")
    ld_prefix = ":".join(ld_prefix_parts)
    runner_cmd = (
        "timeout "
        f"{timeout_sec:.3f} "
        "mainboard -d modules/control/control_component/dag/control.dag "
        f"-p {process_group} -s CYBER_DEFAULT"
    )
    if runner == "gdb":
        runner_cmd = (
            "timeout "
            f"{timeout_sec:.3f} "
            "gdb -batch -ex 'set pagination off' -ex run -ex bt "
            "--args mainboard -d modules/control/control_component/dag/control.dag "
            f"-p {process_group} -s CYBER_DEFAULT"
        )
    return textwrap.dedent(
        f"""
        set -o pipefail
        pkill -f 'modules/control/control_component/dag/control.dag' >/dev/null 2>&1 || true
        {legacy_setup}
        {overlay_setup}
        export APOLLO_ROOT=/apollo
        export APOLLO_ROOT_DIR=/apollo
        export APOLLO_DISTRIBUTION_HOME=/opt/apollo/neo
        [ -f /opt/apollo/neo/setup.sh ] && source /opt/apollo/neo/setup.sh
        [ -f /apollo/cyber/setup.bash ] && source /apollo/cyber/setup.bash
        export APOLLO_PLUGIN_INDEX_PATH=/opt/apollo/neo/share/cyber_plugin_index
        export CYBER_DOMAIN_ID=80
        export LD_LIBRARY_PATH={ld_prefix}:/opt/apollo/neo/lib:/opt/apollo/neo/lib64:/opt/apollo/neo/lib/third_party/rtklib
        cd /apollo
        log=/tmp/tb_control_smoke_{variant}.log
        rm -f "$log"
        {runner_cmd} >"$log" 2>&1
        rc=$?
        echo "RC=$rc"
        echo "---LOG---"
        tail -80 "$log" || true
        pkill -f 'modules/control/control_component/dag/control.dag' >/dev/null 2>&1 || true
        """
    ).strip() + "\n"


def _classify_crash(text: str) -> tuple[bool, str]:
    lowered = text.lower()
    if "attempt to free invalid pointer" in lowered:
        return True, "tcmalloc_invalid_free"
    if "segmentation fault" in lowered or "sigsegv" in lowered:
        return True, "segfault"
    if "error while loading shared libraries" in lowered:
        return True, "shared_library_error"
    return False, ""


def _run_variant(
    *,
    container: str,
    timeout_sec: float,
    legacy: bool,
    legacy_backup_path: str,
    control_runtime_source_dir: str,
    runner: str,
    variant: str,
) -> SmokeResult:
    script = _shell_script(
        timeout_sec=timeout_sec,
        legacy=legacy,
        legacy_backup_path=legacy_backup_path,
        control_runtime_source_dir=control_runtime_source_dir,
        runner=runner,
        variant=variant,
    )
    proc = subprocess.run(
        ["docker", "exec", "-i", container, "bash", "-s"],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    log_tail = ""
    if "---LOG---" in stdout:
        log_tail = stdout.split("---LOG---", 1)[1].lstrip()
    crash_detected, crash_reason = _classify_crash(stdout + "\n" + stderr + "\n" + log_tail)
    return SmokeResult(
        variant=variant,
        container=container,
        timeout_sec=timeout_sec,
        legacy_libcyber_enabled=legacy,
        control_runtime_overlay_enabled=bool(control_runtime_source_dir),
        control_runtime_source_dir=str(control_runtime_source_dir or ""),
        returncode=int(proc.returncode),
        shell_stdout=stdout,
        shell_stderr=stderr,
        log_tail=log_tail,
        crash_detected=crash_detected,
        crash_reason=crash_reason,
        runner=runner,
    )


def _write_outputs(output_dir: Path, results: Sequence[SmokeResult]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    payload = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "results": [asdict(item) for item in results],
    }
    (output_dir / f"apollo_control_runtime_smoke_{stamp}.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    for item in results:
        (output_dir / f"apollo_control_runtime_smoke_{stamp}_{item.variant}.log").write_text(
            item.shell_stdout + ("\n---STDERR---\n" + item.shell_stderr if item.shell_stderr else ""),
            encoding="utf-8",
        )


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    results = [
        _run_variant(
            container=args.container,
            timeout_sec=float(args.timeout_sec),
            legacy=legacy,
            legacy_backup_path=str(args.legacy_libcyber_backup_path),
            control_runtime_source_dir=str(args.control_runtime_source_dir),
            runner=str(args.runner),
            variant=variant,
        )
        for variant, legacy in _variant_modes(str(args.mode))
    ]
    for result in results:
        print(f"=== {result.variant} ===")
        print(f"returncode={result.returncode}")
        print(f"legacy_libcyber_enabled={result.legacy_libcyber_enabled}")
        print(f"runner={result.runner}")
        print(f"crash_detected={result.crash_detected}")
        print(f"crash_reason={result.crash_reason or 'none'}")
        if result.shell_stdout.strip():
            print(result.shell_stdout.rstrip())
        if result.shell_stderr.strip():
            print("---STDERR---")
            print(result.shell_stderr.rstrip())
    if args.output_dir:
        _write_outputs(Path(str(args.output_dir)).expanduser().resolve(), results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
