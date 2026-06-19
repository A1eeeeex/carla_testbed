#!/usr/bin/env python3
# OPERATIONAL HELPER: local Apollo/CyberRT smoke probe, not a platform architecture layer.
# Do not add new platform logic here; move reusable code into carla_testbed.adapters.apollo diagnostics.
# Migration target: carla_testbed.adapters.apollo runtime diagnostics.
"""Run a minimal Apollo control runtime smoke inside the Apollo container.

This tool is intentionally narrow: it starts only the control component DAG
inside the target container and captures whether the process crashes early.
It is useful for comparing the current live Cyber runtime against a legacy
`libcyber.so` backup without burning a full Town01 online run.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import textwrap
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


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
    exact_overlay_enabled: bool
    exact_overlay_selected_count: int
    exact_overlay_missing_count: int
    exact_overlay_restored_count: int
    exact_overlay_records: list[dict[str, str]]
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
        "--exact-overlay-manifest",
        default="",
        help=(
            "Optional apollo_control_runtime_overlay_manifest.json from an online run. "
            "When set, the smoke temporarily copies each selected source file to its "
            "exact target path inside the container, then restores originals on exit."
        ),
    )
    parser.add_argument(
        "--exact-overlay-name-regex",
        default="",
        help="Optional regex filter applied to exact overlay record names.",
    )
    parser.add_argument(
        "--exact-overlay-target-regex",
        default="",
        help="Optional regex filter applied to exact overlay record target paths.",
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


def _load_exact_overlay_records(
    manifest_path: str,
    *,
    name_regex: str = "",
    target_regex: str = "",
) -> list[dict[str, str]]:
    if not manifest_path:
        return []
    path = Path(manifest_path).expanduser().resolve()
    data = json.loads(path.read_text(encoding="utf-8"))
    raw_records = list(data.get("records") or [])
    name_filter = re.compile(name_regex) if name_regex else None
    target_filter = re.compile(target_regex) if target_regex else None
    records: list[dict[str, str]] = []
    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or Path(str(raw.get("target") or "")).name)
        source = str(raw.get("source") or "")
        target = str(raw.get("target") or "")
        if not source or not target:
            continue
        if name_filter and not name_filter.search(name):
            continue
        if target_filter and not target_filter.search(target):
            continue
        records.append({"name": name, "source": source, "target": target})
    if not records:
        raise ValueError(
            "exact overlay manifest selected no records; check manifest path and regex filters"
        )
    return records


def _parse_exact_overlay_stats(text: str) -> dict[str, Any]:
    marker = "TB_EXACT_OVERLAY_RESULT="
    stats: dict[str, Any] = {}
    for line in text.splitlines():
        if not line.startswith(marker):
            continue
        try:
            candidate = json.loads(line[len(marker) :])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            stats.update(candidate)
    return stats


def _exact_overlay_shell(variant: str, exact_overlay_records: Sequence[dict[str, str]]) -> str:
    if not exact_overlay_records:
        return ""
    records_json = json.dumps(list(exact_overlay_records), ensure_ascii=False, indent=2)
    safe_variant = "".join(ch if ch.isalnum() else "_" for ch in variant)
    return (
        f"exact_records=/tmp/tb_control_exact_overlay_records_{safe_variant}.json\n"
        f"exact_state=/tmp/tb_control_exact_overlay_state_{safe_variant}.json\n"
        "cat > \"$exact_records\" <<'TB_EXACT_OVERLAY_RECORDS'\n"
        f"{records_json}\n"
        "TB_EXACT_OVERLAY_RECORDS\n"
        "restore_exact_overlay() {\n"
        "python3 - \"$exact_state\" <<'PY'\n"
        "import json, os, shutil, sys\n"
        "from pathlib import Path\n"
        "\n"
        "state_path = Path(sys.argv[1])\n"
        "payload = {\n"
        "    \"stage\": \"restore\",\n"
        "    \"enabled\": state_path.exists(),\n"
        "    \"restored_count\": 0,\n"
        "    \"missing_backup_count\": 0,\n"
        "}\n"
        "if state_path.exists():\n"
        "    state = json.loads(state_path.read_text())\n"
        "    for record in state.get(\"records\") or []:\n"
        "        backup = Path(str(record.get(\"backup\") or \"\"))\n"
        "        target = Path(str(record.get(\"target\") or \"\"))\n"
        "        if not backup.exists() or not target:\n"
        "            payload[\"missing_backup_count\"] += 1\n"
        "            continue\n"
        "        tmp_target = target.parent / (target.name + \".codex_smoke_restore_tmp\")\n"
        "        shutil.copy2(backup, tmp_target)\n"
        "        os.replace(tmp_target, target)\n"
        "        payload[\"restored_count\"] += 1\n"
        "    backup_root = Path(str(state.get(\"backup_root\") or \"\"))\n"
        "    if backup_root.exists():\n"
        "        shutil.rmtree(backup_root, ignore_errors=True)\n"
        "    state_path.unlink(missing_ok=True)\n"
        "print(\"TB_EXACT_OVERLAY_RESULT=\" + json.dumps(payload, ensure_ascii=False))\n"
        "PY\n"
        "}\n"
        "trap restore_exact_overlay EXIT\n"
        "python3 - \"$exact_records\" \"$exact_state\" <<'PY'\n"
        "import json, os, shutil, sys, time\n"
        "from pathlib import Path\n"
        "\n"
        "records_path = Path(sys.argv[1])\n"
        "state_path = Path(sys.argv[2])\n"
        "requested = json.loads(records_path.read_text())\n"
        f"backup_root = Path(f\"/tmp/tb_control_exact_overlay_{safe_variant}_{{int(time.time())}}\")\n"
        "backup_root.mkdir(parents=True, exist_ok=True)\n"
        "staged = []\n"
        "missing = []\n"
        "for idx, record in enumerate(requested):\n"
        "    source = Path(str(record.get(\"source\") or \"\"))\n"
        "    target = Path(str(record.get(\"target\") or \"\"))\n"
        "    name = str(record.get(\"name\") or target.name)\n"
        "    if not source.exists() or not target.exists():\n"
        "        missing.append({\n"
        "            \"name\": name,\n"
        "            \"source\": str(source),\n"
        "            \"target\": str(target),\n"
        "            \"source_exists\": source.exists(),\n"
        "            \"target_exists\": target.exists(),\n"
        "        })\n"
        "        continue\n"
        "    backup = backup_root / f\"{idx:03d}_{target.name}\"\n"
        "    shutil.copy2(target, backup)\n"
        "    tmp_target = target.parent / (target.name + \".codex_smoke_overlay_tmp\")\n"
        "    shutil.copy2(source, tmp_target)\n"
        "    os.replace(tmp_target, target)\n"
        "    staged.append({\n"
        "        \"name\": name,\n"
        "        \"source\": str(source),\n"
        "        \"target\": str(target),\n"
        "        \"backup\": str(backup),\n"
        "    })\n"
        "state_path.write_text(json.dumps({\n"
        "    \"backup_root\": str(backup_root),\n"
        "    \"records\": staged,\n"
        "    \"missing\": missing,\n"
        "}, indent=2, ensure_ascii=False))\n"
        "print(\"TB_EXACT_OVERLAY_RESULT=\" + json.dumps({\n"
        "    \"stage\": \"apply\",\n"
        "    \"enabled\": True,\n"
        "    \"requested_count\": len(requested),\n"
        "    \"selected_count\": len(staged),\n"
        "    \"missing_count\": len(missing),\n"
        "    \"missing\": missing,\n"
        "}, ensure_ascii=False))\n"
        "PY"
    )


def _shell_script(
    *,
    timeout_sec: float,
    legacy: bool,
    legacy_backup_path: str,
    control_runtime_source_dir: str,
    exact_overlay_records: Sequence[dict[str, str]],
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
    exact_overlay_setup = _exact_overlay_shell(variant, exact_overlay_records)
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
        {exact_overlay_setup}
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
    exact_overlay_records: Sequence[dict[str, str]],
    runner: str,
    variant: str,
) -> SmokeResult:
    script = _shell_script(
        timeout_sec=timeout_sec,
        legacy=legacy,
        legacy_backup_path=legacy_backup_path,
        control_runtime_source_dir=control_runtime_source_dir,
        exact_overlay_records=exact_overlay_records,
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
    exact_stats = _parse_exact_overlay_stats(stdout + "\n" + stderr)
    crash_detected, crash_reason = _classify_crash(stdout + "\n" + stderr + "\n" + log_tail)
    return SmokeResult(
        variant=variant,
        container=container,
        timeout_sec=timeout_sec,
        legacy_libcyber_enabled=legacy,
        control_runtime_overlay_enabled=bool(control_runtime_source_dir),
        control_runtime_source_dir=str(control_runtime_source_dir or ""),
        exact_overlay_enabled=bool(exact_overlay_records),
        exact_overlay_selected_count=int(exact_stats.get("selected_count") or 0),
        exact_overlay_missing_count=int(exact_stats.get("missing_count") or 0),
        exact_overlay_restored_count=int(exact_stats.get("restored_count") or 0),
        exact_overlay_records=list(exact_overlay_records),
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
    exact_overlay_records = _load_exact_overlay_records(
        str(args.exact_overlay_manifest),
        name_regex=str(args.exact_overlay_name_regex),
        target_regex=str(args.exact_overlay_target_regex),
    )
    results = [
        _run_variant(
            container=args.container,
            timeout_sec=float(args.timeout_sec),
            legacy=legacy,
            legacy_backup_path=str(args.legacy_libcyber_backup_path),
            control_runtime_source_dir=str(args.control_runtime_source_dir),
            exact_overlay_records=exact_overlay_records,
            runner=str(args.runner),
            variant=variant,
        )
        for variant, legacy in _variant_modes(str(args.mode))
    ]
    for result in results:
        print(f"=== {result.variant} ===")
        print(f"returncode={result.returncode}")
        print(f"legacy_libcyber_enabled={result.legacy_libcyber_enabled}")
        print(f"exact_overlay_enabled={result.exact_overlay_enabled}")
        print(f"exact_overlay_selected_count={result.exact_overlay_selected_count}")
        print(f"exact_overlay_missing_count={result.exact_overlay_missing_count}")
        print(f"exact_overlay_restored_count={result.exact_overlay_restored_count}")
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
