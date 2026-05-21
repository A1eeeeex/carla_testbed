#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().isoformat()


def _sha256(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _line_of(text: str, needle: str) -> Optional[int]:
    for idx, line in enumerate(text.splitlines(), start=1):
        if needle in line:
            return idx
    return None


def _scan_text_markers(path: Path) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        'exists': path.exists(),
        'size_bytes': None,
        'mtime_epoch': None,
        'mtime_iso': None,
        'sha256': None,
        'markers': {},
    }
    if not path.exists():
        return payload
    stat = path.stat()
    text = path.read_text(encoding='utf-8', errors='ignore')
    payload.update(
        {
            'size_bytes': int(stat.st_size),
            'mtime_epoch': float(stat.st_mtime),
            'mtime_iso': _iso(float(stat.st_mtime)),
            'sha256': _sha256(path),
            'markers': {
                'planning_no_trajectory_log': {
                    'needle': 'planning has no trajectory point. planning_seq_num:',
                    'present': 'planning has no trajectory point. planning_seq_num:' in text,
                    'line': _line_of(text, 'planning has no trajectory point. planning_seq_num:'),
                },
                'safe_hold_publish': {
                    'needle': 'control_cmd_writer_->Write(control_command);',
                    'present': 'control_cmd_writer_->Write(control_command);' in text,
                    'line': _line_of(text, 'control_cmd_writer_->Write(control_command);'),
                },
            },
        }
    )
    return payload


def _scan_binary(path: Path) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        'exists': path.exists(),
        'size_bytes': None,
        'mtime_epoch': None,
        'mtime_iso': None,
        'sha256': None,
        'string_markers': {},
        'dynamic_symbols': {},
        'needed_libraries': [],
    }
    if not path.exists():
        return payload
    stat = path.stat()
    data = path.read_bytes()
    needles = [
        'planning has no trajectory point. planning_seq_num:',
        'Control waiting for first nonempty planning trajectory before enabling closed-loop control.',
        'Control entering safe hold because planning trajectory is temporarily empty.',
    ]
    nm_proc = subprocess.run(
        ['nm', '-D', '-C', str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    nm_text = nm_proc.stdout if nm_proc.returncode == 0 else ''
    objdump_proc = subprocess.run(
        ['objdump', '-p', str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    needed_libraries: List[str] = []
    if objdump_proc.returncode == 0:
        for line in objdump_proc.stdout.splitlines():
            if 'NEEDED' in line:
                needed_libraries.append(line.split()[-1].strip())
    symbol_needles = [
        'apollo::control::ControlComponent::Proc()',
        'apollo::control::ControlComponent::ProduceControlCommand(apollo::control::ControlCommand*)',
        'apollo::control::ControlComponent::CheckInput(apollo::control::LocalView*)',
    ]
    payload.update(
        {
            'size_bytes': int(stat.st_size),
            'mtime_epoch': float(stat.st_mtime),
            'mtime_iso': _iso(float(stat.st_mtime)),
            'sha256': _sha256(path),
            'string_markers': {needle: (needle.encode('utf-8') in data) for needle in needles},
            'dynamic_symbols': {needle: (needle in nm_text) for needle in symbol_needles},
            'needed_libraries': needed_libraries,
        }
    )
    return payload


def _docker_inspect(container_name: str) -> Dict[str, Any]:
    cmd = [
        'docker',
        'inspect',
        '-f',
        '{{.Name}} {{.Config.Image}} {{.State.Status}}',
        container_name,
    ]
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    except Exception as exc:
        return {'available': False, 'error': str(exc)}
    if proc.returncode != 0:
        return {
            'available': False,
            'returncode': int(proc.returncode),
            'stderr': proc.stderr.strip(),
        }
    parts = proc.stdout.strip().split(maxsplit=2)
    if len(parts) != 3:
        return {'available': False, 'stdout': proc.stdout.strip()}
    return {
        'available': True,
        'name': parts[0],
        'image': parts[1],
        'status': parts[2],
    }


def collect_report(args: argparse.Namespace) -> Dict[str, Any]:
    source_path = Path(args.source).expanduser().resolve()
    wrapper_library_path = Path(args.library).expanduser().resolve()
    wrapper_backup_path = Path(args.backup_library).expanduser().resolve()
    impl_library_path = Path(args.impl_library).expanduser().resolve()
    impl_backup_path = Path(args.impl_backup_library).expanduser().resolve()
    control_lib_path = Path(args.apollo_control_lib).expanduser().resolve()
    control_lib_backup_path = Path(args.apollo_control_lib_backup).expanduser().resolve()

    source = _scan_text_markers(source_path)
    wrapper_library = _scan_binary(wrapper_library_path)
    wrapper_backup = _scan_binary(wrapper_backup_path)
    impl_library = _scan_binary(impl_library_path)
    impl_backup = _scan_binary(impl_backup_path)
    control_lib = _scan_binary(control_lib_path)
    control_lib_backup = _scan_binary(control_lib_backup_path)
    docker_state = _docker_inspect(args.container_name)

    wrapper_same_hash_as_backup = (
        bool(wrapper_library.get('sha256'))
        and bool(wrapper_backup.get('sha256'))
        and str(wrapper_library.get('sha256')) == str(wrapper_backup.get('sha256'))
    )
    impl_same_hash_as_backup = (
        bool(impl_library.get('sha256'))
        and bool(impl_backup.get('sha256'))
        and str(impl_library.get('sha256')) == str(impl_backup.get('sha256'))
    )
    control_lib_same_hash_as_backup = (
        bool(control_lib.get('sha256'))
        and bool(control_lib_backup.get('sha256'))
        and str(control_lib.get('sha256')) == str(control_lib_backup.get('sha256'))
    )
    source_newer_than_impl_library = (
        source.get('mtime_epoch') is not None
        and impl_library.get('mtime_epoch') is not None
        and float(source['mtime_epoch']) > float(impl_library['mtime_epoch'])
    )
    impl_library_newer_or_equal_to_source = (
        source.get('mtime_epoch') is not None
        and impl_library.get('mtime_epoch') is not None
        and float(impl_library['mtime_epoch']) >= float(source['mtime_epoch'])
    )
    container_running = bool(docker_state.get('available')) and str(docker_state.get('status')) == 'running'
    safe_hold_visible_in_source = bool(
        source.get('markers', {}).get('planning_no_trajectory_log', {}).get('present')
        and source.get('markers', {}).get('safe_hold_publish', {}).get('present')
    )
    wrapper_points_to_impl = 'libDO_NOT_IMPORT_control_component.so' in list(
        wrapper_library.get('needed_libraries') or []
    )
    impl_has_control_component_symbols = bool(
        impl_library.get('dynamic_symbols', {}).get(
            'apollo::control::ControlComponent::Proc()'
        )
    )
    safe_hold_visible_in_impl_strings = bool(
        impl_library.get('string_markers', {}).get(
            'Control waiting for first nonempty planning trajectory before enabling closed-loop control.'
        )
        and impl_library.get('string_markers', {}).get(
            'Control entering safe hold because planning trajectory is temporarily empty.'
        )
    )
    planning_no_trajectory_visible_in_impl_strings = bool(
        impl_library.get('string_markers', {}).get(
            'planning has no trajectory point. planning_seq_num:'
        )
    )
    deployment_likely_updated = bool(
        safe_hold_visible_in_source
        and wrapper_points_to_impl
        and impl_has_control_component_symbols
        and impl_library_newer_or_equal_to_source
        and not impl_same_hash_as_backup
    )
    deployment_confirmed = bool(
        deployment_likely_updated
        and safe_hold_visible_in_impl_strings
        and container_running
    )
    why_not_confirmed = []
    if not safe_hold_visible_in_source:
        why_not_confirmed.append('current control source does not show the expected safe-hold markers')
    if not wrapper_points_to_impl:
        why_not_confirmed.append('wrapper control component library does not point to libDO_NOT_IMPORT_control_component.so')
    if not impl_has_control_component_symbols:
        why_not_confirmed.append('implementation library does not surface ControlComponent runtime symbols')
    if source_newer_than_impl_library:
        why_not_confirmed.append('current control source is newer than the installed implementation library')
    if impl_same_hash_as_backup:
        why_not_confirmed.append('installed implementation library hash is identical to the old backup snapshot')
    if control_lib_same_hash_as_backup:
        why_not_confirmed.append('installed apollo_control_lib hash is identical to the old backup snapshot')
    if not planning_no_trajectory_visible_in_impl_strings:
        why_not_confirmed.append('implementation library string scan does not surface the planning_seq_num empty-trajectory marker')
    if not safe_hold_visible_in_impl_strings:
        why_not_confirmed.append('implementation library string scan does not surface the expected safe-hold markers')
    if not container_running:
        why_not_confirmed.append('runtime Apollo container is not currently running')

    return {
        'checked_at': datetime.now().astimezone().isoformat(),
        'container_name': args.container_name,
        'source_path': str(source_path),
        'wrapper_library_path': str(wrapper_library_path),
        'wrapper_backup_library_path': str(wrapper_backup_path),
        'impl_library_path': str(impl_library_path),
        'impl_backup_library_path': str(impl_backup_path),
        'apollo_control_lib_path': str(control_lib_path),
        'apollo_control_lib_backup_path': str(control_lib_backup_path),
        'docker_container': docker_state,
        'source': source,
        'wrapper_library': wrapper_library,
        'wrapper_backup_library': wrapper_backup,
        'impl_library': impl_library,
        'impl_backup_library': impl_backup,
        'apollo_control_lib': control_lib,
        'apollo_control_lib_backup': control_lib_backup,
        'wrapper_same_hash_as_backup': wrapper_same_hash_as_backup,
        'impl_same_hash_as_backup': impl_same_hash_as_backup,
        'apollo_control_lib_same_hash_as_backup': control_lib_same_hash_as_backup,
        'source_newer_than_impl_library': source_newer_than_impl_library,
        'impl_library_newer_or_equal_to_source': impl_library_newer_or_equal_to_source,
        'deployment_judgment': {
            'safe_hold_visible_in_source': safe_hold_visible_in_source,
            'wrapper_points_to_impl': wrapper_points_to_impl,
            'impl_has_control_component_symbols': impl_has_control_component_symbols,
            'planning_no_trajectory_visible_in_impl_strings': planning_no_trajectory_visible_in_impl_strings,
            'safe_hold_visible_in_impl_strings': safe_hold_visible_in_impl_strings,
            'deployment_likely_updated': deployment_likely_updated,
            'deployment_confirmed': deployment_confirmed,
            'why_not_confirmed': why_not_confirmed,
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def write_report(path: Path, payload: Dict[str, Any]) -> None:
    docker_state = payload.get('docker_container') or {}
    source = payload.get('source') or {}
    wrapper_library = payload.get('wrapper_library') or {}
    wrapper_backup = payload.get('wrapper_backup_library') or {}
    impl_library = payload.get('impl_library') or {}
    impl_backup = payload.get('impl_backup_library') or {}
    control_lib = payload.get('apollo_control_lib') or {}
    control_lib_backup = payload.get('apollo_control_lib_backup') or {}
    judgment = payload.get('deployment_judgment') or {}
    reasons = [item for item in list(judgment.get('why_not_confirmed') or []) if item]
    lines = [
        '# Apollo Control Safe-Hold Deployment Check',
        '',
        f"- checked_at: `{payload.get('checked_at')}`",
        f"- container_name: `{payload.get('container_name')}`",
        f"- source_newer_than_impl_library: `{payload.get('source_newer_than_impl_library')}`",
        f"- impl_library_newer_or_equal_to_source: `{payload.get('impl_library_newer_or_equal_to_source')}`",
        f"- wrapper_same_hash_as_backup: `{payload.get('wrapper_same_hash_as_backup')}`",
        f"- impl_same_hash_as_backup: `{payload.get('impl_same_hash_as_backup')}`",
        f"- apollo_control_lib_same_hash_as_backup: `{payload.get('apollo_control_lib_same_hash_as_backup')}`",
        f"- deployment_likely_updated: `{judgment.get('deployment_likely_updated')}`",
        f"- deployment_confirmed: `{judgment.get('deployment_confirmed')}`",
        '',
        '## Runtime Container',
        '',
        f"- available: `{docker_state.get('available')}`",
        f"- image: `{docker_state.get('image') or 'n/a'}`",
        f"- status: `{docker_state.get('status') or 'n/a'}`",
        '',
        '## Source',
        '',
        f"- path: `{payload.get('source_path')}`",
        f"- mtime: `{source.get('mtime_iso') or 'n/a'}`",
        f"- sha256: `{source.get('sha256') or 'n/a'}`",
        f"- planning_no_trajectory_log_line: `{source.get('markers', {}).get('planning_no_trajectory_log', {}).get('line')}`",
        f"- safe_hold_publish_line: `{source.get('markers', {}).get('safe_hold_publish', {}).get('line')}`",
        '',
        '## Libraries',
        '',
        f"- wrapper_library: `{payload.get('wrapper_library_path')}`",
        f"- wrapper_library_mtime: `{wrapper_library.get('mtime_iso') or 'n/a'}`",
        f"- wrapper_library_sha256: `{wrapper_library.get('sha256') or 'n/a'}`",
        f"- wrapper_backup_library: `{payload.get('wrapper_backup_library_path')}`",
        f"- wrapper_backup_library_mtime: `{wrapper_backup.get('mtime_iso') or 'n/a'}`",
        f"- wrapper_backup_library_sha256: `{wrapper_backup.get('sha256') or 'n/a'}`",
        f"- impl_library: `{payload.get('impl_library_path')}`",
        f"- impl_library_mtime: `{impl_library.get('mtime_iso') or 'n/a'}`",
        f"- impl_library_sha256: `{impl_library.get('sha256') or 'n/a'}`",
        f"- impl_backup_library: `{payload.get('impl_backup_library_path')}`",
        f"- impl_backup_library_mtime: `{impl_backup.get('mtime_iso') or 'n/a'}`",
        f"- impl_backup_library_sha256: `{impl_backup.get('sha256') or 'n/a'}`",
        f"- apollo_control_lib: `{payload.get('apollo_control_lib_path')}`",
        f"- apollo_control_lib_mtime: `{control_lib.get('mtime_iso') or 'n/a'}`",
        f"- apollo_control_lib_sha256: `{control_lib.get('sha256') or 'n/a'}`",
        f"- apollo_control_lib_backup: `{payload.get('apollo_control_lib_backup_path')}`",
        f"- apollo_control_lib_backup_mtime: `{control_lib_backup.get('mtime_iso') or 'n/a'}`",
        f"- apollo_control_lib_backup_sha256: `{control_lib_backup.get('sha256') or 'n/a'}`",
        '',
        '## Binary Marker Scan',
        '',
    ]
    for needle, present in sorted((wrapper_library.get('string_markers') or {}).items()):
        lines.append(f"- wrapper `{needle}`: `{present}`")
    for needle, present in sorted((impl_library.get('string_markers') or {}).items()):
        lines.append(f"- impl `{needle}`: `{present}`")
    for needle, present in sorted((impl_library.get('dynamic_symbols') or {}).items()):
        lines.append(f"- impl symbol `{needle}`: `{present}`")
    lines.append(f"- wrapper needs `libDO_NOT_IMPORT_control_component.so`: `{judgment.get('wrapper_points_to_impl')}`")
    lines.extend(
        [
            '',
            '## Judgment',
            '',
            '- `libcontrol_component.so` is only the wrapper plugin. The runtime implementation lives in `libDO_NOT_IMPORT_control_component.so`.',
            '- Deployment is considered confirmed only when the implementation library exposes `ControlComponent` symbols, carries the safe-hold strings, is not identical to the old backup snapshot, and the Apollo container is running.',
            '',
            '## Why Deployment Is Not Confirmed',
            '',
        ]
    )
    if reasons:
        for reason in reasons:
            lines.append(f'- {reason}')
    else:
        lines.append('- no blocking reasons recorded')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> None:
    parser = argparse.ArgumentParser(description='Check whether Apollo control safe-hold source changes are deployed')
    parser.add_argument(
        '--source',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src/modules/control/control_component/control_component.cc',
    )
    parser.add_argument(
        '--library',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so',
    )
    parser.add_argument(
        '--backup-library',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so.bak_20260324_100639',
    )
    parser.add_argument(
        '--impl-library',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component/libDO_NOT_IMPORT_control_component.so',
    )
    parser.add_argument(
        '--impl-backup-library',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component/libDO_NOT_IMPORT_control_component.so.bak_20260324_100639',
    )
    parser.add_argument(
        '--apollo-control-lib',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component/libapollo_control_lib.so',
    )
    parser.add_argument(
        '--apollo-control-lib-backup',
        default='/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component/libapollo_control_lib.so.bak_20260324_100639',
    )
    parser.add_argument('--container-name', default='apollo_neo_dev_10.0.0_pkg')
    parser.add_argument(
        '--json-out',
        type=Path,
        default=REPO_ROOT / 'artifacts' / 'apollo_control_safe_hold_deployment_check.json',
    )
    parser.add_argument(
        '--report-out',
        type=Path,
        default=REPO_ROOT / 'artifacts' / 'apollo_control_safe_hold_deployment_check.md',
    )
    args = parser.parse_args()

    payload = collect_report(args)
    write_json(Path(args.json_out).expanduser().resolve(), payload)
    write_report(Path(args.report_out).expanduser().resolve(), payload)
    print('[apollo-control-safe-hold-check] report written')


if __name__ == '__main__':
    main()
