#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BAZEL_ROOT = Path('/home/ubuntu/Apollo10.0/application-core/.cache/bazel')
DEFAULT_LIVE_DIR = Path(
    '/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/lib/modules/control/control_component'
)
LIB_NAMES = [
    'libcontrol_component.so',
    'libDO_NOT_IMPORT_control_component.so',
    'libapollo_control_lib.so',
]


def _sha256(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _discover_latest(root: Path, name: str) -> Optional[Path]:
    candidates = sorted(
        root.rglob(name),
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
    )
    return candidates[-1] if candidates else None


def _copy_with_backup(src: Path, dst: Path, backup_suffix: str) -> Dict[str, object]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    before_sha = _sha256(dst)
    src_sha = _sha256(src)
    changed = before_sha != src_sha
    backup_path: Optional[Path] = None
    if dst.exists() and changed:
        backup_path = dst.with_name(f'{dst.name}.bak_{backup_suffix}')
        shutil.copy2(dst, backup_path)
    if changed:
        shutil.copy2(src, dst)
    return {
        'source_path': str(src),
        'source_sha256': src_sha,
        'live_path': str(dst),
        'live_sha256_before': before_sha,
        'live_sha256_after': _sha256(dst),
        'changed': changed,
        'backup_path': str(backup_path) if backup_path else None,
    }


def _write_report(path: Path, payload: Dict[str, object]) -> None:
    lines: List[str] = [
        '# Apollo Control Runtime Artifact Deploy',
        '',
        f"- deployed_at: `{payload.get('deployed_at')}`",
        f"- bazel_root: `{payload.get('bazel_root')}`",
        f"- live_dir: `{payload.get('live_dir')}`",
        '',
        '## Results',
        '',
    ]
    for name in LIB_NAMES:
        item = (payload.get('libraries') or {}).get(name, {})
        lines.extend(
            [
                f"### `{name}`",
                '',
                f"- source_path: `{item.get('source_path')}`",
                f"- source_sha256: `{item.get('source_sha256')}`",
                f"- live_path: `{item.get('live_path')}`",
                f"- live_sha256_before: `{item.get('live_sha256_before')}`",
                f"- live_sha256_after: `{item.get('live_sha256_after')}`",
                f"- changed: `{item.get('changed')}`",
                f"- backup_path: `{item.get('backup_path') or 'n/a'}`",
                '',
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines), encoding='utf-8')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Deploy Apollo control wrapper plus implementation libraries from bazel outputs'
    )
    parser.add_argument('--bazel-root', type=Path, default=DEFAULT_BAZEL_ROOT)
    parser.add_argument('--live-dir', type=Path, default=DEFAULT_LIVE_DIR)
    parser.add_argument(
        '--json-out',
        type=Path,
        default=REPO_ROOT / 'artifacts' / 'apollo_control_runtime_deploy_manifest.json',
    )
    parser.add_argument(
        '--report-out',
        type=Path,
        default=REPO_ROOT / 'artifacts' / 'apollo_control_runtime_deploy_manifest.md',
    )
    args = parser.parse_args()

    bazel_root = args.bazel_root.expanduser().resolve()
    live_dir = args.live_dir.expanduser().resolve()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    libraries: Dict[str, object] = {}

    for name in LIB_NAMES:
        src = _discover_latest(bazel_root, name)
        if src is None:
            raise FileNotFoundError(f'failed to discover latest bazel artifact for {name} under {bazel_root}')
        dst = live_dir / name
        libraries[name] = _copy_with_backup(src, dst, timestamp)

    payload: Dict[str, object] = {
        'deployed_at': datetime.now().astimezone().isoformat(),
        'bazel_root': str(bazel_root),
        'live_dir': str(live_dir),
        'libraries': libraries,
    }
    json_out = args.json_out.expanduser().resolve()
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    _write_report(args.report_out.expanduser().resolve(), payload)
    print('[apollo-control-runtime-deploy] manifest written')


if __name__ == '__main__':
    main()
