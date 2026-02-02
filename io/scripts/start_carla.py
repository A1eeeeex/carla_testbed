#!/usr/bin/env python3
from __future__ import annotations

import sys
import time
import importlib.util
from pathlib import Path

# reuse followstop parser and defaults
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_from_path(py_path: Path, attr: str):
    spec = importlib.util.spec_from_file_location(py_path.stem, py_path)
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, attr)
    raise ImportError(f"cannot load {attr} from {py_path}")


build_arg_parser = _load_from_path(ROOT / "examples" / "run_followstop.py", "build_arg_parser")
_wait_for_carla = _load_from_path(ROOT / "examples" / "run_followstop.py", "_wait_for_carla")
CarlaLauncher = _load_from_path(ROOT / "io" / "carla" / "launcher.py", "CarlaLauncher")


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    ts = int(time.time())
    run_dir = args.run_dir or (ROOT / "runs" / f"carla_launch_{ts}")
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    if not args.start_carla:
        ok = _wait_for_carla(args.host, args.port, timeout=5.0)
        if ok:
            print(f"[carla] already running at {args.host}:{args.port}")
            raise SystemExit(0)
        else:
            print(f"[carla][ERROR] not running at {args.host}:{args.port}; add --start-carla to launch")
            raise SystemExit(1)

    launcher = CarlaLauncher(
        carla_root=args.carla_root,
        host=args.host,
        port=args.port,
        town=args.town,
        extra_args=args.carla_extra_args,
        foreground=args.carla_foreground,
        run_dir=run_dir,
    )
    try:
        launcher.start()
        if launcher.wait_ready(timeout_s=180.0, poll_s=1.0):
            print(f"[carla] ready at {args.host}:{args.port}")
            raise SystemExit(0)
        else:
            print("[carla][ERROR] CARLA 未在超时时间内就绪")
            print(launcher.diagnose_tail())
            raise SystemExit(1)
    except Exception as exc:
        print(f"[carla][ERROR] launch failed: {exc}")
        print(launcher.diagnose_tail())
        raise SystemExit(1)
    finally:
        # do not stop if reused
        launcher.stop()


if __name__ == "__main__":
    main()
