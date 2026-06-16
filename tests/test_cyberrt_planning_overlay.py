from __future__ import annotations

import shlex
import subprocess

from tbio.backends.cyberrt import CyberRTBackend


def _extract_python_overlay_script(shell: str) -> str:
    marker = "python3 -c "
    assert marker in shell
    return shlex.split(shell)[2]


def test_planning_overlay_can_disable_control_interactive_replan() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {
                        "enable_control_interactive_replan": False,
                    }
                }
            }
        }
    )

    shell = backend._planning_flags_overlay_shell()
    script = _extract_python_overlay_script(shell)

    assert "--enable_control_interactive_replan=false" in shell
    assert "prefixes.append('--enable_control_interactive_replan=')" in script


def test_planning_overlay_default_does_not_touch_control_interactive_replan() -> None:
    shell = CyberRTBackend({})._planning_flags_overlay_shell()

    assert "--enable_control_interactive_replan" not in shell


def test_planning_overlay_replaces_existing_control_interactive_replan_flag(tmp_path, monkeypatch) -> None:
    src = tmp_path / "planning.conf"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text(
        "--flagfile=modules/common/data/global_flagfile.txt\n"
        "--enable_control_interactive_replan=true\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.conf"

    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {
                        "enable_control_interactive_replan": False,
                    }
                }
            }
        }
    )
    script = _extract_python_overlay_script(backend._planning_flags_overlay_shell())
    script = script.replace(
        "'/apollo/modules/planning/planning_component/conf/planning.conf'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/planning/planning_component/conf/planning.conf'",
        repr(str(tmp_path / "missing-share.conf")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/planning/planning_component/conf/planning.conf'",
        repr(str(tmp_path / "missing-src.conf")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/planning/planning_component/conf/planning.conf'); ",
        f"out=Path({str(out)!r}); ",
    )

    completed = subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    assert completed.returncode == 0
    assert "--enable_control_interactive_replan=true" not in out.read_text(encoding="utf-8")
    assert "--enable_control_interactive_replan=false" in out.read_text(encoding="utf-8")


def test_control_runtime_dag_overlay_sets_timer_interval(tmp_path) -> None:
    src = tmp_path / "control.dag"
    src.write_text(
        "module_config {\n"
        "    timer_components {\n"
        "        class_name : \"ControlComponent\"\n"
        "        config {\n"
        "            interval: 10\n"
        "        }\n"
        "    }\n"
        "}\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.dag"
    backend = CyberRTBackend(
        {"algo": {"apollo": {"control_runtime": {"control_interval_ms": 50}}}}
    )
    script = _extract_python_overlay_script(backend._control_dag_overlay_shell())
    script = script.replace(
        "'/apollo/modules/control/control_component/dag/control.dag'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/control/control_component/dag/control.dag'",
        repr(str(tmp_path / "missing-share.dag")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/control/control_component/dag/control.dag'",
        repr(str(tmp_path / "missing-src.dag")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/control/control_component/dag/control.dag'); ",
        f"out=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "interval: 50" in text
    assert "interval: 10" not in text


def test_control_runtime_flags_overlay_sets_control_period(tmp_path) -> None:
    src = tmp_path / "control.conf"
    src.write_text(
        "--pipeline_file=modules/control/control_component/conf/pipeline.pb.txt\n"
        "--control_period=0.01\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.conf"
    backend = CyberRTBackend(
        {"algo": {"apollo": {"control_runtime": {"control_interval_ms": 50}}}}
    )
    script = _extract_python_overlay_script(backend._control_flags_overlay_shell())
    script = script.replace(
        "'/apollo/modules/control/control_component/conf/control.conf'",
        repr(str(src)),
    )
    script = script.replace(
        "'/opt/apollo/neo/share/modules/control/control_component/conf/control.conf'",
        repr(str(tmp_path / "missing-share.conf")),
    )
    script = script.replace(
        "'/opt/apollo/neo/src/modules/control/control_component/conf/control.conf'",
        repr(str(tmp_path / "missing-src.conf")),
    )
    script = script.replace(
        "out=Path('/apollo_workspace/conf_overlay/modules/control/control_component/conf/control.conf'); ",
        f"out=Path({str(out)!r}); ",
    )

    subprocess.run(["python3", "-c", script], check=True, text=True, capture_output=True)

    text = out.read_text(encoding="utf-8")
    assert "--control_period=0.01" not in text
    assert "--control_period=0.050" in text


def test_control_runtime_overlays_default_do_not_touch_control_runtime_files() -> None:
    backend = CyberRTBackend({})

    assert "control_interval_ms" not in backend._control_dag_overlay_shell()
    assert "interval:" not in backend._control_dag_overlay_shell()
    assert "--control_period=" not in backend._control_flags_overlay_shell()
