from __future__ import annotations

from carla_testbed.runner.hooks import FrameContext, RunHook
from carla_testbed.runner.tick_loop import (
    HookDispatcher,
    adapt_tick_callbacks,
    compute_wall_time_pacing_sleep,
    hook_error_summaries,
)


class RecordingHook(RunHook):
    def __init__(self, name: str, calls: list[str], fail_method: str | None = None):
        self.name = name
        self.calls = calls
        self.fail_method = fail_method

    def _record(self, method: str) -> None:
        self.calls.append(f"{self.name}.{method}")
        if self.fail_method == method:
            raise RuntimeError(f"{self.name} {method} failed")

    def on_run_start(self, context) -> None:
        self._record("on_run_start")

    def before_tick(self, frame_context: FrameContext) -> None:
        self._record("before_tick")

    def after_world_tick(self, frame_context: FrameContext) -> None:
        self._record(f"after_world_tick:{frame_context.frame_id}")

    def after_state_collect(self, frame_context: FrameContext) -> None:
        self._record("after_state_collect")

    def before_control_apply(self, frame_context: FrameContext) -> None:
        self._record("before_control_apply")

    def after_control_apply(self, frame_context: FrameContext) -> None:
        self._record("after_control_apply")

    def on_run_end(self, context) -> None:
        self._record("on_run_end")

    def close(self) -> None:
        self._record("close")


def test_hook_dispatcher_calls_stages_in_order() -> None:
    calls: list[str] = []
    dispatcher = HookDispatcher([RecordingHook("hook", calls)], warn=lambda message: calls.append(message))
    frame_context = FrameContext(frame_id=7, sim_time_s=0.35, wall_time_s=1.0, step=3)

    dispatcher.notify("on_run_start", {"run_id": "demo"})
    dispatcher.notify("before_tick", frame_context)
    dispatcher.notify("after_world_tick", frame_context)
    dispatcher.notify("after_state_collect", frame_context)
    dispatcher.notify("before_control_apply", frame_context)
    dispatcher.notify("after_control_apply", frame_context)
    dispatcher.notify("on_run_end", {"run_id": "demo"})
    dispatcher.close()

    assert calls == [
        "hook.on_run_start",
        "hook.before_tick",
        "hook.after_world_tick:7",
        "hook.after_state_collect",
        "hook.before_control_apply",
        "hook.after_control_apply",
        "hook.on_run_end",
        "hook.close",
    ]
    assert dispatcher.errors == []


def test_hook_dispatcher_records_errors_and_continues() -> None:
    calls: list[str] = []
    warnings: list[str] = []
    dispatcher = HookDispatcher(
        [
            RecordingHook("bad", calls, fail_method="after_state_collect"),
            RecordingHook("good", calls),
        ],
        warn=warnings.append,
    )

    dispatcher.notify("after_state_collect", FrameContext(frame_id=1, sim_time_s=0.05))

    assert calls == ["bad.after_state_collect", "good.after_state_collect"]
    assert len(dispatcher.errors) == 1
    assert dispatcher.errors[0].hook_name == "bad"
    assert dispatcher.errors[0].method_name == "after_state_collect"
    assert warnings and "run hook failed hook=bad method=after_state_collect" in warnings[0]
    assert hook_error_summaries(dispatcher.errors)[0]["error_type"] == "RuntimeError"


def test_hook_dispatcher_returns_per_hook_timings() -> None:
    calls: list[str] = []
    dispatcher = HookDispatcher(
        [
            RecordingHook("first hook", calls),
            RecordingHook("second", calls),
        ]
    )

    timings = dispatcher.notify("after_world_tick", FrameContext(frame_id=3, sim_time_s=0.15))

    assert [timing.hook_name for timing in timings] == ["first hook", "second"]
    assert [timing.method_name for timing in timings] == ["after_world_tick", "after_world_tick"]
    assert all(timing.duration_s >= 0.0 for timing in timings)
    assert all(timing.error is False for timing in timings)


def test_legacy_tick_callback_adapter_names_callback_source() -> None:
    def sample_callback(frame_id, timestamp) -> None:
        pass

    hooks = adapt_tick_callbacks([sample_callback])

    assert len(hooks) == 1
    assert hooks[0].name.startswith("tick_callback_0:")
    assert "sample_callback" in hooks[0].name


def test_legacy_tick_callback_adapter_supports_keyword_and_positional_callbacks() -> None:
    calls: list[tuple] = []

    def keyword_callback(**kwargs) -> None:
        calls.append(("keyword", kwargs["frame_id"], kwargs["timestamp"], kwargs["step"]))

    def positional_callback(frame_id, timestamp) -> None:
        calls.append(("positional", frame_id, timestamp))

    dispatcher = HookDispatcher(adapt_tick_callbacks([keyword_callback, positional_callback]))
    dispatcher.notify("after_world_tick", FrameContext(frame_id=9, sim_time_s=0.45, step=4))

    assert calls == [
        ("keyword", 9, 0.45, 4),
        ("positional", 9, 0.45),
    ]


def test_compute_wall_time_pacing_sleep_fills_remaining_tick_interval() -> None:
    sleep_s = compute_wall_time_pacing_sleep(
        frame_loop_start_wall_s=10.0,
        now_wall_s=10.012,
        target_interval_s=0.05,
        max_sleep_s=0.05,
    )

    assert abs(sleep_s - 0.038) < 1e-9


def test_compute_wall_time_pacing_sleep_caps_and_skips_when_late() -> None:
    capped = compute_wall_time_pacing_sleep(
        frame_loop_start_wall_s=10.0,
        now_wall_s=10.001,
        target_interval_s=0.05,
        max_sleep_s=0.01,
    )
    late = compute_wall_time_pacing_sleep(
        frame_loop_start_wall_s=10.0,
        now_wall_s=10.08,
        target_interval_s=0.05,
        max_sleep_s=0.05,
    )

    assert abs(capped - 0.01) < 1e-9
    assert late == 0.0
