from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any, Callable, Iterable

from .hooks import FrameContext, RunHook


@dataclass(frozen=True)
class RunHookError:
    hook_name: str
    method_name: str
    error: Exception


@dataclass(frozen=True)
class RunHookTiming:
    hook_name: str
    method_name: str
    duration_s: float
    error: bool = False


class CallbackRunHook(RunHook):
    """Compatibility adapter for legacy tick_callbacks."""

    def __init__(self, callback: Callable[..., None], *, name: str):
        self.callback = callback
        self.name = name

    def after_world_tick(self, frame_context: FrameContext) -> None:
        try:
            self.callback(
                frame_id=frame_context.frame_id,
                timestamp=frame_context.sim_time_s,
                step=frame_context.step,
            )
        except TypeError:
            self.callback(frame_context.frame_id, frame_context.sim_time_s)


class HookDispatcher:
    def __init__(
        self,
        hooks: Iterable[RunHook] | None = None,
        *,
        warn: Callable[[str], None] | None = None,
        max_warnings_per_hook_method: int = 3,
    ):
        self.hooks: list[RunHook] = list(hooks or [])
        self.errors: list[RunHookError] = []
        self._warn = warn or print
        self._max_warnings = max(1, max_warnings_per_hook_method)
        self._failure_counts: dict[tuple[str, str], int] = {}

    def add(self, hook: RunHook) -> None:
        self.hooks.append(hook)

    def notify(self, method_name: str, context: Any) -> list[RunHookTiming]:
        timings: list[RunHookTiming] = []
        for idx, hook in enumerate(self.hooks):
            method = getattr(hook, method_name, None)
            if method is None:
                continue
            hook_name = getattr(hook, "name", f"{hook.__class__.__name__}#{idx}")
            started = time.perf_counter()
            error = False
            try:
                method(context)
            except Exception as exc:
                error = True
                self._record_error(hook_name=hook_name, method_name=method_name, error=exc)
            timings.append(
                RunHookTiming(
                    hook_name=str(hook_name),
                    method_name=str(method_name),
                    duration_s=max(0.0, time.perf_counter() - started),
                    error=error,
                )
            )
        return timings

    def close(self) -> None:
        for idx, hook in enumerate(self.hooks):
            method = getattr(hook, "close", None)
            if method is None:
                continue
            hook_name = getattr(hook, "name", f"{hook.__class__.__name__}#{idx}")
            try:
                method()
            except Exception as exc:
                self._record_error(hook_name=hook_name, method_name="close", error=exc)

    def _record_error(self, *, hook_name: str, method_name: str, error: Exception) -> None:
        self.errors.append(RunHookError(hook_name=hook_name, method_name=method_name, error=error))
        key = (hook_name, method_name)
        count = self._failure_counts.get(key, 0) + 1
        self._failure_counts[key] = count
        if count <= self._max_warnings:
            self._warn(
                f"[WARN] run hook failed hook={hook_name} method={method_name} "
                f"count={count}: {type(error).__name__}: {error}"
            )


def adapt_tick_callbacks(callbacks: Iterable[Callable[..., None]] | None) -> list[RunHook]:
    hooks: list[RunHook] = []
    for idx, callback in enumerate(callbacks or []):
        if callback is None:
            continue
        module_name = getattr(callback, "__module__", "") or ""
        qualname = getattr(callback, "__qualname__", "") or getattr(callback, "__name__", "") or ""
        source_name = ".".join(part for part in (module_name, qualname) if part)
        callback_name = f"tick_callback_{idx}"
        if source_name:
            callback_name = f"{callback_name}:{source_name}"
        hooks.append(CallbackRunHook(callback, name=callback_name))
    return hooks


def compute_wall_time_pacing_sleep(
    *,
    frame_loop_start_wall_s: float,
    now_wall_s: float,
    target_interval_s: float | None,
    max_sleep_s: float | None = None,
) -> float:
    """Return how long to sleep to keep one sim tick near a wall-clock interval."""
    try:
        frame_start = float(frame_loop_start_wall_s)
        now = float(now_wall_s)
        target = float(target_interval_s)
    except (TypeError, ValueError):
        return 0.0
    if not (math.isfinite(frame_start) and math.isfinite(now) and math.isfinite(target)):
        return 0.0
    if target <= 0.0:
        return 0.0
    elapsed = max(0.0, now - frame_start)
    sleep_s = max(0.0, target - elapsed)
    if max_sleep_s is not None:
        try:
            cap = float(max_sleep_s)
        except (TypeError, ValueError):
            cap = None
        if cap is not None and math.isfinite(cap) and cap >= 0.0:
            sleep_s = min(sleep_s, cap)
    return sleep_s


def hook_error_summaries(errors: Iterable[RunHookError]) -> list[dict[str, str]]:
    return [
        {
            "hook_name": err.hook_name,
            "method_name": err.method_name,
            "error_type": type(err.error).__name__,
            "message": str(err.error),
        }
        for err in errors
    ]
