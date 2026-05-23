from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from .hooks import FrameContext, RunHook


@dataclass(frozen=True)
class RunHookError:
    hook_name: str
    method_name: str
    error: Exception


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

    def notify(self, method_name: str, context: Any) -> None:
        for idx, hook in enumerate(self.hooks):
            method = getattr(hook, method_name, None)
            if method is None:
                continue
            hook_name = getattr(hook, "name", f"{hook.__class__.__name__}#{idx}")
            try:
                method(context)
            except Exception as exc:
                self._record_error(hook_name=hook_name, method_name=method_name, error=exc)

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
        hooks.append(CallbackRunHook(callback, name=f"tick_callback_{idx}"))
    return hooks


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
