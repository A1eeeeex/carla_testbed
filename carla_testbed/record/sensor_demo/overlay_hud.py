from __future__ import annotations

from typing import Any, Dict, MutableMapping, Optional

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None

from carla_testbed.record.sensor_demo.hud_renderer import HUDRenderer


def _build_renderer_cfg(hud_ctx: Optional[MutableMapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(hud_ctx, MutableMapping):
        return {}
    cfg: Dict[str, Any] = {}
    if hud_ctx.get("hud_mode") is not None:
        cfg["hud_mode"] = hud_ctx.get("hud_mode")
    if hud_ctx.get("hud_col_w") is not None:
        cfg["col_w"] = hud_ctx.get("hud_col_w")
    if hud_ctx.get("hud_margin_l") is not None:
        cfg["margin_l"] = hud_ctx.get("hud_margin_l")
    if hud_ctx.get("hud_margin_b") is not None:
        cfg["margin_b"] = hud_ctx.get("hud_margin_b")
    if hud_ctx.get("hud_gap_y") is not None:
        cfg["gap_y"] = hud_ctx.get("hud_gap_y")
    if hud_ctx.get("hud_blur_ksize") is not None:
        cfg["blur_ksize"] = hud_ctx.get("hud_blur_ksize")
    if hud_ctx.get("hud_radius") is not None:
        cfg["radius"] = hud_ctx.get("hud_radius")
    return cfg


def _ensure_renderer(hud_ctx: Optional[MutableMapping[str, Any]]) -> HUDRenderer:
    if isinstance(hud_ctx, MutableMapping):
        renderer = hud_ctx.get("_hud_renderer")
        if isinstance(renderer, HUDRenderer):
            mode = hud_ctx.get("hud_mode")
            if mode:
                renderer.set_mode(str(mode))
            return renderer
        renderer = HUDRenderer(_build_renderer_cfg(hud_ctx))
        hud_ctx["_hud_renderer"] = renderer
        return renderer
    return HUDRenderer({})


def draw_hud(
    img,
    frame_id=None,
    timestamp=None,
    imu=None,
    gnss=None,
    events=None,
    stats=None,
    metrics: Optional[Dict[str, Any]] = None,
    hud_ctx: Optional[MutableMapping[str, Any]] = None,
):
    """Render professional left-stack HUD overlay on BGR frame."""
    if cv2 is None:
        return img
    renderer = _ensure_renderer(hud_ctx)
    merged: Dict[str, Any] = dict(metrics or {})
    if timestamp is not None and merged.get("timestamp") is None:
        merged["timestamp"] = timestamp
    if frame_id is not None and merged.get("frame") is None:
        merged["frame"] = frame_id
    if isinstance(stats, dict):
        if "fps" in stats and merged.get("fps") is None:
            merged["fps"] = stats.get("fps")
        if "loc_ok" in stats and merged.get("loc_ok") is None:
            merged["loc_ok"] = stats.get("loc_ok")
        if "plan_ok" in stats and merged.get("plan_ok") is None:
            merged["plan_ok"] = stats.get("plan_ok")
        if "ctrl_ok" in stats and merged.get("ctrl_ok") is None:
            merged["ctrl_ok"] = stats.get("ctrl_ok")
    return renderer.render(img, merged)
