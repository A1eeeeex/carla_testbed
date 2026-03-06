from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in ("", None):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "ok", "active"}:
        return True
    if text in {"0", "false", "no", "off", "fault", "fail"}:
        return False
    return default


def rounded_rectangle(
    draw: ImageDraw.ImageDraw,
    rect: Tuple[int, int, int, int],
    radius: int,
    fill: Tuple[int, int, int, int],
    outline: Optional[Tuple[int, int, int, int]] = None,
    width: int = 1,
) -> None:
    draw.rounded_rectangle(rect, radius=radius, fill=fill, outline=outline, width=width)


def draw_text(
    draw: ImageDraw.ImageDraw,
    pos: Tuple[int, int],
    text: str,
    font: ImageFont.ImageFont,
    fill: Tuple[int, int, int, int],
    anchor: Optional[str] = None,
) -> None:
    draw.text(pos, text, font=font, fill=fill, anchor=anchor)


def draw_status_dot(
    draw: ImageDraw.ImageDraw,
    center: Tuple[int, int],
    r: int,
    state: str,
) -> None:
    state_key = str(state or "warn").strip().lower()
    if state_key == "ok":
        color = (84, 214, 111, 245)
    elif state_key in {"fault", "fail", "error"}:
        color = (239, 93, 93, 245)
    else:
        color = (247, 201, 90, 245)
    cx, cy = center
    draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=color, outline=(245, 248, 255, 210), width=1)


def rgba_panel_with_blur(
    frame_bgr: np.ndarray,
    rect: Tuple[int, int, int, int],
    blur_ksize: int,
    panel_rgba: Tuple[int, int, int, int],
    radius: int,
    outline_rgba: Tuple[int, int, int, int],
) -> np.ndarray:
    h, w = frame_bgr.shape[:2]
    x1, y1, x2, y2 = rect
    x1 = int(max(0, min(w - 1, x1)))
    y1 = int(max(0, min(h - 1, y1)))
    x2 = int(max(x1 + 1, min(w, x2)))
    y2 = int(max(y1 + 1, min(h, y2)))
    if x2 <= x1 or y2 <= y1:
        return frame_bgr

    out = frame_bgr.copy()
    roi = out[y1:y2, x1:x2]

    k = int(max(3, blur_ksize))
    if (k % 2) == 0:
        k += 1
    blurred = cv2.GaussianBlur(roi, (k, k), 0)
    out[y1:y2, x1:x2] = blurred

    roi_rgba = cv2.cvtColor(out[y1:y2, x1:x2], cv2.COLOR_BGR2RGBA)
    roi_img = Image.fromarray(roi_rgba)
    panel = Image.new("RGBA", (x2 - x1, y2 - y1), (0, 0, 0, 0))
    panel_draw = ImageDraw.Draw(panel, "RGBA")
    rounded_rectangle(
        panel_draw,
        (0, 0, x2 - x1 - 1, y2 - y1 - 1),
        radius=max(4, int(radius)),
        fill=panel_rgba,
        outline=outline_rgba,
        width=1,
    )
    out[y1:y2, x1:x2] = cv2.cvtColor(np.array(Image.alpha_composite(roi_img, panel)), cv2.COLOR_RGBA2BGR)
    return out


@dataclass
class _Theme:
    panel_rgba: Tuple[int, int, int, int] = (13, 18, 28, 116)
    panel_outline_rgba: Tuple[int, int, int, int] = (205, 220, 248, 110)
    title_text: Tuple[int, int, int, int] = (235, 242, 255, 245)
    body_text: Tuple[int, int, int, int] = (246, 250, 255, 245)
    sub_text: Tuple[int, int, int, int] = (180, 196, 220, 232)
    dim_text: Tuple[int, int, int, int] = (143, 158, 182, 220)
    throttle_color: Tuple[int, int, int, int] = (107, 198, 248, 235)
    throttle_act_color: Tuple[int, int, int, int] = (76, 151, 191, 205)
    brake_color: Tuple[int, int, int, int] = (245, 106, 106, 235)
    brake_act_color: Tuple[int, int, int, int] = (194, 82, 82, 205)
    steer_color: Tuple[int, int, int, int] = (153, 196, 255, 235)
    speed_line: Tuple[int, int, int, int] = (114, 208, 255, 230)
    gap_line: Tuple[int, int, int, int] = (145, 238, 161, 230)
    grid_line: Tuple[int, int, int, int] = (105, 117, 140, 120)
    danger_band: Tuple[int, int, int, int] = (173, 58, 58, 44)
    warn_band: Tuple[int, int, int, int] = (182, 132, 42, 36)


class HUDRenderer:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = dict(cfg or {})
        self.mode = str(cfg.get("hud_mode", cfg.get("mode", "driving"))).strip().lower()
        if self.mode not in {"driving", "debug"}:
            self.mode = "driving"
        self.col_w = int(_clamp(float(cfg.get("col_w", 360)), 320.0, 380.0))
        self.margin_l = int(cfg.get("margin_l", 24))
        self.margin_b = int(cfg.get("margin_b", 24))
        self.gap_y = int(cfg.get("gap_y", 12))
        self.radius = int(cfg.get("radius", 14))
        self.blur_ksize = int(cfg.get("blur_ksize", 21))
        self.history_window_sec = float(cfg.get("history_window_sec", 10.0))
        self.theme = _Theme()
        self.history: deque = deque()
        self._last_ts: Optional[float] = None
        self._load_fonts()

    def _load_font_with_candidates(self, candidates: Iterable[str], size: int) -> ImageFont.ImageFont:
        for path in candidates:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def _load_fonts(self) -> None:
        sans_candidates = (
            "Roboto-Regular.ttf",
            "Inter-Regular.ttf",
            "NotoSans-Regular.ttf",
            "DejaVuSans.ttf",
            "/usr/share/fonts/truetype/roboto/Roboto-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        )
        mono_candidates = (
            "RobotoMono-Regular.ttf",
            "DejaVuSansMono.ttf",
            "/usr/share/fonts/truetype/roboto/RobotoMono-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        )
        self.font_speed = self._load_font_with_candidates(mono_candidates, 54)
        self.font_h1 = self._load_font_with_candidates(sans_candidates, 20)
        self.font_h2 = self._load_font_with_candidates(sans_candidates, 16)
        self.font_body = self._load_font_with_candidates(mono_candidates, 18)
        self.font_small = self._load_font_with_candidates(sans_candidates, 13)
        self.font_mini = self._load_font_with_candidates(sans_candidates, 12)

    def set_mode(self, mode: str) -> None:
        mode_key = str(mode or "").strip().lower()
        if mode_key in {"driving", "debug"}:
            self.mode = mode_key

    def toggle_mode(self) -> None:
        self.mode = "debug" if self.mode == "driving" else "driving"

    def _pick(self, metrics: Dict[str, Any], keys: Iterable[str], default: Optional[float] = None) -> Optional[float]:
        for key in keys:
            if key in metrics:
                value = _as_float(metrics.get(key), None)
                if value is not None:
                    return value
        return default

    def _normalize_metrics(self, metrics: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        src = dict(metrics or {})
        speed_kmh = self._pick(src, ("speed_kmh", "speed_kph", "v_kph"), None)
        if speed_kmh is None:
            speed_mps = self._pick(src, ("speed_mps", "v_mps", "dbg_v"), 0.0) or 0.0
            speed_kmh = speed_mps * 3.6
        speed_limit_kmh = self._pick(
            src,
            ("speed_limit_kmh", "speed_limit_kph", "dbg_v_cap_preview", "dbg_v_cap", "v_cap_kmh"),
            None,
        )
        acc_active = _as_bool(src.get("acc_active"), default=True)
        gap_m = self._pick(src, ("gap_m", "dbg_gap_euclid", "dbg_gap_s", "d_euclid_m"), None)
        rel_speed_mps = self._pick(src, ("rel_speed_mps", "dbg_closing_v_s"), None)
        lead_detected = src.get("lead_detected")
        if lead_detected is None:
            lead_detected = gap_m is not None and math.isfinite(gap_m)
        lead_detected = _as_bool(lead_detected, default=False)

        speed_mps = float(speed_kmh) / 3.6
        headway_s = self._pick(src, ("headway_s",), None)
        if headway_s is None and gap_m is not None and speed_mps > 0.3:
            headway_s = float(gap_m) / max(speed_mps, 0.1)

        throttle_cmd = self._pick(src, ("throttle_cmd", "cmd_throttle", "dbg_thr", "throttle"), 0.0) or 0.0
        throttle_act = self._pick(src, ("throttle_act", "applied_throttle"), throttle_cmd) or 0.0
        brake_cmd = self._pick(src, ("brake_cmd", "cmd_brake", "dbg_brk", "brake"), 0.0) or 0.0
        brake_act = self._pick(src, ("brake_act", "applied_brake"), brake_cmd) or 0.0
        steer_cmd_deg = self._pick(src, ("steer_cmd_deg",), None)
        if steer_cmd_deg is None:
            steer_cmd_norm = self._pick(src, ("cmd_steer", "dbg_steer", "steer"), 0.0) or 0.0
            steer_cmd_deg = float(steer_cmd_norm) * 30.0
        steer_act_deg = self._pick(src, ("steer_act_deg",), steer_cmd_deg) or 0.0

        loc_raw = src.get("loc_ok")
        plan_raw = src.get("plan_ok")
        ctrl_raw = src.get("ctrl_ok")
        loc_ok = None if loc_raw is None else _as_bool(loc_raw, default=False)
        plan_ok = None if plan_raw is None else _as_bool(plan_raw, default=False)
        ctrl_ok = None if ctrl_raw is None else _as_bool(ctrl_raw, default=False)
        fps = self._pick(src, ("fps",), None)
        timestamp = self._pick(src, ("timestamp", "t"), None)
        mode_from_metrics = str(src.get("hud_mode", "")).strip().lower()
        if mode_from_metrics in {"driving", "debug"}:
            self.mode = mode_from_metrics

        ttc_s = None
        if gap_m is not None and rel_speed_mps is not None and rel_speed_mps < -0.2:
            ttc_s = float(gap_m) / max(abs(float(rel_speed_mps)), 0.1)

        return {
            "speed_kmh": float(speed_kmh),
            "speed_limit_kmh": float(speed_limit_kmh) if speed_limit_kmh is not None else None,
            "acc_active": bool(acc_active),
            "gap_m": float(gap_m) if gap_m is not None else None,
            "headway_s": float(headway_s) if headway_s is not None else None,
            "rel_speed_mps": float(rel_speed_mps) if rel_speed_mps is not None else None,
            "lead_detected": bool(lead_detected),
            "throttle_cmd": _clamp(float(throttle_cmd), 0.0, 1.0),
            "throttle_act": _clamp(float(throttle_act), 0.0, 1.0),
            "brake_cmd": _clamp(float(brake_cmd), 0.0, 1.0),
            "brake_act": _clamp(float(brake_act), 0.0, 1.0),
            "steer_cmd_deg": float(steer_cmd_deg),
            "steer_act_deg": float(steer_act_deg),
            "loc_ok": loc_ok,
            "plan_ok": plan_ok,
            "ctrl_ok": ctrl_ok,
            "fps": float(fps) if fps is not None else None,
            "timestamp": float(timestamp) if timestamp is not None else None,
            "ttc_s": float(ttc_s) if ttc_s is not None else None,
        }

    def _update_history(self, m: Dict[str, Any]) -> None:
        ts = m.get("timestamp")
        if ts is None:
            if self._last_ts is None:
                ts = 0.0
            else:
                ts = self._last_ts + 0.05
        ts = float(ts)
        if self._last_ts is not None and ts <= self._last_ts:
            ts = self._last_ts + 0.001
        self._last_ts = ts

        self.history.append(
            {
                "t": ts,
                "speed_kmh": m.get("speed_kmh"),
                "gap_m": m.get("gap_m"),
                "headway_s": m.get("headway_s"),
                "ttc_s": m.get("ttc_s"),
            }
        )
        while self.history and (ts - self.history[0]["t"]) > self.history_window_sec:
            self.history.popleft()

    def _card_layout(self, frame_h: int) -> Dict[str, Tuple[int, int, int, int]]:
        card_h = {
            "speed": 154,
            "follow": 130,
            "control": 146,
            "trend": 176,
            "footer": 34,
        }
        if self.mode == "debug":
            order = ("speed", "follow", "control", "trend", "footer")
        else:
            order = ("speed", "follow", "footer")
        total_h = sum(card_h[k] for k in order) + self.gap_y * (len(order) - 1)
        y = max(8, frame_h - self.margin_b - total_h)
        x = self.margin_l
        rects: Dict[str, Tuple[int, int, int, int]] = {}
        for key in order:
            h = card_h[key]
            rects[key] = (x, y, x + self.col_w, y + h)
            y += h + self.gap_y
        return rects

    def _draw_limit_sign(self, draw: ImageDraw.ImageDraw, x: int, y: int, value: Optional[float]) -> None:
        r = 27
        draw.ellipse((x - r, y - r, x + r, y + r), fill=(255, 252, 252, 245), outline=(225, 58, 58, 240), width=4)
        txt = "--" if value is None else f"{int(round(value))}"
        draw_text(draw, (x, y - 10), txt, self.font_h1, (32, 36, 45, 240))
        draw_text(draw, (x, y + 9), "km/h", self.font_mini, (78, 84, 95, 230), anchor="mm")

    def _draw_dual_bar(
        self,
        draw: ImageDraw.ImageDraw,
        x: int,
        y: int,
        w: int,
        cmd: float,
        act: float,
        color_cmd: Tuple[int, int, int, int],
        color_act: Tuple[int, int, int, int],
    ) -> None:
        h = 9
        rounded_rectangle(draw, (x, y, x + w, y + h), 4, fill=(72, 83, 104, 175))
        rounded_rectangle(draw, (x, y + h + 5, x + w, y + h * 2 + 5), 4, fill=(72, 83, 104, 145))
        rounded_rectangle(draw, (x, y, x + int(w * _clamp(cmd, 0.0, 1.0)), y + h), 4, fill=color_cmd)
        rounded_rectangle(draw, (x, y + h + 5, x + int(w * _clamp(act, 0.0, 1.0)), y + h * 2 + 5), 4, fill=color_act)

    def draw_card_speed(self, draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, metrics: Dict[str, Any]) -> None:
        speed = metrics["speed_kmh"]
        limit = metrics.get("speed_limit_kmh")
        acc_active = metrics.get("acc_active", False)
        over_limit = (limit is not None) and (speed > (limit + 2.0))
        speed_color = (255, 168, 168, 245) if over_limit else self.theme.body_text
        draw_text(draw, (x + 16, y + 12), "Speed", self.font_h2, self.theme.sub_text)
        draw_text(draw, (x + 16, y + 42), f"{speed:5.1f}", self.font_speed, speed_color)
        draw_text(draw, (x + 178, y + 90), "km/h", self.font_h2, self.theme.sub_text)

        badge = "ACC ACTIVE" if acc_active else "ACC OFF"
        badge_fill = (54, 153, 88, 210) if acc_active else (137, 97, 50, 210)
        tw = int(draw.textlength(badge, font=self.font_small)) + 24
        bx2 = x + w - 90
        bx1 = bx2 - tw
        by1 = y + 16
        by2 = by1 + 28
        rounded_rectangle(draw, (bx1, by1, bx2, by2), 12, fill=badge_fill, outline=(240, 244, 252, 180), width=1)
        draw_text(draw, (bx1 + 12, by1 + 7), badge, self.font_small, (252, 252, 252, 240))

        self._draw_limit_sign(draw, x + w - 48, y + 102, limit)

    def draw_card_follow(self, draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, metrics: Dict[str, Any]) -> None:
        draw_text(draw, (x + 16, y + 10), "Follow", self.font_h2, self.theme.sub_text)
        gap = metrics.get("gap_m")
        headway = metrics.get("headway_s")
        rel_v = metrics.get("rel_speed_mps")
        lead = metrics.get("lead_detected", False)

        gap_text = "--.- m" if gap is None else f"{gap:4.1f} m"
        hw_text = "--.- s" if headway is None else f"{headway:4.2f} s"
        rel_text = "--.- m/s" if rel_v is None else f"{rel_v:+4.2f} m/s"
        lead_text = "YES" if lead else "NO"

        hw_color = self.theme.body_text
        if headway is not None and headway < 1.2:
            hw_color = (247, 121, 121, 240)
        elif headway is not None and headway < 1.8:
            hw_color = (247, 204, 126, 240)

        draw_text(draw, (x + 16, y + 36), "Gap", self.font_small, self.theme.dim_text)
        draw_text(draw, (x + 86, y + 34), gap_text, self.font_body, self.theme.body_text)
        draw_text(draw, (x + 16, y + 66), "Headway", self.font_small, self.theme.dim_text)
        draw_text(draw, (x + 86, y + 64), hw_text, self.font_body, hw_color)
        draw_text(draw, (x + 16, y + 96), "Δv", self.font_small, self.theme.dim_text)
        draw_text(draw, (x + 86, y + 94), rel_text, self.font_body, self.theme.body_text)

        draw_text(draw, (x + w - 116, y + 36), "Lead", self.font_small, self.theme.dim_text)
        lead_fill = (70, 170, 103, 215) if lead else (130, 96, 96, 205)
        rounded_rectangle(draw, (x + w - 122, y + 58, x + w - 26, y + 92), 10, fill=lead_fill)
        draw_text(draw, (x + w - 74, y + 68), lead_text, self.font_h1, (248, 252, 255, 240), anchor="ma")

    def draw_card_control(self, draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, metrics: Dict[str, Any]) -> None:
        draw_text(draw, (x + 16, y + 10), "Control (Cmd / Act)", self.font_h2, self.theme.sub_text)
        bar_x = x + 118
        bar_w = w - 136
        row_y = y + 34

        thr_cmd = metrics.get("throttle_cmd", 0.0)
        thr_act = metrics.get("throttle_act", 0.0)
        brk_cmd = metrics.get("brake_cmd", 0.0)
        brk_act = metrics.get("brake_act", 0.0)
        steer_cmd_deg = metrics.get("steer_cmd_deg", 0.0)
        steer_act_deg = metrics.get("steer_act_deg", 0.0)

        draw_text(draw, (x + 16, row_y - 1), "Throttle", self.font_small, self.theme.dim_text)
        self._draw_dual_bar(draw, bar_x, row_y, bar_w, thr_cmd, thr_act, self.theme.throttle_color, self.theme.throttle_act_color)
        draw_text(draw, (bar_x + bar_w - 2, row_y - 1), f"{thr_cmd*100:3.0f}/{thr_act*100:3.0f}%", self.font_small, self.theme.body_text, anchor="ra")

        row_y += 40
        draw_text(draw, (x + 16, row_y - 1), "Brake", self.font_small, self.theme.dim_text)
        self._draw_dual_bar(draw, bar_x, row_y, bar_w, brk_cmd, brk_act, self.theme.brake_color, self.theme.brake_act_color)
        draw_text(draw, (bar_x + bar_w - 2, row_y - 1), f"{brk_cmd*100:3.0f}/{brk_act*100:3.0f}%", self.font_small, self.theme.body_text, anchor="ra")

        row_y += 40
        draw_text(draw, (x + 16, row_y - 1), "Steer", self.font_small, self.theme.dim_text)
        steer_norm_cmd = _clamp(abs(steer_cmd_deg) / 35.0, 0.0, 1.0)
        steer_norm_act = _clamp(abs(steer_act_deg) / 35.0, 0.0, 1.0)
        self._draw_dual_bar(draw, bar_x, row_y, bar_w, steer_norm_cmd, steer_norm_act, self.theme.steer_color, (124, 158, 212, 205))
        draw_text(draw, (bar_x + bar_w - 2, row_y - 1), f"{steer_cmd_deg:+5.1f}/{steer_act_deg:+5.1f}°", self.font_small, self.theme.body_text, anchor="ra")

    def draw_card_trend(self, draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, history: deque) -> None:
        draw_text(draw, (x + 16, y + 10), "Trend (10s)", self.font_h2, self.theme.sub_text)
        draw_text(draw, (x + 126, y + 10), "Speed(km/h)", self.font_mini, self.theme.speed_line)
        draw_text(draw, (x + 222, y + 10), "Gap(m)", self.font_mini, self.theme.gap_line)
        chart = (x + 14, y + 34, x + w - 14, y + h - 14)
        rounded_rectangle(draw, chart, 8, fill=(20, 26, 38, 92), outline=(158, 175, 205, 120), width=1)

        if len(history) < 2:
            return

        c_x1, c_y1, c_x2, c_y2 = chart
        plot_x1, plot_y1 = c_x1 + 8, c_y1 + 8
        plot_x2, plot_y2 = c_x2 - 8, c_y2 - 8
        if plot_x2 <= plot_x1 or plot_y2 <= plot_y1:
            return

        hazard_level = 0
        for item in history:
            ttc = item.get("ttc_s")
            hw = item.get("headway_s")
            if ttc is not None and ttc < 2.0:
                hazard_level = 2
                break
            if hw is not None and hw < 1.5:
                hazard_level = max(hazard_level, 1)
        if hazard_level == 2:
            rounded_rectangle(draw, (plot_x1, plot_y1, plot_x2, plot_y2), 6, fill=self.theme.danger_band)
        elif hazard_level == 1:
            rounded_rectangle(draw, (plot_x1, plot_y1, plot_x2, plot_y2), 6, fill=self.theme.warn_band)

        for frac in (0.25, 0.5, 0.75):
            gy = int(plot_y2 - frac * (plot_y2 - plot_y1))
            draw.line((plot_x1, gy, plot_x2, gy), fill=self.theme.grid_line, width=1)

        t0 = history[0]["t"]
        t1 = history[-1]["t"]
        span = max(t1 - t0, 1e-3)
        speed_vals = [float(item["speed_kmh"]) for item in history if item.get("speed_kmh") is not None]
        gap_vals = [float(item["gap_m"]) for item in history if item.get("gap_m") is not None]
        speed_max = max(80.0, max(speed_vals, default=0.0) * 1.15 + 5.0)
        gap_max = max(30.0, max(gap_vals, default=0.0) * 1.2 + 2.0)

        pts_speed = []
        pts_gap = []
        for item in history:
            t = float(item["t"])
            x_coord = plot_x1 + int((t - t0) / span * (plot_x2 - plot_x1))
            speed = item.get("speed_kmh")
            if speed is not None:
                speed_norm = _clamp(float(speed) / speed_max, 0.0, 1.0)
                y_coord = plot_y2 - int(speed_norm * (plot_y2 - plot_y1))
                pts_speed.append((x_coord, y_coord))
            gap = item.get("gap_m")
            if gap is not None:
                gap_norm = _clamp(float(gap) / gap_max, 0.0, 1.0)
                y_coord = plot_y2 - int(gap_norm * (plot_y2 - plot_y1))
                pts_gap.append((x_coord, y_coord))
        if len(pts_speed) >= 2:
            draw.line(pts_speed, fill=self.theme.speed_line, width=3)
        if len(pts_gap) >= 2:
            draw.line(pts_gap, fill=self.theme.gap_line, width=3)

        draw_text(draw, (plot_x1, plot_y2 + 2), "0s", self.font_mini, self.theme.dim_text)
        draw_text(draw, (plot_x2, plot_y2 + 2), "10s", self.font_mini, self.theme.dim_text, anchor="ra")

    def draw_footer_status(self, draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, metrics: Dict[str, Any]) -> None:
        draw_text(draw, (x + 14, y + 9), "SYS", self.font_small, self.theme.sub_text)
        def _state(value: Any) -> str:
            if value is None:
                return "warn"
            return "ok" if bool(value) else "fault"
        slots = [
            ("LOC", _state(metrics.get("loc_ok"))),
            ("PLAN", _state(metrics.get("plan_ok"))),
            ("CTRL", _state(metrics.get("ctrl_ok"))),
        ]
        px = x + 56
        for label, state in slots:
            draw_status_dot(draw, (px, y + 17), 5, state)
            draw_text(draw, (px + 10, y + 8), label, self.font_small, self.theme.body_text)
            px += 74
        fps = metrics.get("fps")
        fps_text = "--" if fps is None else f"{fps:4.1f}"
        draw_text(draw, (x + w - 14, y + 8), f"FPS {fps_text}", self.font_small, self.theme.sub_text, anchor="ra")

    def render(self, frame_bgr: np.ndarray, metrics: Optional[Dict[str, Any]]) -> np.ndarray:
        if frame_bgr is None or frame_bgr.size == 0:
            return frame_bgr

        m = self._normalize_metrics(metrics)
        if _as_bool((metrics or {}).get("toggle_mode"), default=False):
            self.toggle_mode()
        self._update_history(m)

        h, _ = frame_bgr.shape[:2]
        rects = self._card_layout(h)
        panel_order = [key for key in ("speed", "follow", "control", "trend", "footer") if key in rects]

        frame = frame_bgr.copy()
        for key in panel_order:
            frame = rgba_panel_with_blur(
                frame,
                rects[key],
                blur_ksize=self.blur_ksize,
                panel_rgba=self.theme.panel_rgba,
                radius=self.radius,
                outline_rgba=self.theme.panel_outline_rgba,
            )

        base_rgba = cv2.cvtColor(frame, cv2.COLOR_BGR2RGBA)
        base_img = Image.fromarray(base_rgba)
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay, "RGBA")

        for key in panel_order:
            x1, y1, x2, y2 = rects[key]
            cw, ch = x2 - x1, y2 - y1
            if key == "speed":
                self.draw_card_speed(draw, x1, y1, cw, ch, m)
            elif key == "follow":
                self.draw_card_follow(draw, x1, y1, cw, ch, m)
            elif key == "control":
                self.draw_card_control(draw, x1, y1, cw, ch, m)
            elif key == "trend":
                self.draw_card_trend(draw, x1, y1, cw, ch, self.history)
            elif key == "footer":
                self.draw_footer_status(draw, x1, y1, cw, ch, m)

        out_rgba = np.array(Image.alpha_composite(base_img, overlay))
        return cv2.cvtColor(out_rgba, cv2.COLOR_RGBA2BGR)
