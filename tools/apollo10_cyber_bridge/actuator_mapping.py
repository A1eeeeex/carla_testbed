from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _dedupe_pairs(pairs: Sequence[Tuple[float, float]]) -> List[Tuple[float, float]]:
    grouped: Dict[float, List[float]] = {}
    for x_val, y_val in pairs:
        grouped.setdefault(float(x_val), []).append(float(y_val))
    out: List[Tuple[float, float]] = []
    for x_val in sorted(grouped):
        ys = grouped[x_val]
        out.append((x_val, sum(ys) / max(len(ys), 1)))
    return out


def _interp_pairs(pairs: Sequence[Tuple[float, float]], query: float) -> Optional[float]:
    if not pairs:
        return None
    if len(pairs) == 1:
        return float(pairs[0][1])
    q = float(query)
    if q <= float(pairs[0][0]):
        return float(pairs[0][1])
    if q >= float(pairs[-1][0]):
        return float(pairs[-1][1])
    for left, right in zip(pairs[:-1], pairs[1:]):
        x0, y0 = float(left[0]), float(left[1])
        x1, y1 = float(right[0]), float(right[1])
        if q > x1:
            continue
        if abs(x1 - x0) <= 1e-9:
            return y1
        ratio = (q - x0) / (x1 - x0)
        return y0 + ratio * (y1 - y0)
    return float(pairs[-1][1])


def _records_to_pairs(
    records: Sequence[Dict[str, Any]],
    *,
    x_key: str,
    y_key: str,
    absolute: bool = False,
    positive_only: bool = False,
) -> List[Tuple[float, float]]:
    pairs: List[Tuple[float, float]] = []
    for item in records:
        x_val = _safe_float(item.get(x_key))
        y_val = _safe_float(item.get(y_key))
        if x_val is None or y_val is None:
            continue
        if absolute:
            x_val = abs(x_val)
            y_val = abs(y_val)
        if positive_only and (x_val < 0.0 or y_val < 0.0):
            continue
        pairs.append((x_val, y_val))
    return _dedupe_pairs(pairs)


def build_inverse_table(
    records: Sequence[Dict[str, Any]],
    *,
    measured_key: str,
    command_key: str,
    target_key: str,
    absolute: bool = False,
    positive_only: bool = False,
) -> List[Dict[str, float]]:
    pairs = _records_to_pairs(
        records,
        x_key=measured_key,
        y_key=command_key,
        absolute=absolute,
        positive_only=positive_only,
    )
    out: List[Dict[str, float]] = []
    for measured, command in pairs:
        out.append({target_key: float(measured), command_key: float(command)})
    return out


@dataclass
class AxisTable:
    pairs: List[Tuple[float, float]]

    @classmethod
    def from_records(
        cls,
        records: Sequence[Dict[str, Any]],
        *,
        input_key: str,
        output_key: str,
        absolute: bool = False,
        positive_only: bool = False,
    ) -> "AxisTable":
        return cls(
            pairs=_records_to_pairs(
                records,
                x_key=input_key,
                y_key=output_key,
                absolute=absolute,
                positive_only=positive_only,
            )
        )

    def lookup(self, value: float) -> Optional[float]:
        return _interp_pairs(self.pairs, value)

    def to_rows(self, *, x_key: str, y_key: str) -> List[Dict[str, float]]:
        return [{x_key: float(x_val), y_key: float(y_val)} for x_val, y_val in self.pairs]


@dataclass
class SpeedBin:
    speed_min_mps: float
    speed_max_mps: float
    table: AxisTable
    metadata: Dict[str, Any]

    @property
    def center_mps(self) -> float:
        return 0.5 * (self.speed_min_mps + self.speed_max_mps)

    def contains(self, speed_mps: float) -> bool:
        return self.speed_min_mps <= speed_mps < self.speed_max_mps


@dataclass
class SectionTable:
    entry_speed_mps: float
    table: AxisTable
    metadata: Dict[str, Any]


class ActuatorCalibration:
    def __init__(self, payload: Optional[Dict[str, Any]] = None, *, source_path: Optional[Path] = None) -> None:
        self.payload = payload or {}
        self.source_path = source_path
        self.schema_version = int(self.payload.get("schema_version", 1) or 1)
        self._steering_angle_table = self._build_simple_table(
            ("steering", "inverse", "target_front_wheel_angle_deg_to_carla_cmd"),
            input_key="target_front_wheel_angle_deg",
            output_key="carla_steer_cmd",
        )
        self._steering_curvature_table = self._build_simple_table(
            ("steering", "inverse", "target_curvature_to_carla_cmd"),
            input_key="target_curvature",
            output_key="carla_steer_cmd",
        )
        self._throttle_bins = self._build_speed_bins(
            ("throttle", "speed_bins"),
            inverse_key="target_accel_mps2_to_throttle_cmd",
            input_key="target_accel_mps2",
            output_key="throttle_cmd",
        )
        self._throttle_low_speed_launch = self._build_simple_table_from_measurements(
            ("throttle", "low_speed", "launch", "inverse", "target_speed_at_eval_mps_to_throttle_cmd"),
            input_key="target_speed_at_eval_mps",
            output_key="throttle_cmd",
            positive_only=True,
        )
        self._throttle_low_speed_launch_meta = self._nested(("throttle", "low_speed", "launch"))
        self._throttle_low_speed_launch_boost = self._build_simple_table_from_measurements(
            ("throttle", "low_speed", "launch_boost", "inverse", "target_accel_mps2_to_throttle_cmd"),
            input_key="target_accel_mps2",
            output_key="throttle_cmd",
            positive_only=True,
        )
        self._throttle_low_speed_launch_boost_meta = self._nested(("throttle", "low_speed", "launch_boost"))
        self._throttle_low_speed_crawl = self._build_section_tables(
            ("throttle", "low_speed", "crawl"),
            inverse_key="target_speed_retained_mps_to_throttle_cmd",
            input_key="target_speed_retained_mps",
            output_key="throttle_cmd",
            positive_only=True,
        )
        self._throttle_low_speed_crawl_boost = self._build_section_tables(
            ("throttle", "low_speed", "crawl_boost"),
            inverse_key="target_accel_mps2_to_throttle_cmd",
            input_key="target_accel_mps2",
            output_key="throttle_cmd",
            positive_only=True,
        )
        self._throttle_mid_speed_boost = self._build_section_tables(
            ("throttle", "mid_speed_boost"),
            inverse_key="target_accel_mps2_to_throttle_cmd",
            input_key="target_accel_mps2",
            output_key="throttle_cmd",
            positive_only=True,
        )
        self._brake_bins = self._build_speed_bins(
            ("brake", "speed_bins"),
            inverse_key="target_decel_mps2_to_brake_cmd",
            input_key="target_decel_mps2",
            output_key="brake_cmd",
        )
        self._brake_low_speed_hold = self._build_simple_table_from_measurements(
            ("brake", "low_speed", "hold", "measurements"),
            input_key="brake_cmd",
            output_key="brake_cmd",
            positive_only=True,
        )
        self._brake_low_speed_hold_meta = self._nested(("brake", "low_speed", "hold"))
        self._brake_low_speed_hold_cmd = _safe_float(
            (self._nested(("brake", "low_speed", "hold", "summary")) or {}).get("hold_cmd")
        )
        self._brake_low_speed_rolling = self._build_section_tables(
            ("brake", "low_speed", "rolling"),
            inverse_key="target_speed_drop_mps_to_brake_cmd",
            input_key="target_speed_drop_mps",
            output_key="brake_cmd",
            positive_only=True,
        )

    def _nested(self, keys: Sequence[str]) -> Dict[str, Any]:
        cur: Any = self.payload
        for key in keys:
            if not isinstance(cur, dict):
                return {}
            cur = cur.get(key)
        return cur if isinstance(cur, dict) else {}

    def _nested_list(self, keys: Sequence[str]) -> List[Dict[str, Any]]:
        cur: Any = self.payload
        for key in keys:
            if not isinstance(cur, dict):
                return []
            cur = cur.get(key)
        if not isinstance(cur, list):
            return []
        return [item for item in cur if isinstance(item, dict)]

    def _build_simple_table(
        self,
        keys: Sequence[str],
        *,
        input_key: str,
        output_key: str,
    ) -> AxisTable:
        return AxisTable.from_records(
            self._nested_list(keys),
            input_key=input_key,
            output_key=output_key,
        )

    def _build_simple_table_from_measurements(
        self,
        keys: Sequence[str],
        *,
        input_key: str,
        output_key: str,
        positive_only: bool = False,
    ) -> AxisTable:
        return AxisTable.from_records(
            self._nested_list(keys),
            input_key=input_key,
            output_key=output_key,
            positive_only=positive_only,
        )

    def _build_speed_bins(
        self,
        keys: Sequence[str],
        *,
        inverse_key: str,
        input_key: str,
        output_key: str,
    ) -> List[SpeedBin]:
        bins: List[SpeedBin] = []
        for item in self._nested_list(keys):
            inverse_rows = item.get("inverse", {}) if isinstance(item.get("inverse"), dict) else {}
            rows = inverse_rows.get(inverse_key) if isinstance(inverse_rows, dict) else None
            if not isinstance(rows, list):
                continue
            speed_min = _safe_float(item.get("speed_min_mps"))
            speed_max = _safe_float(item.get("speed_max_mps"))
            if speed_min is None or speed_max is None:
                continue
            table = AxisTable.from_records(rows, input_key=input_key, output_key=output_key)
            bins.append(
                SpeedBin(
                    speed_min_mps=float(speed_min),
                    speed_max_mps=float(speed_max),
                    table=table,
                    metadata=dict(item),
                )
            )
        return bins

    def _build_section_tables(
        self,
        keys: Sequence[str],
        *,
        inverse_key: str,
        input_key: str,
        output_key: str,
        positive_only: bool = False,
    ) -> List[SectionTable]:
        sections: List[SectionTable] = []
        for item in self._nested_list(keys):
            quality = item.get("quality", {}) if isinstance(item.get("quality"), dict) else {}
            if quality.get("reliable") is False:
                continue
            entry_speed = _safe_float(item.get("entry_speed_mps"))
            if entry_speed is None:
                continue
            inv = item.get("inverse", {}) if isinstance(item.get("inverse"), dict) else {}
            rows = inv.get(inverse_key)
            if not isinstance(rows, list):
                continue
            table = AxisTable.from_records(
                [row for row in rows if isinstance(row, dict)],
                input_key=input_key,
                output_key=output_key,
                positive_only=positive_only,
            )
            if not table.pairs:
                continue
            sections.append(
                SectionTable(
                    entry_speed_mps=float(entry_speed),
                    table=table,
                    metadata=dict(item),
                )
            )
        return sections

    @property
    def loaded(self) -> bool:
        return bool(self.payload)

    def status(self) -> Dict[str, Any]:
        return {
            "loaded": self.loaded,
            "source_path": str(self.source_path) if self.source_path is not None else "",
            "schema_version": self.schema_version,
            "steering_pairs": len(self._steering_angle_table.pairs),
            "steering_curvature_pairs": len(self._steering_curvature_table.pairs),
            "throttle_speed_bins": len(self._throttle_bins),
            "throttle_low_speed_launch_pairs": len(self._throttle_low_speed_launch.pairs),
            "throttle_low_speed_launch_boost_pairs": len(self._throttle_low_speed_launch_boost.pairs),
            "throttle_low_speed_crawl_sections": len(self._throttle_low_speed_crawl),
            "throttle_low_speed_crawl_boost_sections": len(self._throttle_low_speed_crawl_boost),
            "throttle_mid_speed_boost_sections": len(self._throttle_mid_speed_boost),
            "brake_speed_bins": len(self._brake_bins),
            "brake_low_speed_hold_cmd": self._brake_low_speed_hold_cmd,
            "brake_low_speed_rolling_sections": len(self._brake_low_speed_rolling),
        }

    def steering_cmd_for_angle(self, angle_deg: float) -> Optional[float]:
        sign = -1.0 if float(angle_deg) < 0.0 else 1.0
        mapped = self._steering_angle_table.lookup(abs(float(angle_deg)))
        if mapped is None:
            return None
        return clamp(sign * float(mapped), -1.0, 1.0)

    def steering_cmd_for_curvature(self, curvature: float) -> Optional[float]:
        sign = -1.0 if float(curvature) < 0.0 else 1.0
        mapped = self._steering_curvature_table.lookup(abs(float(curvature)))
        if mapped is None:
            return None
        return clamp(sign * float(mapped), -1.0, 1.0)

    def _select_speed_bin(self, bins: Sequence[SpeedBin], speed_mps: float) -> Optional[SpeedBin]:
        if not bins:
            return None
        speed = max(0.0, float(speed_mps))
        for item in bins:
            if item.contains(speed):
                return item
        return min(bins, key=lambda item: abs(item.center_mps - speed))

    def _select_section(self, sections: Sequence[SectionTable], speed_mps: float) -> Optional[SectionTable]:
        if not sections:
            return None
        speed = max(0.0, float(speed_mps))
        return min(sections, key=lambda item: abs(item.entry_speed_mps - speed))

    def _lookup_if_in_range(self, table: AxisTable, value: float) -> Optional[float]:
        if not table.pairs:
            return None
        q = float(value)
        x_min = float(table.pairs[0][0])
        if q < x_min:
            return None
        return table.lookup(q)

    @staticmethod
    def _boost_assist_cmd(
        table: AxisTable,
        *,
        target_value: float,
        target_value_max: float,
        base_cmd: Optional[float],
    ) -> Optional[float]:
        if not table.pairs:
            return base_cmd
        max_target = float(table.pairs[-1][0])
        max_cmd = float(table.pairs[-1][1])
        if target_value <= max_target or target_value_max <= max_target or max_cmd >= 0.999:
            return base_cmd
        excess_ratio = clamp(
            (float(target_value) - max_target) / max(float(target_value_max) - max_target, 1e-6),
            0.0,
            1.0,
        )
        assisted = max_cmd + (1.0 - max_cmd) * excess_ratio
        if base_cmd is None:
            return assisted
        return max(float(base_cmd), float(assisted))

    def throttle_mapping_for_accel(
        self,
        target_accel_mps2: float,
        *,
        speed_mps: float,
        target_accel_max_mps2: Optional[float] = None,
    ) -> Dict[str, Any]:
        if target_accel_mps2 <= 0.0:
            return {"cmd": 0.0, "source": "zero_request"}
        speed = max(0.0, float(speed_mps))
        target_accel_max = max(float(target_accel_max_mps2 or target_accel_mps2), float(target_accel_mps2))
        crawl_hold_speed_limit = 8.0
        crawl_boost_speed_limit = 10.0
        if speed < 1.0 and self._safe_reliable(self._throttle_low_speed_launch_meta):
            launch_eval_sec = _safe_float((self._throttle_low_speed_launch_meta.get("probe", {}) or {}).get("eval_sec")) or 0.8
            target_speed = max(0.0, speed + float(target_accel_mps2) * float(launch_eval_sec))
            mapped = self._throttle_low_speed_launch.lookup(float(target_speed))
            source = "physical_low_speed_launch"
            if self._safe_reliable(self._throttle_low_speed_launch_boost_meta):
                boost = self._lookup_if_in_range(self._throttle_low_speed_launch_boost, float(target_accel_mps2))
                if boost is not None:
                    if mapped is None or float(boost) > float(mapped):
                        mapped = boost
                        source = "physical_low_speed_launch_boost"
                assisted = self._boost_assist_cmd(
                    self._throttle_low_speed_launch_boost,
                    target_value=float(target_accel_mps2),
                    target_value_max=target_accel_max,
                    base_cmd=(None if mapped is None else float(mapped)),
                )
                if assisted is not None and (mapped is None or float(assisted) > float(mapped) + 1e-6):
                    mapped = assisted
                    source = "physical_low_speed_launch_boost_assist"
            if mapped is not None:
                return {"cmd": clamp(float(mapped), 0.0, 1.0), "source": source}
        if speed < crawl_boost_speed_limit:
            mapped = None
            source = ""
            section = self._select_section(self._throttle_low_speed_crawl, speed)
            if speed < crawl_hold_speed_limit and section is not None:
                eval_sec = _safe_float(((section.metadata.get("probe", {}) or {}).get("eval_sec"))) or 0.8
                target_speed = max(0.0, speed + float(target_accel_mps2) * float(eval_sec))
                mapped = section.table.lookup(float(target_speed))
                if mapped is not None:
                    source = f"physical_low_speed_crawl_{section.entry_speed_mps:.1f}"
            boost_section = self._select_section(self._throttle_low_speed_crawl_boost, speed)
            if boost_section is not None:
                boost = self._lookup_if_in_range(boost_section.table, float(target_accel_mps2))
                if boost is not None and (mapped is None or float(boost) > float(mapped)):
                    mapped = boost
                    source = f"physical_low_speed_crawl_boost_{boost_section.entry_speed_mps:.1f}"
                assisted = self._boost_assist_cmd(
                    boost_section.table,
                    target_value=float(target_accel_mps2),
                    target_value_max=target_accel_max,
                    base_cmd=(None if mapped is None else float(mapped)),
                )
                if assisted is not None and (mapped is None or float(assisted) > float(mapped) + 1e-6):
                    mapped = assisted
                    source = f"physical_low_speed_crawl_boost_assist_{boost_section.entry_speed_mps:.1f}"
            if mapped is not None:
                return {"cmd": clamp(float(mapped), 0.0, 1.0), "source": source}
        speed_bin = self._select_speed_bin(self._throttle_bins, speed)
        if speed_bin is None:
            return {"cmd": None, "source": "missing_speed_bin"}
        mapped = speed_bin.table.lookup(float(target_accel_mps2))
        if mapped is None:
            return {"cmd": None, "source": "missing_inverse"}
        return {"cmd": clamp(float(mapped), 0.0, 1.0), "source": "physical_inverse_accel"}

    def throttle_cmd_for_accel(self, target_accel_mps2: float, *, speed_mps: float) -> Optional[float]:
        return self.throttle_mapping_for_accel(target_accel_mps2, speed_mps=speed_mps).get("cmd")

    def brake_mapping_for_decel(self, target_decel_mps2: float, *, speed_mps: float) -> Dict[str, Any]:
        if target_decel_mps2 <= 0.0:
            return {"cmd": 0.0, "source": "zero_request"}
        speed = max(0.0, float(speed_mps))
        if speed < 1.0 and self._safe_reliable(self._brake_low_speed_hold_meta) and self._brake_low_speed_hold_cmd is not None:
            return {"cmd": clamp(float(self._brake_low_speed_hold_cmd), 0.0, 1.0), "source": "physical_low_speed_hold"}
        if speed < 8.0:
            section = self._select_section(self._brake_low_speed_rolling, speed)
            if section is not None:
                eval_sec = _safe_float(((section.metadata.get("probe", {}) or {}).get("eval_sec"))) or 0.3
                target_drop = max(0.0, float(target_decel_mps2) * float(eval_sec))
                mapped = section.table.lookup(float(target_drop))
                if mapped is not None:
                    return {
                        "cmd": clamp(float(mapped), 0.0, 1.0),
                        "source": f"physical_low_speed_rolling_{section.entry_speed_mps:.1f}",
                    }
        speed_bin = self._select_speed_bin(self._brake_bins, speed)
        if speed_bin is None:
            return {"cmd": None, "source": "missing_speed_bin"}
        mapped = speed_bin.table.lookup(float(target_decel_mps2))
        if mapped is None:
            return {"cmd": None, "source": "missing_inverse"}
        return {"cmd": clamp(float(mapped), 0.0, 1.0), "source": "physical_inverse_decel"}

    def brake_cmd_for_decel(self, target_decel_mps2: float, *, speed_mps: float) -> Optional[float]:
        return self.brake_mapping_for_decel(target_decel_mps2, speed_mps=speed_mps).get("cmd")

    @staticmethod
    def _safe_reliable(meta: Dict[str, Any]) -> bool:
        quality = meta.get("quality") if isinstance(meta, dict) else None
        return bool(isinstance(quality, dict) and quality.get("reliable"))

    def brake_deadzone_cmd(self) -> Optional[float]:
        return _safe_float(self._nested(("brake", "summary")).get("deadzone_cmd"))


def resolve_calibration_path(raw_path: str, *, repo_root: Optional[Path] = None, artifacts_dir: Optional[Path] = None) -> Optional[Path]:
    text = str(raw_path or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    candidates: List[Path] = []
    if path.is_absolute():
        candidates.append(path)
    else:
        if artifacts_dir is not None:
            candidates.append((artifacts_dir / path).resolve())
        if repo_root is not None:
            candidates.append((repo_root / path).resolve())
        candidates.append((Path.cwd() / path).resolve())
        candidates.append(path.resolve())
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else None


def load_actuator_calibration(path: Optional[Path]) -> ActuatorCalibration:
    if path is None or not path.exists():
        return ActuatorCalibration()
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return ActuatorCalibration()
    if not isinstance(payload, dict):
        return ActuatorCalibration()
    return ActuatorCalibration(payload, source_path=path)
