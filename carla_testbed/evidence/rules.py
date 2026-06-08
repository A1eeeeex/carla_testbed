from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence


RULE_OPS = {"==", "!=", "<", "<=", ">", ">=", "in", "not_in", "exists"}


def evaluate_rules(
    rules: Sequence[Mapping[str, Any]],
    *,
    bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    artifacts = bundle.get("artifacts") if isinstance(bundle.get("artifacts"), Mapping) else {}
    context = bundle.get("plan") if isinstance(bundle.get("plan"), Mapping) else {}
    for rule in rules:
        result = _evaluate_rule(rule, artifacts, context=context)
        results.append(result)
    return results


def _evaluate_rule(
    rule: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    rule_id = str(rule.get("id") or "unnamed_rule")
    report_name = str(rule.get("report") or "")
    path = str(rule.get("path") or "")
    op = str(rule.get("op") or "==")
    expected = _get_path(context, str(rule.get("value_from"))) if rule.get("value_from") else rule.get("value")
    when = str(rule.get("when") or "").strip()
    if when and not _condition_matches(when, context):
        return _rule_result(rule_id, report_name, path, op, expected, None, "skipped", "when condition not met")
    if op not in RULE_OPS:
        return _rule_result(rule_id, report_name, path, op, expected, None, "fail", f"unsupported op {op!r}")
    artifact = artifacts.get(report_name)
    if not isinstance(artifact, Mapping):
        return _rule_result(rule_id, report_name, path, op, expected, None, "insufficient_data", "report artifact missing")
    report_path = artifact.get("path")
    if not report_path:
        return _rule_result(rule_id, report_name, path, op, expected, None, "insufficient_data", "report path missing")
    report = _read_json(Path(str(report_path)))
    if not report:
        return _rule_result(rule_id, report_name, path, op, expected, None, "insufficient_data", "report unreadable")
    actual = _get_path(report, path)
    if op == "exists":
        passed = actual is not None
    elif actual is None:
        return _rule_result(rule_id, report_name, path, op, expected, actual, "insufficient_data", "metric missing")
    else:
        try:
            passed = _compare(actual, op, expected)
        except (TypeError, ValueError) as exc:
            return _rule_result(
                rule_id,
                report_name,
                path,
                op,
                expected,
                actual,
                "fail",
                f"comparison failed: {exc}",
            )
    return _rule_result(
        rule_id,
        report_name,
        path,
        op,
        expected,
        actual,
        "pass" if passed else "fail",
        None if passed else "metric did not satisfy rule",
    )


def _compare(actual: Any, op: str, expected: Any) -> bool:
    if op == "==":
        return actual == expected
    if op == "!=":
        return actual != expected
    if op == "<":
        return float(actual) < float(expected)
    if op == "<=":
        return float(actual) <= float(expected)
    if op == ">":
        return float(actual) > float(expected)
    if op == ">=":
        return float(actual) >= float(expected)
    if op == "in":
        if isinstance(expected, (list, tuple, set)):
            return actual in expected
        return str(actual) in {str(expected)}
    if op == "not_in":
        if isinstance(expected, (list, tuple, set)):
            return actual not in expected
        return str(actual) not in {str(expected)}
    raise ValueError(f"unsupported op {op!r}")


def _get_path(payload: Mapping[str, Any], path: str) -> Any:
    if not path:
        return payload
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping):
            current = current.get(part)
            continue
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if 0 <= index < len(current) else None
            continue
        return None
    return current


def _condition_matches(condition: str, context: Mapping[str, Any]) -> bool:
    for op in ("==", "!="):
        if op not in condition:
            continue
        left, right = [part.strip() for part in condition.split(op, 1)]
        actual = _get_path(context, left)
        expected = _parse_literal(right)
        return (actual == expected) if op == "==" else (actual != expected)
    return bool(_get_path(context, condition))


def _parse_literal(value: str) -> Any:
    lower = value.strip().lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in {"null", "none"}:
        return None
    stripped = value.strip().strip('"').strip("'")
    try:
        if "." in stripped:
            return float(stripped)
        return int(stripped)
    except ValueError:
        return stripped


def _rule_result(
    rule_id: str,
    report: str,
    path: str,
    op: str,
    expected: Any,
    actual: Any,
    status: str,
    blocking_reason: str | None,
) -> dict[str, Any]:
    result = {
        "id": rule_id,
        "report": report,
        "path": path,
        "op": op,
        "expected": expected,
        "actual": actual,
        "status": status,
    }
    if blocking_reason:
        result["blocking_reason"] = blocking_reason
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}
