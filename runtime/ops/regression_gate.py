"""
runtime/ops/regression_gate.py
==============================

Test-automation playbook — output-shape regression gate.

Context
-------

``runtime/conversation/meridian_capability_context.py`` claims Meridian
follows a "test-automation playbook" whenever he touches the operator
briefing / routing / attribution / delivery stacks.  Until now the
playbook was a prose paragraph — nothing actually compared the live
output to a known-good baseline.

This module gives us a small, deterministic shape-check against a
locked-in JSON fixture::

    run_regression_gate(
        "operator_briefing",
        produced,
        fixture_path="runtime/fixtures/regression/operator_briefing.json",
    )

"Shape" here means: same top-level keys, and — for a dict whose values
contain primitives — the same *types* at each leaf.  We do **not**
compare values; that would fail for every fresh briefing.  We compare
*schema*, so a rename, a missing key, or a type flip (dict→list,
int→str) is caught.

Feature flag
------------

``MERIDIAN_PLAYBOOK_ENFORCEMENT`` (default: ``warn_only``) — same flag
as ``prompt_review`` so the operator only has one knob to turn.

    - ``off``        — every call returns ``ok=True`` immediately.
    - ``warn_only``  — runs the comparison, logs & emits, returns
                       ``ok=True`` regardless.
    - ``enforce``    — returns ``ok=False`` if the shape drifted.

The gate is safe to wire into a scheduler cron or a CI hook; it
reads-only from disk and emits a single ``save_event`` per call.

No fixture yet?
---------------

If ``fixture_path`` does not exist we treat the call as a *seed*
opportunity: we write the current produced shape to disk (with
``_SEED`` marker) so the next run has something to compare against.
Seeding only happens in ``warn_only`` / ``off`` modes — ``enforce``
refuses to auto-seed, because that would defeat the gate.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("meridian.regression_gate")

_FLAG_ENV = "MERIDIAN_PLAYBOOK_ENFORCEMENT"


def _mode() -> str:
    raw = os.getenv(_FLAG_ENV, "warn_only").strip().lower()
    if raw in ("off", "disabled", "0", "false"):
        return "off"
    if raw in ("enforce", "strict", "block"):
        return "enforce"
    return "warn_only"


# ---------------------------------------------------------------------------
# Shape extraction & comparison
# ---------------------------------------------------------------------------


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    if isinstance(value, tuple):
        return "list"  # treat tuple like list for shape purposes
    return value.__class__.__name__


def _shape_of(value: Any, depth: int = 4) -> Any:
    """Recursively derive a comparable 'shape' for a JSON-ish payload.

    Depth-limited so huge structures don't blow up comparison time.
    """
    if depth <= 0:
        return _type_name(value)
    if isinstance(value, dict):
        return {k: _shape_of(v, depth - 1) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        if not value:
            return ["<empty>"]
        # Homogenise: compare against the first element's shape.
        return [_shape_of(value[0], depth - 1)]
    return _type_name(value)


def _diff_shapes(expected: Any, actual: Any, path: str = "$") -> list[str]:
    """Return a list of human-readable drift descriptions."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        diffs: list[str] = []
        missing = set(expected) - set(actual)
        added = set(actual) - set(expected)
        for key in sorted(missing):
            diffs.append(f"missing key {path}.{key}")
        for key in sorted(added):
            diffs.append(f"unexpected key {path}.{key}")
        for key in set(expected) & set(actual):
            diffs.extend(_diff_shapes(expected[key], actual[key], f"{path}.{key}"))
        return diffs
    if isinstance(expected, list) and isinstance(actual, list):
        if not expected and not actual:
            return []
        if not expected or not actual:
            return [f"list size drift at {path}: expected empty={not expected}, got empty={not actual}"]
        return _diff_shapes(expected[0], actual[0], f"{path}[0]")
    if expected != actual:
        return [f"type drift at {path}: expected={expected} got={actual}"]
    return []


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def _load_fixture(fixture_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(fixture_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger.warning("[REGRESSION_GATE] failed to read %s: %s", fixture_path, exc)
        return None


def _write_seed(fixture_path: Path, shape: Any) -> None:
    try:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(
            json.dumps(
                {"_SEED": True, "shape": shape},
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        logger.info("[REGRESSION_GATE] seeded fixture at %s", fixture_path)
    except Exception as exc:  # pragma: no cover
        logger.warning("[REGRESSION_GATE] failed to seed %s: %s", fixture_path, exc)


def run_regression_gate(
    label: str,
    produced: Any,
    *,
    fixture_path: str | os.PathLike[str],
) -> dict[str, Any]:
    """Compare ``produced`` against the locked fixture at ``fixture_path``.

    Returns a verdict::

        {
            "ok": bool,
            "mode": "off" | "warn_only" | "enforce",
            "label": str,
            "drift": list[str],
            "seeded": bool,
        }
    """
    mode = _mode()
    if mode == "off":
        return {"ok": True, "mode": mode, "label": label, "drift": [], "seeded": False}

    path = Path(fixture_path)
    fixture = _load_fixture(path)
    actual_shape = _shape_of(produced)

    if fixture is None:
        # No baseline yet — in non-enforce modes, seed it for next time.
        if mode != "enforce":
            _write_seed(path, actual_shape)
            try:
                from memory.structured.db import save_event

                save_event(
                    "regression_gate",
                    {"label": label, "mode": mode, "ok": True, "seeded": True},
                )
            except Exception:  # pragma: no cover
                pass
            return {"ok": True, "mode": mode, "label": label, "drift": [], "seeded": True}
        return {
            "ok": False,
            "mode": mode,
            "label": label,
            "drift": [f"no baseline at {path}"],
            "seeded": False,
        }

    expected_shape = fixture.get("shape") if isinstance(fixture, dict) else fixture
    drift = _diff_shapes(expected_shape, actual_shape)
    verdict_ok = not drift if mode == "enforce" else True

    if drift:
        logger.warning(
            "[REGRESSION_GATE] label=%s mode=%s drift_count=%d first=%s",
            label, mode, len(drift), drift[0] if drift else "",
        )

    try:
        from memory.structured.db import save_event

        save_event(
            "regression_gate",
            {
                "label": label,
                "mode": mode,
                "ok": verdict_ok,
                "drift_count": len(drift),
                "drift_sample": drift[:5],
            },
        )
    except Exception:  # pragma: no cover
        pass

    return {
        "ok": verdict_ok,
        "mode": mode,
        "label": label,
        "drift": drift,
        "seeded": False,
    }


__all__ = ["run_regression_gate"]
