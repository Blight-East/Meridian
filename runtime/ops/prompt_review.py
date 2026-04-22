"""
runtime/ops/prompt_review.py
============================

Prompt-engineering playbook — moved from prose to an enforced hook.

Context
-------

``runtime/conversation/meridian_capability_context.py`` describes a
"prompt-engineering playbook" that Meridian is supposed to follow when
writing any prompt-shaped artifact: outreach messages, LLM system
prompts, rewrite instructions, etc.  Until now the playbook was a
paragraph of prose that Meridian could recite but nothing else ever
checked.  This module turns the same rubric into a deterministic
review function:

    review_prompt(text) -> dict[
        "ok": bool,
        "missing": list[str],   # criteria names that were not satisfied
        "score": int,           # 0..5
        "issues": list[str],    # human-readable descriptions
        "mode": "off" | "warn_only" | "enforce",
    ]

The rubric is intentionally shallow — it is a *lint*, not a quality
grader.  It flags messages that look structurally wrong (no audience,
no call to action, no tone), not messages that are merely bland.  The
goal is to catch regressions from the playbook, not to replace
human judgment.

Criteria (5)
------------

1. ``has_audience``       — mentions the recipient explicitly
                            (merchant name, role, or "you/your").
2. ``has_constraint``     — contains at least one explicit constraint
                            or qualifier (price, risk, compliance,
                            timing, feature guarantee).
3. ``has_call_to_action`` — contains an imperative asking for the
                            recipient to do something (reply, book,
                            try, confirm, review, click).
4. ``has_tone``           — the message is non-trivially long and
                            avoids obvious tone anti-patterns
                            (ALL CAPS, repeated !!!, all-lowercase
                            single sentence).
5. ``has_desired_output`` — the author's expected next step is
                            knowable from the message body (reply,
                            phone call, link click, demo etc.).

Feature flag
------------

``MERIDIAN_PLAYBOOK_ENFORCEMENT`` (default: ``warn_only``)

    - ``off``        — every call returns ``ok=True`` immediately.
    - ``warn_only``  — runs the lint, logs & emits a ``save_event``,
                       always returns ``ok=True`` so no send is blocked.
    - ``enforce``    — runs the lint, logs & emits, returns
                       ``ok=False`` if any criterion is missing.

The review is wired into generator call paths as ``warn_only``; flip
to ``enforce`` only after you've observed the warnings and decided the
rubric matches your taste.

Caller contract
---------------

A caller should treat ``ok=False`` as a *hint* to either rewrite or
skip, not a hard failure.  Never wrap a network send inside this
function — the review is advisory.  The pattern is::

    verdict = review_prompt(body, label="gmail_outreach")
    if not verdict["ok"]:
        logger.warning("[PROMPT_REVIEW] %s missing=%s", label, verdict["missing"])
        if verdict["mode"] == "enforce":
            return {"status": "rejected_by_prompt_review", "verdict": verdict}
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

logger = logging.getLogger("meridian.prompt_review")

_FLAG_ENV = "MERIDIAN_PLAYBOOK_ENFORCEMENT"


def _mode() -> str:
    raw = os.getenv(_FLAG_ENV, "warn_only").strip().lower()
    if raw in ("off", "disabled", "0", "false"):
        return "off"
    if raw in ("enforce", "strict", "block"):
        return "enforce"
    return "warn_only"


# ---------------------------------------------------------------------------
# Rubric
# ---------------------------------------------------------------------------

_AUDIENCE_TOKENS = re.compile(
    r"\b(you|your|team|merchant|shop|store|owner|founder|folks|hey|hi)\b",
    re.IGNORECASE,
)

_CTA_TOKENS = re.compile(
    r"\b(reply|respond|book|schedule|call|click|confirm|try|try out|open|"
    r"review|check|let me know|demo|download|sign up|subscribe|start|"
    r"approve|send|request|tell me|share|install|switch|test)\b",
    re.IGNORECASE,
)

_CONSTRAINT_TOKENS = re.compile(
    r"\b(price|cost|fee|rate|risk|compliance|pci|kyc|"
    r"within|by\s+[A-Z][a-z]+|today|tomorrow|this\s+week|"
    r"guarantee|sla|uptime|latency|encrypted|secure|minimum|maximum|"
    r"free|discount|trial|days?|hours?)\b",
    re.IGNORECASE,
)

_OUTPUT_TOKENS = re.compile(
    r"\b(reply|respond|book|call|link|demo|meeting|intro|"
    r"email|dm|message|answer|response|chat|text)\b",
    re.IGNORECASE,
)

_TONE_ANTIPATTERNS = (
    re.compile(r"[A-Z]{6,}"),                    # SCREAMING
    re.compile(r"!!{2,}"),                       # multiple bangs
    re.compile(r"\?\?{2,}"),                     # multiple question marks
)


def _check_has_audience(text: str) -> bool:
    return bool(_AUDIENCE_TOKENS.search(text))


def _check_has_call_to_action(text: str) -> bool:
    return bool(_CTA_TOKENS.search(text))


def _check_has_constraint(text: str) -> bool:
    return bool(_CONSTRAINT_TOKENS.search(text))


def _check_has_desired_output(text: str) -> bool:
    return bool(_OUTPUT_TOKENS.search(text))


def _check_has_tone(text: str) -> bool:
    stripped = text.strip()
    if len(stripped) < 20:
        return False
    for anti in _TONE_ANTIPATTERNS:
        if anti.search(stripped):
            return False
    return True


_CRITERIA: tuple[tuple[str, Any], ...] = (
    ("has_audience", _check_has_audience),
    ("has_call_to_action", _check_has_call_to_action),
    ("has_constraint", _check_has_constraint),
    ("has_desired_output", _check_has_desired_output),
    ("has_tone", _check_has_tone),
)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def review_prompt(text: str | None, *, label: str = "prompt") -> dict[str, Any]:
    """Apply the rubric to ``text`` and emit a verdict.

    Always returns a dict.  The returned ``ok`` flag respects the
    current ``MERIDIAN_PLAYBOOK_ENFORCEMENT`` mode:

    - ``off``       — ``ok=True``, ``missing=[]``, ``score=0`` (skip).
    - ``warn_only`` — ``ok=True``, full rubric result emitted to logs
                      and the events stream.
    - ``enforce``   — ``ok`` matches ``not missing``.

    ``label`` is a free-form tag (e.g. ``"gmail_outreach"``,
    ``"follow_up"``) included in the emitted event for after-the-fact
    debugging.
    """
    mode = _mode()
    if mode == "off" or not isinstance(text, str) or not text.strip():
        return {
            "ok": True,
            "missing": [],
            "score": 0,
            "issues": [],
            "mode": mode,
            "skipped": True,
        }

    missing: list[str] = []
    issues: list[str] = []
    for name, check in _CRITERIA:
        try:
            passed = bool(check(text))
        except Exception:  # pragma: no cover — a broken regex must not break a send
            passed = True
        if not passed:
            missing.append(name)
            issues.append(f"prompt missing {name}")

    score = len(_CRITERIA) - len(missing)
    verdict_ok = not missing if mode == "enforce" else True
    verdict = {
        "ok": verdict_ok,
        "missing": missing,
        "score": score,
        "issues": issues,
        "mode": mode,
        "label": label,
    }

    if missing:
        logger.warning(
            "[PROMPT_REVIEW] label=%s mode=%s missing=%s score=%d/%d",
            label, mode, missing, score, len(_CRITERIA),
        )

    try:
        from memory.structured.db import save_event

        save_event(
            "prompt_review",
            {
                "label": label,
                "mode": mode,
                "ok": verdict_ok,
                "score": score,
                "missing": missing,
            },
        )
    except Exception:  # pragma: no cover — telemetry must never block a send
        pass

    return verdict


__all__ = ["review_prompt"]
