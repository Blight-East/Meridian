"""
outcome_fill_audit.py — Daily audit that the learning loop is closing.

Reads recent decisions vs. recent outcomes from learning_feedback_ledger_raw.
Alerts the operator if no outcome has been recorded in the last 24h *and*
the deploy grace period has elapsed (so the very first day after wiring
the hooks doesn't false-fire).

Persists each run as a `learning_outcome_fill_audit` event so the trendline
is queryable from the events table.
"""
from __future__ import annotations
import os
from datetime import datetime, timezone, timedelta

from sqlalchemy import text

from memory.structured.db import engine, save_event_canonical
from runtime.ops.operator_alerts import send_operator_alert
from config.logging_config import get_logger

logger = get_logger("outcome_fill_audit")

GRACE_PERIOD_HOURS = int(os.getenv("AGENT_FLUX_OUTCOME_AUDIT_GRACE_HOURS", "48"))
ALERT_COOLDOWN_HOURS = int(os.getenv("AGENT_FLUX_OUTCOME_AUDIT_ALERT_COOLDOWN_HOURS", "24"))


def _stats(conn) -> dict:
    row = conn.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours')::int
                AS decisions_24h,
            COUNT(*) FILTER (WHERE outcome_at  > NOW() - INTERVAL '24 hours')::int
                AS outcomes_24h,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days')::int
                AS decisions_7d,
            COUNT(*) FILTER (WHERE outcome_at  > NOW() - INTERVAL '7 days')::int
                AS outcomes_7d,
            MIN(created_at)                                         AS first_decision_at,
            MAX(outcome_at)                                         AS last_outcome_at
        FROM learning_feedback_ledger_raw
    """)).mappings().one()
    return dict(row)


def audit_outcome_fill() -> dict:
    with engine.connect() as conn:
        s = _stats(conn)

    fill_24h_pct = (s["outcomes_24h"] * 100.0 / s["decisions_24h"]) if s["decisions_24h"] else 0.0
    fill_7d_pct  = (s["outcomes_7d"]  * 100.0 / s["decisions_7d"])  if s["decisions_7d"]  else 0.0

    payload = {
        "decisions_24h": s["decisions_24h"],
        "outcomes_24h": s["outcomes_24h"],
        "fill_24h_pct": round(fill_24h_pct, 3),
        "decisions_7d": s["decisions_7d"],
        "outcomes_7d": s["outcomes_7d"],
        "fill_7d_pct": round(fill_7d_pct, 3),
        "first_decision_at": s["first_decision_at"].isoformat() if s["first_decision_at"] else None,
        "last_outcome_at": s["last_outcome_at"].isoformat() if s["last_outcome_at"] else None,
    }

    grace_passed = False
    if s["first_decision_at"]:
        grace_passed = (datetime.now(timezone.utc) - s["first_decision_at"]) > timedelta(hours=GRACE_PERIOD_HOURS)

    should_alert = (
        grace_passed
        and s["decisions_24h"] > 0
        and s["outcomes_24h"] == 0
    )
    payload["alert_fired"] = bool(should_alert)
    payload["grace_passed"] = bool(grace_passed)

    # The canonical event also serves as the cooldown record (1 per day).
    save_event_canonical(
        "learning_outcome_fill_audit",
        payload,
        idempotency_key=f"outcome_audit:{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')}",
        subsystem="learning",
    )

    if should_alert:
        try:
            send_operator_alert(
                "⚠️ Learning loop regression — 0 outcomes recorded in last 24h.\n"
                f"Decisions (24h): {s['decisions_24h']}\n"
                f"Outcomes (7d):   {s['outcomes_7d']} of {s['decisions_7d']}\n"
                f"Last outcome:    {payload['last_outcome_at'] or 'never'}\n"
                "Check: stripe webhook hits, deal_lifecycle terminal transitions, "
                "operator approve/reject signal_id resolution."
            )
        except Exception as e:
            logger.warning(f"send_operator_alert failed in outcome_fill_audit: {e}")

    logger.info(
        f"outcome_fill_audit: 24h fill={fill_24h_pct:.2f}% "
        f"({s['outcomes_24h']}/{s['decisions_24h']}), 7d fill={fill_7d_pct:.2f}% "
        f"({s['outcomes_7d']}/{s['decisions_7d']}), alert={should_alert}"
    )
    return payload


if __name__ == "__main__":
    import json
    print(json.dumps(audit_outcome_fill(), indent=2, default=str))
