#!/usr/bin/env python3
"""
check_outcome_fill.py — Audit the learning_feedback_ledger outcome fill rate.

The ledger holds one row per agent decision. Outcomes (Stripe conversion,
deal stage transition, operator approve/reject) are written back via
record_outcome() when reality reveals them. If the wiring is right, the
fill rate over recent windows should grow over time. If it stays flat at
zero after the deploy grace period, an outcome hook is broken.

Usage:
    python3 deploy/check_outcome_fill.py
    python3 deploy/check_outcome_fill.py --json
    python3 deploy/check_outcome_fill.py --since-hours 24
    python3 deploy/check_outcome_fill.py --alert-after-hours 48
        # exit non-zero if 0 outcomes in last 24h *and* the deploy-grace
        # window (--alert-after-hours, anchored on first decision row)
        # has passed.
"""
from __future__ import annotations
import os, sys, json, argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import text
from memory.structured.db import engine


WINDOWS = [("1h", "1 hour"), ("24h", "24 hours"), ("7d", "7 days"), ("30d", "30 days")]


def _window_stats(conn, interval: str) -> dict:
    row = conn.execute(text(f"""
        SELECT
            COUNT(*)::int                                   AS total,
            COUNT(*) FILTER (WHERE outcome_at IS NOT NULL)::int AS with_outcome,
            COUNT(DISTINCT outcome_type)
                FILTER (WHERE outcome_at IS NOT NULL)::int  AS distinct_outcome_types
        FROM learning_feedback_ledger_raw
        WHERE created_at > NOW() - INTERVAL '{interval}'
    """)).mappings().one()
    pct = (row["with_outcome"] * 100.0 / row["total"]) if row["total"] else 0.0
    return {**dict(row), "fill_pct": round(pct, 3)}


def _outcome_breakdown(conn) -> list[dict]:
    rows = conn.execute(text("""
        SELECT outcome_type,
               COUNT(*)::int AS n,
               MIN(outcome_at) AS first_at,
               MAX(outcome_at) AS last_at
        FROM learning_feedback_ledger_raw
        WHERE outcome_at IS NOT NULL
          AND outcome_at > NOW() - INTERVAL '30 days'
        GROUP BY outcome_type
        ORDER BY n DESC
    """)).mappings().all()
    out = []
    for r in rows:
        out.append({
            "outcome_type": r["outcome_type"],
            "count": r["n"],
            "first_at": r["first_at"].isoformat() if r["first_at"] else None,
            "last_at": r["last_at"].isoformat() if r["last_at"] else None,
        })
    return out


def _ledger_age(conn) -> dict:
    row = conn.execute(text("""
        SELECT MIN(created_at) AS first, MAX(created_at) AS last,
               COUNT(*)::int    AS total,
               COUNT(*) FILTER (WHERE outcome_at IS NOT NULL)::int AS total_with_outcome
        FROM learning_feedback_ledger_raw
    """)).mappings().one()
    return {
        "first_decision_at": row["first"].isoformat() if row["first"] else None,
        "last_decision_at": row["last"].isoformat() if row["last"] else None,
        "total_decisions": row["total"],
        "total_with_outcome": row["total_with_outcome"],
    }


def collect() -> dict:
    with engine.connect() as conn:
        windows = {key: _window_stats(conn, interval) for key, interval in WINDOWS}
        breakdown = _outcome_breakdown(conn)
        age = _ledger_age(conn)
    return {"windows": windows, "by_outcome_type": breakdown, "ledger": age}


def render_human(d: dict) -> str:
    lines = ["Learning Feedback Ledger — Outcome Fill",
             "=" * 50]
    age = d["ledger"]
    lines.append(f"Decisions total:      {age['total_decisions']}  "
                 f"(with outcome: {age['total_with_outcome']})")
    lines.append(f"First decision:       {age['first_decision_at'] or 'n/a'}")
    lines.append(f"Last decision:        {age['last_decision_at'] or 'n/a'}")
    lines.append("")
    lines.append("Window     decisions  with_outcome  fill%   distinct_types")
    lines.append("-" * 60)
    for key, _ in WINDOWS:
        w = d["windows"][key]
        lines.append(f"{key:<10} {w['total']:>9}  {w['with_outcome']:>12}  "
                     f"{w['fill_pct']:>5}%   {w['distinct_outcome_types']}")
    lines.append("")
    lines.append("Outcomes by type (last 30d):")
    if not d["by_outcome_type"]:
        lines.append("  (none)")
    else:
        for r in d["by_outcome_type"]:
            lines.append(f"  {r['outcome_type']:<24} count={r['count']:<4}  "
                         f"last={r['last_at']}")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description="Audit learning ledger outcome fill rate.")
    p.add_argument("--json", action="store_true")
    p.add_argument("--since-hours", type=int, default=24,
                   help="Window for the alert decision (default: 24).")
    p.add_argument("--alert-after-hours", type=int, default=48,
                   help="Don't alert until at least N hours have passed since "
                        "the first ledger decision (deploy grace). Default: 48.")
    args = p.parse_args()

    d = collect()

    if args.json:
        print(json.dumps(d, indent=2, default=str))
    else:
        print(render_human(d))

    # Alert decision: fail (exit 2) if recent window has 0 outcomes AND we are
    # past the grace period. Exit 0 otherwise. Exit 1 reserved for read errors.
    from datetime import datetime, timezone
    age = d["ledger"]
    if not age["first_decision_at"]:
        sys.exit(0)
    first = datetime.fromisoformat(age["first_decision_at"])
    grace_passed = (datetime.now(timezone.utc) - first).total_seconds() > args.alert_after_hours * 3600

    window_key = f"{args.since_hours}h" if args.since_hours in (1, 24) else "7d"
    recent = d["windows"].get(window_key) or d["windows"]["24h"]
    if grace_passed and recent["with_outcome"] == 0 and recent["total"] > 0:
        print(f"\n⚠  ALERT: 0 outcomes in last {window_key} (grace period elapsed).",
              file=sys.stderr)
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
