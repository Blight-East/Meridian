#!/usr/bin/env python3
"""
a1_pull_stripe_truth.py — Stripe Conversion Reconciliation

Pulls completed checkout sessions from Stripe and reconciles against
local `customers` and `merchant_opportunities` tables. Idempotent — safe
to run repeatedly.

Usage:
    python3 deploy/a1_pull_stripe_truth.py [--dry-run]
"""
from __future__ import annotations
import os, sys, json, argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import stripe
from sqlalchemy import create_engine, text
from config.logging_config import get_logger

logger = get_logger("stripe_backfill")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True)
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")


def pull_stripe_sessions(*, limit: int = 100) -> list[dict]:
    """Fetch all completed checkout sessions from Stripe."""
    sessions = []
    has_more = True
    starting_after = None
    while has_more and len(sessions) < limit:
        params = {"limit": min(100, limit - len(sessions)), "status": "complete"}
        if starting_after:
            params["starting_after"] = starting_after
        page = stripe.checkout.Session.list(**params)
        sessions.extend(page.data)
        has_more = page.has_more
        if page.data:
            starting_after = page.data[-1].id
    return sessions


def reconcile(*, dry_run: bool = False) -> dict:
    """Reconcile Stripe truth against local DB."""
    sessions = pull_stripe_sessions()
    logger.info(f"Pulled {len(sessions)} completed sessions from Stripe")

    results = {
        "total_sessions": len(sessions),
        "already_tracked": 0,
        "newly_reconciled": 0,
        "opp_matched": 0,
        "errors": 0,
        "details": [],
    }

    if not sessions:
        logger.info("No completed sessions to reconcile")
        return results

    with engine.connect() as c:
        for session in sessions:
            sid = session.id
            email = (getattr(session, "customer_details", None) or {}).get("email", "unknown")
            cust_id = session.customer

            # Check if already in processed_stripe_events
            exists = c.execute(
                text("SELECT 1 FROM processed_stripe_events WHERE event_id = :id"),
                {"id": f"backfill:{sid}"},
            ).fetchone()
            if exists:
                results["already_tracked"] += 1
                continue

            detail = {"session_id": sid, "email": email, "action": ""}

            if dry_run:
                detail["action"] = "would_reconcile"
                results["details"].append(detail)
                results["newly_reconciled"] += 1
                continue

            try:
                # Insert/update customer
                c.execute(text("""
                    INSERT INTO customers (email, stripe_customer_id, stripe_session_id, subscription_status)
                    VALUES (:email, :cid, :sid, 'active')
                    ON CONFLICT (stripe_session_id) DO NOTHING
                """), {"email": email, "cid": cust_id, "sid": sid})

                # Try to match against merchant_opportunities
                opp = c.execute(text("""
                    UPDATE merchant_opportunities SET status = 'converted'
                    WHERE checkout_url LIKE :pat AND status != 'converted'
                    RETURNING id, merchant_domain
                """), {"pat": f"%{sid}%"}).fetchone()

                if opp:
                    results["opp_matched"] += 1
                    detail["matched_opp_id"] = opp[0]
                    detail["matched_domain"] = opp[1]

                # Record in processed_stripe_events for idempotency
                c.execute(text("""
                    INSERT INTO processed_stripe_events (event_id, event_type)
                    VALUES (:id, 'backfill_reconciliation')
                    ON CONFLICT DO NOTHING
                """), {"id": f"backfill:{sid}"})

                c.commit()
                detail["action"] = "reconciled"
                results["newly_reconciled"] += 1
            except Exception as ex:
                c.rollback()
                detail["action"] = f"error:{type(ex).__name__}"
                results["errors"] += 1
                logger.error(f"Reconciliation error for {sid}: {ex}")

            results["details"].append(detail)

    return results


def main():
    parser = argparse.ArgumentParser(description="Stripe conversion reconciliation")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    if not stripe.api_key:
        logger.error("STRIPE_SECRET_KEY not set")
        sys.exit(1)

    result = reconcile(dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))

    if result["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
