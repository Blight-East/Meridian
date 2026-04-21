"""
Backfill Merchant Identity
One-time script to resolve merchant identities for all existing signals.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from merchant_identity import resolve_merchant_identity

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

BATCH_SIZE = 500


def backfill():
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM signals")).scalar()
        already_linked = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE merchant_id IS NOT NULL"
        )).scalar() or 0
        print(f"Total signals: {total}")
        print(f"Already linked: {already_linked}")
        print(f"To process: {total - already_linked}")

        offset = 0
        resolved = 0
        new_merchants = 0

        # Get merchant count before
        merchants_before = conn.execute(text("SELECT COUNT(*) FROM merchants")).scalar()

        while offset < total:
            rows = conn.execute(text("""
                SELECT id, content, priority_score FROM signals
                WHERE merchant_id IS NULL
                ORDER BY id
                LIMIT :limit OFFSET :offset
            """), {"limit": BATCH_SIZE, "offset": offset}).fetchall()

            if not rows:
                break

            for row in rows:
                signal_id = row[0]
                content = row[1] or ""
                priority_score = row[2] or 0

                mid = resolve_merchant_identity(signal_id, content, priority_score)
                if mid:
                    resolved += 1

            offset += BATCH_SIZE
            print(f"  Processed batch (offset {offset})... {resolved} resolved so far")

        merchants_after = conn.execute(text("SELECT COUNT(*) FROM merchants")).scalar()
        new_merchants = merchants_after - merchants_before

    print(f"\nBackfill complete:")
    print(f"  Signals resolved to merchants: {resolved}")
    print(f"  New merchants created: {new_merchants}")
    print(f"  Total merchants now: {merchants_after}")

    # Show top merchants
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT m.canonical_name, m.domain, m.distress_score,
                   (SELECT COUNT(*) FROM merchant_signals ms WHERE ms.merchant_id = m.id) as sigs
            FROM merchants m ORDER BY m.distress_score DESC LIMIT 10
        """)).fetchall()

    print(f"\nTop merchants by distress score:")
    for r in rows:
        domain_str = f" ({r[1]})" if r[1] else ""
        print(f"  {r[0]}{domain_str}: score={r[2]:.1f}, signals={r[3]}")


if __name__ == "__main__":
    backfill()
