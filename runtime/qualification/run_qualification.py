import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import text
from runtime.qualification.lead_qualifier import qualify_lead
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("qualification")
from memory.structured.db import engine

_SCHEMA_READY = False
_QUALIFIED_LEAD_COLUMNS = None


def _ensure_qualification_schema(force=False):
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return

    from runtime.ops.schema_migrations import with_advisory_lock as _with_lock

    try:
        with _with_lock("qualification_schema"), engine.connect() as conn:
            conn.execute(text("SET lock_timeout = '10s'"))
            conn.execute(text("SET statement_timeout = '60s'"))
            conn.execute(text("""
                ALTER TABLE qualified_leads
                ADD COLUMN IF NOT EXISTS lead_priority DOUBLE PRECISION DEFAULT 0,
                ADD COLUMN IF NOT EXISTS lifecycle_status TEXT DEFAULT 'new',
                ADD COLUMN IF NOT EXISTS merchant_name TEXT,
                ADD COLUMN IF NOT EXISTS merchant_website TEXT,
                ADD COLUMN IF NOT EXISTS industry TEXT,
                ADD COLUMN IF NOT EXISTS location TEXT,
                ADD COLUMN IF NOT EXISTS investigation_notes TEXT
            """))
            conn.commit()
        _SCHEMA_READY = True
    except Exception as exc:
        logger.warning("Qualification schema repair deferred: %s", exc)


def _qualified_lead_columns(conn, force=False):
    global _QUALIFIED_LEAD_COLUMNS
    if _QUALIFIED_LEAD_COLUMNS is not None and not force:
        return _QUALIFIED_LEAD_COLUMNS
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'qualified_leads'
    """)).fetchall()
    _QUALIFIED_LEAD_COLUMNS = {row[0] for row in rows}
    return _QUALIFIED_LEAD_COLUMNS


def get_cluster_size_for_signal(signal_id):
    with engine.connect() as conn:
        res = conn.execute(
            text("""
                SELECT cluster_size 
                FROM clusters 
                WHERE :sid = ANY(signal_ids)
                ORDER BY cluster_size DESC LIMIT 1
            """), {"sid": int(signal_id)}
        ).scalar()
        return res if res else 0

def run_qualification():
    start = time.time()
    qualified = 0
    _ensure_qualification_schema()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT o.id AS opp_id, o.signal_id, o.opportunity_score, s.content, s.source
            FROM opportunities o
            JOIN signals s ON s.id = o.signal_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM qualified_leads ql
                WHERE ql.signal_id = o.signal_id
            )
            ORDER BY o.opportunity_score DESC
            LIMIT 50
        """))
        candidates = [dict(row._mapping) for row in rows]

    for cand in candidates:
        result = qualify_lead(cand["content"], cand["opportunity_score"])

        if not result.get("eligible_for_pipeline"):
            save_event("lead_qualification_suppressed", {
                "signal_id": cand["signal_id"],
                "qualification_score": result.get("qualification_score"),
                "icp_fit_score": result.get("icp_fit_score"),
                "icp_fit_label": result.get("icp_fit_label"),
                "disqualifier_reason": result.get("disqualifier_reason") or "",
            })
            continue

        if result["qualification_score"] >= 60:
            cluster_size = get_cluster_size_for_signal(cand["signal_id"])
            revenue_bonus = 40 if result["revenue_detected"] else 0
            
            lead_priority = result["qualification_score"] + (cluster_size * 2) + revenue_bonus

            with engine.connect() as conn:
                available_columns = _qualified_lead_columns(conn, force=True)
                insert_columns = ["signal_id", "qualification_score", "processor", "revenue_detected"]
                insert_values = [":signal_id", ":score", ":processor", ":revenue"]
                insert_params = {
                    "signal_id": cand["signal_id"],
                    "score": result["qualification_score"],
                    "processor": result["processor"],
                    "revenue": result["revenue_detected"],
                }
                if "lead_priority" in available_columns:
                    insert_columns.append("lead_priority")
                    insert_values.append(":priority")
                    insert_params["priority"] = lead_priority
                if "lifecycle_status" in available_columns:
                    insert_columns.append("lifecycle_status")
                    insert_values.append("'new'")
                conn.execute(
                    text(f"""
                        INSERT INTO qualified_leads ({", ".join(insert_columns)})
                        VALUES ({", ".join(insert_values)})
                    """),
                    insert_params,
                )
                conn.commit()

            save_event("lead_qualified", {
                "signal_id": cand["signal_id"],
                "qualification_score": result["qualification_score"],
                "icp_fit_score": result.get("icp_fit_score"),
                "icp_fit_label": result.get("icp_fit_label"),
                "lead_priority": lead_priority,
                "processor": result["processor"],
                "revenue_detected": result["revenue_detected"],
            })
            save_event("lead_lifecycle_updated", {"signal_id": cand["signal_id"], "status": "new"})
            qualified += 1

    elapsed = round(time.time() - start, 2)
    if qualified > 0:
        save_event("qualification_batch_complete", {"qualified": qualified, "elapsed_seconds": elapsed})
        logger.info(f"Qualified {qualified} leads in {elapsed}s")
    return {"qualified": qualified, "elapsed_seconds": elapsed}

if __name__ == "__main__":
    print("Running lead qualification...")
    result = run_qualification()
    print(f"Result: {result}")
