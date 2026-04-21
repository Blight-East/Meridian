import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from runtime.qualification.lead_qualifier import qualify_lead
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("qualification")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def get_cluster_size_for_signal(signal_id):
    with engine.connect() as conn:
        res = conn.execute(
            text("""
                SELECT cluster_size 
                FROM clusters 
                WHERE signal_ids @> CAST(:sid AS jsonb)
                ORDER BY cluster_size DESC LIMIT 1
            """), {"sid": f"[{signal_id}]"}
        ).scalar()
        return res if res else 0

def run_qualification():
    start = time.time()
    qualified = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT o.id AS opp_id, o.signal_id, o.opportunity_score, s.content, s.source
            FROM opportunities o
            JOIN signals s ON s.id = o.signal_id
            WHERE o.signal_id NOT IN (SELECT signal_id FROM qualified_leads)
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
                conn.execute(text("""
                    INSERT INTO qualified_leads (signal_id, qualification_score, processor, revenue_detected, lead_priority, lifecycle_status)
                    VALUES (:signal_id, :score, :processor, :revenue, :priority, 'new')
                """), {
                    "signal_id": cand["signal_id"],
                    "score": result["qualification_score"],
                    "processor": result["processor"],
                    "revenue": result["revenue_detected"],
                    "priority": lead_priority
                })
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
