import sys, os, time, json, redis
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from config.logging_config import get_logger
from runtime.qualification.lead_qualifier import qualify_lead

logger = get_logger("opportunity_extractor")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def extract_opportunities():
    start = time.time()
    extracted = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, priority_score, age_weight, content, source
            FROM signals
            WHERE priority_score >= 60
              AND merchant_relevant = TRUE
              AND revenue_detected = TRUE
              AND id NOT IN (SELECT signal_id FROM opportunities)
        """))
        signals = [dict(row._mapping) for row in rows]

    for sig in signals:
        qualification = qualify_lead(sig["content"], sig["priority_score"])
        if not qualification.get("eligible_for_pipeline"):
            save_event("opportunity_extraction_suppressed", {
                "signal_id": sig["id"],
                "qualification_score": qualification.get("qualification_score"),
                "icp_fit_score": qualification.get("icp_fit_score"),
                "icp_fit_label": qualification.get("icp_fit_label"),
                "disqualifier_reason": qualification.get("disqualifier_reason") or "",
                "content_preview": sig["content"][:80],
            })
            continue
        opp_score = sig["priority_score"] * (sig.get("age_weight") or 1.0)
        
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO opportunities (signal_id, opportunity_score)
                VALUES (:signal_id, :score)
            """), {"signal_id": sig["id"], "score": opp_score})
            conn.commit()
            
        save_event("opportunity_extracted", {
            "signal_id": sig["id"],
            "content_preview": sig["content"][:80],
            "score": opp_score
        })
        extracted += 1

        if opp_score >= 80:
            save_event("auto_investigation_triggered", {"signal_id": sig["id"], "score": opp_score})
            r.rpush("agent_tasks", json.dumps({"task": "investigate leads"}))
            logger.info(f"Auto-investigation triggered for signal {sig['id']}")

    elapsed = round(time.time() - start, 2)
    logger.info(f"Extracted {extracted} opportunities in {elapsed}s")
    return {"extracted": extracted, "elapsed_seconds": elapsed}

if __name__ == "__main__":
    print(extract_opportunities())
