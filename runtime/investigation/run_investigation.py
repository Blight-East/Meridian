import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from runtime.investigation.lead_investigator import investigate_lead
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("investigation")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def run_investigation():
    start = time.time()
    investigated = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ql.id, ql.signal_id, ql.qualification_score, s.content, s.source
            FROM qualified_leads ql
            JOIN signals s ON s.id = ql.signal_id
            WHERE ql.investigated = FALSE
            LIMIT 20
        """))
        leads = [dict(row._mapping) for row in rows]

    for lead in leads:
        try:
            result = investigate_lead(lead["content"], lead.get("source"))

            with engine.connect() as conn:
                conn.execute(text("""
                    UPDATE qualified_leads
                    SET merchant_name = :name,
                        merchant_website = :site,
                        industry = :industry,
                        location = :location,
                        investigation_notes = :notes,
                        investigated = TRUE
                    WHERE id = :id
                """), {
                    "name": result.get("merchant_name"),
                    "site": result.get("merchant_website"),
                    "industry": result.get("industry"),
                    "location": result.get("location"),
                    "notes": result.get("notes", "")[:500],
                    "id": lead["id"],
                })
                conn.commit()

            save_event("lead_investigated", {
                "lead_id": lead["id"],
                "merchant_name": result.get("merchant_name"),
                "industry": result.get("industry"),
                "processor": None,
            })
            investigated += 1
        except Exception as e:
            logger.error(f"Investigation error for lead {lead['id']}: {e}")

    elapsed = round(time.time() - start, 2)
    if investigated > 0:
        save_event("investigation_batch_complete", {"investigated": investigated, "elapsed_seconds": elapsed})
        logger.info(f"Investigated {investigated} leads in {elapsed}s")
    return {"investigated": investigated, "elapsed_seconds": elapsed}

if __name__ == "__main__":
    print("Running lead investigation...")
    result = run_investigation()
    print(f"Result: {result}")
