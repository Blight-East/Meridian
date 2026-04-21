import sys, os, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from memory.semantic.vector_store import search_similar
from config.logging_config import get_logger
from collections import Counter

logger = get_logger("self_improvement")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def merge_canonical_leads():
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT c.id as canonical_id, s.content, c.duplicate_count
                FROM canonical_leads c
                JOIN signals s ON c.primary_signal_id = s.id
                ORDER BY c.id ASC
            """)).fetchall()
            
            merged = set()
            merges_performed = 0
            
            for i in range(len(rows)):
                cid = rows[i][0]
                if cid in merged: continue
                content = rows[i][1]
                
                similar = search_similar(content, limit=10)
                for sim in similar:
                    similarity = float(sim.get("score", sim.get("similarity", 0.0)) or 0.0)
                    match_text = sim.get("text") or sim.get("content") or ""
                    if similarity > 0.90 and match_text:
                        match_row = conn.execute(text("SELECT canonical_lead_id FROM signals WHERE content=:c AND canonical_lead_id IS NOT NULL AND canonical_lead_id != :cid"), {"c": match_text, "cid": cid}).fetchone()
                        if match_row:
                            target_cid = match_row[0]
                            if target_cid not in merged:
                                conn.execute(text("UPDATE canonical_leads SET duplicate_count = duplicate_count + (SELECT duplicate_count + 1 FROM canonical_leads WHERE id = :t) WHERE id = :c"), {"t": target_cid, "c": cid})
                                conn.execute(text("UPDATE signals SET canonical_lead_id = :c WHERE canonical_lead_id = :t"), {"c": cid, "t": target_cid})
                                conn.execute(text("DELETE FROM canonical_leads WHERE id = :t"), {"t": target_cid})
                                conn.commit()
                                merged.add(target_cid)
                                merges_performed += 1
                                save_event("canonical_leads_merged", {"surviving_lead": cid, "merged_lead": target_cid})
                                logger.info(f"Merged canonical lead {target_cid} into {cid}")
                                
            if merges_performed > 0:
                logger.info(f"Merged {merges_performed} canonical leads this cycle")
    except Exception as e:
        logger.error(f"Canonical merge error: {e}")

def self_improvement_cycle():
    logger.info("Starting self-improvement cycle...")
    
    # 4. Merge canonical leads first
    merge_canonical_leads()
    
    # 1. Self Improve Scanner queries with Bayesian smoothing
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT s.content FROM qualified_leads q
                JOIN signals s ON q.signal_id = s.id
                WHERE q.created_at >= NOW() - INTERVAL '3 days'
            """)).fetchall()
            
            words = []
            for r in rows:
                c = r[0].lower()
                c = re.sub(r'\[.*?\]', '', c)
                words.extend(re.findall(r'\b[a-z]{5,}\b', c))
            
            common = Counter(words).most_common(20)
            
            for w, count in common:
                if w in ['stripe', 'paypal', 'shopify', 'square', 'money', 'business', 'account', 'funds', 'holding', 'withheld', 'terminated', 'cashback']:
                    query = f"payment processor {w}" if w not in ['stripe', 'paypal', 'shopify', 'withheld', 'cashback', 'terminated'] else f"stripe {w}"
                else:
                    query = f"stripe {w}"
                    
                # Bayesian Smoothing implementation
                conn.execute(text("""
                    INSERT INTO query_performance (keyword, signals_generated, leads_generated, success_rate)
                    VALUES (:k, 10, :l, CAST(:l + 1 AS FLOAT)/(10 + 5))
                    ON CONFLICT (keyword) DO UPDATE SET 
                        leads_generated = query_performance.leads_generated + :l,
                        success_rate = CAST(query_performance.leads_generated + :l + 1 AS FLOAT) / (query_performance.signals_generated + 5)
                """), {"k": query, "l": count})
            
            conn.commit()
            
            save_event("query_performance_updated", {"new_queries_processed": len(common)})
            save_event("self_improvement_cycle_completed", {"queries_updated": len(common)})
            logger.info("Self-improvement cycle finished")
    except Exception as e:
        logger.error(f"Self-improvement error: {e}")

if __name__ == "__main__":
    self_improvement_cycle()
