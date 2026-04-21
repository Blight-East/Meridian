import sys, os, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from memory.semantic.vector_store import search_similar
from runtime.ops.alerts import send_operator_alert
from config.logging_config import get_logger
from runtime.intelligence.processor_classifier import classify_processor

logger = get_logger("processor_discovery")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def run_processor_discovery():
    logger.info("Running processor failure discovery...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, content, industry, detected_at 
                FROM signals
                WHERE detected_at >= NOW() - INTERVAL '12 hours'
            """)).fetchall()
            
            # Find signals with NO known processor
            unknowns = [r for r in rows if classify_processor(r[1])[0] == "unknown"]
            
            processed = set()
            anomalies = 0
            
            for unk in unknowns:
                u_id, u_content, u_ind = unk[0], unk[1], unk[2]
                if u_id in processed: continue
                
                similar = search_similar(u_content, limit=20)
                
                # Filter similar to only last 12 hours
                cluster = []
                for sim in similar:
                    if sim["score"] >= 0.85:
                        sim_row = conn.execute(text("SELECT id, content FROM signals WHERE content = :c AND detected_at >= NOW() - INTERVAL '12 hours'"), {"c": sim["text"]}).fetchone()
                        if sim_row:
                            cluster.append(sim_row)
                            processed.add(sim_row[0])
                
                cluster_size = len(cluster)
                if cluster_size >= 6:
                    # Attempt deeper inference inside the cluster
                    suspected_processor = "unknown"
                    for cr in cluster:
                        p, conf = classify_processor(cr[1])
                        if p != "unknown": 
                            suspected_processor = p
                            break
                            
                    conn.execute(text("""
                        INSERT INTO processor_anomalies (suspected_processor, industry, cluster_size, anomaly_score)
                        VALUES (:p, :i, :s, :a)
                    """), {"p": suspected_processor, "i": u_ind or "unknown", "s": cluster_size, "a": cluster_size * 1.5})
                    conn.commit()
                    
                    msg = f"⚠️ Possible payment processor failure detected.\n\nCluster signals: {cluster_size}\nIndustry: {u_ind or 'unknown'}\nSuspected processor: {suspected_processor}\nExample: \"{u_content[:60]}...\""
                    send_operator_alert(msg)
                    
                    save_event("processor_anomaly_detected", {
                        "cluster_size": cluster_size,
                        "industry": u_ind,
                        "suspected_processor": suspected_processor
                    })
                    logger.warning(f"Anomaly detected: {cluster_size} signals, inferred processor: {suspected_processor}")
                    anomalies += 1
            
            if anomalies == 0:
                logger.info("No unknown processor anomalies detected.")
                
    except Exception as e:
        logger.error(f"Discovery error: {e}")

if __name__ == "__main__":
    run_processor_discovery()
