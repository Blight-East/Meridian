import sys, os, time, json, datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from memory.semantic.vector_store import store_embedding, search_similar
from runtime.ops.alerts import send_operator_alert
from config.logging_config import get_logger
from runtime.intelligence.processor_classifier import PROCESSOR_TOKENS

logger = get_logger("pre_distress")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

# Mock sources to simulate gathering from status pages, github, webhooks, etc.
MOCK_SOURCES = [
    {"source": "Stripe Status", "content": "Elevated API latency detected on charge endpoints."},
    {"source": "GitHub Issues", "content": "Stripe webhook delivery failing intermittently today."},
    {"source": "Developer Forum", "content": "Anyone else seeing Stripe payout queue backlog?"},
    {"source": "PayPal Status", "content": "All systems operational."},
    {"source": "Shopify Status", "content": "Investigating intermittent checkout errors."}
]

def infer_processor(text_content):
    lower_c = text_content.lower()
    for p in PROCESSOR_TOKENS.keys():
        if p in lower_c: return p
    return "unknown"

def run_pre_distress_monitor():
    logger.info("Running Pre-Distress Early Warning Engine...")
    try:
        # In actual prod, we would fetch these from real APIs
        signals = MOCK_SOURCES
        
        processor_stress = {}
        for sig in signals:
            proc = infer_processor(sig["content"])
            if proc == "unknown": continue
            
            c = sig["content"].lower()
            stress_pts = 0.0
            if "latency" in c or "timeout" in c: stress_pts += 2.0
            if "webhook" in c or "backlog" in c or "queue" in c: stress_pts += 3.0
            if "elevated" in c or "failing" in c or "errors" in c: stress_pts += 2.5
            
            if stress_pts > 0:
                processor_stress[proc] = processor_stress.get(proc, 0.0) + stress_pts
                
        for proc, score in processor_stress.items():
            if score >= 6.0: # threshold for early warning
                with engine.connect() as conn:
                    # check if we already alerted in last 4 hours
                    recent = conn.execute(text("""
                        SELECT COUNT(*) FROM infrastructure_signals
                        WHERE processor = :p AND detected_at >= NOW() - INTERVAL '4 hours'
                    """), {"p": proc}).scalar()
                    
                    if not recent:
                        conn.execute(text("""
                            INSERT INTO infrastructure_signals (processor, signal_type, stress_score)
                            VALUES (:p, 'api_latency_webhook_delay', :s)
                        """), {"p": proc, "s": round(score, 1)})
                        conn.commit()
                        
                        msg = f"⚠️ Infrastructure Stress Detected\n\nProcessor: {proc.title()}\n\nDeveloper API latency complaints increasing.\nWebhook delivery delays reported.\n\nMerchant complaints likely to follow within hours."
                        send_operator_alert(msg)
                        save_event("infrastructure_stress_detected", {"processor": proc, "stress_score": score})
                        logger.warning(f"Early warning triggered for {proc} with stress {score}")
                        
        logger.info(f"Pre-distress monitor completed. Scanned {len(signals)} infrastructure signals.")

    except Exception as e:
        logger.error(f"Pre-distress monitor error: {e}")

if __name__ == "__main__":
    run_pre_distress_monitor()
