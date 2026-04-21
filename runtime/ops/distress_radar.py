import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from runtime.ops.alerts import send_operator_alert
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("distress_radar")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def run_distress_radar():
    logger.info("Running Merchant Distress Radar...")
    try:
        with engine.connect() as conn:
            rows_24h = conn.execute(text("""
                SELECT industry, region, COUNT(*) as cnt_24h 
                FROM signals 
                WHERE detected_at >= NOW() - INTERVAL '1 day' 
                  AND industry != 'unknown' AND industry IS NOT NULL
                GROUP BY industry, region
            """)).fetchall()
            
            rows_7d = conn.execute(text("""
                SELECT industry, region, COUNT(*)/7.0 as baseline_7d 
                FROM signals 
                WHERE detected_at >= NOW() - INTERVAL '7 days'
                  AND industry != 'unknown' AND industry IS NOT NULL
                GROUP BY industry, region
            """)).fetchall()
            
            base_map = {(r[0], r[1]): r[2] for r in rows_7d}
            triggered = False
            
            for r in rows_24h:
                industry, region, vol = r[0], r[1] or 'global', r[2]
                baseline = base_map.get((industry, region), 1.0)
                
                threshold = max(6.0, 3.0 * baseline)
                if vol >= threshold:
                    confidence = min(1.0, float(vol) / threshold)
                    msg = f"🚨 Merchant Distress Radar\n\nAnomaly spiking in {industry.upper()} ({region.upper()}).\n\nSignals last 24h: {vol}\n7-day baseline: {round(baseline,1)}\nConfidence: {round(confidence,2)}\n\nPossible systemic processor issue detected."
                    send_operator_alert(msg)
                    save_event("distress_radar_triggered", {"industry": industry, "region": region, "volume_24h": vol, "baseline": float(baseline), "confidence": confidence})
                    logger.info(f"Radar triggered for {industry} in {region}: {vol} vs {baseline}")
                    triggered = True
                    
            if not triggered:
                logger.info("No anomalies detected.")
                    
    except Exception as e:
        logger.error(f"Radar error: {e}")

if __name__ == "__main__":
    run_distress_radar()
