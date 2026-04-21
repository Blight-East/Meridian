import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from runtime.ops.alerts import send_operator_alert
from config.logging_config import get_logger

logger = get_logger("failure_forecast")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

PROCESSORS = ["stripe", "paypal", "shopify", "square", "braintree", "authorize", "adyen", "paddle"]

def infer_processor(text_content):
    if not text_content: return "unknown"
    lower_c = text_content.lower()
    for p in PROCESSORS:
        if p in lower_c:
            return p
    return "unknown"

def run_failure_forecast():
    logger.info("Running predictive failure forecast...")
    try:
        with engine.connect() as conn:
            # signals today
            rows_today = conn.execute(text("""
                SELECT content, industry FROM signals WHERE detected_at >= NOW() - INTERVAL '24 hours'
            """)).fetchall()
            
            # signals yesterday
            rows_yest = conn.execute(text("""
                SELECT content, industry FROM signals WHERE detected_at >= NOW() - INTERVAL '48 hours' AND detected_at < NOW() - INTERVAL '24 hours'
            """)).fetchall()
            
            # Aggregate by processor, industry
            agg_t = {}
            for r in rows_today:
                c, i = r[0], r[1] or "unknown"
                p = infer_processor(c)
                key = (p, i)
                agg_t[key] = agg_t.get(key, 0) + 1
                
            agg_y = {}
            for r in rows_yest:
                c, i = r[0], r[1] or "unknown"
                p = infer_processor(c)
                key = (p, i)
                agg_y[key] = agg_y.get(key, 0) + 1
            
            forecasts = 0
            for key, sigs_t in agg_t.items():
                if sigs_t < 5: continue
                
                p, i = key
                sigs_y = agg_y.get(key, 0)
                
                trend_slope = sigs_t - sigs_y
                predicted_vol = float(sigs_t + (trend_slope * 2))
                
                if predicted_vol >= 15: # anomaly threshold
                    conn.execute(text("""
                        INSERT INTO processor_forecasts (processor, industry, predicted_volume)
                        VALUES (:p, :i, :v)
                    """), {"p": p, "i": i, "v": predicted_vol})
                    conn.commit()
                    
                    msg = f"⚠️ Emerging processor failure predicted.\n\nProcessor: {p.title()}\nIndustry: {i.title()}\nSignals today: {sigs_t}\nProjected next 12h: {int(predicted_vol)}"
                    send_operator_alert(msg)
                    
                    save_event("failure_forecast_generated", {"processor": p, "industry": i, "predicted_volume": predicted_vol})
                    logger.warning(f"Predictive forecast generated: {p} in {i} -> {predicted_vol}")
                    forecasts += 1
            
            if forecasts == 0:
                logger.info("No emerging processor failures predicted.")
                
    except Exception as e:
        logger.error(f"Forecast error: {e}")

if __name__ == "__main__":
    run_failure_forecast()
