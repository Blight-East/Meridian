import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger

logger = get_logger("market_insights")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def generate_insights():
    try:
        with engine.connect() as conn:
            top_cluster = conn.execute(text("SELECT cluster_topic FROM clusters ORDER BY cluster_size DESC LIMIT 1")).scalar()
            
            cnt_this_week = conn.execute(text("SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '7 days'")).scalar() or 0
            cnt_last_week = conn.execute(text("SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '14 days' AND detected_at < NOW() - INTERVAL '7 days'")).scalar() or 1
            
            growth_rate = round(((cnt_this_week - cnt_last_week) / cnt_last_week) * 100, 1)
            
            med_rev = conn.execute(text("""
                SELECT s.content FROM qualified_leads q
                JOIN signals s ON q.signal_id = s.id
                WHERE q.revenue_detected = TRUE
                ORDER BY q.qualification_score DESC LIMIT 1
            """)).scalar()
            
            industry = conn.execute(text("""
                SELECT industry, COUNT(*) as c FROM qualified_leads 
                WHERE industry != 'unknown' AND industry IS NOT NULL GROUP BY industry ORDER BY c DESC LIMIT 1
            """)).scalar()

        return {
            "top_issue": top_cluster or "None",
            "weekly_growth_rate": f"{growth_rate}%",
            "median_revenue_signal": med_rev[:50] if med_rev else "None",
            "dominant_industry": industry or "None"
        }
    except Exception as e:
        logger.error(f"Insight error: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    import json
    print(json.dumps(generate_insights(), indent=2))
