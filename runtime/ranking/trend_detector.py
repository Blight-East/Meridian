import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("trends")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def detect_trends():
    with engine.connect() as conn:
        # Current clusters (last 24h)
        rows = conn.execute(text("""
            SELECT cluster_topic, cluster_size, id
            FROM clusters
            WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ORDER BY cluster_size DESC
        """))
        current_clusters = [dict(row._mapping) for row in rows]

        # Rolling baseline: average cluster volume over previous 3 days
        rows_baseline = conn.execute(text("""
            SELECT cluster_topic, AVG(cluster_size) as avg_size
            FROM clusters
            WHERE created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '4 days'
                                  AND CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY cluster_topic
        """))
        baseline = {row._mapping["cluster_topic"]: float(row._mapping["avg_size"]) for row in rows_baseline}

    trends_detected = 0
    for cluster in current_clusters:
        topic = cluster["cluster_topic"]
        today_count = cluster["cluster_size"]
        avg_baseline = baseline.get(topic, 0)

        if avg_baseline > 0:
            change = (today_count - avg_baseline) / avg_baseline
        elif today_count >= 5:
            change = 0.5  # new cluster with enough signals = moderate
        else:
            change = 0

        if avg_baseline > 0 and today_count >= avg_baseline * 1.5 and today_count >= 3:
            trend_status = "surging"
        elif avg_baseline > 0 and today_count >= avg_baseline * 1.2:
            trend_status = "rising"
        elif avg_baseline == 0 and today_count >= 5:
            trend_status = "new"
        else:
            trend_status = "stable"

        change_pct = round(change * 100, 1)

        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE clusters SET trend_status = :status, trend_change = :change
                WHERE id = :id
            """), {"status": trend_status, "change": change_pct, "id": cluster["id"]})
            conn.commit()

        if trend_status in ("surging", "rising"):
            save_event("trend_detected", {
                "topic": topic, "trend_status": trend_status,
                "change_pct": change_pct, "today_count": today_count,
                "baseline_avg": round(avg_baseline, 1),
            })
            trends_detected += 1

    logger.info(f"Trend detection complete: {trends_detected} trends, {len(current_clusters)} clusters evaluated")
    return {"trends_detected": trends_detected}

if __name__ == "__main__":
    result = detect_trends()
    print(f"Result: {result}")
