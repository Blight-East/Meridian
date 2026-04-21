import sys, os, redis
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from runtime.ops.alerts import send_operator_alert
from runtime.ops.market_insights import generate_insights
from config.logging_config import get_logger
from entity_taxonomy import classify_entity, CLASS_CONSUMER_COMPLAINT, ENTITY_TYPE_PROCESSOR, ENTITY_TYPE_BANK, ENTITY_TYPE_PLATFORM

logger = get_logger("reporting")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def generate_system_status():
    """Generate a comprehensive real-time system status report."""
    with engine.connect() as conn:
        # Pipeline timing
        last_scan = r.get("last_scan_time") or "unknown"

        # Signal metrics
        signals_24h = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

        signals_1h = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '1 hour'"
        )).scalar() or 0

        # Cluster health
        clusters = conn.execute(text(
            "SELECT cluster_topic, cluster_size FROM clusters ORDER BY cluster_size DESC LIMIT 5"
        )).fetchall()

        total_clusters = conn.execute(text("SELECT COUNT(*) FROM clusters")).scalar() or 0

        clusters_above_threshold = conn.execute(text(
            "SELECT COUNT(*) FROM clusters WHERE cluster_size >= 25"
        )).scalar() or 0

        # Processor anomalies
        proc_anomalies = conn.execute(text("""
            SELECT processor, COUNT(*) as cnt FROM qualified_leads
            WHERE created_at >= NOW() - INTERVAL '24 hours'
              AND processor IS NOT NULL AND processor != 'unknown'
            GROUP BY processor ORDER BY cnt DESC LIMIT 5
        """)).fetchall()

        # Active investigations
        investigations_24h = conn.execute(text(
            "SELECT COUNT(*) FROM cluster_intelligence WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

        # Qualified leads
        leads_24h = conn.execute(text(
            "SELECT COUNT(*) FROM qualified_leads WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

        # Top distressed merchants
        top_merchants = conn.execute(text("""
            SELECT q.merchant_name, q.processor, q.qualification_score
            FROM qualified_leads q
            WHERE q.created_at >= NOW() - INTERVAL '24 hours'
              AND q.merchant_name IS NOT NULL AND q.merchant_name != 'unknown'
            ORDER BY q.qualification_score DESC LIMIT 5
        """)).fetchall()

        # Entity taxonomy metrics — classify recent signals
        recent_signals = conn.execute(text("""
            SELECT content FROM signals WHERE detected_at >= NOW() - INTERVAL '24 hours'
        """)).fetchall()

    consumer_filtered = 0
    merchant_detected = 0
    for row in recent_signals:
        info = classify_entity(row[0] or "")
        if info["classification"] == CLASS_CONSUMER_COMPLAINT:
            consumer_filtered += 1
        elif info["entity_type"] in (ENTITY_TYPE_PROCESSOR, ENTITY_TYPE_BANK, ENTITY_TYPE_PLATFORM):
            merchant_detected += 1

    # Format cluster list
    cluster_lines = []
    for c in clusters:
        cluster_lines.append(f"  - {c[0]}: {c[1]} signals")

    # Format processor anomalies
    proc_lines = []
    for p in proc_anomalies:
        proc_lines.append(f"  - {p[0]}: {p[1]} distress signals")

    # Format top merchants
    merchant_lines = []
    for m in top_merchants:
        merchant_lines.append(f"  - {m[0]} ({m[1] or 'unknown'}) score: {m[2]}")

    report = (
        f"AGENT FLUX SYSTEM STATUS\n"
        f"{'=' * 30}\n"
        f"\n"
        f"Pipeline:\n"
        f"  Last scan: {last_scan}\n"
        f"  Signals (1h): {signals_1h}\n"
        f"  Signals (24h): {signals_24h}\n"
        f"\n"
        f"Cluster Health:\n"
        f"  Active clusters: {total_clusters}\n"
        f"  Clusters above threshold: {clusters_above_threshold}\n"
        f"  Top clusters:\n" + ("\n".join(cluster_lines) if cluster_lines else "  (none)") + "\n"
        f"\n"
        f"Processor Anomaly Detection:\n" + ("\n".join(proc_lines) if proc_lines else "  No anomalies detected") + "\n"
        f"\n"
        f"Investigations (24h): {investigations_24h}\n"
        f"Qualified leads (24h): {leads_24h}\n"
        f"\n"
        f"Top Merchants in Distress:\n" + ("\n".join(merchant_lines) if merchant_lines else "  (none)") + "\n"
        f"\n"
        f"Entity Taxonomy:\n"
        f"  Consumer Signals Filtered: {consumer_filtered}\n"
        f"  Merchant Distress Signals: {merchant_detected}\n"
    )

    return report


def generate_daily_report():
    """Generate and send the daily intelligence report."""
    with engine.connect() as conn:
        sigs = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '1 day'"
        )).scalar()
        leads = conn.execute(text(
            "SELECT COUNT(*) FROM qualified_leads WHERE created_at >= NOW() - INTERVAL '1 day'"
        )).scalar()

    ins = generate_insights()
    status = generate_system_status()

    report = (
        f"AGENT FLUX DAILY INTELLIGENCE REPORT\n"
        f"{'=' * 40}\n"
        f"\n"
        f"Signals detected (24h): {sigs or 0}\n"
        f"Qualified leads (24h): {leads or 0}\n"
        f"\n"
        f"Market Insights:\n"
        f"  Top issue: {ins.get('top_issue', 'N/A')}\n"
        f"  Weekly signal change: {ins.get('weekly_growth_rate', '0%')}\n"
        f"  Revenue signal: {str(ins.get('median_revenue_signal', 'N/A'))[:50]}\n"
        f"  Dominant industry: {ins.get('dominant_industry', 'N/A')}\n"
        f"\n"
        f"{status}"
    )
    send_operator_alert(report)
    logger.info("Daily intelligence report sent")


if __name__ == "__main__":
    # When run directly, print system status
    print(generate_system_status())
