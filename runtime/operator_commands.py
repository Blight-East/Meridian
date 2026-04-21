import sys, os, redis
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import create_engine, text
import json
from runtime.ops.status_cache import (
    SYSTEM_STATUS_CACHE_KEY,
    read_status_snapshot,
    stale_status_snapshot,
    unavailable_status_snapshot,
    write_status_snapshot,
)
from runtime.contact_discovery import get_all_contacts

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
SYSTEM_STATUS_QUERY_TIMEOUT_MS = int(os.getenv("AGENT_FLUX_SYSTEM_STATUS_QUERY_TIMEOUT_MS", "1500"))


def get_today_signals():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT source, content, detected_at
            FROM signals
            WHERE detected_at > CURRENT_DATE
            ORDER BY id DESC LIMIT 20
        """))
        return [dict(row._mapping) for row in rows]


def get_signal_summary():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT topic, summary, created_at
            FROM knowledge
            WHERE topic = 'merchant_signal_summary'
            ORDER BY id DESC LIMIT 1
        """))
        results = [dict(row._mapping) for row in rows]
        return results[0] if results else None


def get_system_stats():
    with engine.connect() as conn:
        tasks = conn.execute(text("SELECT COUNT(*) as c FROM tasks")).scalar()
        signals = conn.execute(text("SELECT COUNT(*) as c FROM signals")).scalar()
        events = conn.execute(text("SELECT COUNT(*) as c FROM events")).scalar()
        return {"total_tasks": tasks, "total_signals": signals, "total_events": events}


def _build_system_status_fresh():
    last_scan = r.get("last_scan_time") or "unknown"

    with engine.connect() as conn:
        conn.execute(text(f"SET statement_timeout TO {SYSTEM_STATUS_QUERY_TIMEOUT_MS}"))
        try:
        # Pipeline metrics
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

            # Investigations
            investigations = conn.execute(text(
                "SELECT COUNT(*) FROM cluster_intelligence WHERE created_at >= NOW() - INTERVAL '24 hours'"
            )).scalar() or 0

            # Processor anomalies
            proc_anomalies = conn.execute(text("""
                SELECT processor, COUNT(*) FROM qualified_leads
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                  AND processor IS NOT NULL AND processor != 'unknown'
                GROUP BY processor ORDER BY COUNT(*) DESC LIMIT 5
            """)).fetchall()

            # Top merchants
            merchants = conn.execute(text("""
                SELECT q.merchant_name, q.processor, q.qualification_score
                FROM qualified_leads q
                WHERE q.created_at >= NOW() - INTERVAL '24 hours'
                  AND q.merchant_name IS NOT NULL AND q.merchant_name != 'unknown'
                ORDER BY q.qualification_score DESC LIMIT 5
            """)).fetchall()

            # Leads
            leads_24h = conn.execute(text(
                "SELECT COUNT(*) FROM qualified_leads WHERE created_at >= NOW() - INTERVAL '24 hours'"
            )).scalar() or 0
        finally:
            conn.execute(text("SET statement_timeout TO 0"))

    return {
        "last_scan_time": last_scan,
        "signals_24h": signals_24h,
        "signals_1h": signals_1h,
        "active_clusters": total_clusters,
        "top_clusters": [{"topic": c[0], "size": c[1]} for c in clusters],
        "investigations_24h": investigations,
        "qualified_leads_24h": leads_24h,
        "processor_anomalies": [{"processor": p[0], "count": p[1]} for p in proc_anomalies],
        "top_distressed_merchants": [{"name": m[0], "processor": m[1] or "unknown", "score": m[2]} for m in merchants],
        "pipeline_schedule": {
            "autonomous_market_cycle": "every 30 min",
            "cluster_investigation": "every 20 min",
            "processor_discovery": "every 30 min",
            "distress_radar": "every 60 min",
            "failure_forecast": "every 60 min"
        }
    }


def get_system_status():
    """Comprehensive system status for operator queries."""
    last_scan = r.get("last_scan_time") or "unknown"
    try:
        state = _build_system_status_fresh()
        write_status_snapshot(SYSTEM_STATUS_CACHE_KEY, state)
        return state
    except Exception as e:
        cached = read_status_snapshot(SYSTEM_STATUS_CACHE_KEY)
        if cached:
            return stale_status_snapshot(cached, str(e))
        return unavailable_status_snapshot(str(e), last_scan_time=last_scan)


def get_top_merchants(limit=10):
    """Return top distressed merchants ranked by distress_score."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT m.canonical_name, m.domain, m.distress_score,
                   (SELECT COUNT(*) FROM merchant_signals ms WHERE ms.merchant_id = m.id) as signal_count
            FROM merchants m
            WHERE m.distress_score > 0
            ORDER BY m.distress_score DESC
            LIMIT :lim
        """), {"lim": limit}).fetchall()

    return [{
        "name": r[0],
        "domain": r[1] or "",
        "distress_score": round(r[2], 1),
        "signal_count": r[3],
    } for r in rows]

def get_merchant_contacts(merchant_id: int):
    """Return discovered contacts for a merchant for the Telegram command."""
    with engine.connect() as conn:
        merchant = conn.execute(text("SELECT canonical_name, domain FROM merchants WHERE id = :mid"), {"mid": merchant_id}).fetchone()
        if not merchant:
            return None
    contacts = get_all_contacts(merchant_id)
    return {
        "merchant": merchant[0],
        "domain": merchant[1],
        "contacts": [
            {
                "name": contact.get("contact_name") or "",
                "email": contact.get("email") or "",
                "linkedin": contact.get("linkedin_url") or "",
                "confidence": round(float(contact.get("confidence") or 0.0), 2),
                "source": contact.get("source") or "",
                "page_type": contact.get("page_type") or "",
                "page_url": contact.get("page_url") or "",
                "role_hint": contact.get("role_hint") or "",
                "email_verified": bool(contact.get("email_verified")),
            }
            for contact in contacts
        ],
    }

def get_similar_signals(query: str, limit: int = 5):
    """Return top semantically similar signals for the operator."""
    from semantic_memory import retrieve_similar_signals
    results = retrieve_similar_signals(query, limit=limit)
    return results
