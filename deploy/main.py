import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis, json
from memory.structured.db import get_all_tasks, get_all_signals, get_all_events
from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from runtime.api.stripe_checkout import router as stripe_router
from entity_taxonomy import classify_entity, CLASS_CONSUMER_COMPLAINT, ENTITY_TYPE_PROCESSOR, ENTITY_TYPE_BANK, ENTITY_TYPE_PLATFORM
from merchant_identity import get_merchant_profile, get_merchant_signals
from contact_discovery import get_all_contacts

logger = get_logger("api")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
app = FastAPI(title="Agent Flux Control Node")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://payflux.dev",
        "https://www.payflux.dev",
    ],
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)
app.include_router(stripe_router)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def _s(rows):
    out = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if hasattr(v, 'isoformat'): d[k] = v.isoformat()
        out.append(d)
    return out

@app.get("/")
def root(): return {"status": "Agent Flux online"}

@app.get("/health")
def health(): return {"status": "healthy", "service": "Agent Flux Control Node"}

@app.get("/queue")
def queue_size(): return {"tasks": r.llen("agent_tasks")}

@app.post("/task")
def add_task(task: dict):
    r.rpush("agent_tasks", json.dumps(task))
    return {"queued": task}

@app.get("/tasks")
def list_tasks(): return {"tasks": _s(get_all_tasks())}

@app.get("/signals")
def list_signals(): return {"signals": _s(get_all_signals())}

@app.get("/signals/top")
def top_signals():
    with engine.connect() as c:
        rows = c.execute(text("SELECT id,source,content,priority_score,detected_at,ranked_at FROM signals ORDER BY priority_score DESC LIMIT 20"))
        return {"top_signals": _s([dict(r._mapping) for r in rows])}

@app.get("/clusters")
def list_clusters():
    with engine.connect() as c:
        rows = c.execute(text("SELECT id,cluster_topic,cluster_size,trend_status,trend_change,created_at FROM clusters ORDER BY cluster_size DESC"))
        return {"clusters": _s([dict(r._mapping) for r in rows])}

@app.get("/trends")
def list_trends():
    with engine.connect() as c:
        rows = c.execute(text("SELECT id,cluster_topic,cluster_size,trend_status,trend_change,created_at FROM clusters WHERE trend_status IN ('surging','rising') ORDER BY trend_change DESC"))
        return {"trends": _s([dict(r._mapping) for r in rows])}

@app.get("/opportunities")
def list_opportunities():
    with engine.connect() as c:
        rows = c.execute(text("SELECT o.id,o.opportunity_score,o.created_at,s.content,s.source,s.priority_score FROM opportunities o JOIN signals s ON s.id=o.signal_id ORDER BY o.opportunity_score DESC LIMIT 20"))
        return {"opportunities": _s([dict(r._mapping) for r in rows])}

@app.get("/leads")
def list_leads():
    with engine.connect() as c:
        rows = c.execute(text("SELECT ql.id,ql.qualification_score,ql.processor,ql.revenue_detected,ql.created_at,s.content,s.source,s.priority_score FROM qualified_leads ql JOIN signals s ON s.id=ql.signal_id ORDER BY ql.qualification_score DESC LIMIT 20"))
        return {"leads": _s([dict(r._mapping) for r in rows])}

@app.get("/lead_intelligence")
def lead_intelligence():
    with engine.connect() as c:
        rows = c.execute(text("SELECT ql.*, s.content, s.source FROM qualified_leads ql JOIN signals s ON s.id = ql.signal_id WHERE ql.investigated = TRUE ORDER BY ql.qualification_score DESC LIMIT 20"))
        return {"lead_intelligence": _s([dict(r._mapping) for r in rows])}

@app.get("/metrics")
def signal_metrics():
    with engine.connect() as c:
        total = c.execute(text("SELECT COUNT(*) FROM signals")).scalar()
        by_source = c.execute(text("SELECT CASE WHEN source LIKE '%%reddit%%' THEN 'reddit' WHEN source LIKE '%%hackernews%%' OR source LIKE '%%ycombinator%%' THEN 'hackernews' ELSE 'other' END as src, COUNT(*) as cnt FROM signals GROUP BY src ORDER BY cnt DESC"))
        dist = {r._mapping["src"]: r._mapping["cnt"] for r in by_source}
        opps = c.execute(text("SELECT COUNT(*) FROM opportunities")).scalar()
        leads = c.execute(text("SELECT COUNT(*) FROM qualified_leads")).scalar()
        investigated = c.execute(text("SELECT COUNT(*) FROM qualified_leads WHERE investigated=TRUE")).scalar()

        # Entity taxonomy metrics
        recent_signals = c.execute(text("""
            SELECT content FROM signals WHERE detected_at >= NOW() - INTERVAL '24 hours'
        """)).fetchall()

        # Contact discovery metrics
        contacts_discovered = c.execute(text("SELECT COUNT(*) FROM merchant_contacts")).scalar()
        merchants_with_contacts = c.execute(text("SELECT COUNT(DISTINCT merchant_id) FROM merchant_contacts")).scalar()

        # Semantic memory metrics
        embeddings_count = c.execute(text("SELECT COUNT(*) FROM signal_embeddings")).scalar()
        semantic_cache_hits = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'semantic_cache_hit'")).scalar()

    consumer_filtered = 0
    merchant_detected = 0
    for row in recent_signals:
        info = classify_entity(row[0] or "")
        if info["classification"] == CLASS_CONSUMER_COMPLAINT:
            consumer_filtered += 1
        elif info["entity_type"] in (ENTITY_TYPE_PROCESSOR, ENTITY_TYPE_BANK, ENTITY_TYPE_PLATFORM):
            merchant_detected += 1

    return {
        "total_signals": total, "source_distribution": dist,
        "total_opportunities": opps, "total_leads": leads,
        "investigated_leads": investigated,
        "consumer_signals_filtered": consumer_filtered,
        "merchant_signals_detected": merchant_detected,
        "contacts_discovered": contacts_discovered,
        "merchants_with_contacts": merchants_with_contacts,
        "semantic_embeddings": embeddings_count,
        "semantic_cache_hits": semantic_cache_hits,
        "contacts_found_24h": int(r.get("contacts_found_24h") or 0),
        "contacts_verified_24h": int(r.get("contacts_verified_24h") or 0),
        "contact_discovery_failures_24h": int(r.get("contact_discovery_failures_24h") or 0),
    }

@app.get("/operator_state")
def operator_state():
    with engine.connect() as c:
        sigs = c.execute(text("SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '1 day'")).scalar()
        leads = c.execute(text("SELECT COUNT(*) FROM qualified_leads")).scalar()
        trends = c.execute(text("SELECT COUNT(*) FROM clusters WHERE trend_status IN ('surging', 'rising')")).scalar()
    last_scan = r.get("last_scan_time") or "Unknown"
    return {"signals_today": sigs, "qualified_leads": leads, "active_trends": trends, "last_scan": last_scan}

@app.get("/events")
def list_events(): return {"events": _s(get_all_events())}

@app.get("/distress_radar")
def get_distress_radar():
    with engine.connect() as c:
        rows_24h = c.execute(text("SELECT industry, region, COUNT(*) as cnt_24h FROM signals WHERE detected_at >= NOW() - INTERVAL '1 day' AND industry != 'unknown' AND industry IS NOT NULL GROUP BY industry, region ORDER BY cnt_24h DESC LIMIT 1")).fetchone()
        if not rows_24h: return {"processor": "unknown", "industry": "None", "region": "global", "signals_24h": 0, "baseline": 0.0, "confidence": 0.0}

        industry, region, vol = rows_24h[0], rows_24h[1] or "global", rows_24h[2]
        base = c.execute(text("SELECT COUNT(*)/7.0 FROM signals WHERE detected_at >= NOW() - INTERVAL '7 days' AND industry = :i AND region = :r"), {"i": industry, "r": region}).scalar() or 1.0

        proc_row = c.execute(text("SELECT processor_anomalies.suspected_processor FROM processor_anomalies WHERE industry = :i ORDER BY detected_at DESC LIMIT 1"), {"i": industry}).fetchone()
        proc = proc_row[0] if proc_row else "unknown"

        threshold = max(6.0, 3.0 * base)
        confidence = min(1.0, float(vol) / threshold) if threshold > 0 else 0.0

        return {
            "processor": proc,
            "industry": industry,
            "region": region,
            "signals_24h": vol,
            "baseline": round(base, 1),
            "confidence": round(confidence, 2)
        }

@app.get("/processor_anomalies")
def list_processor_anomalies():
    with engine.connect() as c:
        rows = c.execute(text("SELECT * FROM processor_anomalies ORDER BY detected_at DESC LIMIT 20"))
        return {"processor_anomalies": _s([dict(r._mapping) for r in rows])}

@app.get("/processor_forecasts")
def list_processor_forecasts():
    with engine.connect() as c:
        rows = c.execute(text("SELECT * FROM processor_forecasts ORDER BY generated_at DESC LIMIT 20"))
        return {"processor_forecasts": _s([dict(r._mapping) for r in rows])}

@app.get("/merchants/top_distressed")
def list_top_distressed_merchants():
    with engine.connect() as c:
        rows = c.execute(text("SELECT * FROM merchants ORDER BY distress_score DESC LIMIT 20"))
        return {"top_distressed_merchants": _s([dict(r._mapping) for r in rows])}

@app.get("/merchants/{merchant_id}")
def get_merchant(merchant_id: int):
    profile = get_merchant_profile(merchant_id)
    if not profile:
        return {"error": "Merchant not found"}
    return {"merchant": profile}

@app.get("/merchants/{merchant_id}/signals")
def get_merchant_signals_api(merchant_id: int):
    signals = get_merchant_signals(merchant_id)
    return {"merchant_id": merchant_id, "signals": signals}

@app.get("/merchant_contacts/{merchant_id}")
def get_merchant_contacts_api(merchant_id: int):
    contacts = get_all_contacts(merchant_id)
    return {"merchant_id": merchant_id, "contacts": contacts}

@app.get("/cluster_intelligence")
def get_cluster_intelligence():
    with engine.connect() as c:
        rows = c.execute(text("SELECT cluster_id, processor, industry, cluster_size, merchant_count, intelligence_summary, created_at FROM cluster_intelligence ORDER BY created_at DESC LIMIT 20")).fetchall()
        return {"cluster_intelligence": [{"cluster_id": r[0], "processor": r[1], "industry": r[2], "cluster_size": r[3], "merchant_count": r[4], "summary": r[5]} for r in rows]}

@app.get("/cluster_analysis")
def get_cluster_analysis():
    with engine.connect() as c:
        rows = c.execute(text("SELECT cluster_id, analysis, risk_level, predicted_outcome, generated_at FROM cluster_analysis ORDER BY generated_at DESC LIMIT 20")).fetchall()
        return {"cluster_analysis": [{"cluster_id": r[0], "analysis": r[1], "risk_level": r[2], "predicted_outcome": r[3]} for r in rows]}
