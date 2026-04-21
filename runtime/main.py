import sys, os
import time
import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# ============================================================================
# BOOT INTEGRITY CHECKS — MUST RUN BEFORE ANY OTHER IMPORT.
# ============================================================================
# These three verifiers prove that the source tree, runtime config, and
# installed Python runtime all match what was approved at deploy time.
# `runtime.safety.fingerprint_check` is deliberately stdlib-only so it
# can load before FastAPI, SQLAlchemy, the Stripe SDK, or any module
# that might (now or in the future) do network I/O at import time. A
# fatal verdict in `enforce` mode raises SystemExit before we have
# loaded a single framework byte; a `report`-mode verdict logs CRITICAL
# and lets boot continue (default — see TRUST_MODEL.md "Rollout phases").
#
# Order: source (signed artifact) → config (env/db/redis/nginx) →
# runtime (interpreter + site-packages). Source first so a tampered
# repo fails fastest; runtime last because the site-packages walk is
# the slowest (~30-60s).
#
# DO NOT move these calls below any other import. DO NOT add any import
# above them except `sys`/`os` and the sys.path bootstrap. If you need
# to extend boot integrity, add another stdlib-only checker in
# runtime/safety/ and call it here.
# ============================================================================
from runtime.safety.fingerprint_check import (
    verify_or_die,
    verify_config_or_die,
    verify_runtime_or_die,
)
verify_or_die("api")
verify_config_or_die("api")
verify_runtime_or_die("api")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis, json
from memory.structured.db import (
    get_all_tasks,
    get_all_signals,
    get_all_events,
    save_event,
    save_learning_feedback,
    get_reward_write_failures_24h,
)
from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from runtime.api.stripe_checkout import router as stripe_router
from entity_taxonomy import classify_entity, CLASS_CONSUMER_COMPLAINT, CLASS_MERCHANT_DISTRESS
from merchant_identity import get_merchant_profile, get_merchant_signals
from runtime.health.telemetry import (
    get_component_state,
    get_latency_state,
    heartbeat,
    is_stale,
    record_component_state,
    record_latency,
    utc_now_iso,
)
from runtime.channels.store import get_channel_metrics_snapshot
from runtime.intelligence.merchant_intelligence_summary import get_merchant_intelligence_summary
from runtime.intelligence.payflux_intelligence import get_payflux_intelligence_metrics
from runtime.reasoning.store import get_reasoning_metrics_snapshot
from contact_discovery import get_all_contacts
from runtime.safety.schema_validator import validate_schema

logger = get_logger("api")

# Startup Schema Validation
SCHEMA_VALID, SCHEMA_MSG = validate_schema()
SCHEMA_STATUS = "valid" if SCHEMA_VALID else "mismatch"
SCHEMA_MISSING_ITEMS = SCHEMA_MSG.split(": ")[-1].split(", ") if not SCHEMA_VALID else []
if not SCHEMA_VALID:
    logger.critical(f"STARTUP BLOCKED: {SCHEMA_MSG}")
    pass

# Fail-fast on missing critical tables — surfaces unapplied migrations loudly via PM2.
from memory.structured.db import verify_critical_tables as _verify_critical_tables
try:
    _verify_critical_tables()
except RuntimeError as _e:
    logger.critical(f"STARTUP BLOCKED: {_e}")
    raise

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True, pool_recycle=300)
app = FastAPI(title="Agent Flux Control Node")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://payflux.dev",
        "https://www.payflux.dev",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)
app.include_router(stripe_router)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
METRICS_CACHE_KEY = "agent_flux:metrics:last_success"
METRICS_CACHE_TTL_SECONDS = 60
METRICS_SAMPLE_LIMIT = int(os.getenv("AGENT_FLUX_METRICS_SAMPLE_LIMIT", "200"))
METRICS_STALE_FALLBACK_SECONDS = 900

def _s(rows):
    out = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if hasattr(v, 'isoformat'): d[k] = v.isoformat()
        out.append(d)
    return out


def _health_snapshot():
    queue_depth = r.llen("agent_tasks")
    api_state = get_component_state("api")
    scheduler_state = get_component_state("scheduler")
    worker_state = get_component_state("worker")
    telegram_state = get_component_state("telegram")
    reddit_state = get_component_state("reddit")
    gmail_state = get_component_state("gmail")
    channel_actions_state = get_component_state("channel_actions")
    reasoning_state = get_component_state("reasoning")
    latency_state = get_latency_state()

    def _state_value(state, key):
        return str((state or {}).get(key) or "").strip()

    def _heartbeat_fresh(state, stale_after):
        return not is_stale(_state_value(state, "last_heartbeat_at"), stale_after)

    def _component_health(name, state):
        state = state or {}
        status_value = _state_value(state, "status").lower()
        if name == "scheduler":
            if _state_value(state, "last_error") and not is_stale(_state_value(state, "last_error_at"), 900):
                return {"status": "degraded", "detail": _state_value(state, "last_error") or "recent scheduler error"}
            if _heartbeat_fresh(state, 900):
                if status_value in {"starting", "running"}:
                    current_task = _state_value(state, "current_task")
                    detail = f"{status_value} ({current_task})" if current_task else status_value or "heartbeat fresh"
                    return {"status": "healthy", "detail": detail}
                return {"status": "healthy", "detail": "heartbeat fresh"}
            return {"status": "degraded", "detail": "no recent scheduler heartbeat"}

        if name == "worker":
            if _heartbeat_fresh(state, 600):
                return {"status": "healthy", "detail": status_value or "heartbeat fresh"}
            return {"status": "degraded", "detail": "no recent worker heartbeat"}

        if name == "telegram":
            watchdog_status = _state_value(state, "proactive_watchdog_status").lower()
            headline = _state_value(state, "proactive_delivery_headline")
            adapter_health = _state_value(state, "adapter_health").lower()
            queue_depth = int(_state_value(state, "proactive_queue_depth") or 0)
            if queue_depth == 0 and _heartbeat_fresh(state, 600) and adapter_health in {"", "healthy"}:
                return {"status": "healthy", "detail": "delivery loop healthy"}
            if watchdog_status in {"offline", "degraded", "attention"}:
                return {"status": "degraded", "detail": headline or watchdog_status}
            if _heartbeat_fresh(state, 600) and adapter_health in {"", "healthy"}:
                return {"status": "healthy", "detail": headline or adapter_health or "heartbeat fresh"}
            return {"status": "degraded", "detail": headline or "no recent telegram heartbeat"}

        if name == "api":
            return {"status": "healthy", "detail": _state_value(state, "last_route") or "serving"}

        return {"status": "healthy", "detail": "ok"}

    component_health = {
        "api": _component_health("api", api_state),
        "scheduler": _component_health("scheduler", scheduler_state),
        "worker": _component_health("worker", worker_state),
        "telegram": _component_health("telegram", telegram_state),
    }
    issues = [
        f"{name}: {row['detail']}"
        for name, row in component_health.items()
        if row.get("status") == "degraded"
    ]
    warnings = []
    if is_stale(_state_value(latency_state, "metrics_at"), 900):
        warnings.append("metrics latency snapshot is stale")

    status = "schema_mismatch" if not SCHEMA_VALID else ("degraded" if issues else "healthy")

    return {
        "status": status,
        "issues": issues,
        "warnings": warnings,
        "schema_status": SCHEMA_STATUS,
        "schema_missing_items": SCHEMA_MISSING_ITEMS,
        "service": "Agent Flux Control Node",
        "time": utc_now_iso(),
        "queue": {"depth": queue_depth},
        "component_health": component_health,
        "components": {
            "api": api_state,
            "scheduler": scheduler_state,
            "worker": worker_state,
            "telegram": telegram_state,
            "reddit": reddit_state,
            "gmail": gmail_state,
            "channel_actions": channel_actions_state,
            "reasoning": reasoning_state,
        },
        "latency": latency_state,
        "schema": {
            "valid": SCHEMA_VALID,
            "message": SCHEMA_MSG
        }
    }


def _read_cached_metrics():
    cached = r.get(METRICS_CACHE_KEY)
    if not cached:
        return None
    try:
        p = json.loads(cached)
    except json.JSONDecodeError:
        return None
    # Reject entries written during recovery mode.
    if p.get("written_under_mode") != "normal":
        return None
    return p


def _write_cached_metrics(payload):
    from runtime.safety.recovery_mode import is_recovery_mode
    if is_recovery_mode():
        return  # never cache during recovery
    payload["written_under_mode"] = "normal"
    r.setex(METRICS_CACHE_KEY, METRICS_STALE_FALLBACK_SECONDS, json.dumps(payload))


def _set_statement_timeout(conn, milliseconds):
    conn.execute(text(f"SET statement_timeout TO {int(milliseconds)}"))


def _reset_statement_timeout(conn):
    conn.execute(text("SET statement_timeout TO 0"))


def _compute_metrics_payload():
    started = time.time()
    with engine.connect() as c:
        _set_statement_timeout(c, 4000)
        try:
            total = c.execute(text("SELECT COUNT(*) FROM signals")).scalar()
            by_source = c.execute(text("SELECT CASE WHEN source LIKE '%%reddit%%' THEN 'reddit' WHEN source LIKE '%%hackernews%%' OR source LIKE '%%ycombinator%%' THEN 'hackernews' WHEN source = 'twitter' THEN 'twitter' WHEN source = 'trustpilot' THEN 'trustpilot' WHEN source = 'shopify_forum' THEN 'shopify_forum' WHEN source = 'shopify_community' THEN 'shopify_community' WHEN source = 'stripe_forum' THEN 'stripe_forum' WHEN source = 'stack_overflow' THEN 'stack_overflow' ELSE 'other' END as src, COUNT(*) as cnt FROM signals GROUP BY src ORDER BY cnt DESC"))
            dist = {row._mapping["src"]: row._mapping["cnt"] for row in by_source}
            opps = c.execute(text("SELECT COUNT(*) FROM opportunities")).scalar()
            leads = c.execute(text("SELECT COUNT(*) FROM qualified_leads")).scalar()
            investigated = c.execute(text("SELECT COUNT(*) FROM qualified_leads WHERE investigated=TRUE")).scalar()

            recent_signals = c.execute(text("""
                SELECT content
                FROM signals
                WHERE detected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY detected_at DESC
                LIMIT :limit
            """), {"limit": METRICS_SAMPLE_LIMIT}).fetchall()

            contacts_discovered = c.execute(text("SELECT COUNT(*) FROM merchant_contacts")).scalar()
            merchants_with_contacts = c.execute(text("SELECT COUNT(DISTINCT merchant_id) FROM merchant_contacts")).scalar()
            emails_generated = c.execute(text("SELECT COUNT(*) FROM merchant_contacts WHERE source = 'pattern_enumeration'")).scalar() or 0
            emails_verified = c.execute(text("SELECT COUNT(*) FROM merchant_contacts WHERE source = 'pattern_enumeration' AND confidence >= 0.8")).scalar() or 0

            embeddings_count = c.execute(text("SELECT COUNT(*) FROM signal_embeddings")).scalar()
            semantic_cache_hits = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'semantic_cache_hit'")).scalar()

            opus_calls = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'llm_reasoning_call' AND data->>'model' = 'claude-opus-4-6'")).scalar()
            sonnet_calls = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'llm_reasoning_call' AND data->>'model' = 'claude-sonnet-4-6'")).scalar()
            reasoning_router_escalations = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'reasoning_router_escalation'")).scalar()
            reasoning_degraded = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'reasoning_degraded'")).scalar()
            sales_reasoning_calls = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'sales_reasoning_call'")).scalar()
            pattern_predictions_generated = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'pattern_prediction_generated'")).scalar()

            signals_twitter = c.execute(text("SELECT COUNT(*) FROM signals WHERE source = 'twitter'")).scalar()
            signals_trustpilot = c.execute(text("SELECT COUNT(*) FROM signals WHERE source = 'trustpilot'")).scalar()
            signals_shopify = c.execute(text("SELECT COUNT(*) FROM signals WHERE source = 'shopify_forum'")).scalar()
            signals_shopify_community = c.execute(text("SELECT COUNT(*) FROM signals WHERE source = 'shopify_community'")).scalar()
            signals_stripe_forum = c.execute(text("SELECT COUNT(*) FROM signals WHERE source = 'stripe_forum'")).scalar()
            signals_stack_overflow = c.execute(text("SELECT COUNT(*) FROM signals WHERE source = 'stack_overflow'")).scalar()

            opportunities_generated = c.execute(text("SELECT COUNT(*) FROM merchant_opportunities")).scalar() or 0
            opportunities_pending = c.execute(text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'pending_review'")).scalar() or 0
            outreach_sent = c.execute(text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'outreach_sent'")).scalar() or 0
            customers_converted = c.execute(text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'converted'")).scalar() or 0

            try:
                emails_drafted = c.execute(text("SELECT COUNT(*) FROM sales_outreach_events WHERE simulation_mode = TRUE")).scalar() or 0
                emails_sent = c.execute(text("SELECT COUNT(*) FROM sales_outreach_events WHERE simulation_mode = FALSE AND blocked_reason IS NULL")).scalar() or 0
                emails_blocked_legitimacy = c.execute(text("SELECT COUNT(*) FROM sales_outreach_events WHERE blocked_reason = 'legitimacy_failed'")).scalar() or 0
                emails_blocked_warmup = c.execute(text("SELECT COUNT(*) FROM sales_outreach_events WHERE blocked_reason = 'warmup_limit_exceeded'")).scalar() or 0
                duplicate_outreach_blocked = c.execute(text("SELECT COUNT(*) FROM sales_outreach_events WHERE blocked_reason = 'duplicate_outreach'")).scalar() or 0
                outreach_blocked_low_distress = c.execute(text("SELECT COUNT(*) FROM sales_outreach_events WHERE blocked_reason = 'low_distress'")).scalar() or 0
                merchants_contacted_last_30_days = c.execute(text("SELECT COUNT(DISTINCT merchant_id) FROM sales_outreach_events WHERE sent_at >= NOW() - INTERVAL '30 days'")).scalar() or 0
                sales_conversations = c.execute(text("SELECT COUNT(DISTINCT merchant_id) FROM merchant_conversations")).scalar() or 0
            except Exception:
                c.rollback()
                emails_drafted = 0
                emails_sent = 0
                emails_blocked_legitimacy = 0
                emails_blocked_warmup = 0
                duplicate_outreach_blocked = 0
                outreach_blocked_low_distress = 0
                merchants_contacted_last_30_days = 0
                sales_conversations = 0

            conversion_rate = round((customers_converted / emails_sent * 100), 2) if emails_sent > 0 else 0.0

            merchants_attributed = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_attributed'")).scalar() or 0
            unattributed_signals = c.execute(text("SELECT COUNT(*) FROM signals WHERE merchant_id IS NULL")).scalar() or 0

            provisional_domains_created = c.execute(text("SELECT COUNT(*) FROM merchants WHERE domain_confidence = 'provisional'")).scalar() or 0
            merchant_brands_extracted = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_brand_extracted'")).scalar() or 0
            merchants_created_from_brand = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_created_from_brand'")).scalar() or 0
            domains_discovered = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'domain_discovered'")).scalar() or 0
            platform_domains_resolved = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'domain_discovered' AND data->>'source' = 'platform_pattern'")).scalar() or 0
            search_domains_resolved = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'domain_discovered' AND data->>'source' = 'search_result'")).scalar() or 0
            merchant_graph_runs = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_graph_expansion_run'")).scalar() or 0
            merchant_graph_expansions = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_graph_expanded'")).scalar() or 0
            cluster_neighbor_merchants = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_graph_expanded' AND data->>'relationship_type' = 'cluster_neighbor'")).scalar() or 0
            co_mentioned_merchants = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'merchant_graph_expanded' AND data->>'relationship_type' = 'co_mention'")).scalar() or 0

            contact_discovery_attempts_raw = c.execute(text("SELECT COUNT(*) FROM events WHERE event_type = 'tool_execution' AND data->>'action' = 'discover_contacts_for_merchant'")).scalar() or 0
            if contact_discovery_attempts_raw == 0:
                contact_discovery_attempts_raw = merchants_with_contacts
            contact_discovery_attempts = contact_discovery_attempts_raw
            contacts_found_24h = int(r.get("contacts_found_24h") or 0)
            contacts_verified_24h = int(r.get("contacts_verified_24h") or 0)
            contact_discovery_failures_24h = int(r.get("contact_discovery_failures_24h") or 0)

            try:
                diag_health = c.execute(text("SELECT health_status FROM diagnostics_events ORDER BY diagnostic_run_at DESC LIMIT 1")).scalar() or "unknown"
                diag_failures = c.execute(text("SELECT failures_detected FROM diagnostics_events ORDER BY diagnostic_run_at DESC LIMIT 1")).scalar() or []
                diag_repairs = c.execute(text("SELECT repairs_executed FROM diagnostics_events ORDER BY diagnostic_run_at DESC LIMIT 1")).scalar() or []
                diag_last_run = c.execute(text("SELECT diagnostic_run_at FROM diagnostics_events ORDER BY diagnostic_run_at DESC LIMIT 1")).scalar()
                diag_last_run_str = diag_last_run.isoformat() if diag_last_run else None

                # Pass 6: Learning Metrics
                learning_feedback_total = c.execute(text("SELECT COUNT(*) FROM learning_feedback_ledger")).scalar() or 0
                learning_rewards_sum = c.execute(text("SELECT COALESCE(SUM(reward_score), 0) FROM learning_feedback_ledger")).scalar() or 0.0
            except Exception:
                c.rollback()
                diag_health = "unknown"
                diag_failures = []
                diag_repairs = []
                diag_last_run_str = None
                learning_feedback_total = 0
                learning_rewards_sum = 0.0
        finally:
            _reset_statement_timeout(c)

    consumer_filtered = 0
    merchant_detected = 0
    for row in recent_signals:
        info = classify_entity(row[0] or "")
        if info["classification"] != CLASS_MERCHANT_DISTRESS:
            consumer_filtered += 1
        else:
            merchant_detected += 1

    merchant_signal_ratio = round(
        merchant_detected / (merchant_detected + consumer_filtered),
        4,
    ) if (merchant_detected + consumer_filtered) > 0 else 0.0

    payload = {
        "total_signals": total, "source_distribution": dist,
        "total_opportunities": opps, "total_leads": leads,
        "investigated_leads": investigated,
        "consumer_signals_filtered": consumer_filtered,
        "merchant_signals_detected": merchant_detected,
        "merchant_signal_ratio": merchant_signal_ratio,
        "contacts_discovered": contacts_discovered,
        "merchants_with_contacts": merchants_with_contacts,
        "emails_generated": emails_generated,
        "emails_verified": emails_verified,
        "provisional_domains_created": provisional_domains_created,
        "merchant_brands_extracted": merchant_brands_extracted,
        "merchants_created_from_brand": merchants_created_from_brand,
        "domains_discovered": domains_discovered,
        "platform_domains_resolved": platform_domains_resolved,
        "search_domains_resolved": search_domains_resolved,
        "merchant_graph_runs": merchant_graph_runs,
        "merchant_graph_expansions": merchant_graph_expansions,
        "cluster_neighbor_merchants": cluster_neighbor_merchants,
        "co_mentioned_merchants": co_mentioned_merchants,
        "contact_discovery_attempts": contact_discovery_attempts,
        "pipeline_health": diag_health,
        "pipeline_failures_detected": diag_failures,
        "pipeline_repairs_executed": diag_repairs,
        "last_pipeline_diagnostic": diag_last_run_str,
        "semantic_embeddings": embeddings_count,
        "semantic_cache_hits": semantic_cache_hits,
        "contacts_found_24h": contacts_found_24h,
        "contacts_verified_24h": contacts_verified_24h,
        "contact_discovery_failures_24h": contact_discovery_failures_24h,
        "opus_calls": opus_calls,
        "sonnet_calls": sonnet_calls,
        "reasoning_router_escalations": reasoning_router_escalations,
        "reasoning_degraded": reasoning_degraded,
        "sales_reasoning_calls": sales_reasoning_calls,
        "pattern_predictions_generated": pattern_predictions_generated,
        "signals_twitter": signals_twitter,
        "signals_trustpilot": signals_trustpilot,
        "signals_shopify_forum": signals_shopify,
        "signals_shopify_community": signals_shopify_community,
        "signals_stripe_forum": signals_stripe_forum,
        "signals_stack_overflow": signals_stack_overflow,
        "signals_total": total,
        "opportunities_generated": opportunities_generated,
        "opportunities_pending": opportunities_pending,
        "outreach_sent": outreach_sent,
        "emails_drafted": emails_drafted,
        "emails_sent": emails_sent,
        "emails_blocked_legitimacy": emails_blocked_legitimacy,
        "emails_blocked_warmup": emails_blocked_warmup,
        "duplicate_outreach_blocked": duplicate_outreach_blocked,
        "outreach_blocked_low_distress": outreach_blocked_low_distress,
        "merchants_contacted_last_30_days": merchants_contacted_last_30_days,
        "sales_conversations": sales_conversations,
        "opportunities_converted": customers_converted,
        "customers_converted": customers_converted,
        "conversion_rate": conversion_rate,
        "merchants_attributed": merchants_attributed,
        "unattributed_signals": unattributed_signals,
        "learning_feedback_total": learning_feedback_total,
        "learning_rewards_sum": round(float(learning_rewards_sum), 2),
        "learning_reward_write_failures_24h": get_reward_write_failures_24h(),
        "sample_size_recent_signals": len(recent_signals),
        "generated_at": utc_now_iso(),
        "metrics_status": "fresh",
        "metrics_latency_ms": round((time.time() - started) * 1000, 2),
        "schema_status": SCHEMA_STATUS,
        "schema_missing_items": SCHEMA_MISSING_ITEMS
    }
    try:
        channel_metrics = get_channel_metrics_snapshot()
        payload.update(channel_metrics)
    except Exception as e:
        logger.error(f"Error including channel metrics: {e}")

    try:
        reasoning_metrics = get_reasoning_metrics_snapshot()
        payload.update(reasoning_metrics)
    except Exception as e:
        logger.error(f"Error including reasoning metrics: {e}")

    try:
        payload.update(get_payflux_intelligence_metrics())
    except Exception as e:
        logger.error(f"Error including payflux intelligence metrics: {e}")

    try:
        reddit_state = get_component_state("reddit")
        gmail_state = get_component_state("gmail")
        payload["reddit_status"] = reddit_state.get("reddit_status", "unknown")
        payload["gmail_status"] = gmail_state.get("gmail_status", "unknown")
        payload["gmail_triage_status"] = gmail_state.get("gmail_triage_status", gmail_state.get("status", "unknown"))
        payload["last_gmail_triage_at"] = gmail_state.get("last_gmail_triage_at") or payload.get("last_gmail_triage_at")
        payload["last_gmail_triage_error"] = gmail_state.get("last_gmail_triage_error", "")
        payload["last_channel_error"] = (
            reddit_state.get("last_channel_error")
            or gmail_state.get("last_channel_error")
            or get_component_state("channel_actions").get("last_channel_error")
            or ""
        )
    except Exception as e:
        logger.error(f"Error including component status: {e}")

    return payload

@app.get("/")
def root():
    heartbeat("api", ttl=300, last_route="/")
    return {"status": "Agent Flux online"}

@app.get("/health")
def health():
    heartbeat("api", ttl=300, last_route="/health")
    return _health_snapshot()

@app.get("/queue")
def queue_size(): return {"tasks": r.llen("agent_tasks")}

@app.post("/task")
def add_task(task: dict):
    r.rpush("agent_tasks", json.dumps(task))
    heartbeat("api", ttl=300, last_route="/task")
    record_component_state("worker", ttl=600, queue_depth=r.llen("agent_tasks"))
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
    heartbeat("api", ttl=300, last_route="/metrics")

    # Recovery mode: always return degraded without any numeric values.
    from runtime.safety.recovery_mode import is_recovery_mode
    if is_recovery_mode():
        return {
            "metrics_status": "degraded",
            "degraded": True,
            "data": None,
            "reason": "recovery_mode",
            "generated_at": utc_now_iso(),
        }

    cached = _read_cached_metrics()
    if cached and time.time() - float(cached.get("generated_at_epoch", 0)) < METRICS_CACHE_TTL_SECONDS:
        cached["metrics_status"] = "cached"
        return cached

    try:
        payload = _compute_metrics_payload()
        payload["generated_at_epoch"] = time.time()
        _write_cached_metrics(payload)
        record_latency("metrics", payload["metrics_latency_ms"])
        record_component_state("api", ttl=300, last_metrics_success_at=payload["generated_at"])
        return payload
    except Exception as e:
        logger.error(f"metrics collection degraded: {e}")
        if cached:
            # Strip numeric keys that could be stale — never emit fabricated values.
            _DEGRADED_STRIP_KEYS = {
                "signals_total", "conversion_rate", "customers_converted",
                "emails_sent", "emails_drafted", "opportunities_generated",
                "outreach_sent", "merchants_attributed", "unattributed_signals",
            }
            for k in _DEGRADED_STRIP_KEYS:
                cached.pop(k, None)
            cached["metrics_status"] = "stale_fallback"
            cached["degraded"] = True
            cached["generated_at"] = cached.get("generated_at") or utc_now_iso()
            return cached
        # No cache at all — return degraded with NO fabricated numeric values.
        degraded_payload = {
            "metrics_status": "degraded",
            "degraded": True,
            "data": None,
            "reason": f"compute_failed:{type(e).__name__}",
            "generated_at": utc_now_iso(),
        }
        record_component_state("api", ttl=300, last_metrics_error_at=utc_now_iso(), last_metrics_error=str(e))
        return degraded_payload

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


@app.get("/merchant_intelligence/summary")
def merchant_intelligence_summary():
    return get_merchant_intelligence_summary()


# ── Intelligence Reports API ──

@app.get("/intelligence/reports")
def list_intelligence_reports():
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, title, slug, executive_summary, processor, industry,
                   cluster_size, risk_level, created_at
            FROM merchant_intelligence_reports
            ORDER BY created_at DESC
            LIMIT 50
        """)).fetchall()
        return {"reports": [{
            "id": r[0], "title": r[1], "slug": r[2],
            "executive_summary": r[3], "processor": r[4],
            "industry": r[5], "cluster_size": r[6],
            "risk_level": r[7], "created_at": r[8].isoformat() if r[8] else None,
        } for r in rows]}


@app.get("/intelligence/reports/{slug}")
def get_intelligence_report(slug: str):
    with engine.connect() as c:
        row = c.execute(text("""
            SELECT id, title, slug, executive_summary, content, processor, industry,
                   cluster_size, risk_level, metadata, created_at
            FROM merchant_intelligence_reports
            WHERE slug = :slug
        """), {"slug": slug}).fetchone()
        if not row:
            return {"error": "Report not found"}
        return {
            "report": {
                "id": row[0], "title": row[1], "slug": row[2],
                "executive_summary": row[3], "content": row[4],
                "processor": row[5], "industry": row[6],
                "cluster_size": row[7], "risk_level": row[8],
                "metadata": row[9] if row[9] else {},
                "created_at": row[10].isoformat() if row[10] else None,
            }
        }


# ── Deal Opportunity Endpoints ──

@app.get("/deal_opportunities")
def list_deal_opportunities():
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, merchant_id, merchant_domain, processor, distress_topic,
                   sales_strategy, outreach_draft, checkout_url, status, created_at
            FROM merchant_opportunities
            ORDER BY created_at DESC
            LIMIT 50
        """)).fetchall()
        return {"deal_opportunities": [{
            "id": r[0], "merchant_id": r[1], "merchant_domain": r[2],
            "processor": r[3], "distress_topic": r[4],
            "sales_strategy": r[5], "outreach_draft": r[6],
            "checkout_url": r[7], "status": r[8],
            "created_at": r[9].isoformat() if r[9] else None,
        } for r in rows]}


@app.post("/deal_opportunities/{opp_id}/approve")
def approve_opportunity(opp_id: int):
    with engine.connect() as c:
        result = c.execute(text("""
            UPDATE merchant_opportunities SET status = 'approved'
            WHERE id = :oid AND status = 'pending_review'
            RETURNING id
        """), {"oid": opp_id}).fetchone()
        c.commit()
        if not result:
            return {"error": "Opportunity not found or not pending"}
        
        # Pass 4: Reward hook
        try:
            opp_data = c.execute(text("""
                SELECT mo.merchant_id, dl.signal_id
                  FROM merchant_opportunities mo
             LEFT JOIN deal_lifecycle dl ON dl.opportunity_id = mo.id
                 WHERE mo.id = :oid
            """), {"oid": opp_id}).fetchone()
            if opp_data:
                save_learning_feedback({
                    "signal_id": opp_data[1],
                    "merchant_id_candidate": opp_data[0],
                    "opportunity_id": opp_id,
                    "source": "operator_approval",
                    "outcome_type": "merchant_confirmed",
                    "outcome_value": 1.0,
                    "outcome_at": datetime.datetime.utcnow(),
                    "reward_score": 1.0,
                    "notes": "Operator approved deal opportunity"
                }, hook_name="opportunity_approve")
        except Exception as le:
            logger.error(f"Reward hook failed [opportunity_approve] opp_id={opp_id}: {type(le).__name__}: {le}")

        save_event("opportunity_approved", {"opportunity_id": opp_id})
        return {"status": "approved", "opportunity_id": opp_id}


@app.post("/deal_opportunities/{opp_id}/reject")
def reject_opportunity(opp_id: int):
    with engine.connect() as c:
        result = c.execute(text("""
            UPDATE merchant_opportunities SET status = 'rejected'
            WHERE id = :oid AND status = 'pending_review'
            RETURNING id
        """), {"oid": opp_id}).fetchone()
        c.commit()
        if not result:
            return {"error": "Opportunity not found or not pending"}
        
        # Pass 4: Reward hook
        try:
            opp_data = c.execute(text("""
                SELECT mo.merchant_id, dl.signal_id
                  FROM merchant_opportunities mo
             LEFT JOIN deal_lifecycle dl ON dl.opportunity_id = mo.id
                 WHERE mo.id = :oid
            """), {"oid": opp_id}).fetchone()
            if opp_data:
                save_learning_feedback({
                    "signal_id": opp_data[1],
                    "merchant_id_candidate": opp_data[0],
                    "opportunity_id": opp_id,
                    "source": "operator_rejection",
                    "outcome_type": "merchant_rejected",
                    "outcome_value": -1.0,
                    "outcome_at": datetime.datetime.utcnow(),
                    "reward_score": -1.0,
                    "notes": "Operator rejected deal opportunity"
                }, hook_name="opportunity_reject")
        except Exception as le:
            logger.error(f"Reward hook failed [opportunity_reject] opp_id={opp_id}: {type(le).__name__}: {le}")

        save_event("opportunity_rejected", {"opportunity_id": opp_id})
        return {"status": "rejected", "opportunity_id": opp_id}
