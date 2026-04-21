import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# ============================================================================
# BOOT INTEGRITY CHECKS — MUST RUN BEFORE ANY OTHER IMPORT.
# ============================================================================
# Gated on __main__ so module-import sites (e.g. tooling that introspects
# TASK_REGISTRY) do not trigger boot checks. pm2 always launches via
# `python runtime/scheduler/cron_tasks.py`, so __main__ is True in
# production. See runtime/main.py for the full rationale.
# ============================================================================
if __name__ == "__main__":
    from runtime.safety.fingerprint_check import (
        verify_or_die,
        verify_config_or_die,
        verify_runtime_or_die,
    )
    verify_or_die("scheduler")
    verify_config_or_die("scheduler")
    verify_runtime_or_die("scheduler")

import schedule, time, threading, signal
import redis
from sqlalchemy import create_engine, text as sa_text

from runtime.ops.autonomous_agent import autonomous_market_cycle
from runtime.ops.reporting import generate_daily_report
from runtime.ops.self_improvement import self_improvement_cycle
from runtime.ops.distress_radar import run_distress_radar
from runtime.ops.failure_forecast import run_failure_forecast
from runtime.intelligence.contact_discovery import run_contact_discovery, run_contact_verification
from runtime.intelligence.backfill_attribution import run_backfill as run_backfill_attribution
from runtime.intelligence.brand_extraction import run_brand_extraction
from runtime.intelligence.merchant_graph import run_merchant_graph_expansion
from runtime.intelligence.merchant_clustering import update_merchant_clusters
from runtime.intelligence.merchant_entity_extraction import extract_merchants_from_signals
from runtime.intelligence.merchant_neighbor_discovery import run_merchant_neighbor_discovery
from runtime.intelligence.merchant_propagation import propagate_merchant_graph
from runtime.intelligence.merchant_risk_scoring import update_merchant_risk_scores
from runtime.intelligence.merchant_similarity_expansion import expand_similar_merchants
from runtime.intelligence.merchant_signal_classifier import run_merchant_signal_classification
from runtime.intelligence.opportunity_scoring import score_merchants
from runtime.intelligence.opportunity_trigger_engine import run_opportunity_trigger_engine
from runtime.intelligence.signal_resurfacing import run_signal_resurfacing
from runtime.channels.service import run_gmail_triage_cycle
from runtime.ops.report_generator import generate_market_reports
from runtime.ops.pipeline_diagnostics import run_pipeline_diagnostics
from runtime.ops.operator_briefings import send_daily_operator_briefing, send_proactive_operator_updates
from runtime.ops.telegram_delivery_watchdog import run_telegram_delivery_watchdog
from runtime.intelligence.payflux_intelligence import sync_payflux_intelligence
from runtime.intelligence.cluster_investigation import investigate_unclassified_clusters
from runtime.ops.pipeline_audit import audit_sales_pipeline
from runtime.ops.mission_execution import run_mission_execution_loop
from runtime.ops.deal_lifecycle import hydrate_deal_lifecycle
from runtime.ops.deal_lifecycle_reconciliation import (
    cleanup_generic_unknown_distress_deals,
    cleanup_stale_lifecycle_deals,
    reconcile_deal_lifecycle,
    repair_unknown_distress_deals,
)
from runtime.intelligence.sparse_opportunity_enrichment import run_enrich_sparse_opportunities
from runtime.ops.strategic_deliberation import run_strategic_deliberation_loop
from runtime.ops.critic_review import run_brain_critic_loop
from runtime.ops.reply_draft_monitor import run_reply_draft_monitor
from runtime.ops.reply_outcome_monitor import run_reply_outcome_monitor
from runtime.ops.auto_send_monitor import run_auto_send_high_confidence_outreach
from runtime.ops.value_heartbeat import record_value_heartbeat
from runtime.ops.outcome_fill_audit import audit_outcome_fill
from memory.structured.db import save_event
from config.logging_config import get_logger

# Multi-source ingestion imports
from runtime.ingestion.sources.twitter_signals import run_twitter_ingestion
from runtime.ingestion.sources.trustpilot_signals import run_trustpilot_ingestion
from runtime.ingestion.sources.shopify_community_signals import run_shopify_community_ingestion
from runtime.ingestion.sources.stripe_forum_signals import run_stripe_forum_ingestion
from runtime.ingestion.sources.stack_overflow_signals import run_stack_overflow_ingestion

# Deal sourcing and sales imports
from runtime.ops.deal_sourcing import run_deal_sourcing_cycle, promote_opportunities_to_pipeline
from runtime.ops.autonomous_sales import run_autonomous_sales_cycle

# Historical ingestion
from runtime.ingestion.historical_ingestion import run_historical_ingestion
from runtime.health.telemetry import clear_component_fields, heartbeat, record_component_state, utc_now_iso

logger = get_logger("scheduler")
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
SCHEDULER_TASK_TIMEOUT_SECONDS = int(os.getenv("AGENT_FLUX_SCHEDULER_TASK_TIMEOUT_SECONDS", "300"))

# ── Continuous Discovery Engine ──────────────────────────────────────────────
_discovery_lock = threading.Lock()
_discovery_running = False
_task_lock = threading.Lock()
_active_tasks = set()
_discovery_engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True, pool_recycle=300)

# Interval bounds (seconds)
_INTERVAL_HIGH_LOAD = 60    # backlog > 50 pending opportunities
_INTERVAL_MEDIUM_LOAD = 120  # > 10 new signals in last hour
_INTERVAL_IDLE = 300         # system idle
DISABLED_TASKS = {
    item.strip()
    for item in os.getenv("AGENT_FLUX_DISABLED_TASKS", "").split(",")
    if item.strip()
}
CONTINUOUS_DISCOVERY_DISABLED = str(os.getenv("AGENT_FLUX_DISABLE_CONTINUOUS_DISCOVERY", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _get_backlog_stats():
    """Lightweight DB check for adaptive interval calculation."""
    try:
        with _discovery_engine.connect() as conn:
            pending = conn.execute(sa_text(
                "SELECT COUNT(*) FROM merchant_opportunities WHERE status IN ('pending_review','approved')"
            )).scalar() or 0
            recent_signals = conn.execute(sa_text(
                "SELECT COUNT(*) FROM signals WHERE detected_at > NOW() - INTERVAL '1 hour'"
            )).scalar() or 0
        return pending, recent_signals
    except Exception:
        return 0, 0


def _compute_interval(pending_opportunities, new_signals_last_hour):
    """Adaptive sleep: shorter when busy, longer when idle."""
    if pending_opportunities > 50:
        return _INTERVAL_HIGH_LOAD
    elif new_signals_last_hour > 10:
        return _INTERVAL_MEDIUM_LOAD
    else:
        return _INTERVAL_IDLE


def _run_ingestion_cycle():
    """Run all signal ingestion sources sequentially."""
    results = {}
    for name, fn in [
        ("twitter", run_twitter_ingestion),
        ("trustpilot", run_trustpilot_ingestion),
        ("shopify_community", run_shopify_community_ingestion),
        ("stripe_forum", run_stripe_forum_ingestion),
        ("stack_overflow", run_stack_overflow_ingestion),
    ]:
        try:
            result = fn()
            results[name] = result if isinstance(result, (int, dict)) else "ok"
        except Exception as e:
            results[name] = f"error: {e}"
            logger.debug(f"Continuous ingestion {name} error: {e}")
    return results


def run_continuous_discovery():
    """
    Continuous discovery loop that supplements scheduled jobs.
    Runs lightweight ingestion → neighbor discovery → graph expansion
    with adaptive sleep intervals based on system load.
    """
    global _discovery_running

    if not _discovery_lock.acquire(blocking=False):
        logger.info("Continuous discovery loop already running, skipping")
        return

    try:
        _discovery_running = True
        logger.info("Continuous discovery loop started")
        save_event("continuous_discovery_started", {})
        cycle_count = 0

        while _discovery_running:
            cycle_count += 1
            cycle_start = time.time()
            cycle_stats = {
                "cycle": cycle_count,
                "signals_processed": 0,
                "merchants_discovered": 0,
                "neighbors_expanded": 0,
                "graph_expansions": 0,
            }

            # Stage 1: Signal ingestion (lightweight)
            try:
                ingestion_result = _run_ingestion_cycle()
                # Count signals from results
                for source, result in ingestion_result.items():
                    if isinstance(result, dict) and "signals" in result:
                        cycle_stats["signals_processed"] += result["signals"]
                    elif isinstance(result, dict) and "stored" in result:
                        cycle_stats["signals_processed"] += int(result.get("stored") or 0)
                    elif isinstance(result, int):
                        cycle_stats["signals_processed"] += result
            except Exception as e:
                logger.error(f"Continuous ingestion error: {e}")

            # Stage 2: Merchant neighbor discovery (lightweight, small batch)
            try:
                neighbor_result = run_merchant_neighbor_discovery()
                if isinstance(neighbor_result, dict):
                    cycle_stats["neighbors_expanded"] = neighbor_result.get("neighbors_created", 0)
                    cycle_stats["merchants_discovered"] = neighbor_result.get("merchants_processed", 0)
            except Exception as e:
                logger.error(f"Continuous neighbor discovery error: {e}")

            # Stage 3: Graph expansion (lightweight)
            try:
                graph_result = run_merchant_graph_expansion()
                if isinstance(graph_result, dict):
                    cycle_stats["graph_expansions"] = graph_result.get("merchant_graph_expansions", 0)
            except Exception as e:
                logger.error(f"Continuous graph expansion error: {e}")

            # Compute adaptive interval
            pending, recent = _get_backlog_stats()
            interval = _compute_interval(pending, recent)
            cycle_duration = time.time() - cycle_start

            cycle_stats["next_run_in_seconds"] = interval
            cycle_stats["cycle_duration_seconds"] = round(cycle_duration, 2)
            cycle_stats["pending_opportunities"] = pending
            cycle_stats["recent_signals"] = recent

            save_event("continuous_discovery_cycle", cycle_stats)
            record_component_state(
                "scheduler",
                ttl=600,
                last_successful_cycle=utc_now_iso(),
                last_continuous_cycle=utc_now_iso(),
                continuous_cycle_duration_seconds=round(cycle_duration, 2),
            )
            logger.info(
                f"Continuous discovery cycle #{cycle_count} | "
                f"signals={cycle_stats['signals_processed']} | "
                f"neighbors={cycle_stats['neighbors_expanded']} | "
                f"graph={cycle_stats['graph_expansions']} | "
                f"duration={cycle_duration:.1f}s | "
                f"next_in={interval}s"
            )

            # Sleep with interruptibility (check every 5s)
            slept = 0
            while slept < interval and _discovery_running:
                time.sleep(min(5, interval - slept))
                slept += 5

    except Exception as e:
        logger.error(f"Continuous discovery loop crashed: {e}")
        save_event("continuous_discovery_error", {"error": str(e)})
    finally:
        _discovery_running = False
        _discovery_lock.release()
        logger.info("Continuous discovery loop stopped")


def stop_continuous_discovery():
    """Signal the continuous discovery loop to stop gracefully."""
    global _discovery_running
    _discovery_running = False


class SchedulerTaskTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise SchedulerTaskTimeout("scheduler task exceeded timeout")


def _run_with_timeout(fn, timeout_seconds):
    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(max(1, int(timeout_seconds)))
    try:
        return fn()
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def _safe(name, fn):
    with _task_lock:
        if name in _active_tasks:
            logger.warning(f"{name} already running, skipping duplicate scheduled invocation")
            save_event("scheduler_duplicate_skipped", {"task": name})
            return
        _active_tasks.add(name)
    start = time.time()
    try:
        heartbeat("scheduler", ttl=600, current_task=name, current_task_started_at=utc_now_iso())
        _run_with_timeout(fn, SCHEDULER_TASK_TIMEOUT_SECONDS)
        duration = time.time() - start
        logger.info(f"{name} completed in {duration:.2f}s")
        record_component_state(
            "scheduler",
            ttl=600,
            last_successful_cycle=utc_now_iso(),
            last_completed_task=name,
            last_task_duration_seconds=round(duration, 2),
        )
        clear_component_fields("scheduler", "last_error_at", "last_error_task", "last_error", "last_timeout_at", "last_timed_out_task")
        save_event("scheduler_task_completed", {"task": name, "duration_seconds": round(duration, 2)})
    except SchedulerTaskTimeout:
        duration = time.time() - start
        logger.error(f"{name} timed out after {duration:.2f}s")
        save_event("scheduler_timeout", {"task": name, "duration_seconds": round(duration, 2)})
        record_component_state(
            "scheduler",
            ttl=600,
            last_timeout_at=utc_now_iso(),
            last_timed_out_task=name,
            last_task_duration_seconds=round(duration, 2),
        )
    except Exception as e:
        duration = time.time() - start
        logger.error(f"{name} error after {duration:.2f}s: {e}")
        save_event("scheduler_error", {"task": name, "error": str(e)})
        record_component_state(
            "scheduler",
            ttl=600,
            last_error_at=utc_now_iso(),
            last_error_task=name,
            last_error=str(e),
        )
    finally:
        clear_component_fields("scheduler", "current_task", "current_task_started_at")
        with _task_lock:
            _active_tasks.discard(name)

TASK_REGISTRY = [
    {"name": "autonomous_market_cycle", "every": 30, "unit": "minutes", "fn": autonomous_market_cycle, "group": "market"},
    {"name": "run_distress_radar", "every": 60, "unit": "minutes", "fn": run_distress_radar, "group": "market"},
    {"name": "run_failure_forecast", "every": 60, "unit": "minutes", "fn": run_failure_forecast, "group": "market"},
    {"name": "run_contact_discovery", "every": 20, "unit": "minutes", "fn": run_contact_discovery, "group": "contacts", "run_on_startup": True, "startup_priority": 4},
    {"name": "run_contact_verification", "every": 20, "unit": "minutes", "fn": run_contact_verification, "group": "contacts", "run_on_startup": True, "startup_priority": 5},
    {"name": "run_merchant_signal_classification", "every": 30, "unit": "minutes", "fn": run_merchant_signal_classification, "group": "intelligence"},
    {"name": "run_brand_extraction", "every": 30, "unit": "minutes", "fn": run_brand_extraction, "group": "intelligence"},
    {"name": "merchant_entity_extraction", "every": 15, "unit": "minutes", "fn": lambda: extract_merchants_from_signals(limit=100), "group": "intelligence"},
    {"name": "run_merchant_graph_expansion", "every": 30, "unit": "minutes", "fn": run_merchant_graph_expansion, "group": "graph"},
    {"name": "merchant_similarity_expansion", "every": 30, "unit": "minutes", "fn": lambda: expand_similar_merchants(limit=20), "group": "graph"},
    {"name": "merchant_propagation", "every": 30, "unit": "minutes", "fn": propagate_merchant_graph, "group": "graph"},
    {"name": "update_merchant_clusters", "every": 60, "unit": "minutes", "fn": update_merchant_clusters, "group": "graph"},
    {"name": "update_merchant_risk_scores", "every": 60, "unit": "minutes", "fn": update_merchant_risk_scores, "group": "graph"},
    {"name": "run_pipeline_diagnostics", "every": 10, "unit": "minutes", "fn": run_pipeline_diagnostics, "group": "ops"},
    {"name": "run_telegram_delivery_watchdog", "every": 5, "unit": "minutes", "fn": run_telegram_delivery_watchdog, "group": "ops", "run_on_startup": True, "startup_priority": 8},
    {"name": "run_gmail_triage_cycle", "every": 15, "unit": "minutes", "fn": run_gmail_triage_cycle, "group": "channels"},
    {"name": "run_reply_outcome_monitor", "every": 15, "unit": "minutes", "fn": run_reply_outcome_monitor, "group": "operator"},
    {"name": "run_reply_draft_monitor", "every": 15, "unit": "minutes", "fn": run_reply_draft_monitor, "group": "operator"},
    {"name": "run_auto_send_high_confidence_outreach", "every": 15, "unit": "minutes", "fn": run_auto_send_high_confidence_outreach, "group": "operator", "run_on_startup": True, "startup_priority": 32},
    {"name": "sync_payflux_intelligence", "every": 15, "unit": "minutes", "fn": sync_payflux_intelligence, "group": "intelligence"},
    {"name": "send_proactive_operator_updates", "every": 15, "unit": "minutes", "fn": send_proactive_operator_updates, "group": "operator"},
    {"name": "record_value_heartbeat", "every": 15, "unit": "minutes", "fn": record_value_heartbeat, "group": "operator"},
    {"name": "run_enrich_sparse_opportunities", "every": 1, "unit": "hours", "fn": run_enrich_sparse_opportunities, "group": "operator", "run_on_startup": True, "startup_priority": 23},
    {"name": "hydrate_deal_lifecycle", "every": 2, "unit": "hours", "fn": hydrate_deal_lifecycle, "group": "operator", "run_on_startup": True, "startup_priority": 24},
    {"name": "repair_unknown_distress_deals", "every": 2, "unit": "hours", "fn": repair_unknown_distress_deals, "group": "operator", "run_on_startup": True, "startup_priority": 25},
    {"name": "reconcile_deal_lifecycle", "every": 1, "unit": "hours", "fn": reconcile_deal_lifecycle, "group": "operator", "run_on_startup": True, "startup_priority": 26},
    {"name": "cleanup_generic_unknown_distress_deals", "every": 6, "unit": "hours", "fn": cleanup_generic_unknown_distress_deals, "group": "operator", "run_on_startup": True, "startup_priority": 27},
    {"name": "cleanup_stale_lifecycle_deals", "every": 6, "unit": "hours", "fn": cleanup_stale_lifecycle_deals, "group": "operator", "run_on_startup": True, "startup_priority": 28},
    {"name": "run_strategic_deliberation_loop", "every": 6, "unit": "hours", "fn": lambda: run_strategic_deliberation_loop(send_update=False), "group": "operator", "run_on_startup": True, "startup_priority": 40},
    {"name": "run_brain_critic_loop", "every": 6, "unit": "hours", "fn": lambda: run_brain_critic_loop(send_update=False), "group": "operator"},
    {"name": "run_mission_execution_loop", "every": 1, "unit": "hours", "fn": run_mission_execution_loop, "group": "operator", "run_on_startup": True, "startup_priority": 20},
    {"name": "run_merchant_neighbor_discovery", "every": 2, "unit": "hours", "fn": run_merchant_neighbor_discovery, "group": "graph"},
    {"name": "score_merchants", "every": 2, "unit": "hours", "fn": score_merchants, "group": "intelligence"},
    {"name": "run_opportunity_trigger_engine", "every": 2, "unit": "hours", "fn": run_opportunity_trigger_engine, "group": "sales"},
    {"name": "run_signal_resurfacing", "every": 3, "unit": "hours", "fn": run_signal_resurfacing, "group": "intelligence"},
    {"name": "generate_market_reports", "every": 6, "unit": "hours", "fn": generate_market_reports, "group": "operator"},
    {"name": "run_backfill_attribution", "every": 6, "unit": "hours", "fn": run_backfill_attribution, "group": "intelligence"},
    {"name": "self_improvement_cycle", "every": 12, "unit": "hours", "fn": self_improvement_cycle, "group": "ops", "run_on_startup": True, "startup_priority": 60},
    {"name": "generate_daily_report", "every": 24, "unit": "hours", "fn": generate_daily_report, "group": "operator"},
    {"name": "audit_outcome_fill", "every": 24, "unit": "hours", "fn": audit_outcome_fill, "group": "ops"},
    {"name": "send_daily_operator_briefing", "every": 1, "unit": "hours", "fn": send_daily_operator_briefing, "group": "operator", "run_on_startup": True, "startup_priority": 10},
    {"name": "investigate_unclassified_clusters", "every": 30, "unit": "minutes", "fn": investigate_unclassified_clusters, "group": "intelligence"},
    {"name": "audit_sales_pipeline", "every": 60, "unit": "minutes", "fn": audit_sales_pipeline, "group": "sales"},
    {"name": "run_twitter_ingestion", "every": 10, "unit": "minutes", "fn": run_twitter_ingestion, "group": "ingestion"},
    {"name": "run_trustpilot_ingestion", "every": 30, "unit": "minutes", "fn": run_trustpilot_ingestion, "group": "ingestion"},
    {"name": "run_shopify_community_ingestion", "every": 30, "unit": "minutes", "fn": run_shopify_community_ingestion, "group": "ingestion"},
    {"name": "run_stripe_forum_ingestion", "every": 30, "unit": "minutes", "fn": run_stripe_forum_ingestion, "group": "ingestion"},
    {"name": "run_stack_overflow_ingestion", "every": 30, "unit": "minutes", "fn": run_stack_overflow_ingestion, "group": "ingestion"},
    {"name": "run_deal_sourcing_cycle", "every": 30, "unit": "minutes", "fn": run_deal_sourcing_cycle, "group": "sales"},
    {"name": "promote_opportunities_to_pipeline", "every": 30, "unit": "minutes", "fn": promote_opportunities_to_pipeline, "group": "sales"},
    {"name": "run_autonomous_sales_cycle", "every": 30, "unit": "minutes", "fn": run_autonomous_sales_cycle, "group": "sales"},
    {"name": "run_historical_ingestion", "every": 4, "unit": "hours", "fn": run_historical_ingestion, "group": "ingestion"},
]


def _task_interval_label(spec):
    return f"{spec['every']}{spec['unit'][0]}"


def _register_task(spec):
    interval = schedule.every(spec["every"])
    getattr(interval, spec["unit"]).do(lambda task_name=spec["name"], task_fn=spec["fn"]: _safe(task_name, task_fn))


def register_scheduler_tasks():
    enabled = []
    disabled = []
    for spec in TASK_REGISTRY:
        if spec["name"] in DISABLED_TASKS:
            disabled.append(spec["name"])
            continue
        _register_task(spec)
        enabled.append(spec["name"])
    record_component_state(
        "scheduler",
        ttl=600,
        scheduler_enabled_tasks=enabled,
        scheduler_disabled_tasks=disabled,
        scheduler_registry_size=len(TASK_REGISTRY),
        scheduler_registry_loaded_at=utc_now_iso(),
    )
    save_event(
        "scheduler_registry_loaded",
        {
            "enabled_tasks": enabled,
            "disabled_tasks": disabled,
            "registry_size": len(TASK_REGISTRY),
        },
    )
    return enabled, disabled


def _startup_specs():
    specs = [spec for spec in TASK_REGISTRY if spec["name"] not in DISABLED_TASKS and spec.get("run_on_startup")]
    return sorted(specs, key=lambda spec: int(spec.get("startup_priority", 100)))

if __name__ == "__main__":
    # NOTE: integrity checks (verify_or_die / verify_config_or_die /
    # verify_runtime_or_die) ran at the very top of this file, BEFORE
    # any heavy import. By the time we reach this block they have
    # already either passed in `report` mode (logged + metered) or
    # crashed the process in `enforce` mode.
    from runtime.safety.schema_validator import validate_schema
    success, msg = validate_schema()
    if not success:
        logger.critical(f"SCHEDULER STARTUP BLOCKED: {msg}")
        sys.exit(1)

    logger.info("Agent Flux autonomous scheduler started")
    heartbeat("scheduler", ttl=600, status="starting")
    clear_component_fields("scheduler", "current_task", "current_task_started_at")
    enabled_tasks, disabled_tasks = register_scheduler_tasks()
    save_event("scheduler_started", {"tasks": [f"{spec['name']}:{_task_interval_label(spec)}" for spec in TASK_REGISTRY if spec["name"] not in DISABLED_TASKS], "disabled_tasks": disabled_tasks, "continuous_discovery": "disabled" if CONTINUOUS_DISCOVERY_DISABLED else "adaptive"})
    print("[Scheduler] Autonomous loop started with multi-source ingestion + deal sourcing" + ("" if CONTINUOUS_DISCOVERY_DISABLED else " + continuous discovery"))
    for spec in _startup_specs():
        _safe(spec["name"], spec["fn"])
    if "autonomous_market_cycle" not in DISABLED_TASKS:
        _safe("autonomous_market_cycle", autonomous_market_cycle)
    if "sync_payflux_intelligence" not in DISABLED_TASKS:
        _safe("sync_payflux_intelligence", sync_payflux_intelligence)
    if "record_value_heartbeat" not in DISABLED_TASKS:
        _safe("record_value_heartbeat", record_value_heartbeat)

    discovery_thread = None
    if CONTINUOUS_DISCOVERY_DISABLED:
        logger.info("Continuous discovery thread disabled via AGENT_FLUX_DISABLE_CONTINUOUS_DISCOVERY")
    else:
        discovery_thread = threading.Thread(target=run_continuous_discovery, daemon=True, name="continuous-discovery")
        discovery_thread.start()
        logger.info("Continuous discovery thread launched")

    while True:
        try:
            heartbeat(
                "scheduler",
                ttl=600,
                status="running",
                loop_checked_at=utc_now_iso(),
                queue_depth=redis_client.llen("agent_tasks"),
            )
            # Detect dead discovery thread; let PM2 restart us so it relaunches clean.
            if discovery_thread is not None and not discovery_thread.is_alive():
                logger.error("continuous discovery thread died; exiting so PM2 restarts the scheduler")
                save_event("scheduler_discovery_thread_died", {})
                sys.exit(2)
            schedule.run_pending()
            idle = schedule.idle_seconds()
            time.sleep(max(1, idle if idle is not None else 1))
        except SystemExit:
            raise
        except KeyboardInterrupt:
            logger.info("scheduler interrupted; shutting down")
            raise
        except Exception as e:
            logger.exception("scheduler loop iteration failed: %s", e)
            try:
                save_event("scheduler_loop_error", {"error": str(e), "type": type(e).__name__})
            except Exception:
                pass
            time.sleep(5)
