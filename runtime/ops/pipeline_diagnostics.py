"""
Pipeline Diagnostics — Deterministic self-healing pipeline for Agent Flux.
Detects pipeline anomalies and triggers auto-repair sequences without LLM logic.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
import subprocess
import json
from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from runtime.ops.operator_alerts import send_operator_alert
from runtime.intelligence.contact_discovery import run_contact_discovery
from runtime.intelligence.backfill_attribution import run_backfill as run_backfill_attribution
from runtime.intelligence.backfill_attribution import _should_attempt_backfill
from runtime.ops.deal_sourcing import run_deal_sourcing_cycle
from runtime.ops.cluster_investigator import run_cluster_investigation
from memory.structured.db import save_event

logger = get_logger("pipeline_diagnostics")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True, pool_recycle=300)

# Prevent identical repairs more than once per hour
REPAIR_COOLDOWN_SECONDS = 3600
ALERT_SUPPRESSION_SECONDS = 6 * 3600
INGESTION_SIGNAL_STALENESS_SECONDS = 20 * 60
INGESTION_ACTIVITY_WINDOW_MINUTES = 45
CONTACT_DISCOVERY_SCOPE_THRESHOLD = 5
ACTIONABLE_ATTRIBUTION_TOTAL_THRESHOLD = 25
ACTIONABLE_ATTRIBUTION_RECENT_THRESHOLD = 5
INGESTION_EVENT_TYPES = (
    "twitter_ingestion_cycle",
    "trustpilot_ingestion_cycle",
    "shopify_community_ingestion_cycle",
    "stripe_forum_ingestion_cycle",
    "stack_overflow_ingestion_cycle",
)
INGESTION_SCHEDULER_TASKS = (
    "run_twitter_ingestion",
    "run_trustpilot_ingestion",
    "run_shopify_community_ingestion",
    "run_stripe_forum_ingestion",
    "run_stack_overflow_ingestion",
)

_FAILURE_OPERATOR_LABELS = {
    "contact_discovery_stalled": "merchant contact discovery is stalled",
    "deal_sourcing_stalled": "deal sourcing is stalled",
    "embeddings_mismatch": "semantic memory is behind",
    "attribution_failure": "merchant attribution is failing",
    "ingestion_inactive": "source ingestion looks inactive",
}

_REPAIR_OPERATOR_LABELS = {
    "repair_contact_pipeline": "re-ran contact discovery",
    "repair_deal_pipeline": "re-ran deal sourcing",
    "repair_semantic_memory": "backfilled semantic memory",
    "repair_attribution": "re-ran merchant attribution",
    "repair_ingestion": "restarted ingestion scheduling",
}

def _init_diagnostics_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS diagnostics_events (
                id SERIAL PRIMARY KEY,
                diagnostic_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                health_status VARCHAR(50),
                failures_detected JSONB,
                repairs_executed JSONB,
                alert_signature VARCHAR(255),
                alert_sent BOOLEAN DEFAULT FALSE
            );
        """))
        conn.execute(text("""
            ALTER TABLE diagnostics_events
            ADD COLUMN IF NOT EXISTS alert_signature VARCHAR(255)
        """))
        conn.execute(text("""
            ALTER TABLE diagnostics_events
            ADD COLUMN IF NOT EXISTS alert_sent BOOLEAN DEFAULT FALSE
        """))
        conn.execute(text("""
            ALTER TABLE diagnostics_events
            ADD COLUMN IF NOT EXISTS diagnostic_details JSONB DEFAULT '{}'::jsonb
        """))
        conn.commit()

def _check_cooldown(repair_action_name: str) -> bool:
    """Returns True if the action is currently on cooldown."""
    with engine.connect() as conn:
        last_run = conn.execute(text("""
            SELECT diagnostic_run_at 
            FROM diagnostics_events 
            WHERE repairs_executed ? :action
            ORDER BY diagnostic_run_at DESC 
            LIMIT 1
        """), {"action": repair_action_name}).scalar()
        
    if last_run is None:
        return False
        
    seconds_since = (time.time() - last_run.timestamp())
    return seconds_since < REPAIR_COOLDOWN_SECONDS

def _build_alert_signature(failures_detected, repairs_executed) -> str:
    failures_key = ",".join(sorted(set(failures_detected)))
    repairs_key = ",".join(sorted(set(repairs_executed)))
    return f"{failures_key}|{repairs_key}"

def _recent_identical_alert_sent(alert_signature: str) -> bool:
    if not alert_signature:
        return False

    with engine.connect() as conn:
        last_alert = conn.execute(text("""
            SELECT diagnostic_run_at
            FROM diagnostics_events
            WHERE alert_signature = :signature
              AND alert_sent = TRUE
            ORDER BY diagnostic_run_at DESC
            LIMIT 1
        """), {"signature": alert_signature}).scalar()

    if last_alert is None:
        return False

    seconds_since = time.time() - last_alert.timestamp()
    return seconds_since < ALERT_SUPPRESSION_SECONDS

def _get_recent_ingestion_activity():
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                event_type,
                extract(epoch from created_at) AS created_epoch,
                COALESCE((data->>'fetched')::int, 0) AS fetched,
                COALESCE((data->>'stored')::int, 0) AS stored
            FROM events
            WHERE event_type IN ({", ".join(f"'{event_type}'" for event_type in INGESTION_EVENT_TYPES)})
              AND created_at >= NOW() - INTERVAL '{INGESTION_ACTIVITY_WINDOW_MINUTES} minutes'
            ORDER BY created_at DESC
        """)).mappings().all()

        scheduler_errors = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM events
            WHERE event_type = 'scheduler_error'
              AND data->>'task' IN ({", ".join(f"'{task_name}'" for task_name in INGESTION_SCHEDULER_TASKS)})
              AND created_at >= NOW() - INTERVAL '{INGESTION_ACTIVITY_WINDOW_MINUTES} minutes'
        """)).scalar() or 0

    source_types_seen = sorted({row["event_type"] for row in rows})
    latest_cycle_epoch = max((float(row["created_epoch"]) for row in rows), default=None)

    return {
        "cycles_recent": len(rows),
        "sources_seen": source_types_seen,
        "latest_cycle_epoch": latest_cycle_epoch,
        "fetched_recent": sum(int(row["fetched"] or 0) for row in rows),
        "stored_recent": sum(int(row["stored"] or 0) for row in rows),
        "scheduler_errors_recent": int(scheduler_errors),
    }


def _count_phrase(count: int, singular: str, plural: str | None = None) -> str:
    count = int(count or 0)
    if count == 1:
        return f"1 {singular}"
    return f"{count} {plural or singular + 's'}"


def _human_join(items: list[str]) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return ", ".join(cleaned[:-1]) + f", and {cleaned[-1]}"


def _failure_summary_line(failures_detected: list[str], diagnostic_details: dict) -> str:
    details = diagnostic_details or {}
    counts = dict(details.get("counts") or {})
    attribution = dict(details.get("attribution") or {})
    ingestion = dict(details.get("ingestion_activity") or {})
    summaries: list[str] = []

    for failure in failures_detected:
        if failure == "contact_discovery_stalled":
            merchants = int(counts.get("merchants") or 0)
            contacts = int(counts.get("contacts") or 0)
            if merchants > 0:
                summaries.append(
                    f"contact discovery has not produced usable merchant contacts yet ({_count_phrase(contacts, 'contact')} across {_count_phrase(merchants, 'merchant')})"
                )
            else:
                summaries.append("contact discovery has not produced usable merchant contacts yet")
        elif failure == "deal_sourcing_stalled":
            opps = int(counts.get("opportunities") or 0)
            clusters = int(counts.get("clusters") or 0)
            if clusters > 0:
                summaries.append(
                    f"deal sourcing still has {_count_phrase(opps, 'open opportunity')} despite {_count_phrase(clusters, 'meaningful cluster')}"
                )
            else:
                summaries.append(_FAILURE_OPERATOR_LABELS[failure])
        elif failure == "embeddings_mismatch":
            signals = int(counts.get("signals") or 0)
            embeddings = int(counts.get("embeddings") or 0)
            gap = max(0, signals - embeddings)
            if gap > 0:
                summaries.append(f"semantic memory is missing embeddings for {_count_phrase(gap, 'signal')}")
            else:
                summaries.append(_FAILURE_OPERATOR_LABELS[failure])
        elif failure == "attribution_failure":
            unattributed = int(attribution.get("merchant_distress_unattributed") or 0)
            total = int(attribution.get("merchant_distress_total") or 0)
            recent_unattributed = int(attribution.get("recent_merchant_distress_unattributed") or 0)
            recent_total = int(attribution.get("recent_merchant_distress_total") or 0)
            actionable_unattributed = int(attribution.get("actionable_merchant_distress_unattributed") or 0)
            actionable_recent = int(attribution.get("actionable_recent_merchant_distress_unattributed") or 0)
            if actionable_unattributed > 0 and total > 0:
                summary = (
                    f"{actionable_unattributed} repairable merchant-distress signals still are unattributed"
                )
                if recent_total > 0 and actionable_recent > 0:
                    summary += f"; {actionable_recent} repairable cases in the last 24h are still unattributed"
                if unattributed > actionable_unattributed:
                    summary += f" ({unattributed} total unattributed merchant-distress signals remain overall)"
                summaries.append(summary)
            else:
                summaries.append(_FAILURE_OPERATOR_LABELS[failure])
        elif failure == "ingestion_inactive":
            cycles_recent = int(ingestion.get("cycles_recent") or 0)
            summaries.append(
                f"fresh source ingestion activity dropped to {_count_phrase(cycles_recent, 'recent cycle')} in the last {INGESTION_ACTIVITY_WINDOW_MINUTES} minutes"
            )
        else:
            summaries.append(_FAILURE_OPERATOR_LABELS.get(failure, failure.replace("_", " ")))

    return _human_join(summaries)


def _repair_summary_line(repairs_executed: list[str], repairs_deferred: list[str]) -> str:
    executed = [_REPAIR_OPERATOR_LABELS.get(item, item.replace("_", " ")) for item in repairs_executed]
    deferred = [_REPAIR_OPERATOR_LABELS.get(item, item.replace("_", " ")) for item in repairs_deferred]
    if executed and deferred:
        return f"{_human_join(executed)}. Already in cooldown: {_human_join(deferred)}."
    if executed:
        return _human_join(executed) + "."
    if deferred:
        return f"Already in cooldown from a recent repair: {_human_join(deferred)}."
    return ""

# --- Repair Actions ---

def repair_contact_pipeline():
    logger.info("Executing repair_contact_pipeline...")
    try:
        run_contact_discovery()
        return True
    except Exception as e:
        logger.error(f"Failed repair_contact_pipeline: {e}")
        return False

def repair_deal_pipeline():
    logger.info("Executing repair_deal_pipeline...")
    try:
        run_deal_sourcing_cycle()
        return True
    except Exception as e:
        logger.error(f"Failed repair_deal_pipeline: {e}")
        return False

def repair_semantic_memory():
    logger.info("Executing repair_semantic_memory...")
    try:
        # Calls the backfill script
        script_path = os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "backfill_embeddings.py")
        if os.path.exists(script_path):
            subprocess.run(["python3", script_path], check=True)
            return True
        else:
            logger.error(f"Embeddings backfill script not found at {script_path}")
            return False
    except Exception as e:
        logger.error(f"Failed repair_semantic_memory: {e}")
        return False

def repair_attribution():
    logger.info("Executing repair_attribution...")
    try:
        result = run_backfill_attribution()
        save_event("repair_attribution_executed", result)
        return bool(result.get("processed", 0) >= 0)
    except Exception as e:
        logger.error(f"Failed repair_attribution: {e}")
        return False

def repair_ingestion():
    """Trigger a process restart by exiting cleanly so PM2 brings us back up.

    This function runs *inside* the scheduler. Calling `pm2 restart
    agent-flux-scheduler` from within the process being restarted creates a
    race: pm2 sends SIGINT mid-execution, killing the diagnostic write at
    line ~567 before the cooldown row is committed. The next 10-min cycle
    then re-detects the same condition and re-fires the repair → infinite
    restart loop (observed: 580+ restarts in ~12h).

    Fix: write the cooldown sentinel ourselves *first*, then exit non-zero.
    PM2's restart-on-exit brings us back fresh, and the next diagnostic run
    sees the cooldown row and defers.
    """
    logger.info("Executing repair_ingestion: recording cooldown + exiting for PM2 restart...")
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO diagnostics_events (
                    health_status, failures_detected, repairs_executed,
                    alert_signature, alert_sent, diagnostic_details
                ) VALUES ('recovering', :failures, :repairs, NULL, false, :details)
            """), {
                "failures": json.dumps(["ingestion_inactive"]),
                "repairs": json.dumps(["repair_ingestion"]),
                "details": json.dumps({"trigger": "repair_ingestion", "method": "self_exit"}),
            })
            conn.commit()
        save_event("repair_ingestion_executed", {"method": "self_exit", "exit_code": 42})
        logger.info("repair_ingestion: exit(42) for PM2 restart")
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(42)
    except Exception as e:
        logger.error(f"Failed repair_ingestion: {e}")
        return False


def _get_attribution_diagnostics():
    with engine.connect() as conn:
        merchant_total = conn.execute(text("""
            SELECT COUNT(*)
            FROM signals
            WHERE classification = 'merchant_distress'
        """)).scalar() or 0
        merchant_unattributed = conn.execute(text("""
            SELECT COUNT(*)
            FROM signals
            WHERE classification = 'merchant_distress'
              AND merchant_id IS NULL
        """)).scalar() or 0
        recent_merchant_total = conn.execute(text("""
            SELECT COUNT(*)
            FROM signals
            WHERE classification = 'merchant_distress'
              AND detected_at >= NOW() - INTERVAL '24 hours'
        """)).scalar() or 0
        recent_merchant_unattributed = conn.execute(text("""
            SELECT COUNT(*)
            FROM signals
            WHERE classification = 'merchant_distress'
              AND merchant_id IS NULL
              AND detected_at >= NOW() - INTERVAL '24 hours'
        """)).scalar() or 0
        actionable_rows = conn.execute(text("""
            SELECT content, source, merchant_name, detected_at >= NOW() - INTERVAL '24 hours' AS is_recent
            FROM signals
            WHERE classification = 'merchant_distress'
              AND merchant_id IS NULL
        """)).mappings().all()
        last_attributed_at = conn.execute(text("""
            SELECT MAX(created_at)
            FROM events
            WHERE event_type = 'merchant_attributed'
        """)).scalar()

    merchant_unattributed_ratio = round(
        merchant_unattributed / merchant_total, 4
    ) if merchant_total else 0.0
    recent_unattributed_ratio = round(
        recent_merchant_unattributed / recent_merchant_total, 4
    ) if recent_merchant_total else 0.0
    actionable_unattributed = 0
    actionable_recent_unattributed = 0
    for row in actionable_rows:
        if _should_attempt_backfill(
            content=row.get("content") or "",
            source=row.get("source") or "",
            merchant_name=row.get("merchant_name"),
        ):
            actionable_unattributed += 1
            if bool(row.get("is_recent")):
                actionable_recent_unattributed += 1
    actionable_unattributed_ratio = round(
        actionable_unattributed / merchant_total, 4
    ) if merchant_total else 0.0
    actionable_recent_unattributed_ratio = round(
        actionable_recent_unattributed / recent_merchant_total, 4
    ) if recent_merchant_total else 0.0

    return {
        "merchant_distress_total": int(merchant_total),
        "merchant_distress_unattributed": int(merchant_unattributed),
        "merchant_distress_unattributed_ratio": merchant_unattributed_ratio,
        "actionable_merchant_distress_unattributed": int(actionable_unattributed),
        "actionable_merchant_distress_unattributed_ratio": actionable_unattributed_ratio,
        "recent_merchant_distress_total": int(recent_merchant_total),
        "recent_merchant_distress_unattributed": int(recent_merchant_unattributed),
        "recent_merchant_distress_unattributed_ratio": recent_unattributed_ratio,
        "actionable_recent_merchant_distress_unattributed": int(actionable_recent_unattributed),
        "actionable_recent_merchant_distress_unattributed_ratio": actionable_recent_unattributed_ratio,
        "last_merchant_attributed_at": last_attributed_at.isoformat() if last_attributed_at else None,
    }


def _get_contact_discovery_scope():
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COUNT(DISTINCT mo.merchant_id) AS active_merchants,
                COUNT(*) AS active_opportunities
            FROM merchant_opportunities mo
            JOIN merchants m ON m.id = mo.merchant_id
            WHERE mo.merchant_id IS NOT NULL
              AND mo.status IN ('pending_review', 'approved', 'outreach_pending', 'outreach_sent')
              AND COALESCE(m.domain, '') != ''
        """)).mappings().first() or {}
    return {
        "active_merchants": int(row.get("active_merchants") or 0),
        "active_opportunities": int(row.get("active_opportunities") or 0),
    }

# --- Core Diagnostics ---

def run_pipeline_diagnostics():
    """
    Main diagnostic check run by the scheduler.
    Evaluates health conditions and triggers repairs if out of cooldown.
    """
    logger.info("Running pipeline diagnostics check...")
    _init_diagnostics_table()
    
    failures_detected = []
    repairs_executed = []
    repairs_deferred = []
    diagnostic_details = {}
    
    with engine.connect() as conn:
        merchants = conn.execute(text("SELECT COUNT(*) FROM merchants")).scalar() or 0
        contacts = conn.execute(text("SELECT COUNT(*) FROM merchant_contacts")).scalar() or 0
        opps = conn.execute(text("SELECT COUNT(*) FROM merchant_opportunities")).scalar() or 0
        clusters = conn.execute(text("SELECT COUNT(*) FROM clusters WHERE cluster_size >= 3")).scalar() or 0
        
        signals = conn.execute(text("SELECT COUNT(*) FROM signals")).scalar() or 0
        embeddings = conn.execute(text("SELECT COUNT(*) FROM signal_embeddings")).scalar() or 0
        unattributed = conn.execute(text("SELECT COUNT(*) FROM signals WHERE merchant_id IS NULL")).scalar() or 0
        
        # Ingestion activity
        last_signal = conn.execute(text("SELECT extract(epoch from detected_at) FROM signals ORDER BY detected_at DESC LIMIT 1")).scalar()

    ingestion_activity = _get_recent_ingestion_activity()
    attribution_details = _get_attribution_diagnostics()
    contact_scope = _get_contact_discovery_scope()
    diagnostic_details["ingestion_activity"] = ingestion_activity
    diagnostic_details["attribution"] = attribution_details
    diagnostic_details["contact_scope"] = contact_scope
    diagnostic_details["counts"] = {
        "merchants": int(merchants),
        "contacts": int(contacts),
        "opportunities": int(opps),
        "clusters": int(clusters),
        "signals": int(signals),
        "embeddings": int(embeddings),
        "unattributed_signals": int(unattributed),
    }
        
    # 1. Contact discovery stalled
    if (
        contacts == 0
        and merchants > 10
        and int(contact_scope.get("active_merchants") or 0) >= CONTACT_DISCOVERY_SCOPE_THRESHOLD
    ):
        failures_detected.append("contact_discovery_stalled")
        if not _check_cooldown("repair_contact_pipeline"):
            if repair_contact_pipeline():
                repairs_executed.append("repair_contact_pipeline")
        else:
            repairs_deferred.append("repair_contact_pipeline")

    # 2. Opportunity generation stalled
    if opps == 0 and clusters >= 5 and merchants >= 10:
        failures_detected.append("deal_sourcing_stalled")
        if not _check_cooldown("repair_deal_pipeline"):
            if repair_deal_pipeline():
                repairs_executed.append("repair_deal_pipeline")
        else:
            repairs_deferred.append("repair_deal_pipeline")

    # 3. Semantic embeddings mismatch
    if signals > (embeddings + 50): # Give 50 signals buffer
        failures_detected.append("embeddings_mismatch")
        if not _check_cooldown("repair_semantic_memory"):
            if repair_semantic_memory():
                repairs_executed.append("repair_semantic_memory")
        else:
            repairs_deferred.append("repair_semantic_memory")

    # 4. Merchant attribution failure
    attribution_failure = (
        (
            attribution_details["merchant_distress_total"] >= 20
            and attribution_details["actionable_merchant_distress_unattributed"] >= ACTIONABLE_ATTRIBUTION_TOTAL_THRESHOLD
            and attribution_details["actionable_merchant_distress_unattributed_ratio"] >= 0.025
        )
        or (
            attribution_details["recent_merchant_distress_total"] >= 10
            and attribution_details["actionable_recent_merchant_distress_unattributed"] >= ACTIONABLE_ATTRIBUTION_RECENT_THRESHOLD
            and attribution_details["actionable_recent_merchant_distress_unattributed_ratio"] >= 0.2
        )
    )
    if attribution_failure:
        failures_detected.append("attribution_failure")
        if not _check_cooldown("repair_attribution"):
            if repair_attribution():
                repairs_executed.append("repair_attribution")
        else:
            repairs_deferred.append("repair_attribution")

    # 5. Ingestion inactivity (No recent signal plus no recent ingestion job activity)
    if last_signal is not None:
        seconds_since_signal = time.time() - float(last_signal)
        if seconds_since_signal > INGESTION_SIGNAL_STALENESS_SECONDS and signals > 0:
            if ingestion_activity["cycles_recent"] > 0:
                logger.info(
                    "Skipping ingestion repair: source jobs are still active "
                    f"(cycles={ingestion_activity['cycles_recent']}, sources={ingestion_activity['sources_seen']}, "
                    f"fetched={ingestion_activity['fetched_recent']}, stored={ingestion_activity['stored_recent']}, "
                    f"scheduler_errors={ingestion_activity['scheduler_errors_recent']})"
                )
            else:
                logger.warning(
                    "No ingestion cycles recorded in the recent activity window "
                    f"({INGESTION_ACTIVITY_WINDOW_MINUTES}m). last_signal_age={int(seconds_since_signal)}s"
                )
                failures_detected.append("ingestion_inactive")
                if not _check_cooldown("repair_ingestion"):
                    if repair_ingestion():
                        repairs_executed.append("repair_ingestion")
                else:
                    repairs_deferred.append("repair_ingestion")
                    
    # Log and Alert
    health_status = "healthy" if not failures_detected else "degraded"
    if repairs_executed or repairs_deferred:
        health_status = "recovering"

    alert_signature = None
    alert_sent = False
    if repairs_executed:
        alert_signature = _build_alert_signature(failures_detected, repairs_executed)
        if _recent_identical_alert_sent(alert_signature):
            logger.info(f"Suppressing duplicate diagnostics alert for signature={alert_signature}")
        else:
            failure_summary = _failure_summary_line(failures_detected, diagnostic_details)
            repair_summary = _repair_summary_line(repairs_executed, repairs_deferred)
            operator_action = (
                "Operator action: hold for the next cycle unless this repeats after the repairs finish."
                if repairs_executed or repairs_deferred
                else "Operator action: inspect the pipeline if this does not self-clear."
            )
            msg = (
                f"🛠️ *Pipeline Repair Update*\n\n"
                f"What changed: {failure_summary or _human_join([_FAILURE_OPERATOR_LABELS.get(item, item.replace('_', ' ')) for item in failures_detected])}\n"
                f"Auto-repair: {repair_summary or 'No automatic repair was available.'}\n"
                f"Status: {health_status}\n"
                f"{operator_action}"
            )
            send_operator_alert(msg)
            alert_sent = True

    save_event("pipeline_diagnostics_details", {
        "health_status": health_status,
        "failures_detected": failures_detected,
        "repairs_executed": repairs_executed,
        "repairs_deferred": repairs_deferred,
        "details": diagnostic_details,
    })
        
    # Record to DB
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO diagnostics_events (
                health_status,
                failures_detected,
                repairs_executed,
                alert_signature,
                alert_sent,
                diagnostic_details
            )
            VALUES (:status, :failures, :repairs, :alert_signature, :alert_sent, :diagnostic_details)
        """), {
            "status": health_status,
            "failures": json.dumps(failures_detected),
            "repairs": json.dumps(repairs_executed),
            "alert_signature": alert_signature,
            "alert_sent": alert_sent,
            "diagnostic_details": json.dumps(diagnostic_details),
        })
        conn.commit()
        
    logger.info(
        f"Diagnostics complete: Status={health_status}, Failures={failures_detected}, "
        f"Repairs={repairs_executed}, Attribution={attribution_details}"
    )
    
    return {
        "health_status": health_status,
        "failures_detected": failures_detected,
        "repairs_executed": repairs_executed,
        "diagnostic_details": diagnostic_details,
    }

if __name__ == "__main__":
    print(run_pipeline_diagnostics())
