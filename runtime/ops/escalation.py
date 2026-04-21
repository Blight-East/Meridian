"""
Agent Flux Escalation System
Two-way communication between the bot and operator via Telegram.
The bot can ask the operator for help at any time — task failures,
pipeline degradation, approval requests, or anything it can't resolve alone.
"""
import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from datetime import datetime, timezone
import redis
from config.logging_config import get_logger
from memory.structured.db import save_event

logger = get_logger("escalation")
_r = redis.Redis(host="localhost", port=6379, decode_responses=True)

ESCALATION_TTL = 86400 * 3  # 3 days max in Redis

# Category defaults: priority, TTL before auto-resolve, reminder interval
CATEGORY_DEFAULTS = {
    "task_failure":         {"priority": "high",     "ttl_hours": 24, "remind_hours": 4},
    "pipeline_degradation": {"priority": "critical", "ttl_hours": 12, "remind_hours": 2},
    "approval_needed":      {"priority": "normal",   "ttl_hours": 48, "remind_hours": 8},
    "stuck":                {"priority": "high",     "ttl_hours": 24, "remind_hours": 4},
    "info":                 {"priority": "low",      "ttl_hours": 72, "remind_hours": 24},
}

PRIORITY_ICONS = {"critical": "\U0001f534", "high": "\U0001f7e0", "normal": "\U0001f7e1", "low": "\U0001f535"}
CATEGORY_LABELS = {
    "task_failure": "Task Failure",
    "pipeline_degradation": "Pipeline Degradation",
    "approval_needed": "Approval Needed",
    "stuck": "Bot Stuck",
    "info": "Info",
}


# ── Core API ──────────────────────────────────────────────────────

def escalate(category, title, message, context=None, priority=None):
    """
    Create a new escalation and send it to the operator via Telegram.
    Deduplicates: won't create a new one if an identical title is already open.
    Returns the escalation ID (e.g. "E-42").
    """
    defaults = CATEGORY_DEFAULTS.get(category, CATEGORY_DEFAULTS["info"])
    esc_priority = priority or defaults["priority"]

    # Dedup: skip if same title already open
    for existing in get_active_escalations():
        if existing.get("title") == title and existing.get("status") == "open":
            logger.info(f"Skipping duplicate escalation: {title}")
            return existing["id"]

    counter = _r.incr("agent_flux:escalation_counter")
    esc_id = f"E-{counter}"
    now = datetime.now(timezone.utc).isoformat()

    esc_data = {
        "id": esc_id,
        "category": category,
        "priority": esc_priority,
        "title": title,
        "message": message,
        "context_json": json.dumps(context or {}),
        "status": "open",
        "created_at": now,
        "updated_at": now,
        "resolution": "",
        "ttl_hours": str(defaults["ttl_hours"]),
        "remind_hours": str(defaults["remind_hours"]),
        "reminder_count": "0",
        "last_reminded": now,
    }

    key = f"agent_flux:escalation:{esc_id}"
    _r.hset(key, mapping=esc_data)
    _r.expire(key, ESCALATION_TTL)
    _r.zadd("agent_flux:escalations:active", {esc_id: time.time()})

    _send_escalation_telegram(esc_data)

    save_event("escalation_created", {
        "id": esc_id, "category": category,
        "priority": esc_priority, "title": title,
    })
    logger.info(f"Escalation created: {esc_id} [{category}/{esc_priority}] {title}")
    return esc_id


def get_active_escalations():
    """Return all unresolved escalations, ordered by creation time."""
    ids = _r.zrange("agent_flux:escalations:active", 0, -1)
    escalations = []
    for esc_id in ids:
        data = _r.hgetall(f"agent_flux:escalation:{esc_id}")
        if data and data.get("status") != "resolved":
            escalations.append(data)
        elif not data:
            _r.zrem("agent_flux:escalations:active", esc_id)
    return escalations


def get_escalation(esc_id):
    """Get a single escalation by ID."""
    return _r.hgetall(f"agent_flux:escalation:{esc_id}") or None


def resolve_escalation(esc_id, resolution):
    """Mark an escalation as resolved."""
    key = f"agent_flux:escalation:{esc_id}"
    if not _r.exists(key):
        return False

    _r.hset(key, mapping={
        "status": "resolved",
        "resolution": resolution,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    _r.zrem("agent_flux:escalations:active", esc_id)

    save_event("escalation_resolved", {"id": esc_id, "resolution": resolution[:200]})
    logger.info(f"Escalation resolved: {esc_id} -- {resolution[:100]}")
    return True


def acknowledge_escalation(esc_id):
    """Mark as acknowledged (operator saw it, working on it)."""
    key = f"agent_flux:escalation:{esc_id}"
    if not _r.exists(key):
        return False
    _r.hset(key, mapping={
        "status": "acknowledged",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return True


# ── Reminders & Timeouts ─────────────────────────────────────────

def check_reminders():
    """
    Check active escalations for reminder deadlines and auto-resolution.
    Called every 30 minutes from the scheduler.
    """
    active = get_active_escalations()
    now = time.time()
    reminded = 0
    auto_resolved = 0

    for esc in active:
        if esc["status"] == "resolved":
            continue

        age_h = _age_hours(esc)
        ttl_h = float(esc.get("ttl_hours", 24))

        # Auto-resolve if past TTL
        if age_h >= ttl_h:
            resolve_escalation(esc["id"], f"Auto-resolved after {ttl_h:.0f}h with no response")
            auto_resolved += 1
            continue

        # Check if reminder is due
        remind_h = float(esc.get("remind_hours", 4))
        last_reminded = esc.get("last_reminded", esc["created_at"])
        try:
            last_dt = datetime.fromisoformat(last_reminded)
            elapsed_h = (now - last_dt.timestamp()) / 3600
        except (ValueError, TypeError):
            continue

        if elapsed_h >= remind_h:
            count = int(esc.get("reminder_count", 0)) + 1
            _send_reminder_telegram(esc, count, ttl_h - age_h)

            _r.hset(f"agent_flux:escalation:{esc['id']}", mapping={
                "reminder_count": str(count),
                "last_reminded": datetime.now(timezone.utc).isoformat(),
            })
            reminded += 1

    if reminded or auto_resolved:
        logger.info(f"Escalation reminders: {reminded} sent, {auto_resolved} auto-resolved")


# ── Pipeline Health Monitor ───────────────────────────────────────

def pipeline_health_check():
    """
    Periodic health check that detects pipeline degradation
    and creates escalations for issues needing operator attention.
    Called every 60 minutes from the scheduler.
    """
    from sqlalchemy import create_engine, text
    engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

    issues = []

    with engine.connect() as conn:
        # Check 1: Zero signals in last 2 hours
        signals_2h = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '2 hours'"
        )).scalar() or 0

        if signals_2h == 0:
            issues.append({
                "title": "Zero signals ingested in 2 hours",
                "message": (
                    "No new signals have been ingested in the last 2 hours. "
                    "All ingestion sources may be down or blocked.\n\n"
                    "Should I retry all sources now, or is there something specific to check?"
                ),
                "priority": "critical",
                "category": "pipeline_degradation",
                "check": "signal_drought",
            })

        # Check 2: All clusters are unclassified
        total_clusters = conn.execute(text("SELECT COUNT(*) FROM clusters")).scalar() or 0
        unclassified = conn.execute(text(
            "SELECT COUNT(*) FROM clusters WHERE cluster_topic = 'Unclassified merchant distress'"
        )).scalar() or 0

        if total_clusters > 0 and unclassified == total_clusters:
            issues.append({
                "title": "All clusters unclassified",
                "message": (
                    f"All {total_clusters} active clusters are 'Unclassified merchant distress'. "
                    "The classification pipeline may be broken — signals are coming in but "
                    "not being categorized.\n\n"
                    "Should I investigate the signal content, or do you know what changed?"
                ),
                "priority": "high",
                "category": "pipeline_degradation",
                "check": "all_clusters_unclassified",
            })

        # Check 3: Signals coming in but zero qualified leads
        signals_24h = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0
        leads_24h = conn.execute(text(
            "SELECT COUNT(*) FROM qualified_leads WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

        if signals_24h >= 20 and leads_24h == 0:
            issues.append({
                "title": "Zero leads from 24h of signals",
                "message": (
                    f"Ingested {signals_24h} signals in 24h but produced zero qualified leads. "
                    "The qualification pipeline may be broken.\n\n"
                    "Want me to check the ranking and opportunity extraction steps?"
                ),
                "priority": "high",
                "category": "pipeline_degradation",
                "check": "zero_leads",
            })

        # Check 4: Stale pending deals
        try:
            stale_deals = conn.execute(text(
                "SELECT COUNT(*) FROM merchant_opportunities "
                "WHERE status = 'pending_review' "
                "AND created_at <= NOW() - INTERVAL '24 hours'"
            )).scalar() or 0

            if stale_deals >= 3:
                issues.append({
                    "title": f"{stale_deals} deals pending review for 24h+",
                    "message": (
                        f"There are {stale_deals} deal opportunities sitting in "
                        "'pending_review' for over 24 hours.\n\n"
                        "Want me to list them for quick approve/reject?"
                    ),
                    "priority": "normal",
                    "category": "approval_needed",
                    "check": "stale_deals",
                })
        except Exception:
            pass  # Table may not exist yet

    # Check 5: Last scan time too old
    last_scan = _r.get("last_scan_time")
    if last_scan:
        try:
            scan_dt = datetime.fromisoformat(last_scan.replace("Z", "+00:00"))
            minutes_since = (time.time() - scan_dt.timestamp()) / 60
            if minutes_since > 120:
                issues.append({
                    "title": f"No scan in {minutes_since:.0f} minutes",
                    "message": (
                        f"Last pipeline scan was {minutes_since:.0f} minutes ago. "
                        "The autonomous market cycle may be stuck or crashed.\n\n"
                        "Should I check the scheduler logs?"
                    ),
                    "priority": "high",
                    "category": "pipeline_degradation",
                    "check": "stale_scan",
                })
        except (ValueError, TypeError):
            pass

    for issue in issues:
        escalate(
            category=issue["category"],
            title=issue["title"],
            message=issue["message"],
            context={"check": issue["check"]},
            priority=issue["priority"],
        )

    if not issues:
        logger.info("Pipeline health check: all clear")

    return {"issues_found": len(issues)}


# ── Context Injection for Chat Engine ─────────────────────────────

def format_active_for_context():
    """
    Format active escalations as a text block for injection into the
    chat engine's system prompt. Returns empty string if none active.
    """
    active = get_active_escalations()
    if not active:
        return ""

    lines = ["\nACTIVE ESCALATIONS (awaiting operator response):"]
    for esc in active:
        age = _age_hours(esc)
        lines.append(
            f"- [{esc['id']}] ({esc['priority']}) {esc['title']} "
            f"[{esc['status']}, {age:.1f}h ago]: {esc.get('message', '')[:150]}"
        )
    lines.append(
        "\nIf the operator's message relates to any escalation above, "
        "call resolve_escalation with the escalation ID and a summary of "
        "the resolution. If they ask to see escalations, call list_escalations."
    )
    return "\n".join(lines)


# ── Telegram Formatting ──────────────────────────────────────────

def _send_escalation_telegram(esc):
    """Format and queue an escalation message."""
    from runtime.ops.agent_workloop import send_telegram

    icon = PRIORITY_ICONS.get(esc["priority"], "\U0001f7e1")
    label = CATEGORY_LABELS.get(esc["category"], esc["category"])

    text = (
        f"{icon} ESCALATION [{esc['id']}]: {label}\n\n"
        f"{esc['message']}\n\n"
        f"Reply to this chat to respond. I'll understand the context."
    )
    send_telegram(text)


def _send_reminder_telegram(esc, count, hours_remaining):
    """Send a reminder for an unresolved escalation."""
    from runtime.ops.agent_workloop import send_telegram

    text = (
        f"\U0001f514 Reminder #{count} \u2014 [{esc['id']}] still open\n\n"
        f"{esc['title']}\n\n"
        f"Reply here or it will auto-close in {hours_remaining:.0f}h."
    )
    send_telegram(text)


def _age_hours(esc):
    """Calculate how many hours old an escalation is."""
    try:
        created = datetime.fromisoformat(esc["created_at"])
        return (time.time() - created.timestamp()) / 3600
    except (ValueError, TypeError, KeyError):
        return 0
