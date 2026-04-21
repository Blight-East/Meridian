"""
Agent Flux Autonomous Work Loop
Proactive scheduled tasks — the agent works on its own and reports via Telegram.

Each task:
  1. Sends a task-specific prompt to Claude (same engine the operator uses)
  2. Claude calls tools, gathers data, reasons about it
  3. The response is sent to the operator via Telegram
"""
import sys, os, requests, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.conversation.chat_engine import generate_chat_response
from runtime.health.telemetry import get_component_state, is_stale, record_component_state, utc_now_iso
from safety.guard import resolve_operator_delivery_chat

logger = get_logger("agent_workloop")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
OPERATOR_CHAT_ID = os.environ.get("OPERATOR_CHAT_ID", "5940537326")
AGENT_USER_ID = "agent_autonomous"  # internal user ID for autonomous sessions

import redis, json
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
PROACTIVE_QUEUE = "agent_flux:proactive_messages"


TELEGRAM_MAX_LENGTH = 4000


def _record_proactive_delivery_state() -> None:
    queue_depth = _redis.llen(PROACTIVE_QUEUE)
    telegram_state = get_component_state("telegram") or {}
    last_heartbeat = str(telegram_state.get("last_heartbeat_at") or "").strip()
    delivery_health = "healthy"
    if queue_depth >= 10 and is_stale(last_heartbeat, 300):
        delivery_health = "degraded"
    record_component_state(
        "telegram",
        ttl=900,
        last_enqueued_at=utc_now_iso(),
        proactive_queue_depth=queue_depth,
        proactive_delivery_health=delivery_health,
    )


def _split_message(text, max_len=TELEGRAM_MAX_LENGTH):
    """
    Split a long message into chunks at paragraph boundaries.
    Falls back to sentence boundaries, then hard-cuts as a last resort.
    """
    if len(text) <= max_len:
        return [text]

    chunks = []
    remaining = text
    while len(remaining) > max_len:
        # Try to split at a paragraph break (double newline)
        cut = remaining[:max_len].rfind("\n\n")
        if cut > max_len // 3:
            chunks.append(remaining[:cut].rstrip())
            remaining = remaining[cut:].lstrip("\n")
            continue

        # Try single newline
        cut = remaining[:max_len].rfind("\n")
        if cut > max_len // 3:
            chunks.append(remaining[:cut].rstrip())
            remaining = remaining[cut:].lstrip("\n")
            continue

        # Try sentence boundary
        cut = remaining[:max_len].rfind(". ")
        if cut > max_len // 3:
            chunks.append(remaining[:cut + 1])
            remaining = remaining[cut + 2:].lstrip()
            continue

        # Hard cut
        chunks.append(remaining[:max_len])
        remaining = remaining[max_len:]

    if remaining.strip():
        chunks.append(remaining.strip())
    return chunks


def send_telegram(text):
    """
    Queue a proactive message for delivery via the Telegram bot process.
    Splits long messages at paragraph boundaries to avoid truncation.
    """
    try:
        delivery_target = resolve_operator_delivery_chat(OPERATOR_CHAT_ID)
        if not delivery_target:
            logger.warning("No trusted operator chat is bound; skipping proactive message queue")
            save_event("agent_work_delivery_skipped", {"reason": "no_trusted_operator_chat"})
            return False
        chunks = _split_message(text)
        for chunk in chunks:
            _redis.rpush(PROACTIVE_QUEUE, json.dumps({
                "chat_id": delivery_target,
                "text": chunk,
                "timestamp": time.time(),
            }))
        _record_proactive_delivery_state()
        logger.info(f"Proactive message queued ({len(text)} chars, {len(chunks)} part(s))")
        return True
    except Exception as e:
        logger.error(f"Failed to queue proactive message: {e}")
        return False


def run_work_task(task_name, prompt, max_tokens=1500):
    """
    Execute a proactive work task:
    1. Send prompt to Claude via chat engine (with full tool access)
    2. Send result to operator via Telegram
    3. Log the work session

    Each task type gets its own chat context to prevent memory bleed.
    """
    # Separate user_id per task type — prevents context bleed between jobs
    task_user_id = f"agent_{task_name.lower().replace(' ', '_').replace('☀️', 'am').replace('🌙', 'pm')}"

    logger.info(f"Starting work task: {task_name} (context: {task_user_id}, max_tokens: {max_tokens})")
    start = time.time()

    try:
        response = generate_chat_response(task_user_id, prompt, max_tokens=max_tokens)
        elapsed = round(time.time() - start, 1)

        if response:
            header = f"🤖 *Agent Flux — {task_name}*\n\n"
            send_telegram(header + response)

            save_event("agent_work_completed", {
                "task": task_name,
                "elapsed_seconds": elapsed,
                "response_length": len(response),
            })
            logger.info(f"Work task completed: {task_name} ({elapsed}s, {len(response)} chars)")
        else:
            logger.warning(f"Work task returned empty response: {task_name}")

    except Exception as e:
        logger.error(f"Work task failed: {task_name} — {e}")
        save_event("agent_work_failed", {"task": task_name, "error": str(e)})


# ── Data Pre-Fetch Helpers ────────────────────────────────────────

def _prefetch_operator_state():
    """Pre-fetch operator state so prompts always have fresh data."""
    try:
        from runtime.conversation.chat_engine import _get_operator_state
        import json
        state = _get_operator_state()
        return json.dumps(state, indent=2, default=str)
    except Exception as e:
        logger.warning(f"Pre-fetch operator state failed: {e}")
        return None


# ── Scheduled Task Definitions ───────────────────────────────────

def pipeline_briefing():
    """Every 2 hours — concise pipeline status update."""
    state_json = _prefetch_operator_state()
    data_block = f"\n\nCURRENT SYSTEM STATE (live data, just pulled):\n```json\n{state_json}\n```\n" if state_json else ""
    run_work_task("Pipeline Briefing", f"""
        Give me a concise pipeline status briefing.{data_block}
        Cover:
        - Signal volume (last 2h vs 24h trend)
        - Active clusters and their severity
        - New merchants detected
        - Any anomalies or issues that need attention
        Use get_clusters or get_trends if you need to drill deeper into specific items.
        Keep it under 10 lines. Data only, no filler.
    """)


def lead_review():
    """Every 4 hours — investigate top unworked leads."""
    state_json = _prefetch_operator_state()
    data_block = f"\n\nCURRENT SYSTEM STATE (live data):\n```json\n{state_json}\n```\n" if state_json else ""
    run_work_task("Lead Review", f"""
        Review the current top leads and top distressed merchants.{data_block}
        Call get_top_leads and get_top_merchants for the full data. For the 3 most
        promising leads (highest qualification scores + revenue signals), provide:
        - Merchant name and processor
        - Why they're distressed (signal content)
        - Recommended next action (investigate, draft outreach, or skip)
        If a lead looks strong, call sales_context for deeper context.
        Be direct and actionable.
    """, max_tokens=2500)


def deal_pipeline_check():
    """Every 6 hours — review deal opportunities."""
    run_work_task("Deal Pipeline", """
        Check the deal opportunities pipeline. Call list_deal_opportunities and
        list_pending_outreach. For each pending opportunity:
        - Merchant and distress reason
        - Outreach draft quality (good/needs work)
        - Recommend: approve, reject, or hold
        If there are approved but unsent deals, flag them.
        If there are no deals, say so and suggest what could generate more.
    """, max_tokens=2500)


def competitive_intel():
    """Every 8 hours — research market and competitor activity."""
    run_work_task("Market Intel", """
        Research the current payment processor landscape. Run these web searches:
        1. web_search("Stripe account freeze merchant 2026")
        2. web_search("PayPal holding funds merchant account 2026")
        3. web_search("payment processor policy changes 2026")

        For each search, analyze the results. Then summarize:
        - Top 3 findings with source URLs
        - Any new processor policy changes that could create distressed merchants
        - Competitor activity in the merchant services recovery space
        - Emerging payment industry trends

        Use browser_fetch on the most relevant result URLs to get deeper context.
    """, max_tokens=3000)


def morning_briefing():
    """Daily at 9 AM — full overnight recap."""
    state_json = _prefetch_operator_state()
    data_block = f"\n\nCURRENT SYSTEM STATE (live data):\n```json\n{state_json}\n```\n" if state_json else ""
    run_work_task("Morning Briefing ☀️", f"""
        Generate a comprehensive morning briefing for the operator.{data_block}
        Call get_metrics, get_clusters, get_top_leads, and list_deal_opportunities
        to build the full picture. Structure your report as:

        1. OVERNIGHT SUMMARY: What happened in the last 12 hours —
           new signals, cluster changes, merchants detected
        2. PIPELINE HEALTH: Current system status, any errors or degradation
        3. ACTION ITEMS: Top 3 things that need operator attention today —
           deals to review, leads to investigate, opportunities to approve
        4. KEY METRICS: Signal volume, conversion rate, attribution rate

        Format this as a clean, executive-level morning brief.
    """, max_tokens=3500)


def evening_wrap():
    """Daily at 9 PM — day summary and tomorrow's priorities."""
    state_json = _prefetch_operator_state()
    data_block = f"\n\nCURRENT SYSTEM STATE (live data):\n```json\n{state_json}\n```\n" if state_json else ""
    run_work_task("Evening Wrap 🌙", f"""
        Generate an end-of-day wrap-up.{data_block}
        Call get_metrics, get_top_leads, and list_deal_opportunities to
        build the full picture. Structure your report as:

        1. TODAY'S RESULTS: What the pipeline accomplished today —
           signals processed, merchants attributed, deals generated
        2. CONVERSION STATUS: Any leads that converted or moved forward
        3. TOMORROW'S PRIORITIES: Based on current data, what should
           be the focus areas tomorrow
        4. SYSTEM HEALTH: Any issues that need fixing before tomorrow

        Keep it concise but comprehensive.
    """, max_tokens=3500)
