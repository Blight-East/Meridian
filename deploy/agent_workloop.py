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

logger = get_logger("agent_workloop")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
OPERATOR_CHAT_ID = os.environ.get("OPERATOR_CHAT_ID", "5940537326")
AGENT_USER_ID = "agent_autonomous"  # internal user ID for autonomous sessions

import redis, json
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
PROACTIVE_QUEUE = "agent_flux:proactive_messages"


def send_telegram(text):
    """
    Queue a proactive message for delivery via the Telegram bot process.
    The bot process polls this queue and sends messages using its active
    conversation context (avoids Telegram 403 'can't initiate' error).
    """
    try:
        if len(text) > 4000:
            text = text[:3997] + "..."
        _redis.rpush(PROACTIVE_QUEUE, json.dumps({
            "chat_id": OPERATOR_CHAT_ID,
            "text": text,
            "timestamp": time.time(),
        }))
        logger.info(f"Proactive message queued ({len(text)} chars)")
        return True
    except Exception as e:
        logger.error(f"Failed to queue proactive message: {e}")
        return False


def run_work_task(task_name, prompt):
    """
    Execute a proactive work task:
    1. Send prompt to Claude via chat engine (with full tool access)
    2. Send result to operator via Telegram
    3. Log the work session
    """
    logger.info(f"Starting work task: {task_name}")
    start = time.time()

    try:
        response = generate_chat_response(AGENT_USER_ID, prompt)
        elapsed = round(time.time() - start, 1)

        if response:
            header = f"🤖 *Agent Flux — {task_name}*\n\n"
            send_telegram(header + response)

            save_event("agent_work_completed", {
                "task": task_name,
                "elapsed_seconds": elapsed,
                "response_length": len(response),
            })
            logger.info(f"Work task completed: {task_name} ({elapsed}s)")
        else:
            logger.warning(f"Work task returned empty response: {task_name}")

    except Exception as e:
        logger.error(f"Work task failed: {task_name} — {e}")
        save_event("agent_work_failed", {"task": task_name, "error": str(e)})


# ── Scheduled Task Definitions ───────────────────────────────────

def pipeline_briefing():
    """Every 2 hours — concise pipeline status update."""
    run_work_task("Pipeline Briefing", """
        Pull the current operator state and metrics. Give me a concise status briefing:
        - Signal volume (last 2h vs 24h trend)
        - Active clusters and their severity
        - New merchants detected
        - Any anomalies or issues that need attention
        Keep it under 10 lines. Data only, no filler.
    """)


def lead_review():
    """Every 4 hours — investigate top unworked leads."""
    run_work_task("Lead Review", """
        Review the current top leads and top distressed merchants.
        Pick the 3 most promising leads — ones with highest qualification scores
        and revenue signals. For each:
        - Merchant name and processor
        - Why they're distressed
        - Recommended next action (investigate, draft outreach, or skip)
        Be direct and actionable.
    """)


def deal_pipeline_check():
    """Every 6 hours — review deal opportunities."""
    run_work_task("Deal Pipeline", """
        Check the deal opportunities pipeline. List any pending deals.
        For each pending opportunity:
        - Merchant and distress reason
        - Outreach draft quality (good/needs work)
        - Recommend: approve, reject, or hold
        If there are approved but unsent deals, flag them.
        If there are no deals, say so and suggest what could generate more.
    """)


def competitive_intel():
    """Every 8 hours — research market and competitor activity."""
    run_work_task("Market Intel", """
        Search the web for recent news about payment processor issues,
        merchant account freezes, or changes in Stripe/PayPal/Square policies.
        Look for:
        - New processor policy changes that could create distressed merchants
        - Competitor activity in the merchant services recovery space
        - Emerging payment industry trends
        Summarize the top 3 findings with source URLs.
    """)


def morning_briefing():
    """Daily at 9 AM — full overnight recap."""
    run_work_task("Morning Briefing ☀️", """
        Generate a comprehensive morning briefing for the operator:

        1. OVERNIGHT SUMMARY: What happened in the last 12 hours —
           new signals, cluster changes, merchants detected
        2. PIPELINE HEALTH: Current system status, any errors or degradation
        3. ACTION ITEMS: Top 3 things that need operator attention today —
           deals to review, leads to investigate, opportunities to approve
        4. KEY METRICS: Signal volume, conversion rate, attribution rate

        Format this as a clean, executive-level morning brief.
    """)


def evening_wrap():
    """Daily at 9 PM — day summary and tomorrow's priorities."""
    run_work_task("Evening Wrap 🌙", """
        Generate an end-of-day wrap-up:

        1. TODAY'S RESULTS: What the pipeline accomplished today —
           signals processed, merchants attributed, deals generated
        2. CONVERSION STATUS: Any leads that converted or moved forward
        3. TOMORROW'S PRIORITIES: Based on current data, what should
           be the focus areas tomorrow
        4. SYSTEM HEALTH: Any issues that need fixing before tomorrow

        Keep it concise but comprehensive.
    """)
