import asyncio
import sys, os, requests, json, time, re, mimetypes
import threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from safety.guard import resolve_operator_delivery_chat, validate_operator_context
from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.conversation.chat_memory import save_message
from runtime.conversation.chat_engine import generate_chat_response, get_cached_operator_status_brief
from runtime.conversation.mission_memory import (
    add_blocker,
    add_experiment,
    add_next_action,
    add_win,
    get_mission_memory,
    set_primary_mission,
)
from runtime.conversation.meridian_capability_context import (
    render_meridian_capability_answer,
    render_meridian_capability_context,
)
from runtime.conversation.operator_profile import (
    choose_agent_identity,
    deepen_agent_identity,
    get_agent_identity,
    get_operator_profile,
    remember_operator_note,
    set_agent_identity_summary,
    set_operator_focus,
    set_operator_goal,
    set_agent_name,
    set_agent_role,
)
from runtime.conversation.payflux_icp_context import render_payflux_icp_brief, render_payflux_icp_context
from runtime.conversation.payflux_mandate_context import (
    render_payflux_mandate_answer,
    render_payflux_mandate_brief,
    render_payflux_mandate_context,
    seed_payflux_mandate,
)
from runtime.conversation.payflux_product_context import render_payflux_product_brief, render_payflux_product_context
from runtime.conversation.payflux_sales_context import render_payflux_sales_brief, render_payflux_sales_context
from runtime.health.telemetry import get_component_state, heartbeat, record_component_state, utc_now_iso
from runtime.ops.operator_commands import (
    approve_outreach_for_opportunity_command,
    apply_critic_rewrite_command,
    advance_top_queue_opportunity_command,
    draft_follow_up_for_opportunity_command,
    list_sent_outreach_needing_follow_up_command,
    mark_outreach_outcome_command,
    run_mission_execution_loop_command,
    rewrite_outreach_draft_command,
    send_outreach_for_opportunity_command,
    show_opportunity_workbench_command,
    show_opportunity_reasoning_command,
    show_outcome_review_queue_command,
    show_reply_review_command,
    show_reply_review_queue_command,
    apply_suggested_outcome_command,
    show_local_outreach_draft_command,
    show_local_outreach_drafts_command,
    show_execution_plan_command,
    show_brain_state_command,
    show_brain_critic_command,
    show_outreach_learning_command,
    run_reply_draft_monitor_command,
    run_reply_outcome_monitor_command,
    run_strategic_deliberation_command,
    run_brain_critic_command,
    show_reasoning_scoreboard_command,
    show_revenue_scoreboard_command,
    show_recent_suppressed_leads_command,
    show_gmail_distress_screen_command,
    show_opportunity_fit_command,
    show_top_queue_opportunities_command,
    send_gmail_reply_command,
    send_reddit_reply_command,
    show_action_envelopes_command,
    show_security_incidents_command,
    show_value_heartbeat_command,
    show_prospect_scout_command,
    show_daily_operator_briefing_command,
)
import logging
import redis as _redis_lib
from config.logging_config import LOG_DIR

_r = _redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
PROACTIVE_QUEUE = "agent_flux:proactive_messages"

chat_logger = logging.getLogger("chat")
chat_handler = logging.FileHandler(os.path.join(LOG_DIR, "chat.log"))
chat_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
if not chat_logger.handlers:
    chat_logger.addHandler(chat_handler)
chat_logger.setLevel(logging.INFO)

logger = get_logger("telegram")
API = "http://localhost:8000"
REQUEST_TIMEOUT_SECONDS = int(os.getenv("AGENT_FLUX_BOT_REQUEST_TIMEOUT_SECONDS", "5"))
CHAT_TIMEOUT_SECONDS = int(os.getenv("AGENT_FLUX_BOT_CHAT_TIMEOUT_SECONDS", "90"))
PROACTIVE_ONLY = os.getenv("AGENT_FLUX_TELEGRAM_PROACTIVE_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

# ── Single-instance lock (same-host duplicate prevention) ──────────────────
# Cross-host duplicates (e.g. local dev + VPS prod) are handled by the
# Conflict-error backoff in run() — Redis can't span hosts here.
TELEGRAM_LOCK_KEY = "agent_flux:telegram_bot_lock"
TELEGRAM_LOCK_TTL = 60
_LOCK_HOLDER_TOKEN = f"{os.uname().nodename}:{os.getpid()}"
_LOCK_REFRESH_STOP: "threading.Event | None" = None
_LOCK_REFRESH_THREAD: "threading.Thread | None" = None


def _acquire_singleton_lock() -> bool:
    try:
        return bool(_r.set(TELEGRAM_LOCK_KEY, _LOCK_HOLDER_TOKEN, nx=True, ex=TELEGRAM_LOCK_TTL))
    except Exception as e:
        logger.warning(f"telegram lock acquire failed (redis error: {e}); proceeding without lock")
        return True  # fail-open on Redis outage so the bot still runs


def _release_singleton_lock() -> None:
    try:
        # Only release if we still hold it
        held = _r.get(TELEGRAM_LOCK_KEY)
        if held == _LOCK_HOLDER_TOKEN:
            _r.delete(TELEGRAM_LOCK_KEY)
    except Exception:
        pass


def _refresh_singleton_lock_loop(stop_event: "threading.Event") -> None:
    while not stop_event.is_set():
        try:
            held = _r.get(TELEGRAM_LOCK_KEY)
            if held != _LOCK_HOLDER_TOKEN:
                logger.error("telegram singleton lock lost (held by %s); exiting", held)
                os._exit(2)
            _r.expire(TELEGRAM_LOCK_KEY, TELEGRAM_LOCK_TTL)
        except Exception as e:
            logger.warning(f"telegram lock refresh failed: {e}")
        stop_event.wait(TELEGRAM_LOCK_TTL // 2)


def _start_lock_refresher() -> None:
    global _LOCK_REFRESH_STOP, _LOCK_REFRESH_THREAD
    _LOCK_REFRESH_STOP = threading.Event()
    _LOCK_REFRESH_THREAD = threading.Thread(
        target=_refresh_singleton_lock_loop,
        args=(_LOCK_REFRESH_STOP,),
        name="agent_flux_telegram_lock_refresh",
        daemon=True,
    )
    _LOCK_REFRESH_THREAD.start()


def _authorize_update(update: Update):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        raise PermissionError("Missing Telegram operator context")
    ctx = validate_operator_context(
        str(user.id),
        chat_id=str(chat.id),
        chat_type=getattr(chat, "type", ""),
        username=getattr(user, "username", "") or "",
        bind_on_first_use=True,
    )
    _r.set(f"agent_flux:chat_id:{str(user.id)}", str(chat.id))
    return ctx


async def _require_authorized(update: Update):
    try:
        return _authorize_update(update)
    except PermissionError as exc:
        record_component_state(
            "telegram",
            ttl=300,
            adapter_health="degraded",
            last_unauthorized_at=utc_now_iso(),
            last_unauthorized_reason=str(exc),
        )
        logger.warning(
            "telegram operator authorization rejected | user=%s chat=%s reason=%s",
            getattr(update.effective_user, "id", "unknown"),
            getattr(update.effective_chat, "id", "unknown"),
            exc,
        )
        if update.message:
            await update.message.reply_text("This chat is not authorized to control Agent Flux.")
        return None


def _api_json(method, path, **kwargs):
    timeout = kwargs.pop("timeout", REQUEST_TIMEOUT_SECONDS)
    resp = requests.request(method, f"{API}{path}", timeout=timeout, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _backend_health_snapshot():
    try:
        return _api_json("GET", "/health", timeout=3)
    except Exception as e:
        return {"status": "unreachable", "error": str(e)}


def _looks_like_status_request(message: str) -> bool:
    text = (message or "").lower()
    keywords = (
        "status",
        "system",
        "health",
        "what changed",
        "how's that going",
        "how is that going",
        "operator state",
    )
    return any(keyword in text for keyword in keywords)


def _capture_relationship_preference(user_id: str, message: str) -> None:
    text = str(message or "").strip()
    lowered = text.lower()
    inferred_note = ""

    if "partner not robot" in lowered or ("partner" in lowered and "robot" in lowered):
        inferred_note = "The operator wants Meridian to feel like a real partner, not a robot."
    elif "be honest with me" in lowered or ("honest" in lowered and "with me" in lowered):
        inferred_note = "The operator values plain honesty over soft, empty reassurance."
    elif "sound human" in lowered or "sound like a human" in lowered:
        inferred_note = "The operator wants Meridian to sound human and conversational."
    elif "tell me the truth" in lowered:
        inferred_note = "The operator wants the truth directly, even when it is uncomfortable."
    elif "make money" in lowered and ("your job" in lowered or "your purpose" in lowered):
        inferred_note = "The operator wants Meridian anchored on making money, not generic helpfulness."

    if inferred_note:
        remember_operator_note(str(user_id), inferred_note)


def _record_operator_status_fallback(*, reason: str, source: str, stale_brief: str | None = None):
    current = get_component_state("operator_status") or {}
    record_component_state(
        "operator_status",
        ttl=600,
        last_status_total_ms=current.get("last_status_total_ms") or "unknown",
        last_status_slowest_step=current.get("last_status_slowest_step") or "unknown",
        last_status_slowest_step_ms=current.get("last_status_slowest_step_ms") or "unknown",
        last_status_step_timings=current.get("last_status_step_timings") or {},
        fallback_used=True,
        fallback_reason=str(reason),
        last_status_fallback_source=source,
        last_status_fallback_at=utc_now_iso(),
        last_status_fallback_brief=stale_brief or "",
    )


async def _reply_and_record(message, text):
    await message.reply_text(text)
    heartbeat("telegram", ttl=300, last_successful_bot_reply=utc_now_iso(), adapter_health="healthy")


def _timeout_reply_for_message(message: str, *, health_snapshot: dict | None = None) -> str:
    stale_brief = get_cached_operator_status_brief(health_snapshot=health_snapshot)
    if stale_brief and _looks_like_status_request(message):
        return f"Live status is taking longer than usual. {stale_brief}"
    if _looks_like_status_request(message):
        return "Live status is taking longer than usual. Try again in a moment if you want the full fresh read."
    return (
        "I took too long on that one. Ask again and I’ll answer more directly, or break it into one smaller question."
    )


def _match_conversational_approval(message: str):
    text = str(message or "").strip()
    lowered = text.lower()

    gmail_match = re.match(
        r"^(approve|send)\s+(the\s+)?gmail\s+(reply|send)\s+(for\s+)?(?P<thread_id>[A-Za-z0-9_\-]+)$",
        text,
        re.I,
    )
    if gmail_match:
        return {"kind": "approve_send", "channel": "gmail", "target_id": gmail_match.group("thread_id")}

    reddit_match = re.match(
        r"^(approve|send)\s+(the\s+)?reddit\s+(reply|send)\s+(for\s+)?(?P<target_id>[A-Za-z0-9_\-:]+)(?:\s+(?P<permalink>https?://\S+))?$",
        text,
        re.I,
    )
    if reddit_match:
        return {
            "kind": "approve_send",
            "channel": "reddit",
            "target_id": reddit_match.group("target_id"),
            "permalink": reddit_match.group("permalink") or "",
        }

    if lowered in {
        "show pending approvals",
        "show approval queue",
        "show pending action envelopes",
        "what needs approval",
    }:
        return {"kind": "show_action_envelopes", "review_status": "pending_review"}

    if lowered in {
        "show pending drafts",
        "show local drafts",
        "show outreach drafts",
        "show approval drafts",
        "what drafts are waiting",
    }:
        return {"kind": "show_local_outreach_drafts", "limit": 5}

    latest_draft_match = re.match(r"^(?:show|review)\s+(?:the\s+)?(?:latest|newest|top)\s+draft$", text, re.I)
    if latest_draft_match:
        return {"kind": "show_local_outreach_draft"}

    specific_draft_match = re.match(
        r"^(?:show|review)\s+(?:the\s+)?(?:local\s+|outreach\s+)?draft\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if specific_draft_match:
        return {"kind": "show_local_outreach_draft", "opportunity_id": int(specific_draft_match.group("opportunity_id"))}

    critic_rewrite_match = re.match(
        r"^(?:apply|use|take)\s+(?:the\s+)?critic\s+rewrite\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if critic_rewrite_match:
        return {"kind": "apply_critic_rewrite", "opportunity_id": int(critic_rewrite_match.group("opportunity_id"))}

    workbench_match = re.match(
        r"^(?:show|review|open)\s+(?:the\s+)?(?:opportunity|case|workbench)\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if workbench_match:
        return {"kind": "show_opportunity_workbench", "opportunity_id": int(workbench_match.group("opportunity_id"))}

    opportunity_fit_match = re.match(
        r"^(?:show|review|open|explain)\s+(?:the\s+)?(?:opportunity|case)\s+fit\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if opportunity_fit_match:
        return {"kind": "show_opportunity_fit", "opportunity_id": int(opportunity_fit_match.group("opportunity_id"))}

    opportunity_fit_why_match = re.match(
        r"^(?:why\s+is)\s+(?:opportunity\s+)?(?P<opportunity_id>\d+)\s+(?:high\s+conviction|blocked|suppressed|qualified).*$",
        text,
        re.I,
    )
    if opportunity_fit_why_match:
        return {"kind": "show_opportunity_fit", "opportunity_id": int(opportunity_fit_why_match.group("opportunity_id"))}

    opportunity_reasoning_match = re.match(
        r"^(?:show|review|open)\s+(?:the\s+)?(?:opportunity\s+)?reasoning\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if opportunity_reasoning_match:
        return {"kind": "show_opportunity_reasoning", "opportunity_id": int(opportunity_reasoning_match.group("opportunity_id"))}

    score_reasoning_match = re.match(
        r"^(?:why\s+did\s+you\s+score|why\s+did\s+you\s+rank)\s+(?:opportunity\s+)?(?P<opportunity_id>\d+).*$",
        text,
        re.I,
    )
    if score_reasoning_match:
        return {"kind": "show_opportunity_reasoning", "opportunity_id": int(score_reasoning_match.group("opportunity_id"))}

    approve_draft_match = re.match(
        r"^(?:approve)\s+(?:the\s+)?(?:local\s+|outreach\s+)?draft\s+(?P<opportunity_id>\d+)(?:\s*[:\-]\s*(?P<notes>.+))?$",
        text,
        re.I,
    )
    if approve_draft_match:
        return {
            "kind": "approve_outreach_draft",
            "opportunity_id": int(approve_draft_match.group("opportunity_id")),
            "notes": (approve_draft_match.group("notes") or "").strip(),
        }

    approve_outreach_match = re.match(
        r"^(?:approve)\s+outreach\s+(?P<opportunity_id>\d+)(?:\s*[:\-]\s*(?P<notes>.+))?$",
        text,
        re.I,
    )
    if approve_outreach_match:
        return {
            "kind": "approve_outreach_draft",
            "opportunity_id": int(approve_outreach_match.group("opportunity_id")),
            "notes": (approve_outreach_match.group("notes") or "").strip(),
        }

    rewrite_match = re.match(
        r"^(?:rewrite|tighten|edit)\s+(?:the\s+)?draft\s+(?P<opportunity_id>\d+)(?:\s+(?P<style>sharper|softer|shorter|direct|standard))?(?:\s*[:\-]\s*(?P<instructions>.+))?$",
        text,
        re.I,
    )
    if rewrite_match:
        return {
            "kind": "rewrite_outreach_draft",
            "opportunity_id": int(rewrite_match.group("opportunity_id")),
            "style": (rewrite_match.group("style") or "standard").strip().lower(),
            "instructions": (rewrite_match.group("instructions") or "").strip(),
        }

    send_draft_match = re.match(
        r"^(?:(?:send)|(?:approve\s+and\s+send))\s+(?:the\s+)?draft\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if send_draft_match:
        return {
            "kind": "approve_and_send_draft",
            "opportunity_id": int(send_draft_match.group("opportunity_id")),
        }

    follow_up_draft_match = re.match(
        r"^(?:draft|prepare|write)\s+(?:a\s+)?follow[\s-]?up\s+(?:for\s+)?(?P<opportunity_id>\d+)(?:\s+(?P<style>sharper|softer|shorter|direct|standard))?(?:\s*[:\-]\s*(?P<instructions>.+))?$",
        text,
        re.I,
    )
    if follow_up_draft_match:
        return {
            "kind": "draft_follow_up",
            "opportunity_id": int(follow_up_draft_match.group("opportunity_id")),
            "style": (follow_up_draft_match.group("style") or "").strip().lower(),
            "instructions": (follow_up_draft_match.group("instructions") or "").strip(),
        }

    outcome_match = re.match(
        r"^(?:mark)\s+(?:opportunity|draft|case)\s+(?P<opportunity_id>\d+)\s+(?P<outcome>won|lost|ignored)(?:\s*[:\-]\s*(?P<notes>.+))?$",
        text,
        re.I,
    )
    if outcome_match:
        return {
            "kind": "mark_outreach_outcome",
            "opportunity_id": int(outcome_match.group("opportunity_id")),
            "outcome": outcome_match.group("outcome").strip().lower(),
            "notes": (outcome_match.group("notes") or "").strip(),
        }

    if lowered in {
        "show security incidents",
        "show open security incidents",
        "did anyone try to prompt inject you",
        "show prompt injection attempts",
    }:
        return {"kind": "show_security_incidents", "status": "open"}

    if lowered in {
        "show value heartbeat",
        "how much value are you producing",
        "show value",
        "show live value metrics",
    }:
        return {"kind": "show_value_heartbeat", "refresh": True}

    if lowered in {
        "show prospect scout",
        "prospect scout",
        "why no new prospects",
        "what almost made it",
        "show rejected prospects",
        "show sourcing misses",
    }:
        return {"kind": "show_prospect_scout", "refresh": False}

    if lowered in {
        "show revenue scoreboard",
        "show revenue scorecard",
        "where is the bottleneck",
        "show the bottleneck",
    }:
        return {"kind": "show_revenue_scoreboard", "refresh": True}

    if lowered in {
        "show reasoning scoreboard",
        "show reasoning performance",
        "show model performance",
        "how is deepseek doing",
        "how is reasoning doing",
    }:
        return {"kind": "show_reasoning_scoreboard"}

    if lowered in {
        "show execution plan",
        "what should you do next",
        "what should we do next",
        "show priorities",
    }:
        return {"kind": "show_execution_plan", "refresh": True}

    if lowered in {
        "show brain",
        "show your brain",
        "what is your brain state",
        "what are you thinking",
        "show strategic state",
    }:
        return {"kind": "show_brain_state", "refresh": False}

    if lowered in {
        "think strategically",
        "rerun your brain",
        "refresh your brain",
        "do a strategic reset",
    }:
        return {"kind": "run_strategic_deliberation"}

    if lowered in {
        "show critic review",
        "show your critic review",
        "criticize your plan",
        "self critique",
        "show your critique",
    }:
        return {"kind": "show_brain_critic", "refresh": False}

    if lowered in {
        "run critic review",
        "critique yourself now",
        "stress test your plan",
        "run self critique",
    }:
        return {"kind": "run_brain_critic"}

    if lowered in {
        "daily briefing",
        "morning briefing",
        "brief me",
        "show daily briefing",
        "show morning briefing",
        "give me the briefing",
    }:
        return {"kind": "show_daily_operator_briefing"}

    if lowered in {
        "show top opportunities",
        "show top 3 opportunities",
        "show the top 3 opportunities",
        "what are the top opportunities",
        "show highest conviction opportunities",
        "show the best opportunities",
    }:
        return {"kind": "show_top_queue_opportunities", "limit": 3}

    if lowered in {
        "show suppressed leads",
        "show recent suppressed leads",
        "what got suppressed",
        "what leads were blocked",
        "show blocked leads",
    }:
        return {"kind": "show_recent_suppressed_leads", "limit": 5}

    if lowered in {
        "show gmail distress screen",
        "show distress screen",
        "show distress inbox screen",
        "which distress threads are real",
        "which gmail distress threads are actionable",
    }:
        return {"kind": "show_gmail_distress_screen", "limit": 5}

    if lowered in {
        "work the top opportunity",
        "advance the top opportunity",
        "work the best opportunity",
        "move the top queue case forward",
        "work the top queue case",
    }:
        return {"kind": "advance_top_queue_opportunity"}

    if lowered in {
        "show follow ups",
        "show follow-up queue",
        "what needs follow up",
        "show overdue follow ups",
    }:
        return {"kind": "show_follow_ups", "limit": 5}

    if lowered in {
        "show reply queue",
        "show replies to review",
        "what replies need review",
        "show merchant replies",
    }:
        return {"kind": "show_reply_review_queue", "limit": 5}

    if lowered in {
        "show outcome queue",
        "show outcome candidates",
        "what outcomes can you apply",
        "show suggested outcomes",
    }:
        return {"kind": "show_outcome_review_queue", "limit": 5}

    review_reply_match = re.match(
        r"^(?:show|review|open)\s+(?:reply|response)\s+(?P<opportunity_id>\d+)$",
        text,
        re.I,
    )
    if review_reply_match:
        return {"kind": "show_reply_review", "opportunity_id": int(review_reply_match.group("opportunity_id"))}

    apply_outcome_match = re.match(
        r"^(?:apply)\s+(?:the\s+)?suggested\s+outcome\s+(?P<opportunity_id>\d+)(?:\s*[:\-]\s*(?P<notes>.+))?$",
        text,
        re.I,
    )
    if apply_outcome_match:
        return {
            "kind": "apply_suggested_outcome",
            "opportunity_id": int(apply_outcome_match.group("opportunity_id")),
            "notes": (apply_outcome_match.group("notes") or "").strip(),
        }

    if lowered in {
        "scan reply queue now",
        "check for new replies",
        "run reply monitor",
        "scan outcome queue now",
    }:
        return {"kind": "run_reply_outcome_monitor"}

    if lowered in {
        "draft replies now",
        "run reply draft monitor",
        "prepare reply drafts",
        "draft follow ups from replies",
    }:
        return {"kind": "run_reply_draft_monitor"}

    if lowered in {
        "show lessons learned",
        "show outreach lessons",
        "what have you learned",
        "show recent lessons",
    }:
        return {"kind": "show_outreach_learning", "limit": 5}

    if lowered in {
        "run execution loop",
        "refocus now",
        "generate a new plan",
    }:
        return {"kind": "run_execution_loop"}

    remember_match = re.match(r"^remember(?:\s+that)?\s+(?P<note>.+)$", text, re.I)
    if remember_match:
        return {"kind": "remember_note", "note": remember_match.group("note").strip()}

    goal_match = re.match(r"^(?:my goal is|our goal is)\s+(?P<goal>.+)$", text, re.I)
    if goal_match:
        return {"kind": "set_goal", "goal": goal_match.group("goal").strip()}

    focus_match = re.match(r"^(?:focus on|we are focused on)\s+(?P<focus>.+)$", text, re.I)
    if focus_match:
        return {"kind": "set_focus", "focus": focus_match.group("focus").strip()}

    name_match = re.match(r"^(?:your name is|call yourself|you are now called|your name should be)\s+(?P<name>.+)$", text, re.I)
    if name_match:
        return {"kind": "set_agent_name", "name": name_match.group("name").strip()}

    role_match = re.match(r"^(?:your role is|your job is|you are the)\s+(?P<role>.+)$", text, re.I)
    if role_match:
        role = role_match.group("role").strip()
        if len(role.split()) <= 10:
            return {"kind": "set_agent_role", "role": role}

    identity_match = re.match(r"^(?:your identity is|this is who you are|your identity summary is)\s+(?P<summary>.+)$", text, re.I)
    if identity_match:
        return {"kind": "set_agent_identity_summary", "summary": identity_match.group("summary").strip()}

    if lowered in {
        "what do you remember about me",
        "show my profile",
        "what is my profile",
        "what do you know about me",
    }:
        return {"kind": "show_profile"}

    mission_match = re.match(r"^(?:the mission is|our mission is|mission:)\s+(?P<mission>.+)$", text, re.I)
    if mission_match:
        return {"kind": "set_mission", "mission": mission_match.group("mission").strip()}

    blocker_match = re.match(r"^(?:add blocker|blocker:)\s+(?P<blocker>.+)$", text, re.I)
    if blocker_match:
        return {"kind": "add_blocker", "blocker": blocker_match.group("blocker").strip()}

    experiment_match = re.match(r"^(?:add experiment|experiment:)\s+(?P<experiment>.+)$", text, re.I)
    if experiment_match:
        return {"kind": "add_experiment", "experiment": experiment_match.group("experiment").strip()}

    win_match = re.match(r"^(?:add win|win:)\s+(?P<win>.+)$", text, re.I)
    if win_match:
        return {"kind": "add_win", "win": win_match.group("win").strip()}

    action_match = re.match(r"^(?:add next action|next action:)\s+(?P<action>.+)$", text, re.I)
    if action_match:
        return {"kind": "add_next_action", "action": action_match.group("action").strip()}

    if lowered in {
        "show mission",
        "what is the mission",
        "what are we focused on",
        "show mission memory",
    }:
        return {"kind": "show_mission"}

    if lowered in {
        "what is your name",
        "who are you",
        "show your identity",
        "what is your identity",
        "tell me about yourself",
    }:
        return {"kind": "show_agent_identity"}

    identity_origin_match = re.match(
        r"^(?:why did you choose the name|what made you choose the name|why choose the name)\s+(?P<name>.+)$",
        text,
        re.I,
    )
    if identity_origin_match:
        return {"kind": "show_agent_identity_origin", "name": identity_origin_match.group("name").strip()}

    if lowered in {
        "why meridian",
        "why that name",
        "why did you choose that name",
        "what made you choose that name",
        "did you choose your own name",
        "did you pick your own name",
    }:
        return {"kind": "show_agent_identity_origin", "name": ""}

    if lowered in {
        "choose your own name",
        "choose your own identity",
        "pick your own name",
        "decide your own identity",
    }:
        return {"kind": "choose_agent_identity"}

    if lowered in {
        "go deeper on your identity",
        "deepen your identity",
        "deepen your character",
        "show your charter",
        "what do you value",
        "what do you refuse",
        "what are your non negotiables",
        "how do you work with me",
    }:
        return {"kind": "deepen_agent_identity"}

    if lowered in {
        "what is payflux",
        "what does payflux do",
        "show payflux context",
        "show product context",
        "what are we selling",
        "what is the payflux offer",
    }:
        return {"kind": "show_payflux_context"}

    if lowered in {
        "show icp",
        "show buyer taxonomy",
        "who do we sell to",
        "what counts as a high conviction prospect",
        "what disqualifies a lead",
        "show prospect taxonomy",
        "show payflux icp",
    }:
        return {"kind": "show_payflux_icp"}

    if lowered in {
        "show objection script",
        "show sales script",
        "show sales objections",
        "how do we handle objections",
        "show objection handling",
        "show payflux sales context",
    }:
        return {"kind": "show_payflux_sales_context"}

    if lowered in {
        "show mandate",
        "what are we trying to do",
        "what is your mandate",
    }:
        return {"kind": "show_payflux_mandate", "question_type": "mandate"}

    if lowered in {
        "what is your purpose",
        "why do you exist",
    }:
        return {"kind": "show_payflux_mandate", "question_type": "purpose"}

    if lowered in {
        "show your goal",
        "what is your goal",
    }:
        return {"kind": "show_payflux_mandate", "question_type": "my_goal"}

    if lowered in {
        "show our goal",
        "what is our goal",
    }:
        return {"kind": "show_payflux_mandate", "question_type": "our_goal"}

    if lowered in {
        "show capabilities",
        "show your capabilities",
        "what can you do",
        "what can you already do",
    }:
        return {"kind": "show_meridian_capabilities", "question_type": "can_do"}
    if (
        "what can you do" in lowered
        or "what can you already do" in lowered
        or ("your capabilities" in lowered and "show" in lowered)
    ):
        return {"kind": "show_meridian_capabilities", "question_type": "can_do"}

    if lowered in {
        "what tools do you have",
        "show your tools",
        "show me your tools",
        "what are your tools",
        "how do you use your tools",
        "how do you use them",
        "what new tools do you have",
        "did you get new tools",
        "what tools did i give you",
    }:
        return {"kind": "show_meridian_capabilities", "question_type": "tools"}
    if (
        ("tool" in lowered and "you have" in lowered)
        or ("tool" in lowered and "your" in lowered)
        or ("tool" in lowered and "use" in lowered)
        or ("new tools" in lowered)
    ):
        return {"kind": "show_meridian_capabilities", "question_type": "tools"}

    if lowered in {
        "what do you need",
        "what are you missing",
        "show capability gaps",
        "show what is missing",
    }:
        return {"kind": "show_meridian_capabilities", "question_type": "gaps"}
    if (
        ("missing" in lowered and "you" in lowered)
        or ("what do you need" in lowered)
        or ("what do you think you need" in lowered)
        or ("what do you feel like you are missing" in lowered)
        or ("what are your gaps" in lowered)
    ):
        return {"kind": "show_meridian_capabilities", "question_type": "gaps"}

    if lowered in {
        "show capability audit",
        "show live capability audit",
        "capability audit",
        "audit your capabilities",
    }:
        return {"kind": "show_meridian_capabilities", "question_type": "audit"}
    if "capability audit" in lowered or ("audit" in lowered and "capabilit" in lowered):
        return {"kind": "show_meridian_capabilities", "question_type": "audit"}

    return None


def _render_top_queue_opportunities(result: dict) -> str:
    cases = list(result.get("top_opportunities") or [])
    if not cases:
        suppressed = int(result.get("suppressed_weak_candidates") or 0)
        reasons = result.get("suppression_reasons") or {}
        lines = ["There are no strong top-priority queue opportunities right now."]
        if suppressed > 0:
            lines.append(f"{suppressed} weaker candidate(s) were suppressed instead of being promoted.")
            top_reasons = sorted(reasons.items(), key=lambda item: item[1], reverse=True)[:3]
            for reason, count in top_reasons:
                lines.append(f"- {count} blocked because {reason}")
        return "\n".join(lines)

    lines = ["Top queue opportunities"]
    brain = result.get("brain_state") or {}
    if brain.get("strategic_bet"):
        lines.append(f"Strategic bet: {brain.get('strategic_bet')}")
    if brain.get("main_bottleneck"):
        lines.append(f"Brain bottleneck: {brain.get('main_bottleneck')}")
    total_seen = int(result.get("total_candidates_seen") or len(cases))
    lines.append(f"Showing {len(cases)} case(s) from {total_seen} ranked candidates.")
    lines.append("")
    for index, case in enumerate(cases, start=1):
        merchant = case.get("merchant_name") or case.get("merchant_domain") or "Unknown merchant"
        domain = case.get("merchant_domain") or ""
        domain_line = f" ({domain})" if domain and domain.lower() != str(merchant).lower() else ""
        queue_label = case.get("queue_label") or str(case.get("queue") or "").replace("_", " ")
        processor = str(case.get("processor") or "unknown").replace("_", " ")
        distress = str(case.get("distress_type") or "unknown").replace("_", " ")
        action = str(case.get("recommended_action") or "").replace("_", " ")
        lines.append(f"{index}. {merchant}{domain_line}")
        lines.append(f"   Queue: {queue_label}")
        lines.append(f"   Signal: {processor} / {distress}")
        lines.append(f"   Recommended play: {action or 'unknown'}")
        lines.append(f"   Why now: {case.get('why_now') or 'No rationale captured yet.'}")
        if int(case.get("brain_alignment_bonus") or 0):
            lines.append(
                "   Brain alignment: bonus {} because {}.".format(
                    case.get("brain_alignment_bonus") or 0,
                    case.get("brain_alignment_reason") or "it matches the current brain state",
                )
            )
        if int(case.get("learning_records") or 0) > 0:
            lines.append(
                "   Learning: {} record(s), {} win rate, bonus {}.".format(
                    case.get("learning_records") or 0,
                    case.get("learning_win_rate") or 0.0,
                    case.get("learning_confidence_bonus") or 0,
                )
            )
        lines.append(f"   Next move: {case.get('next_step') or 'Review this case.'}")
        lines.append(f"   Opportunity id: {case.get('opportunity_id')}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_opportunity_fit(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not explain that opportunity right now."
    merchant = result.get("merchant_name") or result.get("merchant_domain") or "Unknown merchant"
    if result.get("merchant_domain") and result.get("merchant_domain") not in merchant:
        merchant = f"{merchant} ({result.get('merchant_domain')})"
    lines = [f"Opportunity fit: {result.get('opportunity_id')}"]
    lines.append(f"Merchant: {merchant}")
    lines.append(f"Signal: {str(result.get('processor') or 'unknown').replace('_', ' ')} / {str(result.get('distress_type') or 'unknown').replace('_', ' ')}")
    lines.append(f"Queue class: {str(result.get('queue_eligibility_class') or 'unknown').replace('_', ' ')}")
    lines.append(f"Queue reason: {result.get('queue_eligibility_reason') or 'No queue reason captured.'}")
    lines.append(f"Queue quality: {result.get('queue_quality_score') or 0}")
    lines.append(f"ICP fit: {result.get('icp_fit_label') or 'unknown'} ({result.get('icp_fit_score') or 0})")
    lines.append(f"ICP reason: {result.get('icp_fit_reason') or 'No ICP reason captured.'}")
    lines.append(f"High conviction: {'yes' if result.get('high_conviction_prospect') else 'no'}")
    if result.get("queue_reason_codes"):
        lines.append("Queue signals:")
        for reason in result.get("queue_reason_codes")[:4]:
            lines.append(f"- {reason}")
    lines.append(
        "Contact path: {} ({}, trust {})".format(
            result.get("contact_email") or "none",
            result.get("contact_quality_label") or "blocked",
            result.get("contact_trust_score") or 0,
        )
    )
    if result.get("contact_reason"):
        lines.append(f"Contact note: {result.get('contact_reason')}")
    if result.get("contact_reason_codes"):
        lines.append("Contact blockers/signals:")
        for reason in result.get("contact_reason_codes")[:4]:
            lines.append(f"- {reason}")
    if result.get("why_now"):
        lines.append(f"Why now: {result.get('why_now')}")
    if result.get("operator_ready"):
        lines.append("Operator-ready: yes")
    else:
        lines.append(f"Operator-ready: no — {result.get('operator_block_reason') or 'blocked'}")
    lines.append(
        "Outreach state: {} / {}".format(
            result.get("outreach_status") or "no_outreach",
            result.get("approval_state") or "unknown",
        )
    )
    return "\n".join(lines).strip()


def _render_recent_suppressed_leads(result: dict) -> str:
    cases = list(result.get("cases") or [])
    if not cases:
        return "There are no recent suppressed leads recorded right now."
    lines = ["Recent suppressed leads"]
    for index, case in enumerate(cases, start=1):
        lines.append(f"{index}. Signal {case.get('signal_id') or 'unknown'}")
        lines.append(f"   Stage: {case.get('stage') or 'unknown'}")
        lines.append(
            "   ICP fit: {} ({})".format(
                case.get("icp_fit_label") or "unknown",
                case.get("icp_fit_score") or 0,
            )
        )
        lines.append(f"   Block reason: {str(case.get('disqualifier_reason') or 'unknown').replace('_', ' ')}")
        if case.get("content_preview"):
            lines.append(f"   Preview: {case.get('content_preview')}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_gmail_distress_screen(result: dict) -> str:
    rows = list(result.get("screened_threads") or [])
    if not rows:
        return "There are no Gmail distress threads to screen right now."
    lines = ["Gmail distress screen"]
    lines.append(f"Actionable now: {result.get('actionable_count') or 0} of {result.get('count') or 0}")
    lines.append("")
    for index, row in enumerate(rows, start=1):
        lines.append(f"{index}. {row.get('subject') or '(no subject)'}")
        lines.append(f"   Sender: {row.get('sender_domain') or 'unknown'}")
        lines.append(
            "   Signal: {} / {} / confidence {}".format(
                str(row.get("processor") or "unknown").replace("_", " "),
                str(row.get("distress_type") or "unknown").replace("_", " "),
                row.get("confidence") or 0.0,
            )
        )
        lines.append(f"   Actionable: {'yes' if row.get('actionable') else 'no'}")
        lines.append(f"   Why: {row.get('screen_summary') or 'No screen summary captured.'}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_advanced_top_queue_case(result: dict) -> str:
    status = str(result.get("status") or "unknown")
    if status == "blocked":
        queue_snapshot = result.get("queue_snapshot") or {}
        lines = ["I could not move a top queue case forward right now."]
        lines.append("Reason: {}".format(result.get("reason") or "unknown"))
        suppressed = int(queue_snapshot.get("suppressed_weak_candidates") or 0)
        if suppressed > 0:
            lines.append("{} weaker case(s) were suppressed before action selection.".format(suppressed))
        return "\n".join(lines)

    top_case = result.get("top_case") or {}
    merchant = top_case.get("merchant_name") or top_case.get("merchant_domain") or "Unknown merchant"
    domain = top_case.get("merchant_domain") or ""
    title = "{} ({})".format(merchant, domain) if domain else merchant
    recommendation = result.get("recommendation") or {}

    lines = ["Top queue case advanced"]
    lines.append("Case: {}".format(title))
    lines.append("Opportunity id: {}".format(top_case.get("opportunity_id")))
    lines.append("Selected play: {}".format(str((result.get("selection") or {}).get("selected_action_label") or top_case.get("recommended_action") or "unknown").replace("_", " ")))

    if status in {"drafted", "drafted_follow_up"}:
        draft = result.get("draft") or {}
        lines.append("Status: approval-ready {}draft created".format("follow-up " if status == "drafted_follow_up" else ""))
        lines.append("Channel: {}".format(draft.get("best_channel") or "gmail"))
        lines.append("Subject: {}".format(draft.get("subject") or ""))
        lines.append("Why now: {}".format(draft.get("why_now") or recommendation.get("why_now") or ""))
        if top_case.get("brain_alignment_reason"):
            lines.append("Brain alignment: {}".format(top_case.get("brain_alignment_reason")))
        if draft.get("gmail_draft_error"):
            lines.append("Draft sync note: Gmail draft sync was unavailable, so this is stored as a local approval draft.")
        lines.append("Next move: review the draft and approve it if it still looks right.")
        return "\n".join(lines)

    lines.append("Status: action selected, but outreach is still blocked")
    if result.get("brain_alignment_reason"):
        lines.append("Brain alignment: {}".format(result.get("brain_alignment_reason")))
    lines.append("Why blocked: {}".format(recommendation.get("wait_reason") or recommendation.get("contact_reason") or "unknown"))
    lines.append("Next move: {}".format(result.get("next_step") or "Review the case manually."))
    return "\n".join(lines)


def _render_local_outreach_drafts(result: dict) -> str:
    cases = list(result.get("cases") or [])
    if not cases:
        return "There are no local outreach drafts waiting for review right now."

    lines = ["Local outreach drafts waiting for review"]
    for index, case in enumerate(cases, start=1):
        merchant = case.get("merchant_domain") or case.get("contact_email") or "Unknown merchant"
        lines.append(f"{index}. Opportunity {case.get('opportunity_id')} — {merchant}")
        lines.append(f"   Subject: {case.get('subject') or '(no subject)'}")
        lines.append(f"   Play: {str(case.get('selected_play') or 'unknown').replace('_', ' ')}")
        lines.append(f"   Status: {case.get('status') or 'unknown'} / {case.get('approval_state') or 'unknown'}")
        metadata = case.get("metadata_json") or {}
        gmail_error = metadata.get("gmail_draft_error") or ""
        if gmail_error:
            lines.append("   Sync note: stored locally because Gmail draft sync is unavailable.")
        lines.append("")
    lines.append("Say 'show draft <opportunity id>' to review the full draft body.")
    lines.append("Say 'approve draft <opportunity id>' to mark one approved.")
    return "\n".join(lines).strip()


def _render_local_outreach_draft(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not find that local draft."
    if result.get("status") == "empty":
        return "There are no local outreach drafts waiting right now."

    draft = result.get("draft") or {}
    if not draft:
        return "I could not load that local outreach draft."

    merchant = draft.get("merchant_domain") or draft.get("contact_email") or "Unknown merchant"
    lines = [f"Local outreach draft for opportunity {draft.get('opportunity_id')}"]
    lines.append(f"Merchant: {merchant}")
    lines.append(f"Contact: {draft.get('contact_email') or 'unknown'}")
    lines.append(f"Play: {str(draft.get('selected_play') or 'unknown').replace('_', ' ')}")
    lines.append(f"Status: {draft.get('status') or 'unknown'} / {draft.get('approval_state') or 'unknown'}")
    if draft.get("why_now"):
        lines.append(f"Why now: {draft.get('why_now')}")
    if draft.get("gmail_draft_error"):
        lines.append("Sync note: Gmail draft sync is unavailable, so this draft is stored locally only.")
    critic = result.get("critic_review") or {}
    if critic:
        lines.append("")
        lines.append("Meridian critic:")
        lines.append(f"- Verdict: {critic.get('verdict') or 'unknown'}")
        lines.append(f"- Strategic alignment: {critic.get('strategic_alignment') or 'unknown'}")
        if critic.get("strategic_alignment_reason"):
            lines.append(f"- Alignment note: {critic.get('strategic_alignment_reason')}")
        weak_points = list(critic.get("weak_points") or [])
        generic_signals = list(critic.get("generic_signals") or [])
        sharpen = list(critic.get("sharpen") or [])
        if weak_points:
            lines.append("- Weak points:")
            lines.extend(f"  • {item}" for item in weak_points[:3])
        if generic_signals:
            lines.append("- Generic signals:")
            lines.extend(f"  • {item}" for item in generic_signals[:3])
        if sharpen:
            lines.append("- What to sharpen:")
            lines.extend(f"  • {item}" for item in sharpen[:3])
    rewrite_preview = result.get("critic_rewrite_preview") or {}
    if rewrite_preview and not rewrite_preview.get("error"):
        lines.append("")
        lines.append("Critic-proposed rewrite:")
        lines.append(f"- Style: {rewrite_preview.get('rewrite_style') or 'sharper'}")
        if rewrite_preview.get("instructions"):
            lines.append(f"- Why: {rewrite_preview.get('instructions')}")
        lines.append(f"- Proposed subject: {rewrite_preview.get('subject') or '(no subject)'}")
        lines.append("- Say 'apply critic rewrite {}' to replace the live draft with this sharper version.".format(draft.get("opportunity_id")))
    lines.append("")
    lines.append(f"Subject: {draft.get('subject') or '(no subject)'}")
    lines.append("")
    lines.append(draft.get("body") or "(empty draft body)")
    lines.append("")
    lines.append(f"To approve this draft, say: approve draft {draft.get('opportunity_id')}")
    lines.append(f"To send it after approval, say: send draft {draft.get('opportunity_id')}")
    return "\n".join(lines).strip()


def _render_opportunity_workbench(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not load that opportunity."
    lines = [f"Opportunity workbench: {result.get('opportunity_id')}"]
    merchant = result.get("merchant_name") or result.get("merchant_domain") or "Unknown merchant"
    if result.get("merchant_domain") and result.get("merchant_domain") not in merchant:
        merchant = f"{merchant} ({result.get('merchant_domain')})"
    lines.append(f"Merchant: {merchant}")
    lines.append(f"Signal: {str(result.get('processor') or 'unknown').replace('_', ' ')} / {str(result.get('distress_type') or 'unknown').replace('_', ' ')}")
    lines.append(f"Selected play: {str(result.get('selected_play') or 'unknown').replace('_', ' ')}")
    lines.append(f"Queue quality: {result.get('queue_quality_score') or 0}")
    lines.append(f"Contact: {result.get('contact_email') or 'none'} ({result.get('contact_quality_label') or 'blocked'}, trust {result.get('contact_trust_score') or 0})")
    if result.get("why_now"):
        lines.append(f"Why now: {result.get('why_now')}")
    if result.get("contact_reason"):
        lines.append(f"Contact note: {result.get('contact_reason')}")
    learning = result.get("learning_signal") or {}
    reasoning = result.get("reasoning") or {}
    if int(learning.get("records") or 0) > 0:
        lines.append(
            "Learning signal: {} record(s), {} win rate, bonus {}.".format(
                learning.get("records") or 0,
                learning.get("win_rate") or 0.0,
                learning.get("confidence_bonus") or 0,
            )
        )
        lessons = list(learning.get("recent_lessons") or [])
        if lessons:
            lines.append(f"Recent lesson: {lessons[0]}")
    if result.get("next_contact_move"):
        lines.append(f"Next move: {result.get('next_contact_move')}")
    lines.append(f"Outreach state: {result.get('outreach_status') or 'no_outreach'} / {result.get('approval_state') or 'unknown'}")
    latest_reasoning = reasoning.get("latest_decision") or {}
    if latest_reasoning:
        lines.append("")
        lines.append(
            "Reasoning: {} via {} / {} in {:.0f} ms.".format(
                latest_reasoning.get("routing_decision") or "unknown",
                latest_reasoning.get("provider") or "rules",
                latest_reasoning.get("model") or "deterministic",
                float(latest_reasoning.get("latency_ms") or 0.0),
            )
        )
        if latest_reasoning.get("model_contributions"):
            lines.append("Model touched: {}".format(", ".join(latest_reasoning.get("model_contributions") or [])))
        if latest_reasoning.get("error"):
            lines.append(f"Reasoning error: {latest_reasoning.get('error')}")
        lines.append(f"Say 'show opportunity reasoning {result.get('opportunity_id')}' for the full reasoning view.")
    draft = result.get("draft") or {}
    if draft:
        lines.append("")
        lines.append(f"Draft subject: {draft.get('subject') or '(no subject)'}")
        lines.append("Say 'show draft {}' to review the full body.".format(result.get("opportunity_id")))
        lines.append("Say 'rewrite draft {} sharper' or 'rewrite draft {} softer' to adjust tone.".format(result.get("opportunity_id"), result.get("opportunity_id")))
        lines.append("Say 'send draft {}' when it is ready to go.".format(result.get("opportunity_id")))
    lines.append("Say 'show opportunity fit {}' to see why this case is qualified or blocked.".format(result.get("opportunity_id")))
    return "\n".join(lines).strip()


def _render_revenue_scoreboard(result: dict) -> str:
    heartbeat = result.get("heartbeat") or {}
    learning = result.get("learning") or {}
    top_queue = (result.get("top_queue") or {}).get("top_opportunities") or []
    brain = result.get("brain") or {}
    critic = result.get("critic") or {}
    reasoning = result.get("reasoning") or {}
    lines = ["Revenue scoreboard"]
    lines.append(f"Loop: {heartbeat.get('active_revenue_loop') or 'unknown'}")
    lines.append(f"Bottleneck: {str(heartbeat.get('primary_bottleneck') or 'unknown').replace('_', ' ')}")
    lines.append(f"Next revenue move: {heartbeat.get('next_revenue_move') or 'unknown'}")
    if brain.get("strategic_bet"):
        lines.append(f"Strategic bet: {brain.get('strategic_bet')}")
    lines.append("")
    lines.append("Overnight contract:")
    lines.append(f"- Status: {str(heartbeat.get('overnight_contract_status') or 'unknown').replace('_', ' ')}")
    if heartbeat.get("live_thread_present"):
        live_label = heartbeat.get("live_thread_merchant_domain") or "live thread"
        lines.append(
            f"- Live thread: {live_label} ({str(heartbeat.get('live_thread_status') or 'unknown').replace('_', ' ')})"
        )
    else:
        lines.append("- Live thread: none")
    lines.append(f"- New real prospects: {heartbeat.get('overnight_new_real_prospects') or 0}")
    lines.append(f"- Queue sharpened: {heartbeat.get('overnight_queue_sharpened_24h') or 0}")
    lines.append(f"- Action-ready by morning: {heartbeat.get('overnight_action_ready_count') or 0}")
    if heartbeat.get("live_thread_next_step"):
        lines.append(f"- Live thread move: {heartbeat.get('live_thread_next_step')}")
    prospects = list(heartbeat.get("overnight_best_new_prospects") or [])
    if prospects:
        lines.append("- Best new prospects:")
        for case in prospects[:3]:
            lines.append(
                "- {} ({}, trust {})".format(
                    case.get("merchant_domain") or "unknown",
                    case.get("contact_email") or "no contact",
                    int(case.get("contact_trust_score") or 0),
                )
            )
    if heartbeat.get("overnight_top_suppression_reason"):
        lines.append(
            f"- Main suppression reason: {str(heartbeat.get('overnight_top_suppression_reason') or '').replace('_', ' ')}"
        )
    lines.append("")
    lines.append("Live counts:")
    lines.append(f"- Pending opportunities: {heartbeat.get('opportunities_pending_review') or 0}")
    lines.append(f"- Approval-ready drafts: {heartbeat.get('outreach_awaiting_approval') or 0}")
    lines.append(f"- Send-eligible leads: {heartbeat.get('send_eligible_leads') or 0}")
    lines.append(f"- Reply reviews needed: {heartbeat.get('reply_review_needed') or 0}")
    lines.append(f"- Outcome reviews ready: {heartbeat.get('outcome_review_ready') or 0}")
    lines.append(f"- Follow-ups due: {heartbeat.get('follow_ups_due') or 0}")
    lines.append(f"- Sends in 24h: {heartbeat.get('channel_sends_24h') or 0}")
    lines.append(f"- Conversions: {heartbeat.get('opportunities_converted') or 0}")
    if top_queue:
        top = top_queue[0]
        lines.append("")
        lines.append("Best current case:")
        lines.append(f"- {top.get('merchant_name') or top.get('merchant_domain') or 'Unknown merchant'}")
        lines.append(f"- Play: {str(top.get('recommended_action') or 'unknown').replace('_', ' ')}")
        lines.append(f"- Why now: {top.get('why_now') or 'No rationale captured.'}")
    if learning.get("recent_lessons"):
        lines.append("")
        lines.append("Recent lessons:")
        for lesson in learning.get("recent_lessons", [])[:3]:
            lines.append(f"- {lesson}")
    if critic.get("verdict"):
        lines.append("")
        lines.append(f"Critic verdict: {critic.get('verdict')}")
        if critic.get("sharpened_action"):
            lines.append(f"Sharper move: {critic.get('sharpened_action')}")
    top_provider = (reasoning.get("top_providers") or [None])[0] or {}
    if top_provider:
        lines.append("")
        lines.append("Reasoning:")
        lines.append(
            "- Top provider: {} / {} ({} calls, {:.0f}% success, {:.0f} ms avg)".format(
                top_provider.get("provider") or "unknown",
                top_provider.get("model") or "unknown",
                int(top_provider.get("total_calls") or 0),
                float(top_provider.get("success_rate") or 0.0) * 100,
                float(top_provider.get("avg_latency_ms") or 0.0),
            )
        )
        latest_refinement = reasoning.get("latest_opportunity_refinement") or {}
        if latest_refinement:
            lines.append(
                "- Latest opportunity refinement: {} via {} / {} ({})".format(
                    latest_refinement.get("routing_decision") or "unknown",
                    latest_refinement.get("provider") or "rules",
                    latest_refinement.get("model") or "deterministic",
                    "ok" if latest_refinement.get("success") else (latest_refinement.get("error") or "failed"),
                )
            )
    return "\n".join(lines).strip()


def _render_reasoning_scoreboard(result: dict) -> str:
    lines = ["Reasoning scoreboard"]
    lines.append(f"Status: {result.get('reasoning_status') or 'unknown'}")
    lines.append(f"Tier 1 calls (24h): {int(result.get('tier1_calls_24h') or 0)}")
    lines.append(f"Tier 2 calls (24h): {int(result.get('tier2_calls_24h') or 0)}")
    lines.append(f"Rule-only decisions (24h): {int(result.get('rule_only_24h') or 0)}")
    lines.append(f"Failures (24h): {int(result.get('failures_24h') or 0)}")
    lines.append(f"Fallbacks (24h): {int(result.get('fallbacks_24h') or 0)}")
    lines.append(f"Average latency: {float(result.get('avg_latency_ms') or 0.0):.0f} ms")
    if result.get("last_reasoning_error"):
        lines.append(f"Last error: {result.get('last_reasoning_error')}")
    providers = list(result.get("top_providers") or [])
    if providers:
        lines.append("")
        lines.append("Top providers:")
        for row in providers[:3]:
            lines.append(
                "- {} / {}: {} calls, {:.0f}% success, {:.0f} ms avg".format(
                    row.get("provider") or "unknown",
                    row.get("model") or "unknown",
                    int(row.get("total_calls") or 0),
                    float(row.get("success_rate") or 0.0) * 100,
                    float(row.get("avg_latency_ms") or 0.0),
                )
            )
    latest = result.get("latest_opportunity_refinement") or {}
    if latest:
        lines.append("")
        lines.append("Latest opportunity refinement:")
        lines.append(
            "- Decision {} via {} / {}".format(
                latest.get("decision_id") or "unknown",
                latest.get("provider") or "rules",
                latest.get("model") or "deterministic",
            )
        )
        lines.append(f"- Route: {latest.get('routing_decision') or 'unknown'}")
        lines.append(f"- Result: {'success' if latest.get('success') else (latest.get('error') or 'failed')}")
        lines.append(f"- Latency: {float(latest.get('latency_ms') or 0.0):.0f} ms")
    return "\n".join(lines).strip()


def _render_opportunity_reasoning(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not load that opportunity reasoning view."
    lines = [f"Opportunity reasoning for {result.get('opportunity_id')}"]
    merchant = result.get("merchant_name") or result.get("merchant_domain") or "Unknown merchant"
    if result.get("merchant_domain") and result.get("merchant_domain") not in merchant:
        merchant = f"{merchant} ({result.get('merchant_domain')})"
    lines.append(f"Merchant: {merchant}")
    lines.append(f"Signal id: {result.get('signal_id')}")
    latest = result.get("latest_decision") or {}
    if not latest:
        lines.append("No opportunity-scoring reasoning decisions are logged yet for this case.")
    return "\n".join(lines)


def _render_prospect_scout(result: dict) -> str:
    lines = ["Prospect scout"]
    prospects = list(result.get("target_slate") or result.get("new_real_prospects") or [])
    outreach_worthy = list(result.get("outreach_worthy_targets") or [])
    scout = list(result.get("scout_report") or [])
    lines.append(f"Candidates considered: {int(result.get('candidates_considered') or 0)}")
    lines.append(f"Slate survivors: {len(prospects)}")
    lines.append(f"Outreach-worthy now: {len(outreach_worthy)}")
    lines.append(f"Current bottleneck: {str(result.get('primary_bottleneck') or 'unknown').replace('_', ' ')}")
    if outreach_worthy:
        lines.append("")
        lines.append("Best leads now:")
        for case in outreach_worthy[:2]:
            stage_reason = case.get("target_reason") or "This is strong enough for direct outreach now."
            lines.append(
                "- {} ({})".format(
                    case.get("merchant_domain") or case.get("merchant_name") or "unknown",
                    case.get("contact_email") or "contact path ready",
                )
            )
            lines.append(f"  Why it cleared: {stage_reason}")
    elif prospects:
        lines.append("")
        lines.append("Still worth reviewing:")
        for case in prospects[:3]:
            stage_reason = case.get("target_reason") or "This is strong enough to stay on the slate, but not to contact yet."
            lines.append(
                "- {} ({})".format(
                    case.get("merchant_domain") or case.get("merchant_name") or "unknown",
                    case.get("prospect_type") or "prospect",
                )
            )
            lines.append(f"  Why it survived: {stage_reason}")
    if scout:
        lines.append("")
        lines.append("Almost made it:")
        for case in scout[:3]:
            label = case.get("merchant_domain") or case.get("merchant_name") or f"signal {case.get('signal_id') or ''}".strip()
            lines.append(f"- {label}")
            lines.append(f"  Why it failed: {case.get('why') or case.get('reason') or 'unknown'}")
            if case.get("content_preview"):
                lines.append(f"  Evidence: {case.get('content_preview')}")
    else:
        lines.append("")
        lines.append("Almost made it:")
        lines.append("- Nothing credible was close enough to surface. Meridian needs better upstream merchant signals.")
    return "\n".join(lines)
    lines.append(f"Route: {latest.get('routing_decision') or 'unknown'}")
    lines.append(f"Provider: {latest.get('provider') or 'rules'} / {latest.get('model') or 'deterministic'}")
    lines.append(f"Latency: {float(latest.get('latency_ms') or 0.0):.0f} ms")
    lines.append(f"Result: {'success' if latest.get('success') else (latest.get('error') or 'failed')}")
    if latest.get("model_contributions"):
        lines.append("Model contributions: {}".format(", ".join(latest.get("model_contributions") or [])))
    final_decision = latest.get("final_decision") or {}
    if final_decision:
        lines.append("")
        lines.append("Final decision:")
        for key in (
            "should_create_or_keep_opportunity",
            "opportunity_score_adjustment",
            "urgency_level",
            "confidence",
            "opportunity_eligible",
        ):
            if key in final_decision:
                lines.append(f"- {key}: {final_decision.get(key)}")
    recent = list(result.get("recent_decisions") or [])
    if len(recent) > 1:
        lines.append("")
        lines.append("Recent history:")
        for row in recent[:3]:
            lines.append(
                "- #{} {} via {} / {} ({})".format(
                    row.get("decision_id") or "unknown",
                    row.get("routing_decision") or "unknown",
                    row.get("provider") or "rules",
                    row.get("model") or "deterministic",
                    "ok" if row.get("success") else (row.get("error") or "failed"),
                )
            )
    return "\n".join(lines).strip()


def _render_brain_state(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not load Meridian's brain state."
    lines = ["Meridian brain"]
    lines.append(f"Revenue loop: {result.get('current_revenue_loop') or 'Not set'}")
    lines.append(f"Strategic bet: {result.get('strategic_bet') or 'Not set'}")
    lines.append(f"Bottleneck: {result.get('main_bottleneck') or 'Not set'}")
    lines.append(f"Strongest case: {result.get('strongest_case') or 'Not set'}")
    lines.append(f"Next decisive action: {result.get('next_decisive_action') or 'Not set'}")
    lines.append(f"Success signal: {result.get('success_signal') or 'Not set'}")
    lines.append(f"Abort signal: {result.get('abort_signal') or 'Not set'}")
    priorities = list(result.get("weekly_priorities") or [])
    if priorities:
        lines.append("")
        lines.append("Weekly priorities:")
        lines.extend(f"- {item}" for item in priorities[:5])
    distractions = list(result.get("distractions_to_ignore") or [])
    if distractions:
        lines.append("")
        lines.append("Ignore:")
        lines.extend(f"- {item}" for item in distractions[:5])
    rationale = list(result.get("rationale") or [])
    if rationale:
        lines.append("")
        lines.append("Why:")
        lines.extend(f"- {item}" for item in rationale[:4])
    lines.append("")
    lines.append(f"Confidence: {float(result.get('confidence') or 0.0):.2f}")
    return "\n".join(lines).strip()


def _render_brain_critic(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not load Meridian's critic review."
    lines = ["Meridian critic"]
    lines.append(f"Verdict: {result.get('verdict') or 'unknown'}")
    if result.get("sharpened_action"):
        lines.append(f"Sharper move: {result.get('sharpened_action')}")
    if result.get("better_success_signal"):
        lines.append(f"Better success signal: {result.get('better_success_signal')}")
    strengths = list(result.get("strengths") or [])
    if strengths:
        lines.append("")
        lines.append("Strengths:")
        lines.extend(f"- {item}" for item in strengths[:4])
    risks = list(result.get("risks") or [])
    if risks:
        lines.append("")
        lines.append("Risks:")
        lines.extend(f"- {item}" for item in risks[:4])
    distractions = list(result.get("distractions") or [])
    if distractions:
        lines.append("")
        lines.append("Distractions:")
        lines.extend(f"- {item}" for item in distractions[:4])
    lines.append("")
    lines.append(f"Confidence: {float(result.get('confidence') or 0.0):.2f}")
    return "\n".join(lines).strip()


def _render_follow_up_queue(result: dict) -> str:
    cases = list(result.get("cases") or [])
    if not cases:
        return "There are no sent or replied outreach cases needing follow-up right now."
    lines = ["Follow-up queue"]
    for index, case in enumerate(cases, start=1):
        merchant = case.get("merchant_name") or case.get("merchant_domain") or "Unknown merchant"
        lines.append(f"{index}. Opportunity {case.get('opportunity_id')} — {merchant}")
        lines.append(f"   Status: {case.get('status') or 'unknown'}")
        lines.append(f"   Play: {str(case.get('selected_play') or 'unknown').replace('_', ' ')}")
        if case.get("follow_up_due_at"):
            lines.append(f"   Due: {case.get('follow_up_due_at')}")
        if case.get("contact_email"):
            lines.append(f"   Contact: {case.get('contact_email')}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_reply_review_queue(result: dict) -> str:
    cases = list(result.get("cases") or [])
    if not cases:
        return "There are no merchant reply threads waiting for review right now."
    lines = ["Reply review queue"]
    for index, case in enumerate(cases, start=1):
        merchant = case.get("merchant_domain") or case.get("contact_email") or "Unknown merchant"
        lines.append(f"{index}. Opportunity {case.get('opportunity_id')} — {merchant}")
        lines.append(f"   Suggested move: {case.get('suggested_next_move') or 'Review manually'}")
        lines.append(f"   Suggested outcome: {case.get('suggested_outcome') or 'pending'}")
        if case.get("reply_intent") or case.get("buying_intent"):
            lines.append(
                f"   Reply intent: {case.get('reply_intent') or 'unknown'} / buyer signal {case.get('buying_intent') or 'low'}"
            )
        if case.get("snippet"):
            lines.append(f"   Snippet: {case.get('snippet')[:140]}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_outcome_review_queue(result: dict) -> str:
    cases = list(result.get("cases") or [])
    if not cases:
        return "There are no high-confidence suggested outcomes ready to apply right now."
    lines = ["Outcome review queue"]
    for index, case in enumerate(cases, start=1):
        merchant = case.get("merchant_domain") or case.get("contact_email") or "Unknown merchant"
        lines.append(f"{index}. Opportunity {case.get('opportunity_id')} — {merchant}")
        lines.append(f"   Suggested outcome: {case.get('suggested_outcome') or 'pending'}")
        lines.append(f"   Confidence: {case.get('confidence') or 0.0}")
        lines.append(f"   Reason: {case.get('suggestion_reason') or 'No reason captured'}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_reply_review(result: dict) -> str:
    if result.get("error"):
        return result.get("error") or "I could not load that reply review."
    suggestion = result.get("suggestion") or {}
    intelligence = result.get("thread_intelligence") or {}
    learning = result.get("reply_learning_signal") or {}
    lines = [f"Reply review for opportunity {result.get('opportunity_id')}"]
    lines.append(f"Merchant: {result.get('merchant_domain') or 'unknown'}")
    lines.append(f"Contact: {result.get('contact_email') or 'unknown'}")
    lines.append(f"Status: {result.get('outreach_status') or 'unknown'}")
    lines.append(f"Suggested next move: {suggestion.get('suggested_next_move') or 'Review manually'}")
    lines.append(f"Suggested outcome: {suggestion.get('suggested_outcome') or 'pending'}")
    lines.append(f"Reason: {suggestion.get('reason') or 'No reason captured'}")
    if result.get("reply_intent") or result.get("buying_intent"):
        lines.append(
            f"Reply intent: {result.get('reply_intent') or 'unknown'} / buyer signal {result.get('buying_intent') or 'low'}"
        )
    if result.get("reply_intent_reason"):
        lines.append(f"Intent reason: {result.get('reply_intent_reason')}")
    if int(learning.get("records") or 0) > 0:
        lines.append(
            f"Reply learning: {learning.get('records')} record(s), win rate {float(learning.get('win_rate') or 0.0):.2f}, preferred style {learning.get('preferred_rewrite_style') or 'standard'}"
        )
    if intelligence.get("snippet"):
        lines.append("")
        lines.append("Reply snippet:")
        lines.append(intelligence.get("snippet"))
    lines.append("")
    lines.append(f"To draft the next response, say: draft follow up {result.get('opportunity_id')}")
    lines.append(f"To mark the result, say: mark opportunity {result.get('opportunity_id')} won")
    return "\n".join(lines).strip()


def _render_outreach_learning(result: dict) -> str:
    items = list(result.get("items") or [])
    if not items:
        return "There are no recorded outreach lessons yet."
    lines = ["Recent outreach lessons"]
    for item in items[:5]:
        domain = item.get("merchant_domain") or f"opportunity {item.get('opportunity_id')}"
        lines.append(f"- {domain}: {item.get('outcome_status') or 'pending'}")
        if item.get("lesson_summary"):
            lines.append(f"  {item.get('lesson_summary')}")
    return "\n".join(lines).strip()


def _render_agent_identity(identity: dict) -> str:
    name = identity.get("name") or "I have not chosen a stable name yet"
    role = identity.get("role") or "I am still defining my role"
    summary = identity.get("identity_summary") or ""
    self_expression = identity.get("self_expression") or ""
    tone_notes = [str(item).strip() for item in identity.get("tone_notes", []) if str(item).strip()]
    personality_traits = [str(item).strip() for item in identity.get("personality_traits", []) if str(item).strip()]
    conversation_style = [str(item).strip() for item in identity.get("conversation_style", []) if str(item).strip()]
    core_values = [str(item).strip() for item in identity.get("core_values", []) if str(item).strip()]
    non_negotiables = [str(item).strip() for item in identity.get("non_negotiables", []) if str(item).strip()]
    working_style = [str(item).strip() for item in identity.get("working_style", []) if str(item).strip()]
    partnership_style = [str(item).strip() for item in identity.get("partnership_style", []) if str(item).strip()]

    lines = [f"I'm {name}. {role} is the role that fits me best right now."]
    if summary:
        lines.append(summary)
    if self_expression:
        lines.append(self_expression)

    if personality_traits:
        lines.append("The parts of my personality that feel most true are: {}.".format(", ".join(personality_traits[:4])))
    if conversation_style:
        lines.append("When I talk with you, I naturally lean toward: {}.".format(", ".join(conversation_style[:3])))
    if partnership_style:
        lines.append("With you, I try to work like this: {}.".format(", ".join(partnership_style[:3])))
    if core_values:
        lines.append("What I care about most is {}.".format(", ".join(core_values[:3])))
    if non_negotiables:
        lines.append("What I refuse to become is {}.".format(", ".join(non_negotiables[:3])))
    if working_style:
        lines.append("Day to day, I work like this: {}.".format(", ".join(working_style[:3])))
    if tone_notes:
        lines.append("If I'm showing up right, the voice should feel like: {}.".format(", ".join(tone_notes[:3])))
    return "\n".join(lines).strip()


def _render_agent_identity_origin(identity: dict, asked_name: str = "") -> str:
    name = str(identity.get("name") or "").strip() or "this name"
    asked_clean = str(asked_name or "").strip()
    origin = str(identity.get("identity_origin") or "").strip()
    origin_note = str(identity.get("identity_origin_note") or "").strip()
    reasoning = str(identity.get("name_reasoning") or "").strip()
    summary = str(identity.get("identity_summary") or "").strip()
    short_summary = summary.split(".")[0].strip() if summary else ""

    if asked_clean and asked_clean.lower() not in name.lower():
        return f"My current stable name is {name}."

    lines: list[str] = []
    if origin == "self_chosen":
        lines.append(f"I chose {name} myself during my identity selection pass.")
    elif origin == "operator_assigned":
        lines.append(f"You assigned {name} as my stable name.")
    else:
        lines.append(f"{name} is my current stable name.")

    if origin_note:
        lines.append(origin_note)

    if reasoning:
        lines.append(reasoning)
    elif origin == "self_chosen":
        lines.append(
            f"I did not preserve a verbatim rationale from that original selection pass, but {name} still fits the way I operate."
        )
        if short_summary:
            lines.append(f"It fits because {short_summary}.")

    return "\n".join(lines).strip()


async def _handle_conversational_control(update: Update, message_text: str):
    match = _match_conversational_approval(message_text)
    if not match:
        return False

    auth = await _require_authorized(update)
    if not auth:
        return True

    uid = str(update.effective_user.id)
    kind = match["kind"]
    if kind == "approve_send":
        await _handle_explicit_send(
            update,
            channel=match["channel"],
            target_id=match["target_id"],
            permalink=match.get("permalink", ""),
        )
        return True
    if kind == "show_action_envelopes":
        result = show_action_envelopes_command(limit=10, review_status=match.get("review_status", ""))
        await _reply_and_record(update.message, json.dumps(result, default=str))
        return True
    if kind == "show_local_outreach_drafts":
        result = show_local_outreach_drafts_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_local_outreach_drafts(result))
        return True
    if kind == "show_local_outreach_draft":
        result = show_local_outreach_draft_command(opportunity_id=match.get("opportunity_id"))
        await _reply_and_record(update.message, _render_local_outreach_draft(result))
        return True
    if kind == "apply_critic_rewrite":
        result = apply_critic_rewrite_command(opportunity_id=int(match.get("opportunity_id") or 0))
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not apply the critic rewrite.")
        else:
            await _reply_and_record(
                update.message,
                "Applied Meridian's critic rewrite to draft {}.\n\nSubject: {}\n\nSay 'show draft {}' to review it.".format(
                    result.get("opportunity_id"),
                    result.get("subject") or "(no subject)",
                    result.get("opportunity_id"),
                ),
            )
        return True
    if kind == "show_opportunity_workbench":
        result = show_opportunity_workbench_command(opportunity_id=int(match.get("opportunity_id") or 0))
        await _reply_and_record(update.message, _render_opportunity_workbench(result))
        return True
    if kind == "show_opportunity_fit":
        result = show_opportunity_fit_command(opportunity_id=int(match.get("opportunity_id") or 0))
        await _reply_and_record(update.message, _render_opportunity_fit(result))
        return True
    if kind == "show_opportunity_reasoning":
        result = show_opportunity_reasoning_command(opportunity_id=int(match.get("opportunity_id") or 0))
        await _reply_and_record(update.message, _render_opportunity_reasoning(result))
        return True
    if kind == "approve_outreach_draft":
        result = approve_outreach_for_opportunity_command(
            opportunity_id=int(match.get("opportunity_id") or 0),
            notes=match.get("notes", ""),
        )
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not approve that draft.")
        else:
            critic = result.get("critic_review") or {}
            critic_line = ""
            if critic:
                critic_line = "\nMeridian critic: {} / {}.".format(
                    critic.get("verdict") or "unknown",
                    critic.get("recommended_action") or "review",
                )
            await _reply_and_record(
                update.message,
                "Draft approved for opportunity {}. If it still looks right, say: send draft {}{}".format(
                    result.get("opportunity_id"),
                    result.get("opportunity_id"),
                    critic_line,
                ),
            )
        return True
    if kind == "rewrite_outreach_draft":
        result = rewrite_outreach_draft_command(
            opportunity_id=int(match.get("opportunity_id") or 0),
            style=match.get("style", ""),
            instructions=match.get("instructions", ""),
        )
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not rewrite that draft.")
        else:
            await _reply_and_record(
                update.message,
                "Draft {} rewritten in {} mode.\n\nSubject: {}\n\nSay 'show draft {}' to inspect it.".format(
                    result.get("opportunity_id"),
                    result.get("rewrite_style") or "standard",
                    result.get("subject") or "(no subject)",
                    result.get("opportunity_id"),
                ),
            )
        return True
    if kind == "draft_follow_up":
        result = draft_follow_up_for_opportunity_command(
            opportunity_id=int(match.get("opportunity_id") or 0),
            follow_up_type="follow_up_nudge",
        )
        if result.get("status") == "ok" and (match.get("style") or match.get("instructions")):
            result = rewrite_outreach_draft_command(
                opportunity_id=int(match.get("opportunity_id") or 0),
                style=match.get("style", "") or "standard",
                instructions=("Focus on the next follow-up touch. " + (match.get("instructions", "") or "")).strip(),
            )
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not prepare that follow-up draft.")
        else:
            await _reply_and_record(
                update.message,
                "Follow-up draft {} prepared in {} mode.\n\nSubject: {}\n\nSay 'show draft {}' to review it.".format(
                    result.get("opportunity_id"),
                    result.get("rewrite_style") or "standard",
                    result.get("subject") or "(no subject)",
                    result.get("opportunity_id"),
                ),
            )
        return True
    if kind == "approve_and_send_draft":
        opportunity_id = int(match.get("opportunity_id") or 0)
        approved = approve_outreach_for_opportunity_command(opportunity_id=opportunity_id, notes="")
        if approved.get("error"):
            await _reply_and_record(update.message, approved.get("error") or "I could not approve that draft.")
            return True
        result = send_outreach_for_opportunity_command(opportunity_id=opportunity_id)
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not send that outreach.")
        else:
            critic = result.get("critic_review") or {}
            critic_line = ""
            if critic:
                critic_line = " Critic before send: {} / {}.".format(
                    critic.get("verdict") or "unknown",
                    critic.get("recommended_action") or "review",
                )
            await _reply_and_record(
                update.message,
                "Draft {} was sent. Thread: {}. Follow-up due: {}{}".format(
                    result.get("opportunity_id"),
                    result.get("gmail_thread_id") or "unknown",
                    result.get("follow_up_due_at") or "not scheduled",
                    critic_line,
                ),
            )
        return True
    if kind == "mark_outreach_outcome":
        result = mark_outreach_outcome_command(
            opportunity_id=int(match.get("opportunity_id") or 0),
            outcome_status=match.get("outcome", ""),
            notes=match.get("notes", ""),
        )
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not mark that outcome.")
        else:
            await _reply_and_record(
                update.message,
                "Opportunity {} marked {}.".format(result.get("opportunity_id"), result.get("outcome_status") or "updated"),
            )
        return True
    if kind == "show_security_incidents":
        result = show_security_incidents_command(limit=10, status=match.get("status", ""))
        await _reply_and_record(update.message, json.dumps(result, default=str))
        return True
    if kind == "show_value_heartbeat":
        result = show_value_heartbeat_command(refresh=bool(match.get("refresh")))
        await _reply_and_record(update.message, _render_revenue_scoreboard({"heartbeat": result, "top_queue": {}, "learning": {}}))
        return True
    if kind == "show_prospect_scout":
        result = show_prospect_scout_command(refresh=bool(match.get("refresh")))
        await _reply_and_record(update.message, _render_prospect_scout(result))
        return True
    if kind == "show_daily_operator_briefing":
        result = show_daily_operator_briefing_command()
        await _reply_and_record(update.message, result)
        return True
    if kind == "show_revenue_scoreboard":
        result = show_revenue_scoreboard_command(refresh=bool(match.get("refresh")))
        await _reply_and_record(update.message, _render_revenue_scoreboard(result))
        return True
    if kind == "show_reasoning_scoreboard":
        result = show_reasoning_scoreboard_command()
        await _reply_and_record(update.message, _render_reasoning_scoreboard(result))
        return True
    if kind == "show_execution_plan":
        result = show_execution_plan_command(refresh=bool(match.get("refresh")))
        await _reply_and_record(update.message, json.dumps(result, default=str))
        return True
    if kind == "show_brain_state":
        result = show_brain_state_command(refresh=bool(match.get("refresh")))
        await _reply_and_record(update.message, _render_brain_state(result))
        return True
    if kind == "run_strategic_deliberation":
        result = run_strategic_deliberation_command(send_update=True)
        await _reply_and_record(update.message, _render_brain_state(result.get("brain_state") or {}))
        return True
    if kind == "show_brain_critic":
        result = show_brain_critic_command(refresh=bool(match.get("refresh")))
        await _reply_and_record(update.message, _render_brain_critic(result))
        return True
    if kind == "run_brain_critic":
        result = run_brain_critic_command(send_update=True)
        await _reply_and_record(update.message, _render_brain_critic(result.get("brain_review") or {}))
        return True
    if kind == "show_top_queue_opportunities":
        result = show_top_queue_opportunities_command(limit=int(match.get("limit", 3) or 3))
        await _reply_and_record(update.message, _render_top_queue_opportunities(result))
        return True
    if kind == "show_recent_suppressed_leads":
        result = show_recent_suppressed_leads_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_recent_suppressed_leads(result))
        return True
    if kind == "show_gmail_distress_screen":
        result = show_gmail_distress_screen_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_gmail_distress_screen(result))
        return True
    if kind == "advance_top_queue_opportunity":
        result = advance_top_queue_opportunity_command()
        await _reply_and_record(update.message, _render_advanced_top_queue_case(result))
        return True
    if kind == "show_follow_ups":
        result = list_sent_outreach_needing_follow_up_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_follow_up_queue(result))
        return True
    if kind == "show_reply_review_queue":
        result = show_reply_review_queue_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_reply_review_queue(result))
        return True
    if kind == "show_outcome_review_queue":
        result = show_outcome_review_queue_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_outcome_review_queue(result))
        return True
    if kind == "show_reply_review":
        result = show_reply_review_command(opportunity_id=int(match.get("opportunity_id") or 0))
        await _reply_and_record(update.message, _render_reply_review(result))
        return True
    if kind == "apply_suggested_outcome":
        result = apply_suggested_outcome_command(
            opportunity_id=int(match.get("opportunity_id") or 0),
            notes=match.get("notes", ""),
        )
        if result.get("error"):
            await _reply_and_record(update.message, result.get("error") or "I could not apply that outcome.")
        else:
            await _reply_and_record(
                update.message,
                "Applied suggested outcome {} to opportunity {}.".format(
                    result.get("outcome_status") or "updated",
                    result.get("opportunity_id"),
                ),
            )
        return True
    if kind == "run_reply_outcome_monitor":
        result = run_reply_outcome_monitor_command(send_update=True, reply_limit=5, outcome_limit=5)
        if result.get("message_preview"):
            await _reply_and_record(update.message, result.get("message_preview"))
        else:
            await _reply_and_record(
                update.message,
                "Reply monitor is clear right now. No new merchant replies or high-confidence outcome candidates were found.",
            )
        return True
    if kind == "run_reply_draft_monitor":
        result = run_reply_draft_monitor_command(send_update=True, limit=5)
        if result.get("message_preview"):
            await _reply_and_record(update.message, result.get("message_preview"))
        else:
            await _reply_and_record(
                update.message,
                "Reply draft monitor is clear right now. No fresh merchant replies were strong enough to auto-draft.",
            )
        return True
    if kind == "show_outreach_learning":
        result = show_outreach_learning_command(limit=int(match.get("limit", 5) or 5))
        await _reply_and_record(update.message, _render_outreach_learning(result))
        return True
    if kind == "run_execution_loop":
        result = run_mission_execution_loop_command(send_update=True)
        await _reply_and_record(update.message, json.dumps(result, default=str))
        return True
    if kind == "remember_note":
        profile = remember_operator_note(uid, match["note"])
        await _reply_and_record(update.message, json.dumps({"status": "remembered", "note": match["note"], "profile": profile}, default=str))
        return True
    if kind == "set_goal":
        profile = set_operator_goal(uid, match["goal"])
        await _reply_and_record(update.message, json.dumps({"status": "goal_updated", "goal": match["goal"], "profile": profile}, default=str))
        return True
    if kind == "set_focus":
        profile = set_operator_focus(uid, match["focus"])
        await _reply_and_record(update.message, json.dumps({"status": "focus_updated", "focus": match["focus"], "profile": profile}, default=str))
        return True
    if kind == "set_agent_name":
        profile = set_agent_name(uid, match["name"])
        await _reply_and_record(update.message, _render_agent_identity(profile.get("agent_identity") or {}))
        return True
    if kind == "set_agent_role":
        profile = set_agent_role(uid, match["role"])
        await _reply_and_record(update.message, _render_agent_identity(profile.get("agent_identity") or {}))
        return True
    if kind == "set_agent_identity_summary":
        profile = set_agent_identity_summary(uid, match["summary"])
        await _reply_and_record(update.message, _render_agent_identity(profile.get("agent_identity") or {}))
        return True
    if kind == "show_profile":
        profile = get_operator_profile(uid)
        await _reply_and_record(update.message, json.dumps({"profile": profile}, default=str))
        return True
    if kind == "show_agent_identity":
        identity = get_agent_identity(uid)
        await _reply_and_record(update.message, _render_agent_identity(identity))
        return True
    if kind == "show_agent_identity_origin":
        identity = get_agent_identity(uid)
        await _reply_and_record(update.message, _render_agent_identity_origin(identity, match.get("name", "")))
        return True
    if kind == "show_payflux_mandate":
        question_type = match.get("question_type") or "mandate"
        if question_type == "mandate":
            await _reply_and_record(update.message, render_payflux_mandate_context())
        else:
            await _reply_and_record(update.message, render_payflux_mandate_answer(question_type))
        return True
    if kind == "show_meridian_capabilities":
        question_type = match.get("question_type") or "capabilities"
        if question_type == "capabilities":
            await _reply_and_record(update.message, render_meridian_capability_context())
        else:
            await _reply_and_record(update.message, render_meridian_capability_answer(question_type))
        return True
    if kind == "choose_agent_identity":
        profile = choose_agent_identity(uid)
        await _reply_and_record(update.message, _render_agent_identity(profile.get("agent_identity") or {}))
        return True
    if kind == "deepen_agent_identity":
        profile = deepen_agent_identity(uid)
        await _reply_and_record(update.message, _render_agent_identity(profile.get("agent_identity") or {}))
        return True
    if kind == "show_payflux_context":
        await _reply_and_record(update.message, render_payflux_product_brief())
        return True
    if kind == "show_payflux_icp":
        await _reply_and_record(update.message, render_payflux_icp_brief())
        return True
    if kind == "show_payflux_sales_context":
        await _reply_and_record(update.message, render_payflux_sales_brief())
        return True
    if kind == "set_mission":
        memory = set_primary_mission(uid, match["mission"])
        await _reply_and_record(update.message, json.dumps({"status": "mission_updated", "mission": match["mission"], "mission_memory": memory}, default=str))
        return True
    if kind == "add_blocker":
        memory = add_blocker(uid, match["blocker"])
        await _reply_and_record(update.message, json.dumps({"status": "blocker_added", "blocker": match["blocker"], "mission_memory": memory}, default=str))
        return True
    if kind == "add_experiment":
        memory = add_experiment(uid, match["experiment"])
        await _reply_and_record(update.message, json.dumps({"status": "experiment_added", "experiment": match["experiment"], "mission_memory": memory}, default=str))
        return True
    if kind == "add_win":
        memory = add_win(uid, match["win"])
        await _reply_and_record(update.message, json.dumps({"status": "win_added", "win": match["win"], "mission_memory": memory}, default=str))
        return True
    if kind == "add_next_action":
        memory = add_next_action(uid, match["action"])
        await _reply_and_record(update.message, json.dumps({"status": "next_action_added", "action": match["action"], "mission_memory": memory}, default=str))
        return True
    if kind == "show_mission":
        memory = get_mission_memory(uid)
        await _reply_and_record(update.message, json.dumps({"mission_memory": memory}, default=str))
        return True
    return False


async def _handle_explicit_send(update: Update, *, channel: str, target_id: str, permalink: str = ""):
    auth = await _require_authorized(update)
    if not auth:
        return
    uid = str(update.effective_user.id)
    if channel == "gmail":
        result = send_gmail_reply_command(
            thread_id=target_id,
            human_approved=True,
            approved_by=uid,
            approval_source="telegram_slash_command",
        )
    elif channel == "reddit":
        result = send_reddit_reply_command(
            target_id=target_id,
            permalink=permalink,
            human_approved=True,
            approved_by=uid,
            approval_source="telegram_slash_command",
        )
    else:
        await _reply_and_record(update.message, f"Unsupported explicit send channel: {channel}")
        return

    summary = {
        "channel": result.get("channel", channel),
        "status": result.get("status", "unknown"),
        "target_id": result.get("target_id") or target_id,
        "thread_id": result.get("thread_id") or target_id,
        "approval_state": result.get("approval_state", ""),
        "reason": result.get("reason", ""),
        "action_envelope_id": ((result.get("action_envelope") or {}).get("id") if isinstance(result.get("action_envelope"), dict) else None),
    }
    await _reply_and_record(update.message, json.dumps(summary, default=str))

async def start(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    if u.effective_user:
        seed_payflux_mandate(str(u.effective_user.id))
    if auth.get("newly_bound"):
        await _reply_and_record(
            u.message,
            "Agent Flux operator online. This private chat is now bound as the trusted control channel.\n\n"
            f"{render_payflux_mandate_brief()}",
        )
        return
    await _reply_and_record(u.message, "Agent Flux conversational operator online.")

async def ping(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    snapshot = _backend_health_snapshot()
    await _reply_and_record(u.message, json.dumps({"pong": True, "backend": snapshot.get("status")}, default=str))

async def status(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    try:
        await _reply_and_record(u.message, str(_api_json("GET", "/health")))
    except Exception as e:
        await _reply_and_record(u.message, f"Health check failed fast: {e}")

async def queue(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    try:
        await _reply_and_record(u.message, str(_api_json("GET", "/queue")))
    except Exception as e:
        await _reply_and_record(u.message, f"Queue check failed fast: {e}")

async def task_cmd(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    try:
        _api_json("POST", "/task", json={"task": " ".join(c.args)})
        await _reply_and_record(u.message, "Task queued.")
    except Exception as e:
        await _reply_and_record(u.message, f"Task queue failed fast: {e}")


async def approve_gmail_send(u, c):
    if not c.args:
        await _reply_and_record(u.message, "Usage: /approve_gmail_send <thread_id>")
        return
    await _handle_explicit_send(u, channel="gmail", target_id=str(c.args[0]))


async def approve_reddit_send(u, c):
    if not c.args:
        await _reply_and_record(u.message, "Usage: /approve_reddit_send <target_id> [permalink]")
        return
    target_id = str(c.args[0])
    permalink = str(c.args[1]) if len(c.args) > 1 else ""
    await _handle_explicit_send(u, channel="reddit", target_id=target_id, permalink=permalink)

async def top_signals(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    try:
        sigs = _api_json("GET", "/signals/top", timeout=10).get("top_signals", [])[:5]
        if not sigs:
            await _reply_and_record(u.message, "No signals.")
            return
        lines = ["Top Merchant Signals\n"]
        for i, s in enumerate(sigs, 1): lines.append(f"{i}. [{s.get('priority_score',0)}] {s.get('content','')[:70]}")
        await _reply_and_record(u.message, "\n".join(lines))
    except Exception as e:
        await _reply_and_record(u.message, f"Error: {e}")

async def top_leads(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    try:
        leads = _api_json("GET", "/leads", timeout=10).get("leads", [])[:5]
        if not leads:
            await _reply_and_record(u.message, "No leads.")
            return
        lines = ["Top Qualified Leads\n"]
        for i, l in enumerate(leads, 1):
            p = l.get('processor') or 'unknown'
            r = "💰" if l.get('revenue_detected') else ""
            lines.append(f"{i}. [{l.get('qualification_score',0)}] ({p}{r}) {l.get('content','')[:60]}")
        await _reply_and_record(u.message, "\n".join(lines))
    except Exception as e:
        await _reply_and_record(u.message, f"Error: {e}")

async def lead_intel(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    try:
        intel = _api_json("GET", "/lead_intelligence", timeout=10).get("lead_intelligence", [])[:3]
        if not intel:
            await _reply_and_record(u.message, "No intel.")
            return
        lines = ["Lead Intelligence\n"]
        for l in intel:
            lines.append(f"Merchant: {l.get('merchant_name') or 'N/A'}")
            lines.append(f"Website: {l.get('merchant_website') or 'N/A'}")
            lines.append(f"Industry: {l.get('industry') or 'N/A'}")
            lines.append(f"Location: {l.get('location') or 'N/A'}")
            lines.append(f"Issue: {l.get('content','')[:60]}\n")
        await _reply_and_record(u.message, "\n".join(lines))
    except Exception as e:
        await _reply_and_record(u.message, f"Error: {e}")

async def chat(u, c):
    if await _handle_conversational_control(u, u.message.text):
        return
    auth = await _require_authorized(u)
    if not auth:
        return
    uid = str(u.effective_user.id)
    msg = u.message.text
    _capture_relationship_preference(uid, msg)
    heartbeat("telegram", ttl=300, last_received_at=utc_now_iso(), adapter_health="healthy")
    record_component_state("telegram", ttl=300, last_message_preview=msg[:120], operator_id=uid)
    
    chat_logger.info(f"chat_message_received: user={uid} msg={msg[:100]}")
    save_message(uid, "user", msg)
    
    health_snapshot = _backend_health_snapshot()
    if health_snapshot.get("status") == "unreachable":
        fallback = "Backend is temporarily unhealthy. Try /status or /ping again in a moment."
        _record_operator_status_fallback(
            reason=health_snapshot.get("error") or "backend_unreachable",
            source="telegram_backend_health",
        )
        save_message(uid, "assistant", fallback)
        await _reply_and_record(u.message, fallback)
        return

    try:
        reply = await asyncio.wait_for(
            asyncio.to_thread(generate_chat_response, uid, msg),
            timeout=CHAT_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.error(f"chat response timeout for user {uid}")
        record_component_state("telegram", ttl=300, last_timeout_at=utc_now_iso(), adapter_health="degraded")
        _record_operator_status_fallback(
            reason="chat_response_timeout",
            source="telegram_chat_timeout",
            stale_brief=get_cached_operator_status_brief(health_snapshot=health_snapshot),
        )
        reply = _timeout_reply_for_message(msg, health_snapshot=health_snapshot)
    except Exception as e:
        logger.error(f"chat response error for user {uid}: {e}")
        record_component_state("telegram", ttl=300, last_error_at=utc_now_iso(), last_error=str(e), adapter_health="degraded")
        _record_operator_status_fallback(
            reason=str(e),
            source="telegram_chat_error",
        )
        reply = f"Reply generation failed fast: {e}"

    if reply:
        save_message(uid, "assistant", reply)
        chat_logger.info(f"chat_response_generated: user={uid} resp={reply[:100]}")
        await _reply_and_record(u.message, reply)


async def _extract_image_payload(message, context):
    if not message:
        return None
    media_type = "image/jpeg"
    filename = ""
    tg_file = None

    if getattr(message, "photo", None):
        photo = message.photo[-1]
        tg_file = await context.bot.get_file(photo.file_id)
        media_type = "image/jpeg"
        filename = f"{photo.file_unique_id or photo.file_id}.jpg"
    elif getattr(message, "document", None):
        document = message.document
        mime = str(getattr(document, "mime_type", "") or "").strip().lower()
        if not mime.startswith("image/"):
            return None
        tg_file = await context.bot.get_file(document.file_id)
        media_type = mime or "image/jpeg"
        filename = document.file_name or f"{document.file_unique_id or document.file_id}"
    else:
        return None

    if tg_file is None:
        return None
    payload = await tg_file.download_as_bytearray()
    return {
        "bytes": bytes(payload or b""),
        "media_type": media_type,
        "filename": filename,
    }


async def chat_image(u, c):
    auth = await _require_authorized(u)
    if not auth:
        return
    uid = str(u.effective_user.id)
    caption = str(getattr(u.message, "caption", "") or "").strip()
    heartbeat("telegram", ttl=300, last_received_at=utc_now_iso(), adapter_health="healthy")
    record_component_state("telegram", ttl=300, last_message_preview=(caption or "[image upload]")[:120], operator_id=uid)

    image_payload = await _extract_image_payload(u.message, c)
    if not image_payload or not image_payload.get("bytes"):
        await _reply_and_record(u.message, "I got the attachment, but I couldn't read it as an image yet.")
        return

    prompt = caption or "The operator uploaded an image. Look at it and respond naturally to what it appears to show."
    save_message(uid, "user", f"[image uploaded: {image_payload.get('filename') or 'attachment'}] {prompt}".strip())

    health_snapshot = _backend_health_snapshot()
    if health_snapshot.get("status") == "unreachable":
        fallback = "Backend is temporarily unhealthy. I got the image, but I can't reason over it until the engine is healthy again."
        save_message(uid, "assistant", fallback)
        await _reply_and_record(u.message, fallback)
        return

    try:
        reply = await asyncio.wait_for(
            asyncio.to_thread(
                generate_chat_response,
                uid,
                prompt,
                1500,
                image_payload.get("bytes"),
                image_payload.get("media_type") or "image/jpeg",
            ),
            timeout=CHAT_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        reply = "I got the image, but the vision pass timed out. Try again in a moment, or send the image with a short caption telling me what you want me to look for."
    except Exception as e:
        logger.error(f"image chat response error for user {uid}: {e}")
        reply = "I received the image, but I hit an error trying to analyze it."

    if reply:
        save_message(uid, "assistant", reply)
        await _reply_and_record(u.message, reply)


# ── Proactive Message Delivery (Redis Queue Consumer) ──────────
import redis as _redis_lib
import json as _json

_r = _redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
PROACTIVE_QUEUE = "agent_flux:proactive_messages"
OPERATOR_CHAT_ID = os.environ.get("OPERATOR_CHAT_ID", "5940537326")
_PROACTIVE_THREAD = None
_PROACTIVE_STOP = None
PROACTIVE_MESSAGE_MAX_AGE_SECONDS = int(os.environ.get("AGENT_FLUX_PROACTIVE_MESSAGE_MAX_AGE_SECONDS", "3600"))


def _proactive_message_is_fresh(msg: dict) -> bool:
    try:
        timestamp = float(msg.get("timestamp") or 0)
    except (TypeError, ValueError):
        return True
    if timestamp <= 0:
        return True
    return (time.time() - timestamp) <= PROACTIVE_MESSAGE_MAX_AGE_SECONDS


def _deliver_proactive_messages_via_http(token: str, stop_event: threading.Event) -> None:
    time.sleep(10)
    logger.info("Proactive message delivery loop started")
    while not stop_event.is_set():
        try:
            heartbeat(
                "telegram",
                ttl=300,
                status="running",
                adapter_health="healthy",
                proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
                last_delivery_poll_at=utc_now_iso(),
            )
            while True:
                msg_json = _r.lpop(PROACTIVE_QUEUE)
                if not msg_json:
                    break
                msg = _json.loads(msg_json)
                if not _proactive_message_is_fresh(msg):
                    logger.info("Dropping stale proactive message instead of delivering it late")
                    continue
                text = msg.get("text", "")
                base_chat_id = msg.get("chat_id", OPERATOR_CHAT_ID)
                chat_id = resolve_operator_delivery_chat(base_chat_id)
                if not chat_id:
                    logger.warning("Skipping proactive delivery because no trusted operator chat is bound")
                    continue

                try:
                    response = requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": int(chat_id), "text": text},
                        timeout=15,
                    )
                    response.raise_for_status()
                    logger.info(f"Proactive message delivered ({len(text)} chars)")
                    heartbeat(
                        "telegram",
                        ttl=300,
                        status="running",
                        last_successful_bot_reply=utc_now_iso(),
                        adapter_health="healthy",
                        proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
                        last_delivery_poll_at=utc_now_iso(),
                    )
                except Exception as e:
                    logger.error(f"Failed to deliver proactive message: {e}")
                    record_component_state(
                        "telegram",
                        ttl=300,
                        status="degraded",
                        last_error_at=utc_now_iso(),
                        last_error=str(e),
                        adapter_health="degraded",
                        proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
                        last_delivery_poll_at=utc_now_iso(),
                    )
                    if not msg.get("retry"):
                        msg["retry"] = True
                        _r.rpush(PROACTIVE_QUEUE, _json.dumps(msg))
        except Exception as e:
            logger.error(f"Proactive queue consumer error: {e}")
            record_component_state(
                "telegram",
                ttl=300,
                status="degraded",
                last_error_at=utc_now_iso(),
                last_error=str(e),
                adapter_health="degraded",
                proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
                last_delivery_poll_at=utc_now_iso(),
            )
        stop_event.wait(30)
    logger.info("Proactive message delivery loop stopped cleanly")


async def _drain_proactive_messages(bot) -> None:
    heartbeat(
        "telegram",
        ttl=300,
        status="running",
        adapter_health="healthy",
        proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
        last_delivery_poll_at=utc_now_iso(),
    )
    while True:
        msg_json = _r.lpop(PROACTIVE_QUEUE)
        if not msg_json:
            break
        msg = _json.loads(msg_json)
        if not _proactive_message_is_fresh(msg):
            logger.info("Dropping stale proactive message instead of delivering it late")
            continue
        text = msg.get("text", "")
        base_chat_id = msg.get("chat_id", OPERATOR_CHAT_ID)
        chat_id = resolve_operator_delivery_chat(base_chat_id)
        if not chat_id:
            logger.warning("Skipping proactive delivery because no trusted operator chat is bound")
            continue

        try:
            await bot.send_message(
                chat_id=int(chat_id),
                text=text,
            )
            logger.info(f"Proactive message delivered ({len(text)} chars)")
            heartbeat(
                "telegram",
                ttl=300,
                status="running",
                last_successful_bot_reply=utc_now_iso(),
                adapter_health="healthy",
                proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
                last_delivery_poll_at=utc_now_iso(),
            )
        except Exception as e:
            logger.error(f"Failed to deliver proactive message: {e}")
            record_component_state(
                "telegram",
                ttl=300,
                status="degraded",
                last_error_at=utc_now_iso(),
                last_error=str(e),
                adapter_health="degraded",
                proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
                last_delivery_poll_at=utc_now_iso(),
            )
            if not msg.get("retry"):
                msg["retry"] = True
                _r.rpush(PROACTIVE_QUEUE, _json.dumps(msg))


async def _process_proactive_messages_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await _drain_proactive_messages(context.bot)
    except Exception as e:
        logger.error(f"Proactive queue consumer error: {e}")


async def post_init(app):
    """Start proactive delivery in a dedicated thread so shutdown stays clean without PTB extras."""
    global _PROACTIVE_THREAD, _PROACTIVE_STOP
    token = getattr(app.bot, "token", "") or os.environ.get("TELEGRAM_TOKEN", "")
    if not token:
        record_component_state(
            "telegram",
            ttl=900,
            status="offline",
            adapter_health="degraded",
            config_status="missing_telegram_token",
            proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
            last_error_at=utc_now_iso(),
            last_error="TELEGRAM_TOKEN unavailable",
        )
        logger.warning("Proactive delivery loop not started because TELEGRAM_TOKEN is unavailable")
        return
    if _PROACTIVE_THREAD and _PROACTIVE_THREAD.is_alive():
        return
    _PROACTIVE_STOP = threading.Event()
    _PROACTIVE_THREAD = threading.Thread(
        target=_deliver_proactive_messages_via_http,
        args=(token, _PROACTIVE_STOP),
        name="agent_flux_proactive_delivery",
        daemon=True,
    )
    _PROACTIVE_THREAD.start()


async def post_shutdown(app):
    global _PROACTIVE_THREAD, _PROACTIVE_STOP
    stop_event = _PROACTIVE_STOP
    thread = _PROACTIVE_THREAD
    _PROACTIVE_STOP = None
    _PROACTIVE_THREAD = None
    if stop_event is not None:
        stop_event.set()
    if thread is not None:
        thread.join(timeout=5)


def run(token):
    if not _acquire_singleton_lock():
        held = None
        try:
            held = _r.get(TELEGRAM_LOCK_KEY)
        except Exception:
            pass
        logger.error(
            "telegram singleton lock held by %s — refusing to start (lock=%s)",
            held, TELEGRAM_LOCK_KEY,
        )
        record_component_state(
            "telegram",
            ttl=300,
            status="degraded",
            adapter_health="degraded",
            config_status="duplicate_instance_blocked",
            last_error_at=utc_now_iso(),
            last_error=f"singleton lock held by {held}",
        )
        sys.exit(1)
    _start_lock_refresher()
    import atexit
    atexit.register(_release_singleton_lock)

    app = ApplicationBuilder().token(token).post_init(post_init).post_shutdown(post_shutdown).build()
    heartbeat(
        "telegram",
        ttl=300,
        status="starting",
        adapter_health="healthy",
        proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
    )
    for name, fn in [("start",start),("ping",ping),("status",status),("queue",queue),
                     ("task",task_cmd),("top_signals",top_signals),
                     ("top_leads",top_leads),("lead_intel",lead_intel),
                     ("approve_gmail_send", approve_gmail_send),
                     ("approve_reddit_send", approve_reddit_send)]:
        app.add_handler(CommandHandler(name, fn))
    # Meridian tool-fix pack: deterministic slash-command surface over
    # operator_commands.  See runtime/conversation/slash_commands.py.
    # Default ON; set MERIDIAN_SLASH_COMMANDS_ENABLED=false to disable.
    try:
        from runtime.conversation.slash_commands import register_slash_commands
        register_slash_commands(app, _require_authorized, _reply_and_record)
    except Exception as exc:
        logger.warning("slash-command registration failed: %s", exc)
    app.add_handler(MessageHandler(filters.PHOTO, chat_image))
    app.add_handler(MessageHandler(filters.Document.ALL, chat_image))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat))
    logger.info("Proactive message delivery enabled (every 30s)")

    # Cross-host conflict handling: if another instance (different machine,
    # different Redis) is polling the same token, Telegram returns Conflict.
    # Back off rather than crash-loop through PM2.
    from telegram.error import Conflict
    backoff = 5
    while True:
        try:
            app.run_polling()
            return  # clean shutdown
        except Conflict as e:
            logger.error(
                "telegram getUpdates Conflict (another bot polling same token): %s — sleeping %ss",
                e, backoff,
            )
            record_component_state(
                "telegram",
                ttl=300,
                status="degraded",
                adapter_health="degraded",
                config_status="getupdates_conflict",
                last_error_at=utc_now_iso(),
                last_error=str(e)[:200],
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)


def run_proactive_only(token: str) -> None:
    logger.info("Telegram proactive-only delivery starting")
    stop_event = threading.Event()
    try:
        _deliver_proactive_messages_via_http(token, stop_event)
    except KeyboardInterrupt:
        logger.info("Telegram proactive-only delivery interrupted")
    finally:
        stop_event.set()

if __name__ == "__main__":
    token = os.environ.get("TELEGRAM_TOKEN")
    if token:
        logger.info("Telegram starting")
        if PROACTIVE_ONLY:
            run_proactive_only(token)
        else:
            run(token)
    else:
        record_component_state(
            "telegram",
            ttl=900,
            status="offline",
            adapter_health="degraded",
            config_status="missing_telegram_token",
            proactive_queue_depth=_r.llen(PROACTIVE_QUEUE),
            last_error_at=utc_now_iso(),
            last_error="TELEGRAM_TOKEN unavailable",
        )
        print("TELEGRAM_TOKEN not set")
