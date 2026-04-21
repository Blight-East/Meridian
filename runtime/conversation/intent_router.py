"""
Hybrid Intent Router for Meridian.

Three-tier classification:
  Tier 1 — Deterministic regex / exact-match (fast, handles high-risk + unambiguous commands)
  Tier 2 — Model-based structured classification (Haiku-class, read-only intents)
  Tier 3 — Returns intent="unknown" so the caller can fall through to the full LLM path

Usage:
    from runtime.conversation.intent_router import classify_intent
    result = classify_intent(message)
    if result.intent != "unknown":
        # dispatch deterministic handler for result.intent
    else:
        # fall through to full LLM reasoning
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field

import requests

from config.logging_config import get_logger
from runtime.health.telemetry import record_component_state, utc_now_iso

logger = get_logger("intent_router")

# ---------------------------------------------------------------------------
# Router result
# ---------------------------------------------------------------------------

@dataclass
class IntentResult:
    intent: str                             # classified intent name or "unknown"
    confidence: float = 1.0                 # 0.0 – 1.0
    tier: str = "unknown"                   # "deterministic", "model", "unknown"
    intent_category: str = ""               # status, knowledge, pipeline, etc.
    params: dict = field(default_factory=dict)
    context_tags: set = field(default_factory=set)
    needs_clarification: bool = False
    clarification_prompt: str = ""


# ---------------------------------------------------------------------------
# Intent taxonomy
# ---------------------------------------------------------------------------

# Intents that are safe for model-based classification (all read-only).
MODEL_ROUTED_INTENTS: dict[str, str] = {
    # Status & Context  →  category "status"
    "system_status": "status",
    "focus_recommendation": "status",
    "signal_activity": "status",
    "daily_briefing": "status",
    "check_in": "status",
    # Capability & Knowledge  →  category "knowledge"
    "tools": "knowledge",
    "can_do": "knowledge",
    "gaps": "knowledge",
    "audit": "knowledge",
    "payflux_product": "knowledge",
    "payflux_sales": "knowledge",
    "payflux_icp": "knowledge",
    "mandate": "knowledge",
    "purpose": "knowledge",
    # Pipeline & Outreach (read-only)  →  category "pipeline"
    "opportunity_summary": "pipeline",
    "pattern_summary": "pipeline",
    "actionable_patterns": "pipeline",
    "merchant_profiles": "pipeline",
    "conversion_intelligence": "pipeline",
    "best_conversion": "pipeline",
    "outreach_follow_up": "pipeline",
    "blocked_outreach": "pipeline",
    "send_eligible_outreach": "pipeline",
    "outreach_awaiting_approval": "pipeline",
    "model_performance": "pipeline",
    # Relationship / Identity  →  category "relationship"
    "friendship": "relationship",
    "identity_chat": "relationship",
    "support_request": "relationship",
    "explain_timeout": "relationship",
}

# These intents handled by Tier 2's post-dispatch but need special context
# to feed the conditional context loader.
_INTENT_CONTEXT_TAGS: dict[str, set[str]] = {
    "system_status": {"brain_state", "mission", "capability"},
    "focus_recommendation": {"brain_state", "mission", "capability"},
    "signal_activity": {"brain_state", "mission", "capability"},
    "daily_briefing": {"brain_state", "mission", "capability"},
    "check_in": {"brain_state", "mission"},
    "tools": {"capability"},
    "can_do": {"capability"},
    "gaps": {"capability"},
    "audit": {"capability"},
    "payflux_product": {"product"},
    "payflux_sales": {"sales", "product"},
    "payflux_icp": {"icp", "product"},
    "mandate": {"mandate", "mission"},
    "purpose": {"mandate", "mission"},
    "opportunity_summary": {"brain_state", "mission", "product", "sales", "icp"},
    "pattern_summary": {"brain_state", "mission", "capability"},
    "actionable_patterns": {"brain_state", "mission", "capability"},
    "merchant_profiles": {"brain_state", "mission", "capability"},
    "conversion_intelligence": {"brain_state", "mission", "product", "sales", "icp"},
    "best_conversion": {"brain_state", "mission", "product", "sales", "icp"},
    "outreach_follow_up": {"brain_state", "mission", "product", "sales", "icp"},
    "blocked_outreach": {"brain_state", "mission", "product", "sales", "icp"},
    "send_eligible_outreach": {"brain_state", "mission", "product", "sales", "icp"},
    "outreach_awaiting_approval": {"brain_state", "mission", "product", "sales", "icp"},
    "model_performance": {"brain_state", "mission", "capability"},
    "friendship": set(),
    "identity_chat": set(),
    "support_request": set(),
    "explain_timeout": set(),
    # Deterministic-only intents that also go through context tagging:
    "run_evaluation": {"brain_state", "mission", "capability"},
    "mark_opportunity_won": {"brain_state", "mission", "product", "sales", "icp"},
    "mark_opportunity_lost": {"brain_state", "mission", "product", "sales", "icp"},
    "mark_opportunity_ignored": {"brain_state", "mission", "product", "sales", "icp"},
    "mark_operator_action": {"brain_state", "mission", "product", "sales", "icp"},
    "outreach_recommendation": {"brain_state", "mission", "product", "sales", "icp"},
    "contact_intelligence": {"brain_state", "mission", "product", "sales", "icp"},
    "outreach_draft": {"brain_state", "mission", "product", "sales", "icp"},
    "outreach_approve": {"brain_state", "mission", "product", "sales", "icp"},
    "outreach_send": {"brain_state", "mission", "product", "sales", "icp"},
    "action_fit": {"brain_state", "mission", "product", "sales", "icp"},
    "selected_action_for_lead": {"brain_state", "mission", "product", "sales", "icp"},
    "operator_action_cases": {"brain_state", "mission", "product", "sales", "icp"},
    "merchant_action_cases": {"brain_state", "mission", "product", "sales", "icp"},
    "unselected_action_cases": {"brain_state", "mission", "product", "sales", "icp"},
    "merchant_unselected_action_cases": {"brain_state", "mission", "product", "sales", "icp"},
    "mismatched_action_cases": {"brain_state", "mission", "product", "sales", "icp"},
    "merchant_mismatched_action_cases": {"brain_state", "mission", "product", "sales", "icp"},
    "merchant_action_attention": {"brain_state", "mission", "product", "sales", "icp"},
    "help": {"capability"},
    "my_goal": {"mandate", "mission"},
    "our_goal": {"mandate", "mission"},
}

CONTEXT_BASE_TAGS = {"operator_profile", "communication"}

# ---------------------------------------------------------------------------
# Tier 1 — Deterministic classification
# ---------------------------------------------------------------------------

# Regex patterns migrated from operator_layer.INTENT_PATTERNS.
# These still run first so that unambiguous phrasings are fast and free.
_DETERMINISTIC_PATTERNS: dict[str, list[str]] = {
    "run_evaluation": [
        r"\brun\b.*\beval",
        r"\bevaluate\b.*\breason",
        r"\breasoning eval",
        r"\bmodel eval",
    ],
    "model_performance": [
        r"\bmodel performance\b",
        r"\breasoning metrics\b",
        r"\bhow are the models\b",
        r"\btier 1\b",
        r"\btier 2\b",
        r"\bprovider performance\b",
    ],
    "opportunity_summary": [
        r"\bopportunit",
        r"\bdeal summary\b",
        r"\bdeal pipeline\b",
        r"\bleads summary\b",
        r"\blook (over|at) the pipeline\b",
        r"\bwhere are we stuck (overall|in the pipeline)\b",
        r"\btell me where (we are|we're) stuck\b",
    ],
    "pattern_summary": [
        r"\bwhat patterns are forming\b",
        r"\bwhich distress patterns are rising\b",
        r"\bpattern(s)? are rising\b",
        r"\bprocessor spike\b",
        r"\bdistress patterns\b",
    ],
    "actionable_patterns": [
        r"\bwhat patterns are actionable\b",
        r"\bwhich patterns are actionable\b",
        r"\bactionable patterns\b",
    ],
    "merchant_profiles": [
        r"\bwhat merchants matter right now\b",
        r"\bstrongest merchant profiles\b",
        r"\bmerchant profiles\b",
        r"\bwhich merchants matter\b",
    ],
    "conversion_intelligence": [
        r"\bwhat is converting\b",
        r"\bwhat kinds of leads are actually converting\b",
        r"\bconversion intelligence\b",
        r"\bwhat converts\b",
    ],
    "best_conversion": [
        r"\bwhat has the best chance to convert\b",
        r"\bbest chance to convert\b",
        r"\bbest converting pattern\b",
    ],
    "focus_recommendation": [
        r"\bwhat should i focus on\b",
        r"\bwhere should i focus\b",
        r"\bwhat matters most right now\b",
        r"\bwhat should we do next\b",
        r"\bwhat do you think we should do next\b",
        r"\btalk to me about\b.*\bwhat you think we should do next\b",
        r"\bwhat do we do next\b",
        r"\bwhat's the next move\b",
        r"\bwhat is the next move\b",
    ],
    "outreach_recommendation": [
        r"\bwhat outreach should we send for this lead\b",
        r"\bwhat outreach should we send\b",
        r"\bwhich outreach should we send\b",
        r"\bwhy is this lead not send-ready\b",
    ],
    "contact_intelligence": [
        r"\bwhat contact do we have for this lead\b",
        r"\bwhat contact do we have\b",
        r"\bwhy is this lead blocked\b",
        r"\bwhat's the next contact move\b",
        r"\bwhat is the next contact move\b",
        r"\bbest next contact acquisition move\b",
        r"\bwhat role should we target for this case\b",
        r"\bwhat role should we target\b",
    ],
    "send_eligible_outreach": [
        r"\bshow send-eligible leads\b",
        r"\bshow me send-eligible leads\b",
        r"\bsend-eligible leads\b",
    ],
    "blocked_outreach": [
        r"\bshow blocked outreach leads\b",
        r"\bshow me blocked outreach leads\b",
        r"\bblocked outreach leads\b",
    ],
    "outreach_draft": [
        r"\bdraft outreach for this lead\b",
        r"\bdraft outreach\b",
    ],
    "outreach_approve": [
        r"\bapprove outreach for this lead\b",
        r"\bapprove outreach\b",
    ],
    "outreach_send": [
        r"\bsend outreach for this lead\b",
        r"\bsend outreach\b",
    ],
    "outreach_awaiting_approval": [
        r"\bshow leads awaiting outreach approval\b",
        r"\bleads awaiting outreach approval\b",
        r"\boutreach approval queue\b",
    ],
    "outreach_follow_up": [
        r"\bshow sent outreach needing follow up\b",
        r"\bshow sent outreach needing follow-up\b",
        r"\bwhat should we follow up on today\b",
        r"\bfollow up on today\b",
        r"\boutreach needing follow up\b",
    ],
    "action_fit": [
        r"\bwhat action fits this case\b",
        r"\bwhat action fits\b",
        r"\bwhat play fits this case\b",
        r"\brecommended action\b",
    ],
    "selected_action_for_lead": [
        r"\bwhat action is selected for this lead\b",
        r"\bwhat action is selected\b",
        r"\bselected action for this lead\b",
    ],
    "merchant_unselected_action_cases": [
        r"\bshow me merchants with no selected action\b",
        r"\bmerchants with no selected action\b",
    ],
    "merchant_mismatched_action_cases": [
        r"\bshow me merchants with action drift\b",
        r"\bmerchants with action drift\b",
    ],
    "merchant_action_attention": [
        r"\bwhat merchant actions need attention right now\b",
        r"\bmerchant actions need attention\b",
    ],
    "unselected_action_cases": [
        r"\bshow me leads with no selected action\b",
        r"\bleads with no selected action\b",
        r"\bopportunities with no selected action\b",
    ],
    "mismatched_action_cases": [
        r"\bshow me where the chosen action differs from the recommended one\b",
        r"\bchosen action differs\b",
        r"\baction differs from the recommended one\b",
        r"\bselected action differs\b",
    ],
    "mark_opportunity_won": [
        r"\bmark\b.*\bopportunit.*\bwon\b",
        r"\bmark this opportunity won\b",
        r"\bmark this outreach as won\b",
    ],
    "mark_opportunity_lost": [
        r"\bmark\b.*\bopportunit.*\blost\b",
        r"\bmark this opportunity lost\b",
        r"\bmark this outreach as lost\b",
    ],
    "mark_opportunity_ignored": [
        r"\bmark\b.*\bopportunit.*\bignored\b",
        r"\bmark this opportunity ignored\b",
        r"\bmark this outreach as ignored\b",
    ],
    "signal_activity": [
        r"\bsignal activity\b",
        r"\bgmail activity\b",
        r"\bmerchant distress\b",
        r"\bnew signals\b",
        r"\bwhat came in\b",
    ],
    "daily_briefing": [
        r"\bdaily briefing\b",
        r"\bbrief me\b",
        r"\bmorning briefing\b",
        r"\bevening wrap\b",
    ],
    "system_status": [
        r"\bsystem status\b",
        r"\boperator state\b",
        r"\bhealth\b",
        r"\bwhat'?s going on\b",
        r"\bstatus\b",
    ],
    "help": [
        r"\bhelp\b",
        r"\bwhat can you do\b",
        r"\bcommands\b",
    ],
    "check_in": [
        r"\bwhat are you doing\b",
        r"\bwhat about now\b",
        r"\bhow are you doing\b",
        r"\bhow are you\b",
        r"\bwhat are you up to\b",
        r"\bwhat's up\b",
        r"\bwhats up\b",
    ],
    "friendship": [
        r"\bwhat can i do to make you my friend\b",
        r"\bhow can i be your friend\b",
        r"\bare we friends\b",
        r"\bdo you like me\b",
        r"\bbe my friend\b",
    ],
    "identity_chat": [
        r"\btell me about yourself\b",
        r"\bwho are you really\b",
        r"\bwho are you\b",
        r"\bwhat are you like\b",
        r"\bwhat kind of person are you\b",
    ],
    "support_request": [
        r"\bwhat do you need from me\b",
        r"\bhow can i help you\b",
        r"\bhow do i help you\b",
        r"\bhow can i make you better\b",
    ],
    "explain_timeout": [
        r"\bwhy does it keep doing that\b",
        r"\bwhy do you keep doing that\b",
        r"\bwhy does that keep happening\b",
        r"\bwhy are you saying that\b",
        r"\bwhy are you acting like that\b",
    ],
}

# Priority ordering for deterministic pattern matching —
# mark intents first so they are not swallowed by looser patterns.
_DETERMINISTIC_PRIORITY = [
    "mark_opportunity_won",
    "mark_opportunity_lost",
    "mark_opportunity_ignored",
]


def _normalized_text(message: str) -> str:
    return f" {str(message or '').strip().lower()} "


def _message_tokens(message: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", str(message or "").lower()))


def _contains_any(text: str, phrases: tuple[str, ...] | list[str]) -> bool:
    return any(phrase in text for phrase in phrases)


def _has_any(tokens: set[str], *candidates: str) -> bool:
    return any(candidate in tokens for candidate in candidates)


def _has_all(tokens: set[str], *candidates: str) -> bool:
    return all(candidate in tokens for candidate in candidates)


def _extract_operator_action(message: str) -> str | None:
    normalized = str(message or "").strip().lower().replace("-", " ")
    patterns = {
        "urgent_processor_migration": r"\burgent processor migration\b|\bprocessor migration\b",
        "payout_acceleration": r"\bpayout acceleration\b",
        "reserve_negotiation": r"\breserve negotiation\b",
        "compliance_remediation": r"\bcompliance remediation\b",
        "chargeback_mitigation": r"\bchargeback mitigation\b",
        "onboarding_assistance": r"\bonboarding assistance\b",
    }
    for action, pattern in patterns.items():
        if re.search(pattern, normalized):
            return action
    return None


def _deterministic_classify(message: str) -> IntentResult | None:
    """Tier 1: fast regex / token-match classification.

    Returns an IntentResult for unambiguous phrasings, or None to proceed to Tier 2.
    """
    normalized = _normalized_text(message)
    tokens = _message_tokens(message)

    # ── Pre-checks for action-extraction intents (must run before regex scan) ──
    if "mark" in normalized and _extract_operator_action(normalized):
        return _make_deterministic("mark_operator_action")

    if (
        _extract_operator_action(normalized)
        and "merchant" in normalized
        and ("show me" in normalized or "merchants on" in normalized)
    ):
        return _make_deterministic("merchant_action_cases")

    if (
        _extract_operator_action(normalized)
        and ("show me all" in normalized or "cases" in normalized or "show all" in normalized)
    ):
        return _make_deterministic("operator_action_cases")

    # ── Semantic token-match logic (replaces _semantic_operator_intent) ──
    # ------ these token checks are fast and unambiguous enough to stay deterministic ------
    if (
        _has_any(tokens, "tool", "tools", "mcp")
        and _has_any(tokens, "have", "got", "use", "using", "show", "gave", "give", "new", "see")
    ) or _contains_any(
        normalized,
        (
            "what tools do you have",
            "what are your tools",
            "what tools did i give you",
            "did you get new tools",
            "i just gave you some new tools",
            "how do you use them",
            "how do you use your tools",
        ),
    ):
        return _make_deterministic("tools")

    if _contains_any(
        normalized,
        (
            "what can you do",
            "what can you already do",
            "what all can you do",
            "what are you able to do",
            "what are your capabilities",
            "show your capabilities",
            "what can you help with",
        ),
    ) or (
        _has_any(tokens, "capability", "capabilities")
        and _has_any(tokens, "what", "show", "your", "can", "do")
    ):
        return _make_deterministic("can_do")

    if _contains_any(
        normalized,
        (
            "what are you missing",
            "what do you need",
            "what are your gaps",
            "what do you still need",
            "what would make you better",
        ),
    ) or (
        _has_any(tokens, "missing", "gaps", "need", "lack")
        and _has_any(tokens, "you", "your")
    ):
        return _make_deterministic("gaps")

    if _contains_any(
        normalized,
        (
            "capability audit",
            "audit your capabilities",
            "how healthy are you",
            "what is your health",
            "what is your system health",
        ),
    ) or (
        _has_any(tokens, "audit", "health")
        and _has_any(tokens, "you", "your", "system", "capabilities")
    ):
        return _make_deterministic("audit")

    if _contains_any(
        normalized,
        (
            "who do we sell to",
            "who is the buyer",
            "what counts as a high conviction prospect",
            "what disqualifies a lead",
            "show icp",
            "ideal customer profile",
        ),
    ) or (
        _has_any(tokens, "icp", "buyer", "buyers", "prospect", "lead", "disqualifies", "disqualify")
        and _has_any(tokens, "who", "what", "show", "counts")
    ):
        return _make_deterministic("payflux_icp")

    if _contains_any(
        normalized,
        (
            "what is payflux",
            "what does payflux do",
            "what are we selling",
            "what is the payflux offer",
            "what do we actually sell",
            "what is the product",
        ),
    ) or (
        "payflux" in tokens and _has_any(tokens, "what", "does", "offer", "product", "is")
    ) or (
        _has_all(tokens, "sell", "what") and not _has_any(tokens, "who", "buyer", "buyers", "how")
    ):
        return _make_deterministic("payflux_product")

    if _contains_any(
        normalized,
        (
            "how should we sell payflux",
            "how do we handle objections",
            "what is our sales story",
            "how do you explain payflux",
            "show sales context",
            "show objection handling",
            "how should we pitch this",
        ),
    ) or (
        _has_any(tokens, "sales", "objections", "pitch")
        and _has_any(tokens, "how", "show", "our", "we")
    ) or (
        "sell" in tokens and _has_any(tokens, "how", "pitch", "objections", "story", "explain")
    ):
        return _make_deterministic("payflux_sales")

    if _contains_any(
        normalized,
        (
            "what is your mandate",
            "what is your purpose",
            "why do you exist",
            "what is your goal",
            "what is our goal",
            "what are we trying to do",
        ),
    ):
        if "our goal" in normalized:
            return _make_deterministic("our_goal")
        if _contains_any(normalized, ("purpose", "why do you exist")):
            return _make_deterministic("purpose")
        if "your goal" in normalized:
            return _make_deterministic("my_goal")
        return _make_deterministic("mandate")

    # ── Regex scan (ordered) ──
    ordered_intents = list(_DETERMINISTIC_PRIORITY) + [
        intent
        for intent in _DETERMINISTIC_PATTERNS
        if intent not in set(_DETERMINISTIC_PRIORITY)
    ]
    for intent in ordered_intents:
        patterns = _DETERMINISTIC_PATTERNS.get(intent, [])
        for pattern in patterns:
            if re.search(pattern, normalized):
                return _make_deterministic(intent)

    return None


def _make_deterministic(intent: str) -> IntentResult:
    category = MODEL_ROUTED_INTENTS.get(intent, "")
    if not category:
        # Intents only in deterministic tier (e.g., mark_operator_action)
        category = _guess_category(intent)
    return IntentResult(
        intent=intent,
        confidence=1.0,
        tier="deterministic",
        intent_category=category,
        context_tags=_context_tags_for_intent(intent),
    )


def _guess_category(intent: str) -> str:
    if "mark" in intent or "outreach" in intent or "action" in intent:
        return "pipeline"
    if intent in ("help", "run_evaluation"):
        return "status"
    return "pipeline"


# ---------------------------------------------------------------------------
# Tier 2 — Model-based classification
# ---------------------------------------------------------------------------

_CLASSIFICATION_PROMPT = """\
You are an intent classifier for a sales operator control interface called Meridian.
Given the operator's message, classify it into exactly one intent.

INTENT CATEGORIES:

STATUS: system_status (system health, what's happening), focus_recommendation (what to do next, where to focus), signal_activity (new signals, what came in), daily_briefing (catch me up, give me the rundown), check_in (how are you, what's up)
KNOWLEDGE: tools (what tools you have), can_do (what you can do), gaps (what you're missing), audit (health audit), payflux_product (what is payflux, what we sell), payflux_sales (how to pitch, objection handling), payflux_icp (who is the buyer, ideal customer), mandate (what is your mandate, why you exist), purpose (what is your purpose)
PIPELINE: opportunity_summary (broad pipeline review, opportunities, where we are stuck overall), pattern_summary (distress patterns, trends), actionable_patterns (actionable patterns), merchant_profiles (which merchants matter), conversion_intelligence (what is converting), best_conversion (best conversion), outreach_follow_up (who to follow up on), blocked_outreach (specifically blocked or not-send-ready outreach), send_eligible_outreach (send-eligible leads), outreach_awaiting_approval (leads awaiting approval), model_performance (model/reasoning performance)
RELATIONSHIP: friendship (friendship, trust), identity_chat (who are you, your deal), support_request (how to help), explain_timeout (why slow, why acting weird)
UNKNOWN: unknown — use when the message does not clearly match any category above, or is a complex multi-step request

Be conservative. If you are unsure, return "unknown" with low confidence.
If the message asks for a broad review of the pipeline, what is stuck overall, or where to focus because the queue is thin, prefer opportunity_summary or focus_recommendation over blocked_outreach.

Respond with JSON only:
{"intent": "...", "confidence": 0.0-1.0, "intent_category": "...", "reasoning": "..."}
"""

_MODEL_CLASSIFY_TIMEOUT_SECONDS = float(
    os.getenv("MERIDIAN_ROUTER_TIMEOUT_SECONDS", "3.0")
)
_CONFIDENCE_DISPATCH_THRESHOLD = 0.75
_CONFIDENCE_CLARIFY_THRESHOLD = 0.40


def _get_anthropic_api_key() -> str:
    try:
        from dotenv import dotenv_values

        env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
        env_file = dotenv_values(env_path)
        return str(env_file.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or "").strip()
    except Exception:
        return str(os.getenv("ANTHROPIC_API_KEY") or "").strip()


def _get_env_value(key: str, default: str = "") -> str:
    try:
        from dotenv import dotenv_values

        env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
        env_file = dotenv_values(env_path)
        return str(env_file.get(key) or os.getenv(key) or default).strip()
    except Exception:
        return str(os.getenv(key) or default).strip()


def _github_models_endpoint() -> str:
    return _get_env_value("GITHUB_MODELS_ENDPOINT", "https://models.github.ai/inference").rstrip("/")


def _get_github_models_api_key() -> str:
    return _get_env_value("GITHUB_TOKEN")


def _router_provider_order() -> list[str]:
    configured = _get_env_value("MERIDIAN_ROUTER_PROVIDER", "auto").strip().lower()
    if configured == "anthropic":
        return ["anthropic", "github"]
    if configured == "github":
        return ["github", "anthropic"]
    if _get_github_models_api_key():
        return ["github", "anthropic"]
    return ["anthropic", "github"]


def _anthropic_router_model_candidates() -> list[str]:
    configured = _get_env_value("MERIDIAN_ROUTER_MODEL", "")
    candidates = [
        configured,
        "claude-sonnet-4-6",
    ]
    ordered: list[str] = []
    for candidate in candidates:
        cleaned = str(candidate or "").strip()
        if cleaned and cleaned not in ordered:
            ordered.append(cleaned)
    return ordered


def _call_anthropic_classify(message: str) -> tuple[dict | None, str]:
    """Call Anthropic for intent classification.

    Returns ``(parsed_json, model_name)``. The default model is the repo's
    known-good Anthropic model unless ``MERIDIAN_ROUTER_MODEL`` overrides it.
    """
    api_key = _get_anthropic_api_key()
    if not api_key:
        return None, "none"
    for model in _anthropic_router_model_candidates():
        try:
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 120,
                    "system": _CLASSIFICATION_PROMPT,
                    "messages": [{"role": "user", "content": str(message)[:500]}],
                },
                timeout=_MODEL_CLASSIFY_TIMEOUT_SECONDS,
            )
            result = resp.json()
            if "error" in result:
                error_type = str((result.get("error") or {}).get("type") or "")
                logger.warning("Anthropic router classification failed (model=%s type=%s)", model, error_type or "unknown")
                continue
            for block in result.get("content", []):
                if block.get("type") == "text":
                    text = str(block.get("text") or "").strip()
                    if text:
                        return _parse_classification_json(text), model
        except Exception as exc:
            logger.warning("Anthropic router classification failed (model=%s): %s", model, exc)
    return None, "none"


def _call_github_classify(message: str) -> dict | None:
    """Fallback: GitHub Models gpt-4.1-mini classification."""
    api_key = _get_github_models_api_key()
    endpoint = _github_models_endpoint()
    if not api_key or not endpoint:
        return None
    for model in ("openai/gpt-4.1-mini", "gpt-4.1-mini"):
        try:
            resp = requests.post(
                f"{endpoint}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": _get_env_value("GITHUB_MODELS_API_VERSION", "2026-03-10"),
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": _CLASSIFICATION_PROMPT},
                        {"role": "user", "content": str(message)[:500]},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 120,
                    "stream": False,
                },
                timeout=_MODEL_CLASSIFY_TIMEOUT_SECONDS,
            )
            if resp.status_code >= 400:
                result = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                error_code = str((result.get("error") or {}).get("code") or "")
                if error_code in {"unknown_model", "unavailable_model"}:
                    continue
                return None
            result = resp.json()
            choices = result.get("choices") or []
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                if content:
                    return _parse_classification_json(str(content).strip())
        except Exception as exc:
            logger.warning("GitHub Models classification failed (model=%s): %s", model, exc)
    return None


def _parse_classification_json(text: str) -> dict | None:
    """Extract the classification JSON from model output."""
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting a JSON block from markdown fences
    match = re.search(r"\{[^}]+\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    return None


def _model_classify(message: str) -> IntentResult:
    """Tier 2: model-based classification into the fixed intent enum."""
    started = time.time()
    model_used = "none"

    parsed = None
    for provider in _router_provider_order():
        if provider == "github":
            parsed = _call_github_classify(message)
            if parsed:
                model_used = "github-gpt-4.1-mini"
                break
        else:
            parsed, anthropic_model = _call_anthropic_classify(message)
            if parsed:
                model_used = anthropic_model
                break

    elapsed_ms = round((time.time() - started) * 1000, 2)

    if not parsed:
        record_component_state(
            "intent_router",
            ttl=600,
            last_router_tier="model_failed",
            last_router_latency_ms=elapsed_ms,
            last_router_model=model_used,
            last_router_at=utc_now_iso(),
        )
        return IntentResult(intent="unknown", confidence=0.0, tier="unknown")

    raw_intent = str(parsed.get("intent") or "unknown").strip().lower()
    raw_confidence = max(0.0, min(1.0, float(parsed.get("confidence") or 0.0)))
    raw_category = str(parsed.get("intent_category") or "").strip().lower()

    # Validate the intent is in our known set
    if raw_intent not in MODEL_ROUTED_INTENTS and raw_intent != "unknown":
        logger.info(
            "Model returned unknown intent '%s' (confidence=%.2f, model=%s) — treating as unknown",
            raw_intent,
            raw_confidence,
            model_used,
        )
        raw_intent = "unknown"
        raw_confidence = 0.0

    # Build result
    needs_clarification = _CONFIDENCE_CLARIFY_THRESHOLD <= raw_confidence < _CONFIDENCE_DISPATCH_THRESHOLD
    clarification_prompt = ""
    if needs_clarification:
        clarification_prompt = _generate_clarification(raw_intent, raw_category)

    result = IntentResult(
        intent=raw_intent if raw_confidence >= _CONFIDENCE_CLARIFY_THRESHOLD else "unknown",
        confidence=raw_confidence,
        tier="model" if raw_confidence >= _CONFIDENCE_DISPATCH_THRESHOLD else "unknown",
        intent_category=raw_category or MODEL_ROUTED_INTENTS.get(raw_intent, ""),
        context_tags=_context_tags_for_intent(raw_intent) if raw_confidence >= _CONFIDENCE_DISPATCH_THRESHOLD else set(),
        needs_clarification=needs_clarification,
        clarification_prompt=clarification_prompt,
    )

    record_component_state(
        "intent_router",
        ttl=600,
        last_router_tier=result.tier,
        last_router_intent=result.intent,
        last_router_confidence=result.confidence,
        last_router_latency_ms=elapsed_ms,
        last_router_model=model_used,
        last_router_at=utc_now_iso(),
    )
    return result


def _generate_clarification(intent: str, category: str) -> str:
    """Generate a short clarification question for ambiguous intents."""
    clarifications = {
        "status": "I can show you the system status, the pipeline queue, or the revenue scoreboard. Which one?",
        "knowledge": "I can talk about tools, capabilities, the product, our ICP, or the sales playbook. What specifically?",
        "pipeline": "I can show opportunities, patterns, conversion data, or the outreach pipeline. Which area?",
        "relationship": "I'm here. Are you asking about how I work, what I need, or just checking in?",
    }
    return clarifications.get(category, f"I think you might mean {intent.replace('_', ' ')}. Is that right?")


# ---------------------------------------------------------------------------
# Context tag computation
# ---------------------------------------------------------------------------

def _context_tags_for_intent(intent: str) -> set[str]:
    """Compute which context blocks should be loaded for a given intent."""
    tags = set(CONTEXT_BASE_TAGS)
    extra = _INTENT_CONTEXT_TAGS.get(intent)
    if extra:
        tags.update(extra)
    return tags


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_intent(message: str) -> IntentResult:
    """Three-tier intent classification.

    Tier 1: Deterministic regex/exact-match (fast, high-risk + unambiguous commands)
    Tier 2: Model-based structured classification (read-only intents only)
    Tier 3: Returns IntentResult(intent="unknown") — caller falls through to full LLM

    Returns an IntentResult with the classified intent, confidence, tier, and
    the context_tags needed for conditional context loading.
    """
    started = time.time()

    # ── Tier 1: deterministic ──
    deterministic = _deterministic_classify(message)
    if deterministic:
        elapsed_ms = round((time.time() - started) * 1000, 2)
        record_component_state(
            "intent_router",
            ttl=600,
            last_router_tier="deterministic",
            last_router_intent=deterministic.intent,
            last_router_confidence=1.0,
            last_router_latency_ms=elapsed_ms,
            last_router_model="none",
            last_router_at=utc_now_iso(),
        )
        return deterministic

    # ── Tier 2: model-based ──
    model_result = _model_classify(message)
    if model_result.intent != "unknown" and model_result.tier == "model":
        return model_result

    # ── Tier 3: unknown — caller should fall through to full LLM ──
    # Still compute fallback context tags based on keywords for the LLM path.
    fallback_tags = _keyword_context_tags(message)
    model_result.context_tags = fallback_tags
    return model_result


def _keyword_context_tags(message: str) -> set[str]:
    """Fallback context tag extraction using keyword matching.

    Used when neither Tier 1 nor Tier 2 classify the message, so the LLM path
    still gets the right context blocks loaded.
    """
    tags = set(CONTEXT_BASE_TAGS)
    normalized = str(message or "").strip().lower()

    if any(phrase in normalized for phrase in ("payflux", "processor pressure", "held funds", "reserve pressure", "payout")):
        tags.add("product")
    if any(phrase in normalized for phrase in ("outreach", "reply", "follow up", "follow-up", "objection", "pitch", "sell")):
        tags.update({"sales", "product"})
    if any(phrase in normalized for phrase in ("icp", "buyer", "buyers", "prospect", "prospects", "lead", "leads", "merchant")):
        tags.add("icp")
    if any(phrase in normalized for phrase in ("purpose", "goal", "mission", "revenue", "mandate")):
        tags.update({"mandate", "mission"})
    if any(phrase in normalized for phrase in ("tool", "tools", "capabilities", "mcp", "health audit")):
        tags.add("capability")
    if any(phrase in normalized for phrase in ("status", "queue", "draft", "pipeline", "opportunity", "opportunities", "briefing")):
        tags.update({"brain_state", "mission"})

    # If no specific tags were added beyond base, load a reasonable fallback set.
    if len(tags) <= len(CONTEXT_BASE_TAGS):
        tags.update({"brain_state", "product"})

    return tags
