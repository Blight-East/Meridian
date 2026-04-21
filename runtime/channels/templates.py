from __future__ import annotations

import re

from runtime.channels.base import ChannelTarget, DraftAction
from runtime.reasoning.control_plane import run_reasoning_task


DISTRESS_KEYWORDS = {
    "stripe froze": 0.26,
    "stripe froze my account": 0.34,
    "stripe hold": 0.22,
    "funds held": 0.2,
    "held funds": 0.2,
    "reserve increase": 0.18,
    "reserve": 0.12,
    "payout delayed": 0.18,
    "payout": 0.12,
    "payouts": 0.12,
    "underwriting review": 0.18,
    "underwriting": 0.1,
    "processor alternative": 0.16,
    "looking for a processor": 0.28,
    "alternative to stripe": 0.24,
    "alternative to paypal": 0.22,
    "payment processor": 0.14,
    "processor change": 0.14,
    "account closed": 0.18,
    "account terminated": 0.18,
}

HOSTILE_PATTERNS = (
    "idiot",
    "scam",
    "fraud",
    "shill",
    "ban this",
    "mod removed",
    "you people",
)

BLOCKED_REPLY_PATTERNS = (
    "guaranteed approval",
    "instant approval",
    "no risk",
    "dm me",
    "direct message me",
)


def normalized_text(*parts: str) -> str:
    joined = " ".join(part.strip() for part in parts if part).lower()
    return re.sub(r"\s+", " ", joined).strip()


def confidence_score(text: str, merchant_fit: float = 0.0) -> float:
    lowered = normalized_text(text)
    score = 0.18 + min(max(merchant_fit, 0.0), 0.25)
    for phrase, weight in DISTRESS_KEYWORDS.items():
        if phrase in lowered:
            score += weight
    if "stripe" in lowered or "paypal" in lowered or "square" in lowered:
        score += 0.08
    if "merchant" in lowered or "store" in lowered or "business" in lowered:
        score += 0.06
    if "reddit" in lowered or "gmail" in lowered:
        score -= 0.03
    return round(max(0.0, min(score, 0.99)), 2)


def detect_hostility(text: str) -> bool:
    lowered = normalized_text(text)
    return any(pattern in lowered for pattern in HOSTILE_PATTERNS)


def blocked_phrase_hits(text: str) -> list[str]:
    lowered = normalized_text(text)
    return [pattern for pattern in BLOCKED_REPLY_PATTERNS if pattern in lowered]


def build_reddit_draft(target: ChannelTarget) -> DraftAction:
    body = normalized_text(target.title, target.body)
    confidence = confidence_score(body, merchant_fit=0.18)
    rationale = (
        "High-intent processor-distress language detected in an allowlist candidate thread. "
        "Draft stays educational and avoids unverifiable claims."
    )
    text = (
        "Sorry you’re dealing with that. If payouts were frozen or reserves changed suddenly, "
        "the safest next step is usually to document the timeline, pull every processor notice, "
        "and compare alternatives against your chargeback profile and business model before moving. "
        "If it helps, I can share a short checklist for evaluating Stripe/PayPal alternatives without guessing."
    )
    return DraftAction(
        channel="reddit",
        action_type="reply",
        target_id=target.target_id,
        thread_id=target.thread_id,
        text=text,
        rationale=rationale,
        confidence=confidence,
        metadata={
            "subreddit": target.metadata.get("subreddit", ""),
            "permalink": target.metadata.get("permalink", ""),
            "hostile": detect_hostility(body),
            "blocked_phrase_hits": blocked_phrase_hits(text),
        },
    )


def build_gmail_reply_deterministic(target: ChannelTarget) -> DraftAction:
    body = normalized_text(target.title, target.body)
    intelligence = target.metadata.get("gmail_thread_intelligence", {}) or {}
    processor = intelligence.get("processor", "unknown").replace("_", " ")
    distress_type = intelligence.get("distress_type") or "unknown"
    reason_codes = list((intelligence.get("metadata") or {}).get("reason_codes", []))
    confidence = max(float(intelligence.get("confidence", 0.0) or 0.0), confidence_score(body, merchant_fit=0.32))
    sender_name = target.metadata.get("sender_name") or target.author or "there"
    subject = target.metadata.get("reply_subject") or f"Re: {target.title or 'Merchant follow-up'}"
    issue_label = {
        "account_frozen": "the account freeze",
        "payouts_delayed": "the payout delays",
        "reserve_hold": "the reserve hold",
        "verification_review": "the review process",
        "processor_switch_intent": "the processor search",
        "chargeback_issue": "the dispute pressure",
        "account_terminated": "the account termination",
        "onboarding_rejected": "the onboarding rejection",
    }.get(distress_type, "the payment issue")
    processor_phrase = f" with {processor.title()}" if processor != "unknown" else ""
    rationale = (
        "Merchant-distress triage matched a processor issue and flagged the thread as reply-worthy. "
        "Draft stays concise, acknowledges the problem, and asks two qualification questions max."
    )
    text = (
        f"Hi {sender_name},\n\n"
        f"Sorry you are dealing with {issue_label}{processor_phrase}. From what you shared, "
        "it sounds like the fastest next step is to get clear on what changed and how broadly it is affecting payouts or processing.\n\n"
        "PayFlux is built to help merchants see whether that pressure is getting worse and what to do next before it turns into a bigger cash-flow problem.\n\n"
        "Two quick questions so I do not guess:\n"
        "1. Which processor are you on right now, and what changed most recently?\n"
        "2. Are payouts paused, reserves increased, or is the account fully under review/disabled?\n\n"
        "If helpful, I can send back a short next-step outline for your case without overpromising anything. If you want a lighter first step, there is also a free snapshot before live monitoring.\n\n"
        "Best,\nPayFlux"
    )
    return DraftAction(
        channel="gmail",
        action_type="reply",
        target_id=target.target_id,
        thread_id=target.thread_id,
        subject=subject,
        text=text,
        rationale=rationale,
        confidence=confidence,
        metadata={
            "recipient": target.metadata.get("reply_to") or target.metadata.get("sender_email", ""),
            "blocked_phrase_hits": blocked_phrase_hits(text),
            "do_not_contact": target.metadata.get("do_not_contact", False),
            "thread_category": intelligence.get("thread_category", "customer_support"),
            "processor": intelligence.get("processor", "unknown"),
            "distress_type": distress_type,
            "reply_priority": intelligence.get("reply_priority", "none"),
            "test_thread": intelligence.get("test_thread", False),
            "reason_codes": reason_codes,
        },
    )


def build_gmail_reply(target: ChannelTarget) -> DraftAction:
    intelligence = target.metadata.get("gmail_thread_intelligence", {}) or {}
    reason_codes = list((intelligence.get("metadata") or {}).get("reason_codes", []))
    confidence = max(
        float(intelligence.get("confidence", 0.0) or 0.0),
        confidence_score(normalized_text(target.title, target.body), merchant_fit=0.32),
    )
    draft = build_gmail_reply_deterministic(target)
    if intelligence.get("thread_category") == "merchant_distress":
        refinement = run_reasoning_task(
            "reply_draft_refinement",
            {
                "thread_id": target.thread_id,
                "subject": target.title or "",
                "body": (target.body or "")[:2500],
                "draft_subject": draft.subject,
                "draft_body": draft.text,
                "sender_email": target.metadata.get("sender_email", ""),
            },
            {
                "approved_for_draft": True,
                "draft_style": "merchant_distress_concise",
                "recommended_subject": draft.subject,
                "recommended_body": draft.text,
                "questions_count": draft.text.count("?"),
                "reason_codes": reason_codes,
                "confidence": confidence,
            },
            context={
                "item_type": "gmail_thread",
                "item_id": target.thread_id,
                "channel": "gmail",
                "thread_category": intelligence.get("thread_category"),
                "deterministic_confidence": float(intelligence.get("confidence", confidence) or confidence),
                "reply_priority": intelligence.get("reply_priority"),
                "opportunity_impact_possible": bool(intelligence.get("opportunity_eligible")),
                "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
                "test_thread": bool(intelligence.get("test_thread")),
            },
        )
        decision = refinement["final_decision"]
        if decision.get("approved_for_draft"):
            draft.subject = decision.get("recommended_subject") or draft.subject
            draft.text = decision.get("recommended_body") or draft.text
            draft.confidence = max(draft.confidence, float(decision.get("confidence", 0.0) or 0.0))
        draft.metadata["reasoning_decision_id"] = refinement.get("decision_id")
        draft.metadata["reasoning_route"] = refinement.get("routing", {}).get("routing_decision")
        draft.metadata["reasoning_reason_codes"] = list(decision.get("reason_codes", []))
    return draft
