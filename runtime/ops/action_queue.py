"""
Action Queue — Ranked Executable Steps from Deal Lifecycle
============================================================
Replaces the string-based execution plan with a ranked queue of
structured ActionItem objects. Each item maps a deal's current stage
to a concrete next action with a priority score.

Auto-execution model:
- Everything auto-executes EXCEPT sending outreach to a merchant.
- The operator is the final gate: they review and confirm the send.
- Outcome capture, drafting, reply classification, follow-up scheduling,
  and all internal transitions happen autonomously.

Usage:
    from runtime.ops.action_queue import build_action_queue, render_action_queue
"""
from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field, asdict
from typing import Any

from runtime.ops.deal_lifecycle import (
    DealStage,
    list_active_deals,
)
from runtime.health.telemetry import record_component_state, utc_now_iso

logger = logging.getLogger("meridian.action_queue")


# ── Action Types ─────────────────────────────────────────────────────────────

class ActionType(str, enum.Enum):
    """Types of actions the system can take on a deal."""
    send_outreach = "send_outreach"             # requires operator sign-off
    review_draft = "review_draft"               # present draft to operator
    review_reply = "review_reply"               # present reply to operator
    draft_follow_up = "draft_follow_up"         # auto-draft
    draft_outreach = "draft_outreach"           # auto-draft
    review_opportunity = "review_opportunity"   # auto-process or surface
    capture_outcome = "capture_outcome"         # auto-capture
    qualify_lead = "qualify_lead"               # auto-qualify


# ── Stage → Action Mapping ───────────────────────────────────────────────────
# Core design decision: everything is auto-executable EXCEPT the final send.
# The operator just reviews and signs off.

_STAGE_ACTION_MAP: dict[DealStage, tuple[ActionType, bool]] = {
    # Stage                    → (ActionType,            requires_operator)
    DealStage.outreach_approved: (ActionType.send_outreach, True),     # operator must sign off
    DealStage.outreach_drafted:  (ActionType.review_draft,  True),     # operator reviews draft
    DealStage.reply_received:    (ActionType.review_reply,  True),     # operator reviews reply
    DealStage.follow_up_due:     (ActionType.draft_follow_up, False),  # auto-draft
    DealStage.outreach_sent:     (ActionType.capture_outcome, False),  # auto-check for outcomes
    DealStage.opportunity_created: (ActionType.draft_outreach, False), # auto-draft
    DealStage.lead_qualified:    (ActionType.review_opportunity, False),  # auto-process
    DealStage.signal_detected:   (ActionType.qualify_lead,  False),    # auto-qualify
}

_ACTION_REASON: dict[ActionType, str] = {
    ActionType.send_outreach: "Draft approved and ready — waiting for your sign-off to send.",
    ActionType.review_draft: "Outreach draft ready for review.",
    ActionType.review_reply: "Merchant replied — needs your attention.",
    ActionType.draft_follow_up: "Follow-up is due — auto-drafting.",
    ActionType.draft_outreach: "Opportunity is ready for outreach — auto-drafting.",
    ActionType.review_opportunity: "Qualified lead ready for processing.",
    ActionType.capture_outcome: "Checking for replies and outcomes.",
    ActionType.qualify_lead: "New signal — qualifying automatically.",
}


_HOT_STAGES = {
    DealStage.reply_received,
    DealStage.follow_up_due,
    DealStage.outreach_approved,
    DealStage.outreach_drafted,
    DealStage.outreach_sent,
}


# ── ActionItem ───────────────────────────────────────────────────────────────

@dataclass
class ActionItem:
    """A single executable action in the queue."""
    action_type: ActionType
    opportunity_id: int
    merchant_domain: str
    priority_score: float
    requires_operator: bool
    can_auto_execute: bool
    reason: str
    stage: str
    processor: str = "unknown"
    distress_type: str = "unknown"
    contact_email: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["action_type"] = self.action_type.value
        return result


# ── Queue Builder ────────────────────────────────────────────────────────────

def build_action_queue(*, limit: int = 10) -> list[ActionItem]:
    """
    Build a ranked list of executable actions from the deal lifecycle.

    Returns:
        Sorted list of ActionItems, highest priority first.
        Items that require operator attention come before auto-executable items
        at the same priority level.
    """
    deals = list_active_deals(limit=max(limit * 3, 50))
    items: list[ActionItem] = []

    for deal in deals:
        stage_str = deal.get("current_stage", "signal_detected")
        try:
            stage = DealStage(stage_str)
        except ValueError:
            continue

        mapping = _STAGE_ACTION_MAP.get(stage)
        if not mapping:
            continue

        if not _is_queue_actionable(deal, stage):
            continue

        action_type, requires_operator = mapping
        priority = float(deal.get("priority_score", 0) or 0)

        item = ActionItem(
            action_type=action_type,
            opportunity_id=int(deal.get("opportunity_id", 0)),
            merchant_domain=deal.get("merchant_domain", ""),
            priority_score=priority,
            requires_operator=requires_operator,
            can_auto_execute=not requires_operator,
            reason=_ACTION_REASON.get(action_type, ""),
            stage=stage.value,
            processor=deal.get("processor", "unknown"),
            distress_type=deal.get("distress_type", "unknown"),
            contact_email=deal.get("contact_email", ""),
            metadata={
                "icp_fit_score": deal.get("icp_fit_score", 0),
                "commercial_readiness_score": deal.get("commercial_readiness_score", 0),
                "contact_trust_score": deal.get("contact_trust_score", 0),
            },
        )
        items.append(item)

    # Sort: operator-required items first at same priority, then by priority desc
    items.sort(key=lambda i: (i.requires_operator, i.priority_score), reverse=True)

    # Record telemetry
    operator_count = sum(1 for i in items if i.requires_operator)
    auto_count = sum(1 for i in items if i.can_auto_execute)
    record_component_state(
        "action_queue",
        ttl=600,
        queue_size=len(items),
        operator_actions=operator_count,
        auto_actions=auto_count,
        built_at=utc_now_iso(),
    )

    return items[:limit]


def get_operator_queue(*, limit: int = 5) -> list[ActionItem]:
    """Get only the items that require operator sign-off."""
    items = build_action_queue(limit=limit * 3)
    return [i for i in items if i.requires_operator][:limit]


def get_auto_queue(*, limit: int = 20) -> list[ActionItem]:
    """Get only the items that can be auto-executed."""
    items = build_action_queue(limit=limit * 3)
    return [i for i in items if i.can_auto_execute][:limit]


def build_clarification_queue(*, limit: int = 5) -> list[ActionItem]:
    """Surface ambiguous active deals separately from the main action queue."""
    deals = list_active_deals(limit=max(limit * 4, 40))
    items: list[ActionItem] = []
    for deal in deals:
        stage_str = str(deal.get("current_stage") or "").strip()
        try:
            stage = DealStage(stage_str)
        except ValueError:
            continue

        distress_type = str(deal.get("distress_type") or "unknown").strip().lower()
        icp_fit = int(deal.get("icp_fit_score") or 0)
        commercial = int(deal.get("commercial_readiness_score") or 0)
        contact_trust = int(deal.get("contact_trust_score") or 0)
        merchant_domain = str(deal.get("merchant_domain") or "").strip()
        if stage not in {DealStage.outreach_drafted, DealStage.outreach_approved, DealStage.opportunity_created}:
            continue
        if not merchant_domain or distress_type not in {"", "unknown"}:
            continue
        if contact_trust <= 0 and icp_fit < 40 and commercial < 55:
            continue

        items.append(
            ActionItem(
                action_type=ActionType.review_opportunity,
                opportunity_id=int(deal.get("opportunity_id", 0)),
                merchant_domain=merchant_domain,
                priority_score=float(deal.get("priority_score", 0) or 0),
                requires_operator=True,
                can_auto_execute=False,
                reason="Signal is still ambiguous — decide whether to clarify, suppress, or leave parked.",
                stage=stage.value,
                processor=deal.get("processor", "unknown"),
                distress_type=deal.get("distress_type", "unknown"),
                contact_email=deal.get("contact_email", ""),
                metadata={
                    "icp_fit_score": icp_fit,
                    "commercial_readiness_score": commercial,
                    "contact_trust_score": contact_trust,
                    "clarification_needed": True,
                },
            )
        )

    items.sort(key=lambda i: i.priority_score, reverse=True)
    return items[:limit]


# ── Rendering ────────────────────────────────────────────────────────────────

def render_action_queue(items: list[ActionItem], *, max_items: int = 5) -> str:
    """
    Render the action queue as a concise Telegram-ready message.

    Format emphasizes what the operator needs to do vs. what is auto-handled.
    """
    if not items:
        return "No actions in queue. Pipeline is clear."

    operator_items = [i for i in items if i.requires_operator][:max_items]
    auto_items = [i for i in items if i.can_auto_execute]

    lines: list[str] = []

    if operator_items:
        lines.append("Needs your sign-off:")
        for item in operator_items:
            domain = item.merchant_domain or f"opp #{item.opportunity_id}"
            distress = item.distress_type.replace("_", " ") if item.distress_type != "unknown" else ""
            processor = item.processor.replace("_", " ").title() if item.processor != "unknown" else ""
            context_parts = [p for p in [processor, distress] if p]
            context = f" ({', '.join(context_parts)})" if context_parts else ""
            action_label = _action_label(item.action_type)
            lines.append(f"  → {action_label}: {domain}{context}")

    if auto_items:
        auto_by_type: dict[str, int] = {}
        for item in auto_items:
            label = _action_label(item.action_type)
            auto_by_type[label] = auto_by_type.get(label, 0) + 1
        auto_summary = ", ".join(f"{count} {label}" for label, count in auto_by_type.items())
        lines.append(f"Auto-handling: {auto_summary}")

    return "\n".join(lines)


def render_execution_summary(items: list[ActionItem]) -> dict[str, Any]:
    """Structured summary for embedding in the execution plan."""
    operator_items = [i.to_dict() for i in items if i.requires_operator]
    auto_items = [i.to_dict() for i in items if i.can_auto_execute]
    clarification_items = [i.to_dict() for i in build_clarification_queue(limit=5)]
    return {
        "operator_queue": operator_items[:5],
        "operator_queue_size": len(operator_items),
        "auto_queue_size": len(auto_items),
        "auto_queue_types": list(set(i["action_type"] for i in auto_items)),
        "auto_queue_preview": auto_items[:3],
        "clarification_queue": clarification_items,
        "total_active": len(items),
    }


def _action_label(action_type: ActionType) -> str:
    return {
        ActionType.send_outreach: "Send outreach",
        ActionType.review_draft: "Review draft",
        ActionType.review_reply: "Review reply",
        ActionType.draft_follow_up: "Draft follow-up",
        ActionType.draft_outreach: "Draft outreach",
        ActionType.review_opportunity: "Process opportunity",
        ActionType.capture_outcome: "Check outcome",
        ActionType.qualify_lead: "Qualify lead",
    }.get(action_type, action_type.value.replace("_", " ").title())


def _is_queue_actionable(deal: dict[str, Any], stage: DealStage) -> bool:
    """Suppress legacy ghost rows that are not ready to become operator work."""
    merchant_domain = str(deal.get("merchant_domain") or "").strip().lower()
    distress_type = str(deal.get("distress_type") or "unknown").strip().lower()
    contact_email = str(deal.get("contact_email") or "").strip().lower()
    contact_trust = int(deal.get("contact_trust_score") or 0)
    icp_fit = int(deal.get("icp_fit_score") or 0)
    commercial = int(deal.get("commercial_readiness_score") or 0)

    if stage in {DealStage.outreach_drafted, DealStage.outreach_approved}:
        if distress_type in {"", "unknown"} and icp_fit < 50 and commercial < 70:
            return False

    if stage in _HOT_STAGES:
        return True

    if not merchant_domain:
        return False

    if stage == DealStage.opportunity_created:
        if distress_type in {"", "unknown"}:
            return False
        if contact_trust <= 0 and not contact_email and icp_fit < 40 and commercial < 60:
            return False
        return True

    if stage in {DealStage.lead_qualified, DealStage.signal_detected}:
        if distress_type in {"", "unknown"} and contact_trust <= 0:
            return False
        return commercial >= 40 or icp_fit >= 35 or contact_trust > 0

    return True
