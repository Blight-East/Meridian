"""
Meridian MCP Workflow Tools — Composite Multi-Step Operations
==============================================================
Higher-level tools that compose the atomic pipeline/outreach tools
into guided workflow steps. These give MCP clients (Claude Desktop,
other agents) a structured way to process the queue without having
to know the internal step sequence.

Three tools:
1. meridian_process_queue — top N actionable items with decision context
2. meridian_review_and_decide — full decision package for one deal
3. meridian_batch_outcome — batch-mark outcomes for dead threads
"""
from __future__ import annotations

import logging
from typing import Any

from runtime.ops.action_queue import build_action_queue, render_action_queue
from runtime.ops.deal_lifecycle import (
    DealStage,
    get_deal,
    get_deal_history,
    transition_deal,
    InvalidTransitionError,
)
from runtime.ops.deal_lifecycle_hooks import dispatch_hook
from memory.structured.db import save_event

logger = logging.getLogger("meridian.mcp.workflow")


def register_workflow_tools(mcp) -> None:
    """Register workflow-level tools on the MCP server."""

    @mcp.tool()
    def meridian_process_queue(limit: int = 5) -> dict[str, Any]:
        """Get the top actionable items from the deal pipeline.

        Returns a ranked list of deals that need attention, with:
        - What action is needed (send, review, draft, etc.)
        - Whether it requires operator sign-off
        - The deal context (merchant, distress type, processor)
        - Why this action is next

        Items are sorted by priority. Operator sign-off items
        appear at the top.

        Args:
            limit: Max number of items to return (default 5).
        """
        items = build_action_queue(limit=max(1, min(int(limit), 20)))
        rendered = render_action_queue(items)
        return {
            "queue": [item.to_dict() for item in items],
            "rendered": rendered,
            "total_items": len(items),
            "operator_required": sum(1 for i in items if i.requires_operator),
            "auto_executable": sum(1 for i in items if i.can_auto_execute),
        }

    @mcp.tool()
    def meridian_review_and_decide(opportunity_id: int) -> dict[str, Any]:
        """Get a full decision package for one deal.

        Returns everything needed to make a decision:
        - Current deal stage and lifecycle history
        - Deal context (merchant, distress, processor, contact)
        - The recommended next action
        - Priority score and why it matters

        This replaces the need to call multiple atomic tools
        (get_opportunity_workbench, get_outreach_draft, etc.)
        to understand what is happening with one deal.

        Args:
            opportunity_id: The opportunity ID to review.
        """
        deal = get_deal(int(opportunity_id))
        if not deal:
            return {"error": f"No deal found for opportunity_id={opportunity_id}"}

        history = get_deal_history(int(opportunity_id), limit=10)

        # Map stage to recommended action
        stage_str = deal.get("current_stage", "signal_detected")
        try:
            stage = DealStage(stage_str)
        except ValueError:
            stage = DealStage.signal_detected

        from runtime.ops.action_queue import _STAGE_ACTION_MAP, _ACTION_REASON

        action_info = _STAGE_ACTION_MAP.get(stage)
        if action_info:
            action_type, requires_operator = action_info
            recommended_action = {
                "action": action_type.value,
                "requires_operator": requires_operator,
                "reason": _ACTION_REASON.get(action_type, ""),
            }
        else:
            recommended_action = {
                "action": "none",
                "requires_operator": False,
                "reason": "Deal is in a terminal state.",
            }

        # Serialize datetimes in deal
        serialized_deal = {}
        for key, value in deal.items():
            if hasattr(value, "isoformat"):
                serialized_deal[key] = value.isoformat()
            else:
                serialized_deal[key] = value

        serialized_history = []
        for entry in history:
            h = {}
            for key, value in entry.items():
                if hasattr(value, "isoformat"):
                    h[key] = value.isoformat()
                else:
                    h[key] = value
            serialized_history.append(h)

        return {
            "deal": serialized_deal,
            "stage": stage_str,
            "priority_score": float(deal.get("priority_score", 0) or 0),
            "history": serialized_history,
            "recommended_action": recommended_action,
            "merchant_domain": deal.get("merchant_domain", ""),
            "processor": deal.get("processor", "unknown"),
            "distress_type": deal.get("distress_type", "unknown"),
            "contact_email": deal.get("contact_email", ""),
        }

    @mcp.tool()
    def meridian_batch_outcome(outcomes: list[dict[str, Any]]) -> dict[str, Any]:
        """Mark outcomes for multiple deals at once.

        Each outcome should have:
        - opportunity_id: int
        - status: "won" | "lost" | "ignored"

        This is useful for cleaning up dead threads in bulk
        instead of marking them one at a time.

        Args:
            outcomes: List of {opportunity_id, status} dicts.
        """
        results: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0

        for entry in outcomes:
            opp_id = int(entry.get("opportunity_id", 0))
            status = str(entry.get("status", "")).strip().lower()

            if status not in {"won", "lost", "ignored"}:
                results.append({
                    "opportunity_id": opp_id,
                    "status": "error",
                    "reason": f"Invalid outcome status: {status}. Must be won, lost, or ignored.",
                })
                failed += 1
                continue

            stage_map = {
                "won": DealStage.outcome_won,
                "lost": DealStage.outcome_lost,
                "ignored": DealStage.outcome_ignored,
            }
            target_stage = stage_map[status]

            try:
                deal = transition_deal(
                    opp_id,
                    target_stage,
                    actor="mcp_batch_outcome",
                    metadata={"batch": True},
                )
                dispatch_hook(deal, target_stage.value)
                results.append({
                    "opportunity_id": opp_id,
                    "status": "ok",
                    "new_stage": target_stage.value,
                })
                succeeded += 1
            except InvalidTransitionError as exc:
                results.append({
                    "opportunity_id": opp_id,
                    "status": "error",
                    "reason": str(exc),
                })
                failed += 1
            except ValueError as exc:
                results.append({
                    "opportunity_id": opp_id,
                    "status": "error",
                    "reason": str(exc),
                })
                failed += 1

        save_event("mcp_batch_outcome", {
            "total": len(outcomes),
            "succeeded": succeeded,
            "failed": failed,
        })

        return {
            "total": len(outcomes),
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }

    logger.info("Registered 3 workflow tools")
