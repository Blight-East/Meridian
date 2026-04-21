"""
Meridian MCP Server — Read-Only Pipeline Tools
================================================
Read-only Meridian MCP tools for v1.

Every tool is a thin wrapper around an existing *_command function
from runtime.ops.operator_commands.
"""
from __future__ import annotations

import logging
from typing import Annotated

from fastmcp import FastMCP

logger = logging.getLogger("meridian.mcp.tools.pipeline")


def register_pipeline_tools(mcp: FastMCP) -> None:
    """Register the v1 read-only pipeline tools on the given FastMCP server."""

    # ── Top Opportunities ────────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_top_opportunities",
        description=(
            "Returns the top-ranked pipeline opportunities with queue position, "
            "brain alignment, play, and recommended next step. Use this to see "
            "what needs operator attention right now."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_top_opportunities(
        limit: Annotated[int, "Maximum number of opportunities to return (default 3)"] = 3,
    ) -> dict:
        """Get the top-ranked pipeline opportunities."""
        from runtime.ops.operator_commands import show_top_queue_opportunities_command

        return show_top_queue_opportunities_command(limit=limit)

    # ── Opportunity Workbench ────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_opportunity_workbench",
        description=(
            "Returns the full workbench view for a single opportunity: merchant "
            "context, contact info, draft state, critic review, outreach "
            "recommendation, and learning signal."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_opportunity_workbench(
        opportunity_id: Annotated[int, "The opportunity ID to inspect"],
    ) -> dict:
        """Get the full workbench view for an opportunity."""
        from runtime.ops.operator_commands import show_opportunity_workbench_command

        return show_opportunity_workbench_command(opportunity_id=opportunity_id)

    # ── Follow-Up Queue ──────────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_follow_up_queue",
        description=(
            "Returns sent outreach that is now due or overdue for follow-up. "
            "Each entry includes the opportunity, last sent date, follow-up "
            "type, and recommended action."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_follow_up_queue(
        limit: Annotated[int, "Maximum number of follow-ups to return (default 5)"] = 5,
    ) -> dict:
        """Get sent outreach needing follow-up."""
        from runtime.ops.operator_commands import list_sent_outreach_needing_follow_up_command

        return list_sent_outreach_needing_follow_up_command(limit=limit)

    # ── Reply Review Queue ───────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_reply_review_queue",
        description=(
            "Returns merchant reply threads awaiting operator review. Each "
            "entry includes intent classification (interested, negative, defer, "
            "objection) and a suggested next move."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_reply_review_queue(
        limit: Annotated[int, "Maximum number of replies to return (default 5)"] = 5,
    ) -> dict:
        """Get the reply review queue."""
        from runtime.ops.operator_commands import show_reply_review_queue_command

        return show_reply_review_queue_command(limit=limit)

    # ── Single Reply Review ──────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_reply_review",
        description=(
            "Returns the full reply review for a single opportunity including "
            "thread intelligence, intent analysis, confidence score, and "
            "suggested next move."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_reply_review(
        opportunity_id: Annotated[int, "The opportunity ID to review"],
    ) -> dict:
        """Get the full reply review for an opportunity."""
        from runtime.ops.operator_commands import show_reply_review_command

        return show_reply_review_command(opportunity_id=opportunity_id)

    # ── Daily Briefing ───────────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_daily_briefing",
        description=(
            "Returns the daily operator briefing: pipeline summary, top "
            "opportunities, overnight activity, reply queue status, and "
            "strategic focus areas."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_daily_briefing() -> str | dict:
        """Get the daily operator briefing."""
        from runtime.ops.operator_commands import show_daily_operator_briefing_command

        result = show_daily_operator_briefing_command()
        return result

    # ── Outreach Drafts (list) ───────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_outreach_drafts",
        description=(
            "Returns local outreach drafts awaiting operator review. Each entry "
            "includes the opportunity ID, subject line, draft status, and critic "
            "verdict."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_outreach_drafts(
        limit: Annotated[int, "Maximum number of drafts to return (default 5)"] = 5,
    ) -> dict:
        """Get local outreach drafts."""
        from runtime.ops.operator_commands import show_local_outreach_drafts_command

        return show_local_outreach_drafts_command(limit=limit)

    # ── Single Outreach Draft ────────────────────────────────────────────

    @mcp.tool(
        name="meridian_get_outreach_draft",
        description=(
            "Returns the full draft body, subject line, critic review, and "
            "rewrite preview for a single opportunity."
        ),
        tags={"pipeline", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_outreach_draft(
        opportunity_id: Annotated[int, "The opportunity ID whose draft to inspect"],
    ) -> dict:
        """Get the full outreach draft for an opportunity."""
        from runtime.ops.operator_commands import show_local_outreach_draft_command

        return show_local_outreach_draft_command(opportunity_id=opportunity_id)

    logger.info("Registered 8 v1 read-only pipeline tools")
