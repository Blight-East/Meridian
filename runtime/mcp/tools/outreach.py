"""
Meridian MCP Server — Outreach Action Tools
=============================================
Mutating outreach tools exposed in the Meridian MCP v1 surface.

All action tools log an MCP audit event and also stamp the underlying
command/execution path with source metadata so the command layer remains
the system of record.
"""
from __future__ import annotations

import logging
from typing import Annotated

from fastmcp import FastMCP

logger = logging.getLogger("meridian.mcp.tools.outreach")
MCP_ACTION_SOURCE = "mcp"
MCP_ACTOR_ID = "meridian-mcp"


def _log_action(tool_name: str, params: dict) -> None:
    """Log every action tool invocation for audit trail."""
    from json import dumps

    logger.info("mcp_action_call: tool=%s params=%s", tool_name, dumps(params, default=str)[:500])
    try:
        from memory.structured.db import save_event

        save_event("mcp_tool_call", {"tool": tool_name, "params": params, "source": "mcp"})
    except Exception:
        pass  # Event logging is best-effort; never block an action on it.


def register_outreach_tools(mcp: FastMCP) -> None:
    """Register the v1 outreach action tools on the given FastMCP server."""

    # ── Approve Outreach ─────────────────────────────────────────────────

    @mcp.tool(
        name="meridian_approve_outreach",
        description=(
            "Approves an outreach draft for a given opportunity. Triggers the "
            "Meridian critic review. Does NOT send the outreach — use "
            "meridian_send_outreach after reviewing the critic verdict."
        ),
        tags={"outreach", "action"},
        annotations={"readOnlyHint": False, "idempotentHint": True},
    )
    def approve_outreach(
        opportunity_id: Annotated[int, "The opportunity ID whose draft to approve"],
        notes: Annotated[str, "Optional operator notes for this approval"] = "",
    ) -> dict:
        """Approve an outreach draft (does not send)."""
        _log_action("meridian_approve_outreach", {"opportunity_id": opportunity_id, "notes": notes})
        from runtime.ops.operator_commands import approve_outreach_for_opportunity_command

        return approve_outreach_for_opportunity_command(
            opportunity_id=opportunity_id,
            notes=notes,
            approved_by=MCP_ACTOR_ID,
            approval_source=MCP_ACTION_SOURCE,
        )

    # ── Send Outreach ────────────────────────────────────────────────────

    @mcp.tool(
        name="meridian_send_outreach",
        description=(
            "Sends an already-approved outreach draft via email. The draft must "
            "have been approved first via meridian_approve_outreach. Returns the "
            "Gmail thread ID and scheduled follow-up date."
        ),
        tags={"outreach", "action"},
        annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False},
    )
    def send_outreach(
        opportunity_id: Annotated[int, "The opportunity ID whose approved draft to send"],
    ) -> dict:
        """Send an approved outreach draft."""
        _log_action("meridian_send_outreach", {"opportunity_id": opportunity_id})
        from runtime.ops.operator_commands import send_outreach_for_opportunity_command

        return send_outreach_for_opportunity_command(
            opportunity_id=opportunity_id,
            approved_by=MCP_ACTOR_ID,
            approval_source=MCP_ACTION_SOURCE,
        )

    # ── Rewrite Outreach ─────────────────────────────────────────────────

    @mcp.tool(
        name="meridian_rewrite_outreach",
        description=(
            "Rewrites an outreach draft with a specified style. Available styles "
            "include: standard, concise, aggressive, gentle, technical. You can "
            "also provide free-text instructions for custom rewrites."
        ),
        tags={"outreach", "action"},
        annotations={"readOnlyHint": False, "idempotentHint": False},
    )
    def rewrite_outreach(
        opportunity_id: Annotated[int, "The opportunity ID whose draft to rewrite"],
        style: Annotated[str, "Rewrite style: standard, concise, aggressive, gentle, technical"] = "standard",
        instructions: Annotated[str, "Optional free-text rewrite instructions"] = "",
    ) -> dict:
        """Rewrite an outreach draft with a specific style."""
        _log_action("meridian_rewrite_outreach", {
            "opportunity_id": opportunity_id,
            "style": style,
            "instructions": instructions[:200],
        })
        from runtime.ops.operator_commands import rewrite_outreach_draft_command

        return rewrite_outreach_draft_command(
            opportunity_id=opportunity_id,
            style=style,
            instructions=instructions,
            rewritten_by=MCP_ACTOR_ID,
            rewrite_source=MCP_ACTION_SOURCE,
        )

    logger.info("Registered 3 v1 outreach action tools")
