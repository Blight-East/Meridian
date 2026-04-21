"""
Meridian MCP Server — System / Research Tools
==============================================
Read-only tools for Meridian system health and web search.
"""
from __future__ import annotations

import logging

from fastmcp import FastMCP

logger = logging.getLogger("meridian.mcp.tools.system")

def register_system_tools(mcp: FastMCP) -> None:
    """Register system / research tools on the given FastMCP server."""

    @mcp.tool(
        name="meridian_search_web",
        description=(
            "Search the live web through Meridian's configured Brave Search path. "
            "Use this for current merchant research, official-site discovery, "
            "signal corroboration, and public-web investigation."
        ),
        tags={"research", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def search_web(
        query: str,
        max_results: int = 5,
    ) -> dict:
        """Search the web through Meridian's configured search provider."""
        from tools.web_search import web_search

        return web_search(query, max_results=max_results)

    @mcp.tool(
        name="meridian_get_system_health",
        description=(
            "Returns Meridian's system health: API status, Telegram adapter "
            "health, scheduler health, operator path status, memory subsystem, "
            "and live probe results across all subsystems."
        ),
        tags={"system", "read"},
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )
    def get_system_health() -> dict:
        """Get Meridian system health status."""
        from runtime.conversation.meridian_capability_context import get_meridian_capability_audit

        return get_meridian_capability_audit()

    logger.info("Registered 2 system / research tools")
