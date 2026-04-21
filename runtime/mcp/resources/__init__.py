"""
Meridian MCP Server — Context Resources
=========================================
Four read-only MCP resources exposing Meridian's core context:
  meridian://context/product    — PayFlux product definition, plans, pricing, guardrails
  meridian://context/sales      — Sales goal, reframes, objections, close frames
  meridian://context/icp        — ICP segments, buyer roles, urgency signals, disqualifiers
  meridian://context/capabilities — Meridian self-assessment: built, partial, missing

All resources are thin wrappers around existing get_*_context() functions.
No new business logic lives here.
"""
from __future__ import annotations

import json
import logging

from fastmcp import FastMCP

logger = logging.getLogger("meridian.mcp.resources")


def register_context_resources(mcp: FastMCP) -> None:
    """Register all four context resources on the given FastMCP server instance."""

    @mcp.resource(
        "meridian://context/product",
        name="PayFlux Product Context",
        description=(
            "PayFlux product definition including plans (Free, Pro, Enterprise), "
            "pricing, core promises, commercial guardrails, and what the product "
            "does and does not do."
        ),
        mime_type="application/json",
        tags={"context", "product"},
    )
    def product_context() -> str:
        """Returns the canonical PayFlux product context."""
        from runtime.conversation.payflux_product_context import get_payflux_product_context

        return json.dumps(get_payflux_product_context(), default=str)

    @mcp.resource(
        "meridian://context/sales",
        name="PayFlux Sales Context",
        description=(
            "Sales context for outreach: main sales goal, core reframe, objection "
            "handling, response rules, discovery questions, close frames, and "
            "outreach guidelines."
        ),
        mime_type="application/json",
        tags={"context", "sales"},
    )
    def sales_context() -> str:
        """Returns the canonical PayFlux sales context."""
        from runtime.conversation.payflux_sales_context import get_payflux_sales_context

        return json.dumps(get_payflux_sales_context(), default=str)

    @mcp.resource(
        "meridian://context/icp",
        name="PayFlux ICP Context",
        description=(
            "Ideal Customer Profile: primary segments, buyer roles, urgency "
            "signals, high-conviction prospect definition, disqualifiers, "
            "commercial fit rules, and language rules."
        ),
        mime_type="application/json",
        tags={"context", "icp"},
    )
    def icp_context() -> str:
        """Returns the canonical PayFlux ICP context."""
        from runtime.conversation.payflux_icp_context import get_payflux_icp_context

        return json.dumps(get_payflux_icp_context(), default=str)

    @mcp.resource(
        "meridian://context/capabilities",
        name="Meridian Capability Context",
        description=(
            "Meridian's self-assessment of its own capabilities: what is built, "
            "what is partial, what is missing, and current operational status "
            "across all subsystems."
        ),
        mime_type="application/json",
        tags={"context", "capabilities"},
    )
    def capabilities_context() -> str:
        """Returns the Meridian capability self-assessment context."""
        from runtime.conversation.meridian_capability_context import get_meridian_capability_context

        return json.dumps(get_meridian_capability_context(), default=str)

    logger.info("Registered 4 Meridian context resources")
