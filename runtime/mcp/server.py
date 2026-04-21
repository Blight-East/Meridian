"""
Meridian MCP Server — Entry Point
====================================
Peer adapter alongside telegram_bot.py.  Both call the same
operator_commands.py command layer.

Usage (stdio, for Claude Desktop):
    python -m runtime.mcp.server

Usage (HTTP/SSE, for web clients):
    fastmcp run runtime/mcp/server.py:mcp --transport http --port 9100

The server registers:
  - 4 context resources  (product, sales, ICP, capabilities)
  - 8 read-only tools     (pipeline visibility)
  - 3 action tools        (outreach lifecycle)
  - 3 workflow tools      (composite multi-step operations)
  - 2 system/research tools

Total: 4 resources + 16 tools.
"""
from __future__ import annotations

import logging
import os
import sys

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so `from runtime.ops...` works
# when running as `python -m runtime.mcp.server` from the project root.
# Also strip the raw `runtime/` path if present, otherwise `fastmcp` can
# resolve top-level `mcp` imports to `runtime/mcp`.
# ---------------------------------------------------------------------------
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_runtime_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_package_dir = os.path.abspath(os.path.dirname(__file__))
_shadow_paths = {_runtime_root, _package_dir}
sys.path = [
    entry
    for entry in sys.path
    if os.path.abspath(entry or os.curdir) not in _shadow_paths
]
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fastmcp import FastMCP
from runtime.mcp.auth import build_auth_provider

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("meridian.mcp.server")

# ---------------------------------------------------------------------------
# Create the FastMCP server instance
# ---------------------------------------------------------------------------
mcp = FastMCP(
    name="Meridian",
    instructions=(
        "Meridian is the sales and operator brain for PayFlux. "
        "Use these tools to inspect the sales pipeline, review outreach "
        "drafts, approve and send outreach, manage follow-ups, and read "
        "product/sales context. "
        "Action tools mutate state. Read-only tools expose pipeline and "
        "context visibility for the operator. "
        "This MCP server is private and requires MERIDIAN_MCP_TOKEN."
    ),
    auth=build_auth_provider(),
)

# ---------------------------------------------------------------------------
# Register all components
# ---------------------------------------------------------------------------
from runtime.mcp.resources import register_context_resources
from runtime.mcp.tools.pipeline import register_pipeline_tools
from runtime.mcp.tools.outreach import register_outreach_tools
from runtime.mcp.tools.system import register_system_tools

register_context_resources(mcp)
register_pipeline_tools(mcp)
register_outreach_tools(mcp)
register_system_tools(mcp)

# Workflow tools depend on deal_lifecycle table which may not exist yet.
# Register them lazily so the MCP server can start without them.
_workflow_tool_count = 0
try:
    from runtime.mcp.tools.workflow import register_workflow_tools
    register_workflow_tools(mcp)
    _workflow_tool_count = 3
except Exception as exc:
    logger.warning(
        "Workflow tools not registered (deal lifecycle table may not exist yet): %s", exc
    )

logger.info(
    "Meridian MCP server ready — 4 resources, %d tools (%d atomic + %d workflow)",
    13 + _workflow_tool_count, 13, _workflow_tool_count,
)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
