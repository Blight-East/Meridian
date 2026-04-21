"""
Meridian MCP Server — Auth Middleware
=====================================
Bearer token validation for Meridian MCP.

This server is private-first and expects a single operator token in
MERIDIAN_MCP_TOKEN. FastMCP uses the auth provider for HTTP transports,
and we still require the token to be configured at startup for stdio mode
so the server is never launched in an unauthenticated configuration.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from fastmcp.server.auth import AccessToken, AuthProvider

logger = logging.getLogger("meridian.mcp.auth")

_TOKEN_ENV = "MERIDIAN_MCP_TOKEN"


def get_configured_token(*, required: bool = True) -> str:
    """Return the configured Meridian MCP token.

    When ``required`` is True, raise a clear startup error if the token is not
    configured. v1 is private-only and should never silently boot without it.
    """
    token = (os.environ.get(_TOKEN_ENV) or "").strip()
    if required and not token:
        raise RuntimeError(
            "MERIDIAN_MCP_TOKEN is required to start the Meridian MCP server."
        )
    return token


def validate_bearer_token(token: str) -> dict[str, Any]:
    """
    Validate a bearer token against the configured MCP token.

    Returns
    -------
    dict with keys:
        - valid (bool): Whether the token is accepted.
        - reason (str): Human-readable rejection reason (empty on success).
    """
    configured = get_configured_token(required=True)
    if not token:
        return {"valid": False, "reason": "missing_bearer_token"}
    if token != configured:
        return {"valid": False, "reason": "invalid_bearer_token"}
    return {"valid": True, "reason": ""}


def require_auth(token: str) -> None:
    """
    Raise ValueError if the token is invalid.
    In practice, FastMCP tools call this at the top of every action tool.
    Read-only tools skip auth for now (context is not secret).
    """
    result = validate_bearer_token(token)
    if not result["valid"]:
        raise ValueError(f"MCP auth failed: {result['reason']}")


class MeridianTokenAuth(AuthProvider):
    """Static bearer-token auth provider for Meridian MCP."""

    def __init__(self, token: str):
        super().__init__(required_scopes=["operator"])
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not token or token != self._token:
            return None
        return AccessToken(
            token=token,
            client_id="meridian-operator",
            scopes=["operator"],
            claims={
                "sub": "meridian-operator",
                "auth_source": "meridian_mcp_token",
            },
        )


def build_auth_provider() -> MeridianTokenAuth:
    """Build the FastMCP auth provider from the configured env token."""
    token = get_configured_token(required=True)
    logger.info("Meridian MCP auth configured from %s", _TOKEN_ENV)
    return MeridianTokenAuth(token)
