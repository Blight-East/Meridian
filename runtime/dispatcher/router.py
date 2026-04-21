# Backward-compatible shim. Real logic lives in router_v2.
# The legacy substring-match router is permanently retired.
from runtime.dispatcher.router_v2 import dispatch, SubstringRoutingAttempted  # noqa: F401

__all__ = ["dispatch", "SubstringRoutingAttempted"]
