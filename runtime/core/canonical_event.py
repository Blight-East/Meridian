"""
Canonical event identity. All idempotent writes to `events` flow through here.

Every event gets:
  - payload_signature: md5 of canonicalized JSON payload
  - idempotency_key:   domain-specific or derived from signature
  - source_system:     identifies the emitting host
  - created_at_utc:    ISO timestamp at emission time
"""
from __future__ import annotations
import hashlib, json, os, socket
from datetime import datetime, timezone
from typing import Any

SOURCE_SYSTEM_DEFAULT = os.getenv(
    "PAYFLUX_SOURCE_SYSTEM", f"meridian@{socket.gethostname()}"
)


def canonical_json(obj: Any) -> str:
    """Deterministic JSON serialization (sorted keys, no whitespace)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)


def payload_signature(payload: Any) -> str:
    """md5 hex digest of the canonical JSON form of a payload."""
    return hashlib.md5(canonical_json(payload).encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event(
    *,
    event_type: str,
    payload: dict,
    idempotency_key: str | None = None,
    source_system: str | None = None,
) -> dict:
    """Build a canonical event dict ready for insertion into the events table.

    The returned dict has top-level 'event_type', 'data', 'payload_signature',
    and 'idempotency_key' fields. The 'data' dict includes all payload fields
    plus canonical metadata.
    """
    sig = payload_signature(payload)
    idem = idempotency_key or f"{event_type}:{sig}"
    return {
        "event_type": event_type,
        "data": {
            **payload,
            "payload_signature": sig,
            "idempotency_key":   idem,
            "source_system":     source_system or SOURCE_SYSTEM_DEFAULT,
            "created_at_utc":    utc_now_iso(),
        },
        "payload_signature": sig,
        "idempotency_key":   idem,
    }
