"""
LLM provider adapter.

Exposes one function, ``call_llm(payload, *, timeout=30)``, that accepts an
Anthropic ``/v1/messages``-shaped payload and returns an Anthropic-shaped
response dict. Internally it routes to NVIDIA NIM, GitHub Models, or
Anthropic based on the ``LLM_PROVIDER`` environment variable, with a
transparent single-hop fallback controlled by ``LLM_FALLBACK_PROVIDER``.

Callers keep their existing parsing code; only the transport changes.

Environment:
- ``LLM_PROVIDER``                 primary: ``nvidia_nim`` (default), ``github_models``, ``anthropic``
- ``LLM_FALLBACK_PROVIDER``        secondary on failure (default ``github_models``; set empty to disable)
- ``NVIDIA_NIM_API_KEY``           required for NIM
- ``NVIDIA_NIM_ENDPOINT``          default ``https://integrate.api.nvidia.com/v1``
- ``NVIDIA_NIM_DEFAULT_MODEL``     default ``meta/llama-3.3-70b-instruct`` (used for unknown Anthropic model names)
- ``GITHUB_TOKEN``                 GitHub Models auth (reused from existing repo conventions)
- ``GITHUB_MODELS_ENDPOINT``       default ``https://models.github.ai/inference``
- ``GITHUB_MODELS_API_VERSION``    default ``2026-03-10``
- ``GITHUB_MODELS_DEFAULT_MODEL``  default ``openai/gpt-4.1-mini``
- ``ANTHROPIC_API_KEY``            only needed if ``LLM_PROVIDER=anthropic``
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from dotenv import dotenv_values

from config.logging_config import get_logger

try:
    from memory.structured.db import save_event
except Exception:  # pragma: no cover - defensive import
    def save_event(*_args: Any, **_kwargs: Any) -> None:
        return None


logger = get_logger("llm_provider")

_ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")


# ---------------------------------------------------------------------------
# config helpers
# ---------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    try:
        file_values = dotenv_values(_ENV_FILE_PATH)
    except Exception:
        file_values = {}
    value = file_values.get(name) or os.getenv(name) or default
    return str(value).strip()


def _primary_provider() -> str:
    return _env("LLM_PROVIDER", "nvidia_nim").lower()


def _fallback_provider() -> str:
    return _env("LLM_FALLBACK_PROVIDER", "github_models").lower()


# ---------------------------------------------------------------------------
# model alias maps (Anthropic-style -> provider-native)
# ---------------------------------------------------------------------------

NIM_MODEL_MAP: Dict[str, str] = {
    "claude-sonnet-4-6": "meta/llama-3.3-70b-instruct",
    "claude-opus-4-6": "meta/llama-3.1-405b-instruct",
    "claude-opus-4-7": "meta/llama-3.1-405b-instruct",
    "claude-sonnet-4-5-20250929": "meta/llama-3.3-70b-instruct",
    "claude-opus-4-5-20251101": "meta/llama-3.1-405b-instruct",
    "claude-haiku-4-5-20251001": "meta/llama-3.3-70b-instruct",
    "claude-sonnet-4-20250514": "meta/llama-3.3-70b-instruct",
    "claude-opus-4-20250514": "meta/llama-3.1-405b-instruct",
    "claude-opus-4-1-20250805": "meta/llama-3.1-405b-instruct",
    "claude-3-5-sonnet-20241022": "meta/llama-3.3-70b-instruct",
    "claude-3-5-haiku-20241022": "meta/llama-3.3-70b-instruct",
    "claude-3-opus-20240229": "meta/llama-3.1-405b-instruct",
    "claude-3-haiku-20240307": "meta/llama-3.3-70b-instruct",
}

GITHUB_MODEL_MAP: Dict[str, str] = {
    "claude-sonnet-4-6": "openai/gpt-4.1-mini",
    "claude-opus-4-6": "openai/gpt-4.1",
    "claude-opus-4-7": "openai/gpt-4.1",
    "claude-sonnet-4-5-20250929": "openai/gpt-4.1-mini",
    "claude-opus-4-5-20251101": "openai/gpt-4.1",
    "claude-haiku-4-5-20251001": "openai/gpt-4.1-mini",
    "claude-sonnet-4-20250514": "openai/gpt-4.1-mini",
    "claude-opus-4-20250514": "openai/gpt-4.1",
    "claude-opus-4-1-20250805": "openai/gpt-4.1",
    "claude-3-5-sonnet-20241022": "openai/gpt-4.1-mini",
    "claude-3-5-haiku-20241022": "openai/gpt-4.1-mini",
    "claude-3-opus-20240229": "openai/gpt-4.1",
    "claude-3-haiku-20240307": "openai/gpt-4.1-mini",
}


def _resolve_model(provider: str, anthropic_model: str) -> str:
    name = str(anthropic_model or "").strip()
    if provider == "nvidia_nim":
        return NIM_MODEL_MAP.get(name) or _env("NVIDIA_NIM_DEFAULT_MODEL", "meta/llama-3.3-70b-instruct")
    if provider == "github_models":
        return GITHUB_MODEL_MAP.get(name) or _env("GITHUB_MODELS_DEFAULT_MODEL", "openai/gpt-4.1-mini")
    # anthropic (or unknown) -> pass through unchanged
    return name or _env("ANTHROPIC_DEFAULT_MODEL", "claude-sonnet-4-6")


# ---------------------------------------------------------------------------
# payload / response translation
# ---------------------------------------------------------------------------

def _flatten_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if text:
                    parts.append(str(text))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)
    return str(content or "")


def _anthropic_to_openai(payload: Dict[str, Any], resolved_model: str) -> Dict[str, Any]:
    messages: List[Dict[str, Any]] = []
    system = payload.get("system")
    if system:
        sys_text = _flatten_content(system).strip()
        if sys_text:
            messages.append({"role": "system", "content": sys_text})
    for msg in payload.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "user")
        # OpenAI Chat Completions only understands system/user/assistant/tool roles.
        if role not in ("system", "user", "assistant", "tool"):
            role = "user"
        messages.append({"role": role, "content": _flatten_content(msg.get("content", ""))})
    out: Dict[str, Any] = {
        "model": resolved_model,
        "messages": messages,
        "max_tokens": int(payload.get("max_tokens") or 800),
    }
    for key in ("temperature", "top_p", "stop"):
        if key in payload and payload[key] is not None:
            out[key] = payload[key]
    return out


_OAI_STOP_TO_ANTHROPIC = {
    "stop": "end_turn",
    "length": "max_tokens",
    "content_filter": "stop_sequence",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
}


def _openai_to_anthropic(data: Dict[str, Any], resolved_model: str) -> Dict[str, Any]:
    choice = (data.get("choices") or [{}])[0] if isinstance(data.get("choices"), list) else {}
    msg = choice.get("message") or {}
    text = msg.get("content") or ""
    if isinstance(text, list):  # some providers return list-of-parts
        text = _flatten_content(text)
    finish_reason = choice.get("finish_reason")
    usage = data.get("usage") or {}
    return {
        "id": data.get("id") or "",
        "type": "message",
        "role": "assistant",
        "model": resolved_model,
        "content": [{"type": "text", "text": str(text)}],
        "stop_reason": _OAI_STOP_TO_ANTHROPIC.get(finish_reason, "end_turn"),
        "stop_sequence": None,
        "usage": {
            "input_tokens": int(usage.get("prompt_tokens") or 0),
            "output_tokens": int(usage.get("completion_tokens") or 0),
        },
    }


# ---------------------------------------------------------------------------
# provider callers
# ---------------------------------------------------------------------------

def _post_json(url: str, *, headers: Dict[str, str], payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if resp.status_code >= 400:
        # Surface the server's error body in the RuntimeError so callers can log it.
        body = resp.text[:500]
        raise RuntimeError(f"HTTP {resp.status_code} {resp.reason}: {body}")
    try:
        return resp.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response from {url}: {resp.text[:200]}") from exc


def _call_nvidia_nim(payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    key = _env("NVIDIA_NIM_API_KEY")
    if not key:
        raise RuntimeError("NVIDIA_NIM_API_KEY is not configured")
    endpoint = _env("NVIDIA_NIM_ENDPOINT", "https://integrate.api.nvidia.com/v1").rstrip("/")
    resolved = _resolve_model("nvidia_nim", payload.get("model", ""))
    oai_payload = _anthropic_to_openai(payload, resolved)
    data = _post_json(
        f"{endpoint}/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        payload=oai_payload,
        timeout=timeout,
    )
    return _openai_to_anthropic(data, resolved)


def _call_github_models(payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    key = _env("GITHUB_TOKEN") or _env("GITHUB_MODELS_TOKEN")
    if not key:
        raise RuntimeError("GITHUB_TOKEN (for GitHub Models) is not configured")
    endpoint = _env("GITHUB_MODELS_ENDPOINT", "https://models.github.ai/inference").rstrip("/")
    api_version = _env("GITHUB_MODELS_API_VERSION", "2026-03-10")
    resolved = _resolve_model("github_models", payload.get("model", ""))
    oai_payload = _anthropic_to_openai(payload, resolved)
    data = _post_json(
        f"{endpoint}/chat/completions",
        headers={
            "Authorization": f"Bearer {key}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": api_version,
            "Content-Type": "application/json",
        },
        payload=oai_payload,
        timeout=timeout,
    )
    return _openai_to_anthropic(data, resolved)


def _call_anthropic(payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    key = _env("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured")
    data = _post_json(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        payload=payload,
        timeout=timeout,
    )
    if "error" in data:
        err = data.get("error") or {}
        raise RuntimeError(f"anthropic_error:{err.get('type')}:{err.get('message', '')[:200]}")
    return data


_PROVIDER_DISPATCH: Dict[str, Callable[[Dict[str, Any], int], Dict[str, Any]]] = {
    "nvidia_nim": _call_nvidia_nim,
    "github_models": _call_github_models,
    "anthropic": _call_anthropic,
}


# ---------------------------------------------------------------------------
# public entry point
# ---------------------------------------------------------------------------

def call_llm(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Call the configured LLM provider with an Anthropic-shaped ``payload``.

    Returns an Anthropic-shaped dict: ``{"content": [{"type": "text", "text": ...}], ...}``.
    Raises ``RuntimeError`` only if both primary and fallback providers fail.
    """
    primary = _primary_provider()
    fallback = _fallback_provider()
    providers: List[str] = [primary]
    if fallback and fallback != primary and fallback in _PROVIDER_DISPATCH:
        providers.append(fallback)

    attempted: List[Tuple[str, str]] = []
    start = time.time()
    last_error: Optional[Exception] = None

    for provider in providers:
        fn = _PROVIDER_DISPATCH.get(provider)
        if not fn:
            attempted.append((provider, "unknown_provider"))
            continue
        try:
            result = fn(payload, timeout)
            duration_ms = int((time.time() - start) * 1000)
            try:
                save_event(
                    "llm_provider_call",
                    {
                        "provider": provider,
                        "model_requested": payload.get("model"),
                        "model_resolved": result.get("model"),
                        "ok": True,
                        "duration_ms": duration_ms,
                        "fallback_used": provider != primary,
                        "attempted": attempted,
                    },
                )
            except Exception:
                pass
            if provider != primary:
                logger.info(
                    "LLM fallback succeeded: primary=%s fallback=%s model=%s",
                    primary,
                    provider,
                    result.get("model"),
                )
            return result
        except Exception as exc:
            attempted.append((provider, str(exc)[:240]))
            last_error = exc
            logger.warning("LLM provider %s failed: %s", provider, exc)

    duration_ms = int((time.time() - start) * 1000)
    try:
        save_event(
            "llm_provider_call",
            {
                "provider": "all_failed",
                "model_requested": payload.get("model"),
                "ok": False,
                "duration_ms": duration_ms,
                "attempted": attempted,
            },
        )
    except Exception:
        pass
    raise RuntimeError(
        f"All LLM providers failed (attempted={json.dumps(attempted)[:500]}); last_error={last_error}"
    )


__all__ = ["call_llm", "NIM_MODEL_MAP", "GITHUB_MODEL_MAP"]
