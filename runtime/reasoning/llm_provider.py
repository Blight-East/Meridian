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
    """Return a trimmed env value, preserving ``""`` as an explicit override.

    Uses a precedence chain (``.env`` file → process env → ``default``) rather
    than ``or``-chaining so that an explicitly-empty ``NAME=`` in ``.env``
    (e.g. ``LLM_FALLBACK_PROVIDER=`` to disable fallback) is honoured instead
    of silently falling through to ``default``.
    """
    try:
        file_values = dotenv_values(_ENV_FILE_PATH)
    except Exception:
        file_values = {}
    value = file_values.get(name)
    if value is None:
        value = os.getenv(name)
    if value is None:
        value = default
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
    """Collapse an Anthropic-shaped content payload into a plain string.

    Handles the three Anthropic block types that carry a payload:
    ``text`` blocks (``text`` key), ``tool_result`` blocks (``content`` key —
    may itself be a string or a list of text blocks), and any other dict with
    a stringifiable ``text`` or ``content`` field. Used for system prompts
    and for messages that don't need full tool-use translation.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for block in content:
            if isinstance(block, dict):
                # ``text`` for text/tool_use blocks; ``content`` for tool_result blocks.
                value = block.get("text")
                if value is None:
                    value = block.get("content")
                if value is None:
                    continue
                if isinstance(value, list):
                    parts.append(_flatten_content(value))
                elif isinstance(value, str):
                    if value:
                        parts.append(value)
                else:
                    parts.append(str(value))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(p for p in parts if p)
    return str(content or "")


def _anthropic_tools_to_openai(tools: Any) -> List[Dict[str, Any]]:
    """Translate an Anthropic ``tools`` list to OpenAI function-calling tools."""
    out: List[Dict[str, Any]] = []
    if not isinstance(tools, list):
        return out
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = tool.get("name")
        if not name:
            continue
        out.append({
            "type": "function",
            "function": {
                "name": name,
                "description": tool.get("description") or "",
                "parameters": tool.get("input_schema") or {"type": "object", "properties": {}},
            },
        })
    return out


def _translate_assistant_message(content: Any) -> Dict[str, Any]:
    """Turn an Anthropic assistant message (possibly containing ``tool_use``)
    into the OpenAI assistant-message shape. Text blocks join into
    ``content``; ``tool_use`` blocks become ``tool_calls`` entries.
    """
    text_parts: List[str] = []
    tool_calls: List[Dict[str, Any]] = []
    if isinstance(content, str):
        text_parts.append(content)
    elif isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                if isinstance(block, str):
                    text_parts.append(block)
                continue
            btype = block.get("type")
            if btype == "text":
                t = block.get("text")
                if t:
                    text_parts.append(str(t))
            elif btype == "tool_use":
                try:
                    args_str = json.dumps(block.get("input") or {})
                except (TypeError, ValueError):
                    args_str = "{}"
                tool_calls.append({
                    "id": str(block.get("id") or ""),
                    "type": "function",
                    "function": {
                        "name": str(block.get("name") or ""),
                        "arguments": args_str,
                    },
                })
    msg: Dict[str, Any] = {"role": "assistant", "content": "\n".join(p for p in text_parts if p)}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    return msg


def _translate_user_tool_results(content: Any) -> List[Dict[str, Any]]:
    """If a user message consists of ``tool_result`` blocks, emit one OpenAI
    ``{"role": "tool", ...}`` message per result. Returns an empty list when
    there are no tool_result blocks (caller falls back to plain flattening).
    """
    if not isinstance(content, list):
        return []
    out: List[Dict[str, Any]] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "tool_result":
            continue
        tool_call_id = str(block.get("tool_use_id") or "")
        value = block.get("content")
        if isinstance(value, list):
            value = _flatten_content(value)
        elif not isinstance(value, str):
            value = str(value or "")
        out.append({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": value,
        })
    return out


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
        raw_content = msg.get("content", "")

        if role == "assistant":
            messages.append(_translate_assistant_message(raw_content))
            continue

        if role == "user":
            # A user message may be a tool_result round (list of tool_result blocks)
            # or a normal text turn. Split accordingly so tool context survives.
            tool_msgs = _translate_user_tool_results(raw_content)
            if tool_msgs:
                messages.extend(tool_msgs)
                # Preserve any residual text blocks alongside the tool results.
                if isinstance(raw_content, list):
                    residual_text = _flatten_content([
                        b for b in raw_content
                        if isinstance(b, dict) and b.get("type") != "tool_result"
                    ]).strip()
                    if residual_text:
                        messages.append({"role": "user", "content": residual_text})
                continue

        if role not in ("system", "user", "assistant", "tool"):
            role = "user"
        messages.append({"role": role, "content": _flatten_content(raw_content)})

    out: Dict[str, Any] = {
        "model": resolved_model,
        "messages": messages,
        "max_tokens": int(payload.get("max_tokens") or 800),
    }
    for key in ("temperature", "top_p", "stop"):
        if key in payload and payload[key] is not None:
            out[key] = payload[key]

    tools = payload.get("tools")
    if tools:
        translated = _anthropic_tools_to_openai(tools)
        if translated:
            out["tools"] = translated
            choice = payload.get("tool_choice")
            if isinstance(choice, dict):
                ctype = choice.get("type")
                if ctype == "auto":
                    out["tool_choice"] = "auto"
                elif ctype == "any":
                    out["tool_choice"] = "required"
                elif ctype == "tool" and choice.get("name"):
                    out["tool_choice"] = {"type": "function", "function": {"name": choice["name"]}}
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
    finish_reason = choice.get("finish_reason")
    usage = data.get("usage") or {}

    content_blocks: List[Dict[str, Any]] = []

    text = msg.get("content") or ""
    if isinstance(text, list):
        text = _flatten_content(text)
    text = str(text)
    if text:
        content_blocks.append({"type": "text", "text": text})

    tool_calls = msg.get("tool_calls") or []
    if isinstance(tool_calls, list):
        for tc in tool_calls:
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function") or {}
            raw_args = fn.get("arguments") or "{}"
            try:
                parsed_args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
            except (json.JSONDecodeError, TypeError):
                parsed_args = {"_raw_arguments": str(raw_args)}
            content_blocks.append({
                "type": "tool_use",
                "id": str(tc.get("id") or ""),
                "name": str(fn.get("name") or ""),
                "input": parsed_args if isinstance(parsed_args, dict) else {"_value": parsed_args},
            })

    if not content_blocks:
        content_blocks = [{"type": "text", "text": ""}]

    # Force stop_reason=tool_use when the model emitted tool calls, even if a
    # provider returned a different finish_reason (some NIM models return "stop").
    has_tool_use = any(b.get("type") == "tool_use" for b in content_blocks)
    stop_reason = _OAI_STOP_TO_ANTHROPIC.get(finish_reason, "end_turn")
    if has_tool_use:
        stop_reason = "tool_use"

    return {
        "id": data.get("id") or "",
        "type": "message",
        "role": "assistant",
        "model": resolved_model,
        "content": content_blocks,
        "stop_reason": stop_reason,
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
