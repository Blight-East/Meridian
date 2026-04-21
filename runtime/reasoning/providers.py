from __future__ import annotations

import json
import os
import re
import time

import requests
from dotenv import load_dotenv


ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(ENV_FILE_PATH, override=False)


class ProviderError(RuntimeError):
    def __init__(self, message: str, *, raw_output=None):
        super().__init__(message)
        self.raw_output = raw_output or {}


def provider_is_configured(provider_name: str) -> bool:
    if provider_name == "deepseek":
        return bool(os.getenv("DEEPSEEK_API_KEY"))
    if provider_name == "azure_openai":
        return bool(
            os.getenv("AZURE_OPENAI_API_KEY")
            and os.getenv("AZURE_OPENAI_ENDPOINT")
            and os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        )
    if provider_name == "nvidia_openai_compatible":
        return bool((os.getenv("NVIDIA_API_KEY") or os.getenv("NVAPI_KEY")) and _nvidia_base_url())
    if provider_name == "github_models":
        return bool(os.getenv("GITHUB_TOKEN") and _github_models_endpoint())
    return False


def call_model(
    provider_name: str,
    model_name: str,
    messages: list[dict],
    temperature: float,
    max_tokens: int,
    schema,
    timeout_ms: int,
):
    started = time.time()
    if provider_name == "deepseek":
        raw_output = _call_deepseek(
            model_name=model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_ms=timeout_ms,
        )
    elif provider_name == "nvidia_openai_compatible":
        raw_output = _call_nvidia_openai_compatible(
            model_name=model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_ms=timeout_ms,
        )
    elif provider_name == "azure_openai":
        raw_output = _call_azure_openai(
            model_name=model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_ms=timeout_ms,
        )
    elif provider_name == "github_models":
        raw_output = _call_github_models(
            model_name=model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_ms=timeout_ms,
        )
    else:
        raise ProviderError(f"unsupported_provider:{provider_name}")

    raw_text = _message_content(raw_output)
    parsed = _extract_json_payload(raw_text)
    validated = schema["validate"](parsed)
    return {
        "validated_output": validated,
        "raw_output": raw_output,
        "latency_ms": round((time.time() - started) * 1000, 2),
    }


def _call_deepseek(*, model_name: str, messages: list[dict], temperature: float, max_tokens: int, timeout_ms: int):
    api_key = os.getenv("DEEPSEEK_API_KEY")
    base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1").rstrip("/")
    if not api_key:
        raise ProviderError("provider_not_configured:deepseek")
    return _post_chat_completion(
        url=f"{base_url}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        payload={
            "model": model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        },
        timeout_ms=timeout_ms,
    )


def _call_nvidia_openai_compatible(
    *,
    model_name: str,
    messages: list[dict],
    temperature: float,
    max_tokens: int,
    timeout_ms: int,
):
    api_key = os.getenv("NVIDIA_API_KEY") or os.getenv("NVAPI_KEY")
    base_url = _nvidia_base_url()
    if not api_key or not base_url:
        raise ProviderError("provider_not_configured:nvidia_openai_compatible")
    payload = {
        "model": model_name,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
    }
    top_p = _env_float("NVIDIA_TOP_P", None)
    if top_p is not None:
        payload["top_p"] = top_p
    if _env_bool("NVIDIA_CHAT_TEMPLATE_THINKING", False):
        payload["extra_body"] = {"chat_template_kwargs": {"thinking": True}}
    return _post_chat_completion(
        url=f"{base_url}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        payload=payload,
        timeout_ms=timeout_ms,
    )


def _call_azure_openai(*, model_name: str, messages: list[dict], temperature: float, max_tokens: int, timeout_ms: int):
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    endpoint = (os.getenv("AZURE_OPENAI_ENDPOINT") or "").rstrip("/")
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", model_name)
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21")
    if not api_key or not endpoint or not deployment:
        raise ProviderError("provider_not_configured:azure_openai")
    return _post_chat_completion(
        url=f"{endpoint}/openai/deployments/{deployment}/chat/completions",
        headers={
            "api-key": api_key,
            "Content-Type": "application/json",
        },
        payload={
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout_ms=timeout_ms,
        params={"api-version": api_version},
    )


def _call_github_models(
    *,
    model_name: str,
    messages: list[dict],
    temperature: float,
    max_tokens: int,
    timeout_ms: int,
):
    api_key = os.getenv("GITHUB_TOKEN")
    endpoint = _github_models_endpoint()
    api_version = os.getenv("GITHUB_MODELS_API_VERSION", "2026-03-10")
    if not api_key or not endpoint:
        raise ProviderError("provider_not_configured:github_models")
    return _post_chat_completion(
        url=f"{endpoint}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": api_version,
            "Content-Type": "application/json",
        },
        payload={
            "model": model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        },
        timeout_ms=timeout_ms,
    )


def _post_chat_completion(*, url: str, headers: dict, payload: dict, timeout_ms: int, params: dict | None = None):
    try:
        response = requests.post(
            url,
            params=params,
            headers=headers,
            json=payload,
            timeout=max(float(timeout_ms) / 1000.0, 1.0),
        )
    except requests.Timeout as exc:
        raise ProviderError("timeout") from exc
    except requests.RequestException as exc:
        raise ProviderError(f"request_error:{exc.__class__.__name__}") from exc
    if response.status_code >= 400:
        raise ProviderError(f"http_error:{response.status_code}", raw_output=_safe_json(response))
    return _safe_json(response)


def _nvidia_base_url() -> str:
    return (os.getenv("NVIDIA_BASE_URL") or "https://integrate.api.nvidia.com/v1").rstrip("/")


def _github_models_endpoint() -> str:
    return (os.getenv("GITHUB_MODELS_ENDPOINT") or "https://models.github.ai/inference").rstrip("/")


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(key: str, default: float | None) -> float | None:
    value = os.getenv(key)
    if value is None or not str(value).strip():
        return default
    try:
        return float(value)
    except Exception:
        return default


def _safe_json(response):
    try:
        return response.json()
    except Exception:
        return {"raw_text": response.text}


def _message_content(payload) -> str:
    if not isinstance(payload, dict):
        raise ProviderError("invalid_response_payload", raw_output={"raw": str(payload)})
    choices = payload.get("choices") or []
    if not choices:
        raise ProviderError("missing_choices", raw_output=payload)
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
            elif isinstance(item, str):
                parts.append(item)
        if parts:
            return "\n".join(parts)
    reasoning = message.get("reasoning_content")
    if reasoning:
        return str(reasoning)
    raise ProviderError("missing_message_content", raw_output=payload)


def _extract_json_payload(text: str):
    raw = (text or "").strip()
    if not raw:
        raise ProviderError("empty_model_output", raw_output={"raw_text": raw})
    candidates = [raw]
    fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL)
    candidates.extend(fenced)
    brace_start = raw.find("{")
    brace_end = raw.rfind("}")
    if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
        candidates.append(raw[brace_start : brace_end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except Exception:
            continue
    raise ProviderError("invalid_json_output", raw_output={"raw_text": raw})
