import os
from typing import Any

import httpx

from config.logging_config import get_logger

logger = get_logger("web_search")

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
TAVILY_SEARCH_URL = "https://api.tavily.com/search"
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("SEARCH_TIMEOUT_SECONDS", "8"))


def _get_tavily_api_key() -> str:
    return str(os.getenv("TAVILY_API_KEY") or "").strip()


def _get_brave_api_key() -> str:
    return (
        str(os.getenv("BRAVE_SEARCH_API_KEY") or "").strip()
        or str(os.getenv("SEARCH_API_KEY") or "").strip()
    )


def _normalize_result(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": item.get("title") or "",
        "url": item.get("url") or "",
        "description": item.get("description") or item.get("snippet") or item.get("content") or "",
        "age": item.get("age") or "",
        "language": item.get("language") or "",
        "family_friendly": item.get("family_friendly"),
    }


def _tavily_search(query: str, max_results: int) -> dict[str, Any] | None:
    """Search via Tavily API. Returns normalized result dict or None on failure."""
    api_key = _get_tavily_api_key()
    if not api_key:
        return None

    body = {
        "api_key": api_key,
        "query": query,
        "max_results": max(1, min(int(max_results or 5), 20)),
        "include_answer": False,
        "search_depth": "basic",
    }

    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT_SECONDS, follow_redirects=True) as client:
            response = client.post(TAVILY_SEARCH_URL, json=body)
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        logger.warning("Tavily search failed for %r: %s", query, exc)
        return None

    raw_results = payload.get("results") or []
    normalized = [
        _normalize_result({
            "title": r.get("title") or "",
            "url": r.get("url") or "",
            "description": r.get("content") or "",
        })
        for r in raw_results
        if isinstance(r, dict) and r.get("url")
    ]

    logger.info("Tavily search ok: query=%r results=%d", query, len(normalized))
    return {
        "query": query,
        "results": normalized,
        "web": {"results": normalized},
        "source": "tavily",
    }


def _brave_search(query: str, max_results: int) -> dict[str, Any] | None:
    """Search via Brave Search API. Returns normalized result dict or None on failure."""
    api_key = _get_brave_api_key()
    if not api_key:
        return None

    params = {
        "q": query,
        "count": max(1, min(int(max_results or 5), 20)),
        "text_decorations": False,
        "spellcheck": True,
    }
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
    }

    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT_SECONDS, follow_redirects=True) as client:
            response = client.get(BRAVE_SEARCH_URL, params=params, headers=headers)
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        logger.warning("Brave search failed for %r: %s", query, exc)
        return None

    raw_results = (((payload or {}).get("web") or {}).get("results") or [])
    normalized = [
        _normalize_result(item)
        for item in raw_results
        if isinstance(item, dict) and item.get("url")
    ]

    logger.info("Brave search ok: query=%r results=%d", query, len(normalized))
    return {
        "query": query,
        "results": normalized,
        "web": {"results": normalized},
        "source": "brave",
        "mixed": payload.get("mixed") if isinstance(payload, dict) else None,
        "locations": payload.get("locations") if isinstance(payload, dict) else None,
    }


def web_search(query: str, max_results: int = 5) -> dict[str, Any]:
    """Search the web. Tries Tavily first, falls back to Brave Search."""
    query = str(query or "").strip()
    if not query:
        return {"query": "", "results": [], "web": {"results": []}, "note": "Empty query"}

    # Try Tavily first (primary)
    result = _tavily_search(query, max_results)
    if result and result.get("results"):
        return result

    # Fall back to Brave
    result = _brave_search(query, max_results)
    if result and result.get("results"):
        return result

    # Both failed or returned no results
    has_any_key = _get_tavily_api_key() or _get_brave_api_key()
    if not has_any_key:
        logger.info("Web search requested without any search API key")
        return {
            "query": query,
            "results": [],
            "web": {"results": []},
            "note": "No search API key configured (set TAVILY_API_KEY or BRAVE_SEARCH_API_KEY)",
        }

    logger.warning("All search providers returned no results for %r", query)
    return {
        "query": query,
        "results": [],
        "web": {"results": []},
        "note": "All search providers returned no results",
    }
