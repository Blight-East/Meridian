import os
import requests
import json
import hashlib
import time
import redis
from config.logging_config import get_logger
from safety.rate_limiter import rate_limiter

logger = get_logger("intelligence")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Redis-backed reasoning cache (12 hour TTL)
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
CACHE_TTL = 43200  # 12 hours
CACHE_PREFIX = "reasoning_cache:"


def validate_anthropic_key():
    global ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY missing. Configure /etc/environment")
    logger.info("Anthropic API key loaded successfully.")

validate_anthropic_key()


def _cache_key(prompt_text):
    """Generate a stable cache key from prompt content."""
    return CACHE_PREFIX + hashlib.sha256(prompt_text.encode()).hexdigest()[:32]


def _get_cached(prompt_text):
    """Check Redis for a cached reasoning result."""
    try:
        key = _cache_key(prompt_text)
        cached = _redis.get(key)
        if cached:
            logger.info(f"Reasoning cache hit: {key}")
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Cache read error: {e}")
    return None


def _set_cached(prompt_text, result):
    """Store a reasoning result in Redis with TTL."""
    try:
        key = _cache_key(prompt_text)
        _redis.setex(key, CACHE_TTL, json.dumps(result))
    except Exception as e:
        logger.warning(f"Cache write error: {e}")


def reason(task, use_cache=True):
    """
    Core LLM reasoning function. All autonomous Claude API calls route through here.

    - Rate limited via safety/rate_limiter.py (llm_call: 30/hour)
    - Optionally cached in Redis (12h TTL) to avoid redundant API calls
    - Conversational chat messages do NOT go through this function (they use chat_engine.py)
    """
    # Rate limit check — raises RuntimeError if exceeded
    rate_limiter.check("llm_call")

    if isinstance(task, dict) and "message" in task and "history" in task:
        message = task.get("message", "")
        history = task.get("history", [])

        hist_text = ""
        for h in history:
            role = h.get("role", "unknown").upper()
            content = h.get("content", "")
            hist_text += f"{role}: {content}\n"

        prompt = f"""You are Agent Flux, an operations AI.

Conversation history:
{hist_text}

User message:
{message}

Respond with either:
1) Direct answer
2) Action plan in JSON formatted like: {{"action": "...", "notes": "..."}}

Do not use markdown blocks for JSON, just valid JSON if returning an action plan."""

        # Conversational reasoning is not cached (context-dependent)
        use_cache = False
    else:
        prompt = f"""You are Agent Flux, an operations agent.
Interpret the following task and return a short execution plan.

Task: {task}

Respond with JSON only, no markdown:
{{"action": "...", "notes": "..."}}"""

    # Check cache before making API call
    if use_cache:
        # 1. Exact match cache
        cached = _get_cached(prompt)
        if cached is not None:
            return cached
            
        # 2. Semantic match cache (avoid API cost if we've seen a nearly identical situation)
        try:
            from semantic_memory import retrieve_similar_signals
            similar = retrieve_similar_signals(task if isinstance(task, str) else str(task), limit=1, threshold=0.92)
            if similar:
                # Need to lookup the cached analysis for the historically matched text
                historical_text = similar[0]["content"]
                
                # Reconstruct the exact prompt that would have been used for the historical text
                # (This works best if the 'task' is just the cluster topic/text)
                hist_prompt = f"""You are Agent Flux, an operations agent.
Interpret the following task and return a short execution plan.

Task: {historical_text}

Respond with JSON only, no markdown:
{{"action": "...", "notes": "..."}}"""
                
                hist_cached = _get_cached(hist_prompt)
                if hist_cached is not None:
                    logger.info(f"Semantic Cache Hit! Bypassing API. Matched: '{historical_text}' (score: {similar[0]['similarity']})")
                    from memory.structured.db import save_event
                    save_event("semantic_cache_hit", {"query": str(task), "matched": historical_text})
                    return hist_cached
        except Exception as e:
            logger.warning(f"Semantic cache check failed: {e}")

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=20,
    )

    result = response.json()

    if "content" in result and result["content"]:
        raw = result["content"][0]["text"].strip()
        try:
            parsed = json.loads(raw)
            if use_cache:
                _set_cached(prompt, parsed)
            return parsed
        except json.JSONDecodeError:
            if use_cache:
                _set_cached(prompt, raw)
            return raw

    return {"error": str(result)}
