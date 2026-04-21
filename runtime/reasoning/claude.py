import os
import requests
import json
import hashlib
import time
import redis
from dotenv import dotenv_values, load_dotenv

ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(ENV_FILE_PATH, override=True)
from config.logging_config import get_logger
from safety.rate_limiter import rate_limiter

rate_limiter.limits["llm_call_sonnet"] = 60
rate_limiter.limits["llm_call_opus"] = 10

logger = get_logger("intelligence")

ANTHROPIC_API_KEY = None

# Redis-backed reasoning cache (12 hour TTL)
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
CACHE_TTL = 43200  # 12 hours
CACHE_PREFIX = "reasoning_cache:"


def validate_anthropic_key():
    global ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        env_file_values = dotenv_values(ENV_FILE_PATH)
        ANTHROPIC_API_KEY = env_file_values.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY missing. LLM reasoning will be unavailable until configured.")
        return False
    logger.info("Anthropic API key loaded successfully.")
    return True

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


def _execute_llm_call(prompt, task, use_cache, model_id, rate_limit_key, reasoning_type):
    """Internal function to handle the standard LLM reasoning flow, caching, and parsing."""
    try:
        rate_limiter.check(rate_limit_key)
    except RuntimeError as e:
        logger.warning(f"Rate limit hit for {model_id}: {e}")
        raise

    # Check cache before making API call
    if use_cache:
        # 1. Exact match cache
        cached = _get_cached(prompt)
        if cached is not None:
            if isinstance(cached, dict):
                # Add metadata
                cached["_meta"] = {"model": model_id, "reasoning_type": reasoning_type, "source": "exact_cache"}
            return cached
            
        # 2. Semantic match cache
        try:
            from runtime.intelligence.semantic_memory import retrieve_similar_signals
            similar = retrieve_similar_signals(task if isinstance(task, str) else str(task), limit=1, threshold=0.92)
            if similar:
                historical_text = similar[0]["content"]
                
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
                    
                    if isinstance(hist_cached, dict):
                        hist_cached["_meta"] = {"model": model_id, "reasoning_type": reasoning_type, "source": "semantic_cache"}
                    return hist_cached
        except Exception as e:
            logger.warning(f"Semantic cache check failed: {e}")

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model_id,
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Anthropic API call failed for {model_id}: {e}")
        raise RuntimeError(f"API Error: {e}") 

    result = response.json()

    if "content" in result and result["content"]:
        raw = result["content"][0]["text"].strip()
        
        from memory.structured.db import save_event
        save_event("llm_reasoning_call", {"model": model_id, "task": str(task)[:200]})
        
        try:
            parsed = json.loads(raw)
            if use_cache:
                _set_cached(prompt, parsed)
            parsed["_meta"] = {"model": model_id, "reasoning_type": reasoning_type, "source": "llm"}
            return parsed
        except json.JSONDecodeError:
            if use_cache:
                _set_cached(prompt, raw)
            return raw

    return {"error": str(result)}


def _build_prompt(task):
    """Build standardized LLM prompt based on task input type."""
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
        return prompt, False
    else:
        prompt = f"""You are Agent Flux, an operations agent.
Interpret the following task and return a short execution plan.

Task: {task}

Respond with JSON only, no markdown:
{{"action": "...", "notes": "..."}}"""
        return prompt, True


def reason_fast(task, use_cache=True):
    """
    Tier 1 Reasoning: Claude 3.5 Sonnet
    Used for bulk analysis and standard investigations. Max 60/hr.
    """
    prompt, default_cache = _build_prompt(task)
    _cache = use_cache and default_cache
    
    try:
        return _execute_llm_call(
            prompt=prompt, 
            task=task,
            use_cache=_cache,
            model_id="claude-sonnet-4-6",
            rate_limit_key="llm_call_sonnet",
            reasoning_type="fast"
        )
    except RuntimeError as e:
        logger.error(f"Sonnet reasoning failed: {e}")
        return {"error": str(e)}


def reason_heavy(task, use_cache=True):
    """
    Tier 2 Reasoning: Claude 3 Opus
    Used for critical paths, pattern prediction, strategy generation. Max 10/hr.
    Gracefully falls back to Sonnet if rate limited.
    """
    prompt, default_cache = _build_prompt(task)
    _cache = use_cache and default_cache
    
    try:
        return _execute_llm_call(
            prompt=prompt, 
            task=task,
            use_cache=_cache,
            model_id="claude-opus-4-6",
            rate_limit_key="llm_call_opus",
            reasoning_type="heavy"
        )
    except RuntimeError as e:
        if "Rate limit hit" in str(e):
            logger.warning("Opus rate limit hit! Downgrading request to Sonnet.")
            
            try:
                res = _execute_llm_call(
                    prompt=prompt, 
                    task=task,
                    use_cache=_cache,
                    model_id="claude-sonnet-4-6",
                    rate_limit_key="llm_call_sonnet", # Uses default sonnet limit for fallback
                    reasoning_type="degraded_heavy_to_fast"
                )
                if isinstance(res, dict):
                    res["reasoning_degraded"] = True
                    try:
                        from memory.structured.db import save_event
                        save_event("reasoning_degraded", {"original_model": "claude-3-opus-20240229"})
                    except Exception:
                        pass
                return res
            except Exception as sonnet_e:
                 logger.error(f"Fallback Sonnet reasoning also failed: {sonnet_e}")
                 return {"error": str(sonnet_e)}
        else:
             logger.error(f"Opus reasoning failed: {e}")
             return {"error": str(e)}

# Backward compatibility (wraps router/fast)
def reason(task, use_cache=True):
    """Legacy interface mapping to Tier 1 default reasoning."""
    return reason_fast(task, use_cache)
