import sys, os, json, hashlib
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import redis
from runtime.reasoning.reasoning_router import route_reasoning
from config.logging_config import get_logger
from semantic_memory import retrieve_similar_signals

logger = get_logger("cluster_reasoning")

_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
CACHE_TTL = 43200  # 12 hours
CACHE_PREFIX = "cluster_reasoning:"


def _cache_key(cluster_data):
    """Generate a stable cache key from cluster topic + processor + size range."""
    # Cache on topic + processor + size bucket (rounded to nearest 10)
    # This means similar-sized clusters with same topic reuse analysis
    size_bucket = (cluster_data.get("cluster_size", 0) // 10) * 10
    raw = f"{cluster_data.get('cluster_topic')}:{cluster_data.get('processor')}:{size_bucket}"
    return CACHE_PREFIX + hashlib.sha256(raw.encode()).hexdigest()[:24]


def _get_cached(cluster_data):
    try:
        key = _cache_key(cluster_data)
        cached = _redis.get(key)
        if cached:
            logger.info(f"Cluster reasoning cache hit: {cluster_data.get('cluster_topic')}")
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Cache read error: {e}")
    return None


def _set_cached(cluster_data, result):
    try:
        key = _cache_key(cluster_data)
        _redis.setex(key, CACHE_TTL, json.dumps(result))
    except Exception as e:
        logger.warning(f"Cache write error: {e}")


def _heuristic_analysis(cluster_data):
    """
    Deterministic fallback analysis when the LLM is unavailable (rate limited, API down, etc).
    Uses extracted cluster metrics to produce structured intelligence without an API call.
    """
    topic = cluster_data.get("cluster_topic", "unknown")
    processor = cluster_data.get("processor", "unknown")
    industry = cluster_data.get("industry", "unknown")
    size = cluster_data.get("cluster_size", 0)
    merchant_count = cluster_data.get("merchant_count", 0)
    revenue_signals = cluster_data.get("revenue_signals", 0)
    severity = cluster_data.get("severity", "medium")

    # Determine risk level from severity
    risk_map = {"critical": "critical", "high": "high", "medium": "medium", "low": "low"}
    risk_level = risk_map.get(severity, "medium")

    # Build analysis from available data
    analysis_parts = []

    if processor != "unknown":
        analysis_parts.append(
            f"{processor.title()} is the primary processor involved in this {topic.lower()} cluster."
        )
    else:
        analysis_parts.append(f"Multiple processors may be affected by this {topic.lower()} cluster.")

    analysis_parts.append(f"{size} distress signals detected from {merchant_count} unique merchants.")

    if revenue_signals > 0:
        pct = round((revenue_signals / max(size, 1)) * 100)
        analysis_parts.append(
            f"{revenue_signals} signals ({pct}%) indicate direct revenue impact."
        )

    if industry != "unknown":
        analysis_parts.append(f"Primary industry affected: {industry}.")

    if size >= 50:
        analysis_parts.append("Signal volume indicates a widespread systemic issue.")
    elif size >= 25:
        analysis_parts.append("Signal volume suggests an emerging pattern requiring monitoring.")

    # Predict next event based on topic keywords
    topic_lower = topic.lower()
    if "freeze" in topic_lower or "frozen" in topic_lower:
        predicted = "Escalation to account terminations if freeze conditions persist beyond 48 hours."
    elif "termination" in topic_lower or "shut down" in topic_lower:
        predicted = "Affected merchants will seek alternative processors. Expect migration signals within 72 hours."
    elif "payout" in topic_lower or "reserve" in topic_lower:
        predicted = "Continued payout delays likely. Merchants may escalate to regulatory complaints."
    elif "chargeback" in topic_lower or "dispute" in topic_lower:
        predicted = "Elevated chargeback rates may trigger processor-level reserve requirements."
    elif "gateway" in topic_lower or "outage" in topic_lower:
        predicted = "Gateway recovery expected within 24 hours. Monitor for recurring instability."
    else:
        predicted = "Continued distress signal volume expected. Monitor for cluster growth."

    result = {
        "analysis": " ".join(analysis_parts),
        "risk_level": risk_level,
        "predicted_next_event": predicted,
        "_source": "heuristic"
    }

    logger.info(f"Heuristic analysis generated for {topic} (LLM unavailable)")
    return result


def generate_cluster_reasoning(cluster_data):
    """
    Prompt Claude to analyze a single merchant distress cluster.
    Results are cached for 12 hours keyed on topic + processor + size bucket.
    Falls back to deterministic heuristic analysis if the LLM is rate-limited or unavailable.
    """
    # Check cache first
    cached = _get_cached(cluster_data)
    if cached:
        return cached

    logger.info(f"Generating cluster reasoning for {cluster_data.get('cluster_topic')}...")

    prompt = f"""
Analyze the following merchant distress cluster and determine:
- root cause pattern
- affected industry
- possible processor failure
- expected next development

Cluster Data:
Topic: {cluster_data.get('cluster_topic')}
Dominant Processor: {cluster_data.get('processor')}
Dominant Industry: {cluster_data.get('industry')}
Cluster Size: {cluster_data.get('cluster_size')}
Unique Merchants Involved: {cluster_data.get('merchant_count')}
Revenue-Impacted Signals: {cluster_data.get('revenue_signals', 'unknown')}
Severity: {cluster_data.get('severity', 'unknown')}
Primary Source: {cluster_data.get('primary_source', 'unknown')}
Primary Region: {cluster_data.get('primary_region', 'unknown')}

Sample Signals:
{chr(10).join([f"- {s}" for s in cluster_data.get('sample_signals', [])])}"""

    similar = retrieve_similar_signals(cluster_data.get('cluster_topic', ''), limit=5)
    if similar:
        prompt += f"\n\nSimilar Past Events Detected (for context):\n"
        for s in similar:
            prompt += f"- {s['content']}\n"
    
    prompt += """
Return a valid JSON object ONLY (no markdown blocks) with exactly these keys:
"analysis": (string)
"risk_level": (string, such as 'high', 'medium', 'low')
"predicted_next_event": (string)
"""

    model_name, reasoning_func = route_reasoning(
        task_type="cluster_analysis",
        severity=cluster_data.get('severity', 'low'),
        merchant_value=cluster_data.get('merchant_value', 0)
    )

    try:
        response = reasoning_func(prompt, use_cache=False)
        if isinstance(response, dict) and "analysis" in response:
            _set_cached(cluster_data, response)
            return response
        else:
            logger.error(f"Failed to generate distinct reasoning json: {response}")
            return _heuristic_analysis(cluster_data)
    except RuntimeError as e:
        # Rate limit hit or API unavailable — fail open with heuristic
        logger.warning(f"LLM unavailable for cluster reasoning: {e}")
        return _heuristic_analysis(cluster_data)
    except Exception as e:
        logger.error(f"Unexpected error in cluster reasoning: {e}")
        return _heuristic_analysis(cluster_data)


def generate_batch_cluster_reasoning(clusters_data):
    """
    Analyze multiple clusters in a single LLM call to reduce API usage.
    Used when multiple clusters exceed investigation thresholds simultaneously.

    Args:
        clusters_data: list of cluster_data dicts

    Returns:
        list of reasoning dicts, one per cluster
    """
    if not clusters_data:
        return []

    # Check cache for each — only send uncached ones to the API
    results = [None] * len(clusters_data)
    uncached_indices = []

    for i, cd in enumerate(clusters_data):
        cached = _get_cached(cd)
        if cached:
            results[i] = cached
        else:
            uncached_indices.append(i)

    if not uncached_indices:
        logger.info(f"All {len(clusters_data)} clusters served from cache")
        return results

    logger.info(f"Batch reasoning: {len(uncached_indices)} uncached out of {len(clusters_data)} clusters")

    # Attempt LLM batch call — fall back to heuristics on failure
    try:
        # Format sample signals and construct prompts for uncached clusters
        formatted_clusters = []
        uncached_clusters_data = [clusters_data[i] for i in uncached_indices]

        for idx, c in enumerate(uncached_clusters_data):
            topic = c.get('cluster_topic')
            signals = chr(10).join([f"  - {s}" for s in c.get('sample_signals', [])[:3]])
            block = f"""
--- Cluster {idx + 1} ---
Topic: {topic}
Processor: {c.get('processor')}
Industry: {c.get('industry')}
Size: {c.get('cluster_size')}
Merchants: {c.get('merchant_count')}
Revenue-Impacted: {c.get('revenue_signals', 'unknown')}
Severity: {c.get('severity', 'unknown')}
Sample signals:
{signals}"""
            
            # Pull semantic memory for this cluster
            similar = retrieve_similar_signals(topic, limit=3)
            if similar:
                block += f"\nSimilar Past Events (context):\n"
                for s in similar:
                    block += f"- {s['content']}\n"
                    
            formatted_clusters.append(block)

        joined_clusters = "\n\n".join(formatted_clusters)

        prompt = f"""
Analyze the following {len(uncached_indices)} merchant distress clusters. For EACH cluster provide:
- root cause pattern
- risk level
- predicted next event

{chr(10).join(cluster_blocks)}

Return a valid JSON ARRAY ONLY (no markdown blocks). Each element must have exactly these keys:
"cluster_index": (integer, 1-indexed matching the cluster number above)
"analysis": (string)
"risk_level": (string: 'critical', 'high', 'medium', or 'low')
"predicted_next_event": (string)
"""

        # Determine max severity and value for the batch
        severities = [c.get('severity', 'low') for c in uncached_clusters_data]
        if 'critical' in severities:
            max_severity = 'critical'
        elif 'high' in severities:
            max_severity = 'high'
        elif 'medium' in severities:
            max_severity = 'medium'
        else:
            max_severity = 'low'
            
        merchant_values = [c.get('merchant_value', 0) for c in uncached_clusters_data]
        max_value = max(merchant_values) if merchant_values else 0
            
        model_name, reasoning_func = route_reasoning(
            task_type="cluster_analysis",
            severity=max_severity,
            merchant_value=max_value
        )

        response = reasoning_func(prompt, use_cache=False)

        # Parse batch response
        parsed_results = []
        if isinstance(response, list):
            parsed_results = response
        elif isinstance(response, str):
            try:
                parsed_results = json.loads(response)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse batch reasoning response")
                parsed_results = []

        # Map batch results back to original indices
        for pr in parsed_results:
            if isinstance(pr, dict) and "cluster_index" in pr:
                batch_idx = pr["cluster_index"] - 1  # 1-indexed to 0-indexed
                if 0 <= batch_idx < len(uncached_indices):
                    original_idx = uncached_indices[batch_idx]
                    result = {
                        "analysis": pr.get("analysis", "Analysis unavailable"),
                        "risk_level": pr.get("risk_level", "medium"),
                        "predicted_next_event": pr.get("predicted_next_event", "Unknown")
                    }
                    results[original_idx] = result
                    _set_cached(clusters_data[original_idx], result)

    except RuntimeError as e:
        # Rate limit hit — fail open with heuristics for all uncached clusters
        logger.warning(f"LLM unavailable for batch reasoning: {e}. Falling back to heuristic analysis.")
    except Exception as e:
        logger.error(f"Unexpected error in batch reasoning: {e}. Falling back to heuristic analysis.")

    # Fill any remaining gaps with heuristic analysis
    for i in range(len(results)):
        if results[i] is None:
            results[i] = _heuristic_analysis(clusters_data[i])

    return results
