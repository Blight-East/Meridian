"""
Pattern Prediction Hook
Forecasts processor outages and regulatory events using Opus.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from runtime.reasoning.reasoning_router import route_reasoning
from config.logging_config import get_logger

logger = get_logger("pattern_prediction")


def generate_prediction(cluster_topic, signal_velocity, historical_semantic_matches):
    """
    Forecasts systemic events based on real-time signal velocity and historical analogues.
    """
    model_name, reasoning_func = route_reasoning(
        task_type="pattern_prediction",
        severity="high"
    )
    
    prompt = f"""
You are Agent Flux's predictive intelligence core. Analyze this accelerating cluster and its historical analogues to forecast a potential systemic event (e.g., massive processor outage, regulatory crackdown).

Cluster Topic: {cluster_topic}
Current Velocity: {signal_velocity} signals/hour

Historical Semantic Analogues:
{chr(10).join(['- ' + str(h) for h in historical_semantic_matches])}

Based on how similar past events escalated, predict the most likely trajectory.
Return a valid JSON object ONLY (no markdown blocks) with exactly these keys:
"predicted_event": (string, what will happen)
"confidence": (float between 0.0 and 1.0)
"expected_time_window": (string, e.g., '24h', '72h', '1 week')
"impact_radius": (string, 'low', 'medium', 'high')
"""
    try:
        response = reasoning_func(prompt, use_cache=False)
        try:
            from memory.structured.db import save_event
            save_event("pattern_prediction_generated", {"topic": cluster_topic})
        except Exception:
            pass
        return response
    except Exception as e:
        logger.error(f"Pattern prediction generation failed: {e}")
        return {"error": str(e)}
