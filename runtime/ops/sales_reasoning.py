"""
Sales Conversation Planning Hook
Generates merchant-specific outreach strategies using Opus.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from runtime.reasoning.reasoning_router import route_reasoning
from config.logging_config import get_logger

logger = get_logger("sales_reasoning")


def generate_sales_strategy(merchant_domain, processor, distress_signals, industry):
    """
    Generates a targeted outreach strategy based on specific merchant intelligence.
    Routes to Opus for deep reasoning.
    """
    model_name, reasoning_func = route_reasoning(
        task_type="sales_conversation_strategy",
        severity="high",
        merchant_value=0
    )
    
    prompt = f"""
You are the Agent Flux sales intelligence brain. Please formulate a merchant-specific outreach strategy for a specialized sales conversation.

Merchant Domain: {merchant_domain}
Assumed Processor: {processor}
Industry: {industry}

Observed Distress Signals:
{chr(10).join(['- ' + s for s in distress_signals])}

Generate an actionable blueprint for outreach.
Return a valid JSON object ONLY (no markdown blocks) with exactly these keys:
"angle": (string, primary outreach angle)
"pain_point": (string, specific liquidity/operations pain point observed)
"recommended_pitch": (string, 1-2 sentences exact phrasing to use)
"objection_handling": [(string), list of predicted objections and how to counter]
"""
    try:
        response = reasoning_func(prompt, use_cache=False)
        try:
            from memory.structured.db import save_event
            save_event("sales_reasoning_call", {"domain": merchant_domain})
        except Exception:
            pass
        return response
    except Exception as e:
        logger.error(f"Sales strategy generation failed: {e}")
        return {"error": str(e)}
