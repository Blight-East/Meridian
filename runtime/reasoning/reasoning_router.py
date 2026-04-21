"""
Reasoning Router for Agent Flux

Routes reasoning tasks to either:
- Tier 1: claude-3-5-sonnet (fast, default)
- Tier 2: claude-3-opus (heavy, escalation)

Based on task type, severity, and merchant value.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from runtime.reasoning.claude import reason_fast, reason_heavy
from config.logging_config import get_logger

logger = get_logger("reasoning_router")


def route_reasoning(task_type, severity="low", merchant_value=0, context_size=0):
    """
    Decide which reasoning tier to use for a given task.
    Returns: (model_name, reasoning_function)
    """
    use_opus = False
    reason = "default"

    if task_type == "cluster_analysis":
        if severity in ("high", "critical"):
            use_opus = True
            reason = f"cluster severity {severity}"
    
    elif task_type == "lead_investigation":
        if merchant_value >= 10000:
            use_opus = True
            reason = f"merchant value >= 10k ({merchant_value})"
            
    elif task_type == "pattern_prediction":
        use_opus = True
        reason = "pattern prediction always uses Opus"
        
    elif task_type == "critical_processor_event":
        use_opus = True
        reason = "critical processor event"
        
    elif task_type == "sales_conversation_strategy":
        use_opus = True
        reason = "sales strategy always uses Opus"

    # Fallback / Default
    if use_opus:
        logger.info(f"Routing to OPUS (heavy) for {task_type}. Reason: {reason}")
        try:
            from memory.structured.db import save_event
            save_event("reasoning_router_escalation", {"task_type": task_type, "reason": reason})
        except Exception:
            pass
        return "opus", reason_heavy
    else:
        logger.info(f"Routing to SONNET (fast) for {task_type}. Reason: {reason}")
        return "sonnet", reason_fast
