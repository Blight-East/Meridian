import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from runtime.reasoning.claude import reason
from config.logging_config import get_logger

logger = get_logger("cluster_reasoning")

def generate_cluster_reasoning(cluster_data):
    """
    Prompt Claude to analyze merchant distress clusters.
    """
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

Sample Signals:
{chr(10).join([f"- {s}" for s in cluster_data.get('sample_signals', [])])}

Return a valid JSON object ONLY (no markdown blocks) with exactly these keys:
"analysis": (string)
"risk_level": (string, such as 'high', 'medium', 'low')
"predicted_next_event": (string)
"""

    response = reason(prompt)
    if isinstance(response, dict) and "analysis" in response:
        return response
    else:
        logger.error(f"Failed to generate distinct reasoning json: {response}")
        return {
            "analysis": "High likelihood of processor disruption affecting multiple merchants. Immediate attention advised.",
            "risk_level": "medium",
            "predicted_next_event": "Further escalation of disruption."
        }
