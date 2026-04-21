import sys, os, json, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.ops.alerts import send_operator_alert
from runtime.intelligence.processor_classifier import classify_processor
from runtime.intelligence.cluster_reasoning import generate_cluster_reasoning, generate_batch_cluster_reasoning

logger = get_logger("cluster_investigator")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

INVESTIGATION_THRESHOLD = 25
TREND_CHANGE_THRESHOLD = 50.0


def _estimate_merchant_value(content, qual_score, rev_detected):
    value = 0
    if content:
        amounts = re.findall(r'\$([0-9,]+(?:\.[0-9]{2})?|[0-9]+[kKmM])', content)
        for amt in amounts:
            amt_str = amt.upper().replace(',', '')
            if 'K' in amt_str:
                v = float(amt_str.replace('K', '')) * 1000
            elif 'M' in amt_str:
                v = float(amt_str.replace('M', '')) * 1000000
            else:
                try:
                    v = float(amt_str)
                except ValueError:
                    v = 0
            value += v
    
    if qual_score:
        value += (qual_score * 10)
        
    if rev_detected:
        value += 5000
        
    return value


def _extract_cluster_intelligence(cluster_id, cluster_topic, cluster_size, signal_ids_json):
    """
    Extract structured intelligence from a cluster's signals (DB-only, no LLM calls).
    Returns a dict with all extracted data, or None if no signals found.
    """
    try:
        signal_ids = json.loads(signal_ids_json) if isinstance(signal_ids_json, str) else signal_ids_json
    except (json.JSONDecodeError, TypeError):
        signal_ids = []

    with engine.connect() as conn:
        if signal_ids:
            placeholders = ",".join([str(int(sid)) for sid in signal_ids[:200]])
            rows = conn.execute(text(f"""
                SELECT s.id, s.content, s.merchant_name, s.industry, s.region, s.source,
                       q.revenue_detected, q.qualification_score, q.processor
                FROM signals s
                LEFT JOIN qualified_leads q ON s.id = q.signal_id
                WHERE s.id IN ({placeholders})
                ORDER BY q.qualification_score DESC NULLS LAST
            """)).fetchall()
        else:
            rows = conn.execute(text("""
                SELECT s.id, s.content, s.merchant_name, s.industry, s.region, s.source,
                       q.revenue_detected, q.qualification_score, q.processor
                FROM signals s
                LEFT JOIN qualified_leads q ON s.id = q.signal_id
                WHERE s.detected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY q.qualification_score DESC NULLS LAST
                LIMIT :limit
            """), {"limit": cluster_size}).fetchall()

        if not rows:
            logger.warning(f"No signals found for cluster {cluster_topic}")
            return None

        unique_merchants = set()
        industries = {}
        processors = {}
        sources = {}
        regions = {}
        revenue_signals = 0
        high_score_signals = 0
        sample_contents = []
        total_merchant_value = 0

        for r in rows:
            s_id, content, merchant, industry, region, source, rev_detected, qual_score, lead_proc = r
            
            total_merchant_value += _estimate_merchant_value(content, qual_score, rev_detected)

            if merchant and merchant != "unknown":
                unique_merchants.add(merchant)
            if industry and industry != "unknown":
                industries[industry] = industries.get(industry, 0) + 1
            if lead_proc and lead_proc != "unknown":
                processors[lead_proc] = processors.get(lead_proc, 0) + 1
            else:
                proc, conf = classify_processor(content or "")
                if proc != "unknown":
                    processors[proc] = processors.get(proc, 0) + 1
            if source:
                sources[source] = sources.get(source, 0) + 1
            if region and region != "unknown":
                regions[region] = regions.get(region, 0) + 1
            if rev_detected:
                revenue_signals += 1
            if qual_score and qual_score >= 70:
                high_score_signals += 1
            if content and len(sample_contents) < 8:
                sample_contents.append(content[:200])

        dom_industry = max(industries, key=industries.get) if industries else "unknown"
        dom_processor = max(processors, key=processors.get) if processors else "unknown"
        dom_source = max(sources, key=sources.get) if sources else "unknown"
        dom_region = max(regions, key=regions.get) if regions else "unknown"

        task_type = "cluster_analysis"
        severity = "low"
        if cluster_size >= 50 or revenue_signals >= 10:
            severity = "critical"
        elif cluster_size >= 25 or revenue_signals >= 5:
            severity = "high"
        elif cluster_size >= 10:
            severity = "medium"
            
        if total_merchant_value >= 10000:
            task_type = "lead_investigation"
            severity = "high"

        summary = (
            f"{len(unique_merchants)} unique merchants reporting {cluster_topic.lower()} issues. "
            f"Primary processor: {dom_processor}. Primary industry: {dom_industry}. "
            f"{revenue_signals} signals indicate direct revenue impact."
        )

        # Store intelligence record
        conn.execute(text("""
            INSERT INTO cluster_intelligence
                (cluster_id, processor, industry, cluster_size, merchant_count, intelligence_summary)
            VALUES (:cid, :p, :i, :s, :m, :summ)
        """), {
            "cid": str(cluster_id),
            "p": dom_processor,
            "i": dom_industry,
            "s": cluster_size,
            "m": len(unique_merchants),
            "summ": summary
        })
        conn.commit()

    return {
        "cluster_id": cluster_id,
        "cluster_topic": cluster_topic,
        "cluster_size": cluster_size,
        "unique_merchants": unique_merchants,
        "processors": processors,
        "dom_processor": dom_processor,
        "dom_industry": dom_industry,
        "dom_source": dom_source,
        "dom_region": dom_region,
        "severity": severity,
        "revenue_signals": revenue_signals,
        "summary": summary,
        "sample_contents": sample_contents,
        "reasoning_input": {
            "task_type": task_type,
            "merchant_value": total_merchant_value,
            "cluster_topic": cluster_topic,
            "processor": dom_processor,
            "industry": dom_industry,
            "cluster_size": cluster_size,
            "merchant_count": len(unique_merchants),
            "revenue_signals": revenue_signals,
            "severity": severity,
            "primary_source": dom_source,
            "primary_region": dom_region,
            "sample_signals": sample_contents
        }
    }


def _store_and_alert(intel, reasoning):
    """Store analysis results in DB and send operator alert for one cluster."""
    cluster_id = intel["cluster_id"]
    cluster_topic = intel["cluster_topic"]
    severity = intel["severity"]

    with engine.connect() as conn:
        if reasoning:
            conn.execute(text("""
                INSERT INTO cluster_analysis (cluster_id, analysis, risk_level, predicted_outcome)
                VALUES (:cid, :a, :r, :p)
            """), {
                "cid": str(cluster_id),
                "a": reasoning.get("analysis", "No analysis"),
                "r": reasoning.get("risk_level", severity),
                "p": reasoning.get("predicted_next_event", "unknown")
            })
            conn.commit()

    proc_breakdown = ", ".join(
        [f"{p}: {c}" for p, c in sorted(intel["processors"].items(), key=lambda x: -x[1])]
    ) or "none identified"
    merchants_display = ", ".join(list(intel["unique_merchants"])[:5]) or "none identified"

    alert = (
        f"CLUSTER INVESTIGATION REPORT\n"
        f"{'=' * 35}\n"
        f"Cluster: {cluster_topic}\n"
        f"Severity: {severity.upper()}\n"
        f"Signal count: {intel['cluster_size']}\n"
        f"Unique merchants: {len(intel['unique_merchants'])}\n"
        f"Revenue-impacted signals: {intel['revenue_signals']}\n"
        f"\n"
        f"Processor breakdown: {proc_breakdown}\n"
        f"Industry: {intel['dom_industry']}\n"
        f"Primary source: {intel['dom_source']}\n"
        f"\n"
        f"Top merchants: {merchants_display}\n"
        f"\n"
        f"Analysis:\n{reasoning.get('analysis', intel['summary'])}\n"
        f"\n"
        f"Risk level: {reasoning.get('risk_level', severity)}\n"
        f"Predicted next event: {reasoning.get('predicted_next_event', 'Unknown')}"
    )
    send_operator_alert(alert)

    save_event("cluster_investigation_completed", {
        "cluster_id": cluster_id,
        "topic": cluster_topic,
        "severity": severity,
        "merchant_count": len(intel["unique_merchants"]),
        "processor": intel["dom_processor"]
    })
    logger.info(f"Cluster investigation completed: {cluster_topic} [severity={severity}]")


def run_cluster_investigation():
    """
    Autonomous cluster investigation scanner.
    Triggered every 20 minutes by the scheduler.

    Phase 1: Extract intelligence from DB for all qualifying clusters (no LLM)
    Phase 2: Batch LLM reasoning — single API call when multiple clusters qualify,
             individual calls when only one qualifies
    Phase 3: Store results and send alerts
    """
    logger.info("Scanning for clusters requiring investigation...")
    try:
        with engine.connect() as conn:
            clusters = conn.execute(text("""
                SELECT id, cluster_topic, cluster_size, signal_ids, trend_change, trend_status
                FROM clusters
                WHERE (cluster_size >= :size_threshold OR trend_change >= :trend_threshold)
                  AND id::text NOT IN (
                      SELECT cluster_id FROM cluster_intelligence
                      WHERE created_at >= NOW() - INTERVAL '12 hours'
                  )
                ORDER BY cluster_size DESC
            """), {
                "size_threshold": INVESTIGATION_THRESHOLD,
                "trend_threshold": TREND_CHANGE_THRESHOLD
            }).fetchall()

            if not clusters:
                logger.info("No clusters currently exceed investigation thresholds.")
                return

        logger.info(f"Found {len(clusters)} clusters exceeding investigation thresholds")

        # Phase 1: Extract intelligence (DB-only, no LLM calls)
        intel_results = []
        for c in clusters:
            try:
                intel = _extract_cluster_intelligence(
                    cluster_id=c[0],
                    cluster_topic=c[1],
                    cluster_size=c[2],
                    signal_ids_json=c[3]
                )
                if intel:
                    intel_results.append(intel)
            except Exception as e:
                logger.error(f"Intelligence extraction error for cluster '{c[1]}': {e}")
                save_event("cluster_investigation_error", {"cluster_id": c[0], "error": str(e)})

        if not intel_results:
            logger.info("No cluster intelligence could be extracted.")
            return

        # Phase 2: LLM reasoning — batch when multiple, single when one
        reasoning_inputs = [ir["reasoning_input"] for ir in intel_results]

        if len(reasoning_inputs) == 1:
            # Single cluster — use standard single-call path
            logger.info(f"Single cluster investigation: {intel_results[0]['cluster_topic']}")
            reasoning_results = [generate_cluster_reasoning(reasoning_inputs[0])]
        else:
            # Multiple clusters — batch into a single LLM call
            logger.info(f"Batched cluster investigation: {len(reasoning_inputs)} clusters in single LLM call")
            reasoning_results = generate_batch_cluster_reasoning(reasoning_inputs)

        # Phase 3: Store results and send alerts
        for intel, reasoning in zip(intel_results, reasoning_results):
            try:
                _store_and_alert(intel, reasoning)
            except Exception as e:
                logger.error(f"Alert/storage error for cluster '{intel['cluster_topic']}': {e}")

    except Exception as e:
        logger.error(f"Cluster investigation scanner error: {e}")
        save_event("cluster_investigation_scanner_error", {"error": str(e)})


if __name__ == "__main__":
    run_cluster_investigation()
