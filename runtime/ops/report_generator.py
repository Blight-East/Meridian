"""
Merchant Intelligence Report Generator

Automatically publishes market intelligence reports derived from distress clusters.
Reports drive organic traffic and demonstrate PayFlux domain expertise.

Runs every 6 hours via the scheduler.
"""

import re
import json
import requests
import os
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import create_engine, text
from runtime.ops.alerts import send_operator_alert
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("report_generator")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

ANTHROPIC_API_KEY = None


def _get_api_key():
    global ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        env_file_values = dotenv_values(ENV_FILE_PATH)
        ANTHROPIC_API_KEY = env_file_values.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    return ANTHROPIC_API_KEY


def _slugify(title):
    """Convert a report title into a URL-safe slug."""
    slug = title.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s-]+', '-', slug)
    slug = slug.strip('-')
    return slug[:120]


def generate_market_reports():
    """Main entry point: find report-worthy clusters and generate intelligence reports."""
    logger.info("Starting market report generation cycle")

    clusters = _find_report_worthy_clusters()
    if not clusters:
        logger.info("No report-worthy clusters found this cycle")
        return

    logger.info(f"Found {len(clusters)} report-worthy clusters")
    reports_created = 0

    for cluster in clusters:
        try:
            # Check if we already generated a report for this cluster
            if _report_exists_for_cluster(cluster):
                logger.info(f"Report already exists for cluster: {cluster['topic']}")
                continue

            report = _generate_report(cluster)
            if report:
                _store_report(report)
                _alert_operator(report)
                save_event("report_generated", {
                    "title": report["title"],
                    "slug": report["slug"],
                    "processor": report["processor"],
                    "risk_level": report["risk_level"],
                    "cluster_size": report["cluster_size"],
                })
                reports_created += 1
                logger.info(f"Report generated: {report['title']}")

        except Exception as e:
            logger.error(f"Failed to generate report for cluster {cluster['topic']}: {e}")

    logger.info(f"Report generation cycle complete: {reports_created} new reports")


def _find_report_worthy_clusters():
    """Query clusters that warrant an intelligence report."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                c.id,
                c.cluster_topic,
                c.cluster_size,
                c.signal_ids,
                ca.risk_level,
                ca.analysis,
                ca.predicted_outcome,
                ci.processor,
                ci.industry,
                ci.merchant_count,
                ci.intelligence_summary
            FROM clusters c
            JOIN cluster_analysis ca ON ca.cluster_id = CAST(c.id AS TEXT)
            LEFT JOIN cluster_intelligence ci ON ci.cluster_id = CAST(c.id AS TEXT)
            WHERE c.cluster_size >= 15
              AND ca.risk_level IN ('high', 'critical')
            ORDER BY c.cluster_size DESC
            LIMIT 5
        """)).fetchall()

    clusters = []
    for row in rows:
        cluster = {
            "id": row[0],
            "topic": row[1],
            "size": row[2],
            "signal_ids": row[3] or "",
            "risk_level": row[4],
            "analysis": row[5] or "",
            "predicted_outcome": row[6] or "",
            "processor": row[7] or "Multiple",
            "industry": row[8] or "General",
            "merchant_count": row[9] or 0,
            "intelligence_summary": row[10] or "",
        }

        # Get sample signals for context
        signal_ids = [s.strip() for s in cluster["signal_ids"].split(",") if s.strip()]
        if signal_ids:
            cluster["top_signals"] = _get_signal_samples(signal_ids[:10])
        else:
            cluster["top_signals"] = []

        clusters.append(cluster)

    return clusters


def _get_signal_samples(signal_ids):
    """Fetch a sample of signal content for report context."""
    if not signal_ids:
        return []

    try:
        with engine.connect() as conn:
            placeholders = ",".join(str(int(sid)) for sid in signal_ids)
            rows = conn.execute(text(f"""
                SELECT content, merchant_name, source
                FROM signals
                WHERE id IN ({placeholders})
                LIMIT 8
            """)).fetchall()
        return [{"content": r[0][:300], "merchant": r[1] or "anonymous", "source": r[2] or "unknown"} for r in rows]
    except Exception as e:
        logger.warning(f"Failed to fetch signal samples: {e}")
        return []


def _report_exists_for_cluster(cluster):
    """Check if we already published a report for a similar cluster topic + processor."""
    topic_slug_fragment = _slugify(cluster["topic"])[:40]
    with engine.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) FROM merchant_intelligence_reports
            WHERE slug LIKE :pattern
              AND created_at >= NOW() - INTERVAL '48 hours'
        """), {"pattern": f"%{topic_slug_fragment}%"}).scalar()
    return count > 0


def _generate_report(cluster):
    """Use Claude to generate a structured intelligence report."""
    api_key = _get_api_key()
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not available for report generation")
        return None

    signals_text = ""
    for i, sig in enumerate(cluster["top_signals"][:5], 1):
        signals_text += f"  {i}. [{sig['source']}] {sig['content'][:200]}\n"

    prompt = f"""You are an intelligence analyst at PayFlux, a payment risk infrastructure company.

Generate a market intelligence report based on the following distress cluster data.

CLUSTER DATA:
- Topic: {cluster['topic']}
- Cluster Size: {cluster['size']} signals
- Risk Level: {cluster['risk_level']}
- Primary Processor: {cluster['processor']}
- Industry: {cluster['industry']}
- Merchant Count: {cluster['merchant_count']}
- Existing Analysis: {cluster['analysis'][:500]}
- Predicted Outcome: {cluster['predicted_outcome'][:300]}

SAMPLE SIGNALS:
{signals_text}

Generate a report with EXACTLY this JSON structure:
{{
    "title": "A compelling, specific headline (e.g. 'Stripe Payout Freezes Surge Across Ecommerce Merchants')",
    "executive_summary": "2-3 sentence overview for executives",
    "merchant_impact": "What merchants are experiencing right now (2-3 paragraphs)",
    "financial_impact": "Estimated financial exposure and cash flow implications (1-2 paragraphs)",
    "root_cause": "Root cause hypothesis based on the signal patterns (1-2 paragraphs)",
    "forecast": "What may happen next if this pattern continues (1-2 paragraphs)",
    "mitigation": "How PayFlux helps merchants mitigate this risk (1-2 paragraphs)"
}}

Write in a professional, data-driven tone. Reference specific signal patterns.
Do NOT use markdown formatting inside the JSON values — use plain text only.
Respond with valid JSON only, no markdown code blocks."""

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        response.raise_for_status()
        result = response.json()

        if "content" not in result or not result["content"]:
            logger.error(f"Empty response from Claude for cluster {cluster['topic']}")
            return None

        raw = result["content"][0]["text"].strip()

        # Clean potential markdown wrapping
        if raw.startswith("```"):
            raw = re.sub(r'^```(?:json)?\s*', '', raw)
            raw = re.sub(r'\s*```$', '', raw)

        report_data = json.loads(raw)

        title = report_data.get("title", cluster["topic"])
        slug = _slugify(title)

        # Build full content as structured text
        content_sections = []
        content_sections.append(f"## Executive Summary\n\n{report_data.get('executive_summary', '')}")
        content_sections.append(f"## What Merchants Are Experiencing\n\n{report_data.get('merchant_impact', '')}")
        content_sections.append(f"## Financial Impact\n\n{report_data.get('financial_impact', '')}")
        content_sections.append(f"## Root Cause Analysis\n\n{report_data.get('root_cause', '')}")
        content_sections.append(f"## What May Happen Next\n\n{report_data.get('forecast', '')}")
        content_sections.append(f"## How PayFlux Mitigates This\n\n{report_data.get('mitigation', '')}")

        return {
            "title": title,
            "slug": slug,
            "executive_summary": report_data.get("executive_summary", ""),
            "content": "\n\n".join(content_sections),
            "processor": cluster["processor"],
            "industry": cluster["industry"],
            "cluster_size": cluster["size"],
            "risk_level": cluster["risk_level"],
            "metadata": {
                "cluster_id": cluster["id"],
                "cluster_topic": cluster["topic"],
                "merchant_count": cluster["merchant_count"],
                "generated_at": datetime.utcnow().isoformat(),
            },
        }

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Claude response as JSON: {e}")
        return None
    except Exception as e:
        logger.error(f"Report generation API call failed: {e}")
        return None


def _store_report(report):
    """Insert the report into the database."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO merchant_intelligence_reports
                (title, slug, executive_summary, content, processor, industry,
                 cluster_size, risk_level, metadata)
            VALUES (:title, :slug, :summary, :content, :processor, :industry,
                    :size, :risk, :meta)
            ON CONFLICT (slug) DO NOTHING
        """), {
            "title": report["title"],
            "slug": report["slug"],
            "summary": report["executive_summary"],
            "content": report["content"],
            "processor": report["processor"],
            "industry": report["industry"],
            "size": report["cluster_size"],
            "risk": report["risk_level"],
            "meta": json.dumps(report["metadata"]),
        })
        conn.commit()


def _alert_operator(report):
    """Send Telegram alert about the new report."""
    alert = (
        f"📊 New Merchant Intelligence Report\n\n"
        f"Title: {report['title']}\n"
        f"Processor: {report['processor']}\n"
        f"Risk Level: {report['risk_level']}\n"
        f"Cluster Size: {report['cluster_size']} signals\n\n"
        f"Summary: {report['executive_summary'][:200]}\n\n"
        f"Published at: https://payflux.dev/reports/{report['slug']}"
    )
    send_operator_alert(alert)
