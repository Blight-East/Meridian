import os
import sys

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(_dir)
sys.path.insert(0, project_root)

from config.logging_config import get_logger
from runtime.intelligence.domain_discovery import (
    infer_brand_for_merchant,
    infer_platform_for_merchant,
    persist_discovered_domain,
)

logger = get_logger("backfill_domain_discovery")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")


def run_backfill(limit=None):
    resolved = 0
    platform_resolved = 0
    search_resolved = 0
    failures = 0

    with engine.connect() as conn:
        query = """
            SELECT id, canonical_name
            FROM merchants
            WHERE (domain IS NULL OR domain = '')
              AND distress_score >= 5
            ORDER BY distress_score DESC, last_seen DESC
        """
        if limit:
            query += " LIMIT :limit"
            merchants = conn.execute(text(query), {"limit": limit}).fetchall()
        else:
            merchants = conn.execute(text(query)).fetchall()

        for merchant_id, brand in merchants:
            try:
                platform = infer_platform_for_merchant(conn, merchant_id)
                inferred_brand = infer_brand_for_merchant(conn, merchant_id, brand)
                discovery = persist_discovered_domain(conn, merchant_id, inferred_brand, platform)
                if discovery:
                    resolved += 1
                    if discovery["source"] == "platform_pattern":
                        platform_resolved += 1
                    elif discovery["source"] == "search_result":
                        search_resolved += 1
            except Exception as exc:
                logger.warning(f"Backfill failed for merchant {merchant_id}: {exc}")
                failures += 1

        conn.commit()

    result = {
        "merchants_processed": len(merchants),
        "domains_discovered": resolved,
        "platform_domains_resolved": platform_resolved,
        "search_domains_resolved": search_resolved,
        "failures": failures,
    }
    logger.info(f"Backfill complete: {result}")
    return result


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    print(run_backfill(limit=limit))
