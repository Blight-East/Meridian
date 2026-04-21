"""
Deal Lifecycle Rollout — Post-Deploy Validation Script
=======================================================
Run this on the VPS after deploying the new code.

Usage:
    # Phase C Step 1: Create tables
    python3 -c "from deploy.validate_deal_lifecycle import phase_c_create_tables; phase_c_create_tables()"

    # Phase C Step 2: Dry-run backfill (read-only, shows what would happen)
    python3 -c "from deploy.validate_deal_lifecycle import phase_c_dry_run; phase_c_dry_run()"

    # Phase C Step 3: Real backfill (writes to deal_lifecycle)
    python3 -c "from deploy.validate_deal_lifecycle import phase_c_backfill; phase_c_backfill()"

    # Phase C Step 4: Verify backfill accuracy
    python3 -c "from deploy.validate_deal_lifecycle import phase_c_verify; phase_c_verify()"

    # Phase D: Check action queue readability
    python3 -c "from deploy.validate_deal_lifecycle import phase_d_action_queue; phase_d_action_queue()"

    # Phase E: Enable output quality auto-cleaning (set threshold)
    python3 -c "from deploy.validate_deal_lifecycle import phase_e_check_scores; phase_e_check_scores()"

    # Phase F: Check lifecycle drift against legacy tables
    python3 -c "from deploy.validate_deal_lifecycle import phase_f_drift; phase_f_drift()"

    # Phase G: Hydrate sparse lifecycle rows from live outreach context
    python3 -c "from deploy.validate_deal_lifecycle import phase_g_hydrate; phase_g_hydrate()"
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


def phase_c_create_tables():
    """Phase C Step 1: Create the deal_lifecycle and deal_stage_transitions tables."""
    from runtime.ops.deal_lifecycle import ensure_deal_lifecycle_table
    ensure_deal_lifecycle_table()
    print("✓ Tables created: deal_lifecycle, deal_stage_transitions")

    # Verify
    from memory.structured.db import engine
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM deal_lifecycle")).scalar()
        print(f"  deal_lifecycle row count: {result}")
        result = conn.execute(text("SELECT COUNT(*) FROM deal_stage_transitions")).scalar()
        print(f"  deal_stage_transitions row count: {result}")


def phase_c_dry_run():
    """Phase C Step 2: Dry-run backfill — shows what would be written without writing."""
    from runtime.ops.deal_lifecycle import backfill_deal_lifecycle
    result = backfill_deal_lifecycle(dry_run=True, limit=5000)

    print(f"Dry-run backfill results:")
    print(f"  Rows scanned: {result['rows_scanned']}")
    print(f"  Would create: {result['skipped']}")  # In dry_run mode, skipped = would-create
    print(f"  Errors: {result['errors']}")
    print()

    preview = result.get("preview", [])
    if preview:
        # Summarize stage distribution
        stage_counts: dict[str, int] = {}
        for item in preview:
            stage = item.get("stage", "unknown")
            stage_counts[stage] = stage_counts.get(stage, 0) + 1

        print("Stage distribution (from preview, up to 10 items):")
        for stage, count in sorted(stage_counts.items(), key=lambda x: -x[1]):
            print(f"  {stage}: {count}")
        print()

        # Show first 5 items
        print("First 5 preview items:")
        for item in preview[:5]:
            print(f"  opp={item['opportunity_id']} domain={item['merchant_domain'][:40]} "
                  f"stage={item['stage']} processor={item['processor']} "
                  f"distress={item['distress_type']}")

    print()
    print("Review the above. If the stage distribution looks correct, run phase_c_backfill().")


def phase_c_backfill():
    """Phase C Step 3: Real backfill — writes to deal_lifecycle."""
    from runtime.ops.deal_lifecycle import backfill_deal_lifecycle
    result = backfill_deal_lifecycle(dry_run=False, limit=5000)

    print(f"Backfill results:")
    print(f"  Status: {result['status']}")
    print(f"  Rows scanned: {result['rows_scanned']}")
    print(f"  Created: {result['created']}")
    print(f"  Errors: {result['errors']}")
    print(f"  Duration: {result['duration_seconds']}s")


def phase_c_verify():
    """Phase C Step 4: Verify backfill accuracy by comparing lifecycle vs. existing tables."""
    from memory.structured.db import engine
    from sqlalchemy import text

    with engine.connect() as conn:
        # Total lifecycle deals
        total = conn.execute(text("SELECT COUNT(*) FROM deal_lifecycle")).scalar()
        print(f"Total deals in lifecycle: {total}")

        # Stage distribution
        stages = conn.execute(text("""
            SELECT current_stage, COUNT(*) as cnt
            FROM deal_lifecycle
            GROUP BY current_stage
            ORDER BY cnt DESC
        """)).mappings().all()
        print("\nLifecycle stage distribution:")
        for row in stages:
            print(f"  {row['current_stage']}: {row['cnt']}")

        # Compare with merchant_opportunities
        opp_total = conn.execute(text("SELECT COUNT(*) FROM merchant_opportunities")).scalar()
        print(f"\nMerchant opportunities total: {opp_total}")
        print(f"Lifecycle coverage: {total}/{opp_total} ({round(100*total/max(opp_total,1), 1)}%)")

        # Check for rejected opportunities that should NOT be active
        rejected_active = conn.execute(text("""
            SELECT COUNT(*) FROM deal_lifecycle dl
            JOIN merchant_opportunities mo ON mo.id = dl.opportunity_id
            WHERE mo.status = 'rejected'
            AND dl.current_stage NOT IN ('outcome_won', 'outcome_lost', 'outcome_ignored')
        """)).scalar()
        if rejected_active > 0:
            print(f"\n⚠️  WARNING: {rejected_active} rejected opportunities are active in lifecycle!")
        else:
            print(f"\n✓ No rejected opportunities leaked into active lifecycle")

        # Spot-check: compare 5 random deals
        samples = conn.execute(text("""
            SELECT dl.opportunity_id, dl.current_stage, dl.priority_score,
                   mo.status as opp_status,
                   oa.status as outreach_status, oa.approval_state, oa.outcome_status
            FROM deal_lifecycle dl
            JOIN merchant_opportunities mo ON mo.id = dl.opportunity_id
            LEFT JOIN opportunity_outreach_actions oa ON oa.opportunity_id = dl.opportunity_id
            ORDER BY dl.updated_at DESC
            LIMIT 5
        """)).mappings().all()
        print("\nSpot-check (5 most recent):")
        for s in samples:
            print(f"  opp={s['opportunity_id']} lifecycle={s['current_stage']} "
                  f"opp_status={s['opp_status']} outreach={s['outreach_status']} "
                  f"approval={s['approval_state']} outcome={s['outcome_status']} "
                  f"priority={s['priority_score']}")


def phase_d_action_queue():
    """Phase D: Check if the action queue produces sensible results."""
    from runtime.ops.action_queue import build_action_queue, render_action_queue

    items = build_action_queue(limit=10)
    print(f"Action queue: {len(items)} items")
    print()

    if items:
        rendered = render_action_queue(items)
        print(rendered)
        print()

        operator = [i for i in items if i.requires_operator]
        auto = [i for i in items if i.can_auto_execute]
        print(f"Operator sign-off: {len(operator)} items")
        print(f"Auto-executable: {len(auto)} items")
        print()

        print("Top 5 items detail:")
        for item in items[:5]:
            print(f"  {item.action_type.value}: {item.merchant_domain or 'no domain'} "
                  f"(priority={item.priority_score:.1f}, stage={item.stage})")
    else:
        print("Queue is empty. This is expected if the lifecycle table has no active deals.")


def phase_e_check_scores():
    """Phase E: Check output quality scores on recent conversations to assess false positive rate."""
    from runtime.conversation.output_quality import score_output, _CLEAN_THRESHOLD

    print(f"Current clean threshold: {_CLEAN_THRESHOLD}")
    print(f"  0 = score-only (no cleaning)")
    print(f"  50 = conservative cleaning")
    print(f"  70 = standard cleaning")
    print()

    # Try to pull recent bot responses
    try:
        from memory.structured.db import engine
        from sqlalchemy import text

        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT content FROM conversations
                WHERE role = 'assistant'
                ORDER BY id DESC
                LIMIT 50
            """)).mappings().all()

        if not rows:
            print("No recent chat history found. Skipping score analysis.")
            return

        total = len(rows)
        would_clean = 0
        violations_by_type: dict[str, int] = {}

        for row in rows:
            content = str(row.get("content") or "")
            if not content.strip():
                continue
            quality = score_output(content)
            if quality.score < 70:  # Would clean at standard threshold
                would_clean += 1
            for v in quality.violations:
                violations_by_type[v.category] = violations_by_type.get(v.category, 0) + 1

        print(f"Analyzed {total} recent responses:")
        print(f"  Would clean at threshold=70: {would_clean}/{total} ({round(100*would_clean/max(total,1), 1)}%)")
        print()
        if violations_by_type:
            print("Violation frequency:")
            for cat, cnt in sorted(violations_by_type.items(), key=lambda x: -x[1]):
                print(f"  {cat}: {cnt}")
        else:
            print("No violations found — all responses clean.")
        print()
        print("To enable cleaning, set MERIDIAN_OUTPUT_QUALITY_CLEAN_THRESHOLD=50 in .env")
        print("and restart PM2.")

    except Exception as exc:
        print(f"Could not analyze chat history: {exc}")
        print("You can still enable cleaning by setting the env var manually.")


def phase_f_drift():
    """Phase F: Check for lifecycle drift against the legacy table model."""
    from runtime.ops.deal_lifecycle_reconciliation import reconcile_deal_lifecycle

    result = reconcile_deal_lifecycle(limit=1000)
    print(f"Drift status: {result['status']}")
    print(f"  Checked rows: {result['checked_rows']}")
    print(f"  Lifecycle coverage: {result['coverage_percent']}%")
    print(f"  Missing active deals: {result['missing_active']}")
    print(f"  Rejected active deals: {result['rejected_active']}")
    print(f"  Stage mismatches: {result['stage_mismatches']}")
    print(f"  Sparse active rows: {result['sparse_active_rows']}")
    examples = result.get("examples") or []
    if examples:
        print()
        print("Examples:")
        for example in examples[:5]:
            print(f"  {example}")


def phase_g_hydrate():
    """Phase G: Hydrate sparse lifecycle rows from live outreach context."""
    from runtime.ops.deal_lifecycle import hydrate_deal_lifecycle

    result = hydrate_deal_lifecycle(limit=500)
    print(f"Hydration status: {result['status']}")
    print(f"  Scanned: {result['scanned']}")
    print(f"  Hydrated: {result['hydrated']}")
    print(f"  Skipped: {result['skipped']}")
    print(f"  Errors: {result['errors']}")
    examples = result.get("examples") or []
    if examples:
        print()
        print("Examples:")
        for example in examples[:5]:
            print(f"  {example}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Deal Lifecycle Rollout Validation")
    parser.add_argument("phase", choices=["c1", "c2", "c3", "c4", "d", "e", "f", "g"],
                        help="Which phase to run: c1=create, c2=dry-run, c3=backfill, c4=verify, d=action-queue, e=quality-scores, f=drift-check, g=hydrate")
    args = parser.parse_args()

    {
        "c1": phase_c_create_tables,
        "c2": phase_c_dry_run,
        "c3": phase_c_backfill,
        "c4": phase_c_verify,
        "d": phase_d_action_queue,
        "e": phase_e_check_scores,
        "f": phase_f_drift,
        "g": phase_g_hydrate,
    }[args.phase]()
