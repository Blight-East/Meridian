---
name: merchant-opp-dedup
description: Add UNIQUE constraint on merchant_opportunities to prevent duplicate outreach. Use when investigating duplicate drafts or before enabling high-volume outreach.
phase: 0
status: draft
blast_radius: low (one migration, one INSERT path)
parallelizable: true
---

# Merchant opportunity dedup

## Why

`deal_sourcing.py` `_process_merchant_opportunity()` INSERTs into
`merchant_opportunities` without a uniqueness guarantee. Two concurrent
scheduler cycles can both pass the NOT-IN check and both INSERT, producing
duplicate pending opportunities for the same merchant — which produces
duplicate Telegram approval prompts and (after operator approval)
duplicate outreach drafts.

Audit risk **R2**. Also implicated in noisy Telegram traffic during
scheduler overlap windows.

## Blast radius

- **Schema:** one migration adds `UNIQUE (merchant_id, opportunity_week)`
  on `merchant_opportunities`. Requires a prior de-dupe pass if any
  duplicates exist.
- **Code:** one INSERT converted to `INSERT ... ON CONFLICT DO NOTHING`
  or `ON CONFLICT (merchant_id, opportunity_week) DO UPDATE` (pick one;
  the skill picks DO NOTHING for simplicity).
- **Migration safety:** covered by the advisory-locked migration runner
  introduced in b0eed03. No production lock contention expected.

Does not touch: classifier, outreach, Telegram, worker, Meridian.

## Preconditions

1. `apply-db-hardening-prod` applied **or** you're willing to apply the
   migration manually under an explicit lock.
2. Known prod data state: run the de-dupe query in step 1 and decide
   strategy before running migration.

## Steps

1. **Audit existing duplicates.**
   ```sql
   SELECT merchant_id, DATE_TRUNC('week', created_at) AS week, COUNT(*) AS c
   FROM merchant_opportunities
   WHERE status IN ('pending_review', 'approved')
   GROUP BY 1, 2
   HAVING COUNT(*) > 1
   ORDER BY c DESC LIMIT 50;
   ```
   → **verify**: note the count and list of merchant_ids.

2. **De-dupe existing rows.** For each (merchant_id, week) with >1, keep
   the earliest `status='approved'` row if any, else the earliest
   `created_at`. Move the losers to `merchant_opportunities_dedup_archive`
   (create as a copy of the schema) before deletion — never hard-delete
   without a snapshot.
   → **verify**: audit query from step 1 returns 0 rows.

3. **Write migration `009_merchant_opp_unique.sql`** in
   `deploy/migrations/`:
   ```sql
   ALTER TABLE merchant_opportunities
     ADD COLUMN IF NOT EXISTS opportunity_week DATE
       GENERATED ALWAYS AS (DATE_TRUNC('week', created_at)::DATE) STORED;

   ALTER TABLE merchant_opportunities
     ADD CONSTRAINT merchant_opp_week_unique
     UNIQUE (merchant_id, opportunity_week);
   ```
   Register with `@register_migration` in `runtime/ops/schema_migrations.py`
   (post-b0eed03).
   → **verify**: `python3 -m runtime.ops.schema_migrations --dry-run`
   shows the migration queued; `--apply` in a dev DB applies cleanly.

4. **Update INSERT in `deal_sourcing.py`.** Find
   `_process_merchant_opportunity()`; change the INSERT to:
   ```sql
   INSERT INTO merchant_opportunities (...) VALUES (...)
   ON CONFLICT (merchant_id, opportunity_week) DO NOTHING
   RETURNING id;
   ```
   When DO NOTHING returns no row, log `merchant_opp_dedup_skipped`
   as a telemetry event and return `None` from the function.
   → **verify**: unit test that invokes the function twice with same
   merchant_id in the same week — second call returns None, no new row.

5. **Regression test.** Integration test that runs two threads calling
   `_process_merchant_opportunity()` concurrently for the same merchant.
   Assert exactly one row inserted.
   → **verify**: `pytest tests/intelligence/test_opp_dedup.py` green.

6. **Deploy and monitor.** After deploy, watch `events` for
   `merchant_opp_dedup_skipped` count. If it's non-zero, dedup is
   working. Build a Prom alert (skill `prom-exporter-on-telemetry`) if
   the count exceeds a sane threshold (suggests a loop upstream).

## Rollback

- Drop the UNIQUE constraint: `ALTER TABLE merchant_opportunities
  DROP CONSTRAINT merchant_opp_week_unique;`
- Drop the generated column: `ALTER TABLE merchant_opportunities
  DROP COLUMN opportunity_week;`
- Revert the INSERT change (single commit).

De-duped rows are in `merchant_opportunities_dedup_archive` — restore
if needed.

## Exit criterion

- Audit query returns 0 rows before and after migration.
- UNIQUE constraint visible in `\d+ merchant_opportunities`.
- Regression test green in CI.
- 7 days in prod with zero duplicate operator-approval prompts for the
  same merchant in the same week.

## Notes

- **Why `opportunity_week` as a generated column?** Lets the UNIQUE
  cover "same merchant, same week" without locking to `created_at`
  equality. Operator can still create a legitimate new opportunity for
  the same merchant next week.
- **Why not `(merchant_id, status)`?** Because approved→converted
  transitions should be allowed; the week bucket is what prevents
  noise.
- **Alternative considered:** partial UNIQUE index on `WHERE status =
  'pending_review'`. Rejected because approved duplicates are also
  noise if they overlap.
