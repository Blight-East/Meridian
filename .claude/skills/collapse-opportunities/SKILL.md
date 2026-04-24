---
name: collapse-opportunities
description: Merge the parallel `opportunities` (legacy) and `merchant_opportunities` (rich) tables into one. Phase 1 sequential. Eliminates source-of-truth conflict and orphaned references.
phase: 1
status: draft
blast_radius: high (schema migration + code rewrites + potential data loss window)
parallelizable: false
---

# Collapse `opportunities` and `merchant_opportunities`

## Why

Two tables serve the same concept:

- `opportunities` (legacy) — simple `signal_id → opportunity_score` mapping.
- `merchant_opportunities` (rich) — the table operators actually approve
  outreach from.

Neither has a FK to the other. The audit ran the "delete test" and
could not answer "what breaks if `opportunities` is dropped?" That
ambiguity IS the bug.

Consequences:
- Callers are split. Some code writes to `opportunities`, some to
  `merchant_opportunities`.
- A row in one without a corresponding row in the other is a silent
  logical gap.
- `learning_feedback_ledger.opportunity_id` is ambiguous: which table?

Audit risk **R7**.

## Blast radius

- **Schema:** migrate/merge two tables → one. Cannot be fully
  reversible after deletion.
- **Code:** every reference to `opportunities` or `opportunity_id` that
  targets the legacy table.
- **Data:** orphan rows in `opportunities` that have no merchant
  resolution. Must be audited before delete.

## Preconditions

1. Phase 0 complete.
2. `apply-db-hardening-prod` applied (use the advisory-locked migration
   runner).
3. `unify-tree` complete — ensures there's one code path to update.
4. `fk-and-indexes` complete — ensures FK audit has already cleaned
   the most obvious orphans.
5. Backup verified.

## Design decision

**Keep `merchant_opportunities` as the single canonical table.** It's
richer, operators already interact with it, and it's the write target
for deal_sourcing. Migrate any surviving rows from `opportunities`
into `merchant_opportunities` (where a merchant can be resolved) or
archive them.

## Steps

1. **Audit current state.**
   ```sql
   SELECT
     (SELECT count(*) FROM opportunities) AS legacy_count,
     (SELECT count(*) FROM merchant_opportunities) AS rich_count,
     (SELECT count(*) FROM opportunities o
      LEFT JOIN merchant_opportunities mo ON mo.signal_id = o.signal_id
      WHERE mo.id IS NULL) AS legacy_without_rich;
   ```
   → **verify**: record counts in PR description. Decide migration
   strategy based on `legacy_without_rich`.

2. **Grep all callers.** Every file that references `opportunities`
   (not `merchant_opportunities`):
   ```
   grep -rn '\bFROM opportunities\b\|\bINTO opportunities\b\|\bUPDATE opportunities\b' .
   grep -rn 'opportunities_id\|opportunities(' .
   ```
   → **verify**: exhaustive list attached to PR description.

3. **Migrate surviving legacy rows.** If `opportunities` has rows not
   in `merchant_opportunities`, resolve each:
   - If the signal has a merchant → create `merchant_opportunities` row.
   - If not → archive to `opportunities_archive` (snapshot table),
     don't create.
   ```sql
   INSERT INTO opportunities_archive SELECT * FROM opportunities
     WHERE signal_id NOT IN (SELECT signal_id FROM merchant_opportunities);
   ```
   → **verify**: archive row count equals `legacy_without_rich` from
   step 1.

4. **Disambiguate `learning_feedback_ledger.opportunity_id`.** This
   column is ambiguous today. Add a temporary column
   `opportunity_id_resolved` that's always a `merchant_opportunities.id`;
   backfill from the legacy FK where possible; drop the old column
   after validation.
   → **verify**: 100% of non-null `opportunity_id` values resolve to
   a `merchant_opportunities.id`.

5. **Migration `012_drop_legacy_opportunities.sql`:**
   ```sql
   -- Rename first for reversibility
   ALTER TABLE opportunities RENAME TO opportunities_deprecated;
   -- Keep for 30 days; drop in migration 013 after validation window
   ```
   **Do not drop in this migration.** Rename, wait 30 days, then drop.
   → **verify**: `\d opportunities` returns nothing; `\d opportunities_deprecated`
   returns the old schema.

6. **Rewrite every caller.** Callers that wrote to `opportunities` now
   write to `merchant_opportunities`. Callers that joined on
   `opportunities.signal_id` now join via `merchant_opportunities.signal_id`
   (which needs to exist — check schema).
   → **verify**: grep from step 2 returns zero hits for the legacy
   table name.

7. **Watch for writes.** After deploy, monitor DB logs for any
   `opportunities_deprecated` access (should be zero). If any agent
   still writes there, something was missed.
   → **verify**: `pg_stat_user_tables.seq_scan + idx_scan` for
   `opportunities_deprecated` is 0 after 24h.

8. **After 30 days, drop.** Migration `013_finalize_opportunities_drop.sql`:
   ```sql
   DROP TABLE opportunities_deprecated;
   ```
   → **verify**: table gone; no errors in prod.

## Rollback

Up through step 5 (rename), fully reversible: rename back.

After step 6 (caller rewrites deployed), rollback requires:
- Revert the caller-rewrite PR.
- Rename table back.
- Replay any writes that went to `merchant_opportunities` but should
  have gone to `opportunities` (unlikely but possible).

**After step 8 (drop), not reversible** — restore from backup required.

## Exit criterion

- Every caller writes to `merchant_opportunities` only.
- `learning_feedback_ledger.opportunity_id` resolves to
  `merchant_opportunities.id` always.
- `opportunities_deprecated` table has zero reads/writes for 30
  consecutive days.
- Migration 013 applied, table gone.
- Audit risk R7 cleared for this concept.

## Notes

- **Why the 30-day deprecation window?** Historical logs, external
  tooling, or forgotten cron jobs may still reference the legacy
  table. Renaming + monitoring is cheap insurance.
- **Why not dual-write during migration?** Extra complexity. The
  rewrite is atomic enough that one-shot cutover is fine — all new
  writes go to rich; old rows migrated; legacy renamed.
- **Do not attempt without `unify-tree` complete.** Two trees with
  two sets of callers doubles the risk.
