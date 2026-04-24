---
name: fk-and-indexes
description: Add FK constraints on loose integer references + indexes on hot query paths. Phase 1 sequential. Use after phase 0 ships and before collapse-opportunities.
phase: 1
status: draft
blast_radius: medium (migration, requires backfill checks)
parallelizable: false (sequential with other phase 1 skills)
---

# FK constraints and hot-path indexes

## Why

The schema has several integer columns that act as foreign keys but
aren't declared as such. Audit risk **R6**:

- `signals.merchant_id` → no FK to `merchants(id)`. Orphans possible
  when a merchant row is deleted.
- `learning_feedback_ledger.opportunity_id` → no FK. Writes can land
  against nonexistent opportunities silently.
- `merchant_contacts.merchant_id` → FK exists (good).
- `deal_lifecycle.opportunity_id` → UNIQUE but not FK-declared to
  `merchant_opportunities(id)`.

And missing indexes on hot paths (audit section 8):
- `signals(priority_score DESC, detected_at DESC)` — used in
  `SELECT ... FROM signals ORDER BY priority_score DESC` ad
  infinitum.
- `signals(merchant_id)` — used in every cleanup/attribution query.
- `signals(source, detected_at)` — used in per-source 24h windows.
- `signals(classification, detected_at)` — used in mix queries.
- `merchant_opportunities(status, created_at)` — used in pending-review
  fetch.
- `events(event_type, created_at)` — needed by `prom-exporter-on-telemetry`.

## Blast radius

Single migration. FK adds may fail if orphans exist — pre-scan + clean
is required before constraint creation.

## Preconditions

1. Phase 0 complete.
2. `apply-db-hardening-prod` applied — this migration uses the advisory
   lock runner.
3. Backup verified < 24 hours old.

## Steps

1. **Audit orphans.** For each planned FK, count unmatched rows:
   ```sql
   SELECT count(*) FROM signals s
   LEFT JOIN merchants m ON m.id = s.merchant_id
   WHERE s.merchant_id IS NOT NULL AND m.id IS NULL;

   SELECT count(*) FROM learning_feedback_ledger_raw lfl
   LEFT JOIN merchant_opportunities mo ON mo.id = lfl.opportunity_id
   WHERE lfl.opportunity_id IS NOT NULL AND mo.id IS NULL;
   ```
   → **verify**: record counts. If non-zero, decide: clean (set to
   NULL) or archive (move to `*_orphan_archive`).

2. **Clean orphans.** Prefer `UPDATE ... SET merchant_id = NULL` over
   delete — preserves signal content for future re-attribution. For
   feedback ledger orphans, archive before clearing; that data is
   learning-signal.
   → **verify**: audit queries from step 1 return 0.

3. **Write migration `010_fk_constraints.sql`** in
   `deploy/migrations/`. Use `NOT VALID` + `VALIDATE` pattern to avoid
   long exclusive lock:
   ```sql
   ALTER TABLE signals
     ADD CONSTRAINT signals_merchant_id_fk
     FOREIGN KEY (merchant_id) REFERENCES merchants(id) ON DELETE SET NULL
     NOT VALID;
   ALTER TABLE signals VALIDATE CONSTRAINT signals_merchant_id_fk;

   ALTER TABLE learning_feedback_ledger_raw
     ADD CONSTRAINT lfl_opportunity_id_fk
     FOREIGN KEY (opportunity_id) REFERENCES merchant_opportunities(id)
       ON DELETE SET NULL
     NOT VALID;
   ALTER TABLE learning_feedback_ledger_raw VALIDATE CONSTRAINT lfl_opportunity_id_fk;

   ALTER TABLE deal_lifecycle
     ADD CONSTRAINT deal_lifecycle_opportunity_id_fk
     FOREIGN KEY (opportunity_id) REFERENCES merchant_opportunities(id)
       ON DELETE CASCADE
     NOT VALID;
   ALTER TABLE deal_lifecycle VALIDATE CONSTRAINT deal_lifecycle_opportunity_id_fk;
   ```
   Register via `@register_migration`.
   → **verify**: `python3 -m runtime.ops.schema_migrations --dry-run`
   shows the migration.

4. **Write migration `011_hot_path_indexes.sql`.** Use
   `CREATE INDEX CONCURRENTLY` so reads/writes aren't blocked:
   ```sql
   CREATE INDEX CONCURRENTLY IF NOT EXISTS signals_priority_detected_idx
     ON signals (priority_score DESC NULLS LAST, detected_at DESC);
   CREATE INDEX CONCURRENTLY IF NOT EXISTS signals_merchant_id_idx
     ON signals (merchant_id) WHERE merchant_id IS NOT NULL;
   CREATE INDEX CONCURRENTLY IF NOT EXISTS signals_source_detected_idx
     ON signals (source, detected_at DESC);
   CREATE INDEX CONCURRENTLY IF NOT EXISTS signals_classification_detected_idx
     ON signals (classification, detected_at DESC)
     WHERE classification IS NOT NULL;
   CREATE INDEX CONCURRENTLY IF NOT EXISTS merchant_opps_status_created_idx
     ON merchant_opportunities (status, created_at DESC);
   CREATE INDEX CONCURRENTLY IF NOT EXISTS events_type_created_idx
     ON events (event_type, created_at DESC);
   ```
   **CONCURRENTLY cannot run inside a transaction** — the migration
   runner must support this. If it wraps everything in BEGIN/COMMIT,
   add a runner escape hatch or run these indexes manually in the
   maintenance window.
   → **verify**: `\di+` shows each index with non-zero size after
   creation.

5. **Run EXPLAIN on three representative queries** before and after:
   ```sql
   EXPLAIN (ANALYZE, BUFFERS)
   SELECT id, source, content, priority_score FROM signals
   ORDER BY priority_score DESC LIMIT 20;

   EXPLAIN (ANALYZE, BUFFERS)
   SELECT * FROM merchant_opportunities
   WHERE status = 'pending_review' ORDER BY created_at DESC LIMIT 50;

   EXPLAIN (ANALYZE, BUFFERS)
   SELECT count(*) FROM events
   WHERE event_type = 'tool_invoked' AND created_at > now() - '5 minutes'::interval;
   ```
   → **verify**: plan nodes change from `Seq Scan` to `Index Scan`;
   total query time drops (paste before/after in PR description).

6. **Monitor bloat.** For 7 days post-deploy, watch `pg_stat_user_indexes`
   for the new indexes. If any is unused after a week, drop it — the
   `WHERE` clauses on partial indexes may make them miss query
   planner.
   → **verify**: after 7 days, `idx_scan` > 0 for each new index.

## Rollback

- Drop constraints: `ALTER TABLE ... DROP CONSTRAINT ...` per FK.
- Drop indexes: `DROP INDEX CONCURRENTLY ...`.
- Reset `schema_migrations` table entry for the affected migrations.

Orphan cleanup is not automatically reversible — archives should be
kept for restore.

## Exit criterion

- All FK constraints present and VALID (not NOT VALID).
- All hot-path indexes present and used (idx_scan > 0 within 7 days).
- At least one representative query demonstrates plan improvement
  (Seq Scan → Index Scan).
- Audit risk R6 cleared; audit section 8 (missing indexes) resolved.

## Notes

- **Why NOT VALID + VALIDATE?** Adding an FK with pre-existing data
  acquires an exclusive lock for the entire validation scan. `NOT
  VALID` skips that check on add (lock is brief); `VALIDATE CONSTRAINT`
  later does the scan without the exclusive lock. Two-step is safer in
  prod.
- **Why partial indexes (`WHERE merchant_id IS NOT NULL`)?** If most
  signals don't have a merchant_id yet (ingestion lag), a partial
  index is smaller and faster. Revisit if attribution rate rises past
  80%.
- **pgvector HNSW index on signal_embeddings already exists**
  (`add_semantic_memory.sql:10`). Don't touch.
