---
name: unify-tree
description: Kill the deploy/ tree; canonicalize runtime/. Phase 1 sequential. Eliminates ~30% duplicate-code drift risk and halves the SentenceTransformer memory load.
phase: 1
status: draft
blast_radius: high (repo-wide; requires prod deploy coordination)
parallelizable: false
---

# Unify code tree — kill deploy/

## Why

CLAUDE.md documents this explicitly: "Two parallel Python trees, both on
`PYTHONPATH`." The audit found ~30% of signal/opportunity logic
duplicated between `runtime/` and `deploy/`. Most of the `deploy/*.py`
files are 1-line re-exports from `runtime.intelligence.*`, but a few
are still-live originals.

Consequences of keeping both:
- **Drift.** Two implementations of the same function can slowly
  diverge. Audit already flagged `entity_taxonomy.py:40-60` identical
  today, no guarantee tomorrow.
- **Memory.** SentenceTransformer is imported in both trees (CLAUDE.md
  confirms double-load). The 1.5GB `max_memory_restart` cap on
  scheduler exists specifically because of this.
- **Confusion for agents (human and AI).** Which path is prod running?
  CLAUDE.md answers "check `ecosystem.config.js`" — only runtime/
  entrypoints are listed. So `deploy/` is legacy. Prove it by
  deleting it.

Audit risk **R7** (parallel state).

## Blast radius

- **Entire `deploy/*.py` tree** — delete or migrate.
- **`deploy/migrations/*.sql`** — move to `migrations/` at repo root
  (or `runtime/migrations/`). Update `runtime/ops/schema_migrations.py`
  path.
- **`deploy.sh`** — update paths; no behavior change.
- **`ecosystem.config.js`** — remove `deploy/` from PYTHONPATH.
- **Every `from deploy.X import Y`** call site — rewrite or delete.

Does not touch: `runtime/` internals beyond import updates.

## Preconditions

1. **All phase 0 skills merged.** This PR is big; don't pile it on
   unstable.
2. **Devin coordination:** check if any open Devin PR touches
   `deploy/`. If so, land it first; this skill comes after.
3. **Prod snapshot.** Full backup taken; confirmed restore procedure
   in `docs/runbook/deploy.md`.

## Steps

1. **Inventory.** Produce `docs/deploy_tree_audit.md`:
   ```
   for f in deploy/*.py; do
     echo "=== $f ==="
     head -20 "$f"
     echo "---"
     grep -rn "from deploy.$(basename $f .py)" runtime/ deploy/ | head -5
   done
   ```
   Classify each file: **re-export** (1-line import from runtime),
   **legacy duplicate** (original exists in runtime/ too), **unique
   live** (only in deploy/, prod depends on it).
   → **verify**: audit doc checked in; every `.py` in `deploy/`
   classified.

2. **Migrate unique-live files to runtime/.** For each, find the
   appropriate subpackage in `runtime/` (intelligence, ops, ingestion,
   channels, etc.) and move the file. Update imports in callers.
   → **verify**: `grep -rn 'from deploy\.' runtime/` returns zero
   hits for those files.

3. **Move migrations.** `deploy/migrations/*.sql` → `migrations/` at
   repo root. Update `runtime/ops/schema_migrations.py` path constant.
   Verify `python3 -m runtime.ops.schema_migrations --dry-run` still
   enumerates the same set of migrations.
   → **verify**: `ls migrations/*.sql | wc -l` matches original
   `deploy/migrations/*.sql` count.

4. **Remove re-exports + legacy duplicates.** Delete all `deploy/*.py`
   files classified as re-exports or legacy duplicates. The re-exports
   are safe (runtime has the originals); the legacy duplicates must
   have their runtime equivalents verified as feature-complete first.
   → **verify**: `grep -rn 'from deploy\.' .` returns zero hits
   anywhere (tests, runtime, docs).

5. **Update `ecosystem.config.js`.** Remove `deploy/` from PYTHONPATH:
   ```
   env: { PYTHONPATH: "/opt/agent-flux" }  // was "/opt/agent-flux:/opt/agent-flux/deploy"
   ```
   → **verify**: config parses; `pm2 show agent-flux-api | grep PYTHONPATH`
   shows the single-path version.

6. **Update `deploy.sh`.** Sync paths no longer need to carry
   `deploy/*.py`. Keep syncing `migrations/` (new location).
   → **verify**: next deploy succeeds; prod services start clean.

7. **Delete `deploy/` directory entirely.** Last step. Everything
   under it should now either be moved or gone.
   → **verify**: `ls deploy/ 2>&1 | grep "No such"` — directory gone.

8. **Smoke test in staging.** Restart all services, trigger each cron
   task at least once, run one end-to-end signal → opportunity →
   approval → send cycle (with test merchant). Expect no import errors,
   no missing paths.
   → **verify**: `pm2 logs --lines 500 | grep -i error` returns zero
   import/module errors.

9. **Deploy to prod.** Maintenance window recommended but not
   strictly required; rolling restart via `pm2 reload ecosystem.config.js`
   should suffice since each service restarts independently.
   → **verify**: `/metrics/prom` and Grafana panels green for 1 hour
   post-deploy.

10. **Update CLAUDE.md.** Remove the "two parallel Python trees" note;
    replace with the canonical single-tree description.
    → **verify**: grep CLAUDE.md for `deploy/` — only references should
    be to this skill.

## Rollback

- Revert the PR (single commit, large diff).
- Redeploy with old `ecosystem.config.js`.
- Migrations already applied don't care about path.

Because the deletion step is last and migrations move to a new path
(not delete), rollback is feasible up through step 7. After prod
deploy (step 9), rollback still works but requires a redeploy of the
prior artifact.

## Exit criterion

- `deploy/` directory does not exist in the repo.
- `ecosystem.config.js` has no reference to deploy/.
- CLAUDE.md updated.
- SentenceTransformer loads exactly once (verify in scheduler process
  memory — should drop noticeably below 1.5GB cap).
- 7 days in prod without deploy/-related import errors.
- Audit risk R7 cleared.

## Notes

- **This is the highest-leverage phase 1 skill.** Almost every other
  drift/correctness problem in the audit traces back to "which tree
  is canonical?" Eliminating the question eliminates the class.
- **Do not attempt in parallel with `collapse-opportunities` or
  `fk-and-indexes`.** All three touch shared schema/import surface.
  Sequential.
- **The SentenceTransformer memory win is real but bounded.** Expect
  ~400-600MB reduction in scheduler RSS; doesn't eliminate the
  `max_memory_restart` cap but makes headroom meaningful.
