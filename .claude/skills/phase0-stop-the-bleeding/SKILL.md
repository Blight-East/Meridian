---
name: phase0-stop-the-bleeding
description: Orchestrator for phase 0 fixes — the smallest set of changes that make the system trustworthy enough to ship a paid product on top of. Parallel-safe.
phase: 0
status: draft
blast_radius: medium (6 PRs, each scoped)
parallelizable: true
---

# Phase 0 — Stop the bleeding

## Why

Before building product (phase 2) or redesigning Meridian (phase 3), the
baseline system has to be trustworthy. Phase 0 is the minimum set of fixes
the audit identified as *dangerous-if-unfixed*. Each fix is scoped, reversible,
and independent — they ship as 6 separate PRs off `main`, reviewed individually.

## Sub-skills (all parallelizable)

| Skill | Risk class | Files touched | Est. size |
|---|---|---|---|
| [wire-rate-limiter](../wire-rate-limiter/SKILL.md) | runaway cost | `runtime/conversation/chat_engine.py`, `safety/rate_limiter.py` | ~80 LOC |
| [merchant-opp-dedup](../merchant-opp-dedup/SKILL.md) | duplicate outreach | migration + `deal_sourcing.py` | ~30 LOC + 1 SQL |
| [remove-auto-send-path](../remove-auto-send-path/SKILL.md) | accidental mass send | `runtime/channels/policy.py`, `store.py` | ~40 LOC |
| [dlq-retry-policy](../dlq-retry-policy/SKILL.md) | tasks rot silently | `runtime/worker.py`, new `runtime/ops/dlq.py` | ~150 LOC |
| [prom-exporter-on-telemetry](../prom-exporter-on-telemetry/SKILL.md) | blind to outages | new `runtime/api/metrics_prom.py` + Grafana | ~200 LOC + dashboard JSON |
| [apply-db-hardening-prod](../apply-db-hardening-prod/SKILL.md) | DB lock incidents | `deploy.sh` flag flip, apply SQL | config only |

## Execution order

**All sub-skills are independent** — they touch disjoint files. Run in
parallel worktrees, each as its own PR:

```
git worktree add ../mf-rate-limiter       plan/wire-rate-limiter        origin/main
git worktree add ../mf-opp-dedup          plan/merchant-opp-dedup       origin/main
git worktree add ../mf-auto-send          plan/remove-auto-send-path    origin/main
git worktree add ../mf-dlq                plan/dlq-retry-policy         origin/main
git worktree add ../mf-prom               plan/prom-exporter            origin/main
# apply-db-hardening-prod is config, no worktree needed
```

## Preconditions

1. [PR #6 (LLM provider adapter)](https://github.com/Blight-East/Meridian/pull/6)
   reviewed and merged first — Meridian is degraded in prod until it is.
2. `docs/WORK_IN_FLIGHT.md` checked: no other agent touching these files.
3. [payflux-guardrails](../payflux-guardrails/SKILL.md) read and
   applied.

## Exit criterion (gates phase 1)

All six PRs merged **and** verified in prod:

- Rate limiter: artificial burst triggers `RateLimitExceeded` in logs.
- DLQ: kill a worker mid-task, confirm task moves to `agent_tasks_dlq`
  after retry exhaustion.
- UNIQUE constraint: `\d+ merchant_opportunities` in psql shows the
  constraint.
- Auto-send path: grep confirms `auto_send_high_confidence` handler is
  removed; only `approval_required` remains.
- Prometheus: `curl http://prod/metrics/prom` returns parseable metrics
  for at least these series: `signals_ingested_total`,
  `worker_tasks_inflight`, `scheduler_task_duration_seconds`,
  `tool_invoked_total`.
- DB hardening: `SHOW idle_in_transaction_session_timeout` returns
  `10min`; `schema_migrations` table has rows.

## Rollback

Each sub-skill has its own rollback (single revert). Phase 0 as a whole
has no "rollback all" — if one sub-skill causes issues, revert just that
PR.

## Risks

- **Coordination with Devin.** Devin has been active on this repo.
  Before starting, check `WORK_IN_FLIGHT.md` and the open PR list.
  If Devin has an adjacent PR open on any of these files, coordinate
  first.
- **Migration timing.** `merchant-opp-dedup` adds a UNIQUE constraint.
  If there are existing duplicates in prod, the migration fails. The
  skill's step 2 handles this (de-dupe existing rows before constraint),
  but it requires a maintenance window in prod.

## After phase 0

Phase 1 (`unify-tree`, `collapse-opportunities`, `fk-and-indexes`) can
begin. Phase 2 (`risk-report-mvp`) can begin in parallel with phase 1
because the frontend work is decoupled.
