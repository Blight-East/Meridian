# PayFlux / Meridian Skills

Each skill is a scoped, verifiable playbook for a specific fix or feature.
Invoke with `/<skill-name>` or spawn via an Agent call with a fresh worktree.

## Structure

```
payflux-guardrails/           behavioral — hard rules (karpathy-style)
phase0-stop-the-bleeding/     orchestrator — coordinates phase 0 skills
wire-rate-limiter/            phase 0 — wire safety/rate_limiter.py
merchant-opp-dedup/           phase 0 — UNIQUE constraint on merchant_opportunities
remove-auto-send-path/        phase 0 — kill auto_send_high_confidence mode
dlq-retry-policy/             phase 0 — drain inflight → DLQ with retry
prom-exporter-on-telemetry/   phase 0 — Prom + Grafana on top of tool_invoked events
apply-db-hardening-prod/      phase 0 — apply opt-in parts of b0eed03
fk-and-indexes/               phase 1 — FK constraints + hot-path indexes
unify-tree/                   phase 1 — kill deploy/, canonicalize runtime/
collapse-opportunities/       phase 1 — merge opportunities + merchant_opportunities
meridian-router-redesign/     phase 3 — router + specialists, tool scoping
risk-report-mvp/              phase 2 — free processor risk report wedge
```

## Phase order

- **Phase 0** (parallel): independent fixes, each in its own worktree/PR
- **Phase 1** (sequential): state migrations, touch shared schema
- **Phase 2**: product wedge (risk report + payflux-site)
- **Phase 3**: meridian architecture redesign

## Verification gate between phases

Before phase N+1 starts, phase N must show green on its exit criteria
(listed in each skill's `## Exit criterion` section).

## Skill format

Each `SKILL.md` has: frontmatter, `## Why`, `## Blast radius`, `## Preconditions`,
`## Steps` (each with a per-step verify), `## Rollback`, `## Exit criterion`.
Behavioral skills (payflux-guardrails) are karpathy-style rule lists instead.

## Coordinating with other agents

This repo has multiple AI agents writing to it (Devin + this scaffold).
Before starting any skill, check [docs/WORK_IN_FLIGHT.md](../../docs/WORK_IN_FLIGHT.md)
and update it when you begin. Skills should be stale-safe — if Devin
already shipped the fix, the skill's preconditions will catch it.
