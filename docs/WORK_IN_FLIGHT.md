# Work in Flight

This repo has multiple AI agents (Devin + Claude Code skills) writing to
it in parallel. Without coordination, they stomp on each other. This
doc is the lightweight coordination register.

**Rule: before starting any non-trivial work, read this file and add
an entry. When you finish, remove your entry (or mark `DONE`).**

## How to use

Each entry has:
- **Agent** — who (Devin, claude-code, specific person)
- **Started** — ISO date
- **Skill / PR** — which skill or PR this is
- **Branch** — the branch name
- **Files touched (broad)** — top-level paths
- **ETA** — rough estimate
- **Blocks on** — what must land first

## Currently active

<!-- Add entries here. Remove when done or flip to DONE. -->

### Example format

```
### Agent: Devin
- **Started:** 2026-04-22
- **Skill / PR:** [PR #6 — LLM provider adapter](https://github.com/Blight-East/Meridian/pull/6)
- **Branch:** `devin/1776896834-anthropic-model-audit`
- **Files:** `runtime/reasoning/llm_provider.py`, `runtime/claude.py`, `runtime/conversation/chat_engine.py`, 6 others
- **ETA:** ready for review
- **Blocks on:** founder review + merge
```

## Recently completed (last 14 days)

<!-- Prune aggressively. This section is a memory aid, not an archive. -->

- **2026-04-22** — Devin — [b0eed03 DB hardening](https://github.com/Blight-East/Meridian/commit/b0eed03) — merged to main. Opt-in parts (pg_timeouts.sql application, deploy.sh flag, roles.sql) pending operator — see [apply-db-hardening-prod](../.claude/skills/apply-db-hardening-prod/SKILL.md).
- **2026-04-22** — Devin — [946eb69 Meridian tool-fix pack](https://github.com/Blight-East/Meridian/commit/946eb69) — merged to main. Provides `tool_invoked` telemetry + `@instrument` decorator + slash commands.
- **2026-04-22** — Devin — PR #2 conversion-layer-upgrade — merged.
- **2026-04-22** — founder — [5d02dd5 Fix contact_discovery stall](https://github.com/Blight-East/Meridian/commit/5d02dd5) — merged.

## Conflicts to watch for

- **Phase 0 skills run in parallel worktrees** but touch:
  - `wire-rate-limiter` → `chat_engine.py`. If any other agent edits
    `call_anthropic` / `call_llm`, coordinate.
  - `remove-auto-send-path` → `channels/policy.py`, `store.py`,
    `service.py`. Any channel work needs to check here first.
  - `dlq-retry-policy` → `runtime/worker.py`. Any worker change
    coordinates.
  - `prom-exporter-on-telemetry` → new `runtime/api/metrics_prom.py`
    + `observability/` subtree. Should be conflict-free.
  - `apply-db-hardening-prod` → config/ops only, no code. Conflict-free.
  - `merchant-opp-dedup` → new migration + `deal_sourcing.py` insert.
    Coordinate with any deal-sourcing changes.

- **Phase 1 skills are sequential.** Do not start any of
  `unify-tree`, `collapse-opportunities`, `fk-and-indexes` while any
  other is in flight — they share the migration and import surface.

## Review queue (PRs waiting on operator action)

- [PR #6 — LLM provider adapter](https://github.com/Blight-East/Meridian/pull/6) — **URGENT** (Anthropic billing out of credits; Meridian degraded in prod). Review verdict from claude-code-guide pending inline in the ongoing session.
- [PR #4 — Fix SELECT DISTINCT + urgency_score](https://github.com/Blight-East/Meridian/pull/4) — small bug fix, low stakes.

## Notes for agents

- If you're an AI agent starting work, **always** check `gh pr list`
  and this file before creating a branch.
- If your skill conflicts with a current entry, either wait or pick
  a different skill from `.claude/skills/`.
- Update this file **in the same PR** as your work — don't do it as a
  separate drive-by commit (it creates merge conflicts with other
  agents doing the same).
