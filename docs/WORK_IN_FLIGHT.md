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
- **2026-04-28** — Codex — Meridian stabilization sprint completed locally. Schema drift/runtime fallbacks, outreach parent-state sync, Gmail auth hardening, and contact-discovery seed unblock landed on `deploy-main`. Flat SQL migration `008_meridian_pipeline_stabilization.sql` is ready, but live DDL is still blocked by a stale open DB transaction that operators need to clear.
- **2026-04-28** — claude-code — finished the leaf-level fix Codex left half-done: `UPDATE opportunity_outreach_actions SET status='sent', approval_state='sent'` for 5 opps (8/9/10/12/14) where `sent_at + gmail_thread_id` were set but `status` was still `draft_ready`. Eliminates the Telegram queue's incorrect "Send Outreach" suggestions for already-sent items. Also restarted the stopped `agentflux-telegram` PM2 service and verified payment-node (the Go product) was untouched by Codex's run.
- **2026-04-29** — claude-code — [PR #13 — `draft outreach for X` Telegram command](https://github.com/Blight-East/Meridian/pull/13) — merged. Closes the operator gap where `ELIGIBILITY_OPERATOR_REVIEW_ONLY` opps had no command path; exposes `draft_outreach_for_opportunity_command(allow_best_effort=True)` via Telegram regex. Emits `operator_draft_override` event for tracking override rate.
- **2026-05-02** — claude-code — [PR #14 — trust approved drafts at send + Gmail threadId guard](https://github.com/Blight-East/Meridian/pull/14) — merged + deployed to prod. `send_outreach_for_opportunity` no longer re-validates contact_send_eligible when row already has approval + contact + body. Empty `threadId` no longer 404s `/messages/send`. Both bugs combined had blocked every initial outreach send since 4/19.
- **2026-05-02** — claude-code — [PR #15 — `#` prefix in operator regex](https://github.com/Blight-East/Meridian/pull/15) — merged + deployed. All 16 `(?P<opportunity_id>\d+)` captures now accept optional `#`. Mechanical fix that converts the most-common LLM-hallucination trigger ("Review reply #126" no longer falls through to LLM) into a clean operator success.
- **2026-05-02** — claude-code — [PR #16 — restore english-noise filter on merchant entity extraction](https://github.com/Blight-East/Meridian/pull/16) — merged + deployed. Filter was dropped during the 4/28 sprint; expanded from ~50 to 181 entries to cover article-text artifacts (japan, policy, exactly, learning, account, …). Prevents the next batch of garbage merchant_opportunities. Operationally on prod 14 bug-batch opps revoked + 3 bounce-suspected sends marked lost.
- **2026-05-02** — Codex (committed by claude-code) — [PR #17 — pipeline stabilization sprint](https://github.com/Blight-East/Meridian/pull/17) — merged + deployed. The full 15-file Codex sprint (992 LOC) committed properly with attribution. Migration 008 applied on prod. Schema-mismatch errors went from ~30/30min to 0.
- **2026-05-02** — claude-code — operational on prod (159.65.45.64): `GMAIL_SENDER_EMAIL=hello@payflux.dev` (correct account), `GOOGLE_REFRESH_TOKEN` updated, local Telegram bot stopped (was crash-looping due to duplicate-instance conflict with prod), `redis-cli del agent_flux:telegram_bot_lock` to clear stale singleton. All 5 prod services healthy.

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

- [PR #4 — Fix SELECT DISTINCT + urgency_score](https://github.com/Blight-East/Meridian/pull/4) — superseded by PR #17 (Codex sprint shipped a more comprehensive fix in `runtime/ops/deal_sourcing.py`). Safe to close.
- ~~PR #6 (LLM provider adapter)~~ — merged 2026-04-24.

## Notes for agents

- If you're an AI agent starting work, **always** check `gh pr list`
  and this file before creating a branch.
- If your skill conflicts with a current entry, either wait or pick
  a different skill from `.claude/skills/`.
- Update this file **in the same PR** as your work — don't do it as a
  separate drive-by commit (it creates merge conflicts with other
  agents doing the same).
