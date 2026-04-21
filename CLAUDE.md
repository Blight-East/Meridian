# Agent Flux — Project Context

The four behavioral principles live in `~/.claude/CLAUDE.md` (Think Before Coding, Simplicity First, Surgical Changes, Goal-Driven Execution + autonomous-mode carve-out). This file is project-specific context only.

## What this is

Agent Flux is the autonomous backend powering PayFlux — a merchant-distress intelligence and operator-assist system. Mental model: `signal → merchant → opportunity → operator-reviewed action`. Optimized for merchant operators (not consumer complaints) and processor-linked distress.

## Codebase layout

Two parallel Python trees, both on `PYTHONPATH` (see `ecosystem.config.js`):

- **`runtime/`** — newer, modular layout (api, channels, scheduler, intelligence, investigation, etc.). Entry: `runtime/main.py`.
- **`deploy/`** — older flat layout still running in production. Entry: `deploy/main.py`. SQL migrations live here as `*.sql` files.

When editing, check whether a feature exists in `runtime/`, `deploy/`, or both. Don't duplicate — match where the live code path actually runs.

Other dirs:
- `tools/` — standalone tool implementations (browser, web fetch, db query, gmail oauth)
- `safety/` — `guard.py`, `rate_limiter.py` — respect these on any new outbound action
- `config/` — logging config
- `memory/` — agent semantic memory (separate from Claude's `~/.claude/.../memory/`)

## Sister repo

Frontend lives at `/Users/ct/payflux-prod/payflux-site/` (React, deployed to Netlify). Backend changes that need UI exposure require edits there too.

## Canonical references (read before changing related code)

- [PAYFLUX_CANONICAL_TAXONOMY.md](PAYFLUX_CANONICAL_TAXONOMY.md) — authoritative ontology (merchants, signals, opportunities). Don't invent new categories without updating this.
- [PLAN.md](PLAN.md) — phased autonomous-CEO build plan; check before starting new modules
- [CHANNEL_ACTION_LAYER.md](CHANNEL_ACTION_LAYER.md) — channel/action contract
- [CONTACT_DISCOVERY_STALLED_PLAN.md](CONTACT_DISCOVERY_STALLED_PLAN.md), [SYSTEM_INCIDENT_2026-03-14.md](SYSTEM_INCIDENT_2026-03-14.md) — recent incident/recovery context

## Production

- Host: `159.65.45.64`
- Process manager: PM2, services defined in [ecosystem.config.js](ecosystem.config.js): `agent-flux-api` (uvicorn :8000), `agent-flux-worker`, `agent-flux-scheduler`, `agentflux-telegram`, `meridian-mcp` (server-only).
- Deploy: [deploy.sh](deploy.sh) (SCPs files + PM2 restart). Migrations are run manually after SCP.
- Logs: `$AGENT_FLUX_LOG_DIR` (defaults to `/opt/agent-flux/logs` on prod, `/tmp/agent-flux/logs` locally).
- Local control: [start.sh](start.sh), [stop.sh](stop.sh), [status.sh](status.sh).

## Hard rules (don't break these without asking)

- **Gmail/email outreach is approval-gated.** Never flip `approval_required` to auto-send. Drafts only until operator approves via Telegram.
- **`noise_system` and `test_thread` rows are suppressed** from signal/opportunity creation — don't bypass the filter.
- **Consumer complaints are down-ranked** in `merchant_signal_classifier.py` / `entity_taxonomy.py` — preserve that bias.
- **No autonomous money movement, no auto-send to merchants** — these are the only actions that should still block on human approval even in autonomous mode.

## Scheduler cadence (current)

Set in `runtime/scheduler/cron_tasks.py` (and/or `deploy/cron_tasks.py`). Heartbeats run every 15–60 min; market reports every 6 hr; daily report every 24 hr. Don't add a new schedule entry without checking memory/CPU headroom — scheduler already has a 1.5GB max_memory_restart cap due to a known SentenceTransformer double-load.

## When in doubt

- Check what's actually running: `./status.sh` or PM2 logs.
- Check the canonical taxonomy before adding a new category, signal type, or status enum.
- Prefer editing the path that's actually in production over the prettier one.
