---
name: prom-exporter-on-telemetry
description: Expose existing tool_invoked + scheduler events as Prometheus metrics, ship a Grafana dashboard with 6 critical panels. Use to get real-time system health visibility.
phase: 0
status: draft
blast_radius: low (new endpoint + sidecar, no behavior change)
parallelizable: true
---

# Prom exporter on top of existing telemetry

## Why

As of 946eb69, the codebase emits `tool_invoked` events with
`ok/duration_ms/error` per tool call, on every surface
(worker / telegram / scheduler / chat). That's a goldmine — but it lives
in the `events` table with no aggregation, no alerting, no dashboard.
Operators cannot answer "is the system working right now?" without
SSH'ing and grepping logs.

Audit risk **R8**. The 2026-03-14 incident would have been caught in
minutes by two Prom alerts (ingestion rate drop, worker inflight depth)
— neither exists today.

## Blast radius

- **New module** `runtime/api/metrics_prom.py` — exposes `/metrics/prom`
  on the existing `agent-flux-api` service. Reads aggregations from DB
  + Redis, formats as OpenMetrics.
- **Grafana dashboard JSON** checked into
  `observability/grafana/dashboard-main.json`.
- **Alertmanager rules** checked into
  `observability/alerts/rules.yml`.

Does not touch: pipeline logic, DB schema, worker internals. Purely
read-only exporter.

## Preconditions

1. 946eb69 merged (provides `tool_invoked` events). Already merged.
2. A Prometheus instance reachable from prod. If none exists, step 0:
   stand one up (Docker one-shot is fine).
3. A Grafana instance (same comment).

## Steps

1. **Provision Prom + Grafana.** Minimal Docker Compose at
   `observability/docker-compose.yml` — Prom scrapes
   `http://159.65.45.64:8000/metrics/prom` every 15s, Grafana reads
   from Prom. If user prefers managed (Grafana Cloud), config that
   instead.
   → **verify**: `docker-compose up -d` and
   `curl http://localhost:9090/-/healthy` returns OK.

2. **Write the exporter.** `runtime/api/metrics_prom.py`:
   - Reads from `events` table with window (last 5 min) for counters,
     last 60s for gauges.
   - Reads Redis `agent_tasks` LLEN, `agent_tasks_inflight` LLEN,
     `agent_tasks_retry` ZCARD, `agent_tasks_dlq` LLEN.
   - Reads `agent_flux:health:*` hashes for per-service heartbeat.
   - Outputs OpenMetrics text format.
   → **verify**: `curl http://localhost:8000/metrics/prom` parses with
   `promtool check metrics`.

3. **Metric series to expose** (minimum):
   - `signals_ingested_total{source}` — counter
   - `signals_classified_total{classification}` — counter
   - `opportunities_created_total` — counter
   - `worker_tasks_inflight` — gauge
   - `worker_tasks_retry_depth` — gauge
   - `worker_tasks_dlq_depth` — gauge
   - `tool_invoked_total{surface, tool, ok}` — counter
     (direct from events.event_type='tool_invoked')
   - `tool_duration_seconds{surface, tool}` — histogram
   - `scheduler_task_duration_seconds{task}` — histogram
   - `rate_limit_hit_total{bucket}` — counter
   - `approval_queue_depth` — gauge (from pending_review count)
   → **verify**: `promtool query instant 'worker_tasks_inflight'` returns
   a value after a task runs.

4. **Grafana dashboard** with 6 panels (no more — resist dashboard bloat):
   - Panel 1: Signal ingestion rate (rate()) by source
   - Panel 2: Classification mix (stacked area, last 6h)
   - Panel 3: Opportunity funnel (signals → merchants → opportunities →
     approved → sent) stacked counts
   - Panel 4: Queue depths (all 4 queues) single-stat group
   - Panel 5: Tool invocation success rate (`rate(tool_invoked_total{ok="true"}) / rate(tool_invoked_total)`)
   - Panel 6: p95 scheduler task duration by task name (heatmap)
   → **verify**: dashboard renders with live data within 5 minutes of
   deploy.

5. **Alertmanager rules** (the only two that would have caught
   2026-03-14):
   ```yaml
   - alert: IngestionRateDropped
     expr: rate(signals_ingested_total[6h]) < 0.3 * rate(signals_ingested_total[6h] offset 1d)
     for: 15m
   - alert: WorkerInflightHigh
     expr: worker_tasks_inflight > 50
     for: 10m
   ```
   Plus:
   ```yaml
   - alert: DLQGrowing
     expr: increase(task_abandoned_total[30m]) > 3
   - alert: ToolErrorRateHigh
     expr: rate(tool_invoked_total{ok="false"}[10m]) / rate(tool_invoked_total[10m]) > 0.2
   ```
   Route Alertmanager → Telegram (reuse `send_operator_alert`).
   → **verify**: trigger each alert artificially in staging; confirm
   Telegram message arrives.

6. **Document in `/docs/runbook/observability.md`.** What each panel
   shows, where to look first when paged, how to silence alerts, how
   to add new metrics.
   → **verify**: runbook links resolve.

## Rollback

- Remove the scrape config from Prom → metrics stop collecting.
- Remove `/metrics/prom` endpoint → 404 returns, Prom scrape errors
  but system otherwise unaffected.
- Grafana dashboard JSON can be deleted freely.

No schema change.

## Exit criterion

- Dashboard live, 6 panels populated with real data.
- All 4 alerts fire correctly in staging.
- Operator can answer "is the system working right now?" in 10
  seconds of looking at the dashboard.
- Audit risk R8 cleared.

## Notes

- **Keep the exporter stateless.** It reads from `events` + Redis on
  each scrape. No in-memory aggregation that could drift.
- **Don't query events with unbounded windows.** 5-min counters = 5-min
  query window. Use indexes on `(event_type, created_at)` — if they
  don't exist, add them in the `fk-and-indexes` skill.
- **Expected pay-off window:** first outage caught by an alert
  (vs. by a human noticing) is the point at which this skill has paid
  for itself. Likely within the first month.
