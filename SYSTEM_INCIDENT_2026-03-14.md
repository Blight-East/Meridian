# SYSTEM INCIDENT RECORD: 2026-03-14

## 1. User-Visible Symptoms
- **Stale Reporting**: Bot reported "last scan" times stuck at 2026-03-14 07:42 UTC despite manual triggers (10+ hour gap).
- **Execution Failures**: Repeated "That ran long, so I stopped early" messages in the chat layer.
- **Reporting Untruthfulness**: Pipeline actions reported success (e.g., "Scan started!") but no underlying state changed in Redis or PostgreSQL.
- **Metrics Inconsistency**: `signals_total` reported 0 while `signals_24h` showed 7-8, and `queue_depth` falsely showed 0 while tasks were actually stalled.
- **Scheduler Stalling**: Autonomous scheduler failed to fire discovery cycles or pipeline heartbeats.

## 2. Confirmed Root Causes
- **Service Outage**: `agent-flux-worker` process was DOWN.
- **Process Stalling**: `agent-flux-scheduler` was running but spinning at 86% CPU in a tight failure loop.
- **Schema Mismatches**: Silent failures in the intelligence layer due to missing database columns:
    - `signals.classification`
    - `clusters.signal_ids`
    - `merchant_contacts.contact_name`
- **Missing Side-Effect Logic**: `merchant_scanner.py` lacked the `r_client.set("last_scan_time", ...)` call required for persistent reporting of manual scans.
- **Reporting Bug**: Metrics implementation in `main.py` had a failure path where `signals_total` defaulted to 0 if a sub-query failed or was unmapped.
- **Chat Layer Optimism**: `telegram_bot.py` and `chat_engine.py` were reporting immediate success for background tasks without verifying task ingestion or worker connectivity.

## 3. Exact Schema Fixes Applied
```sql
ALTER TABLE signals ADD COLUMN IF NOT EXISTS classification VARCHAR(255);
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS signal_ids TEXT[];
ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS contact_name VARCHAR(255);
```

## 4. Exact Files Changed
- [merchant_scanner.py](file:///opt/agent-flux/runtime/ingestion/merchant_scanner.py): Added Redis persistence for `last_scan_time`.
- [worker.py](file:///opt/agent-flux/runtime/worker.py): Added explicit handlers for `fix_pipeline` and `run_sales_cycle`.
- [router.py](file:///opt/agent-flux/runtime/dispatcher/router.py): Added keyword routing for pipeline and sales tasks.
- [chat_engine.py](file:///opt/agent-flux/runtime/conversation/chat_engine.py): Fixed untruthful "started" reporting and stale metrics retrieval.
- [telegram_bot.py](file:///opt/agent-flux/runtime/telegram_bot.py): Modified response logic to reflect background queuing.
- [main.py](file:///opt/agent-flux/runtime/main.py): Corrected metrics aggregation and reporting persistence.

## 5. Services Restarted
- `agent-flux-api`
- `agent-flux-scheduler`
- `agent-flux-worker`

## 6. Corrected Production Restart Command
```bash
ssh root@159.65.45.64 "
cd /opt/agent-flux &&
pm2 stop all &&
pm2 delete all &&
pm2 start ecosystem.config.js
"
```

## 7. Production Verification Proof (VPS: 159.65.45.64)
- **Worker Process**: PID `1424454` running in `/opt/agent-flux`.
- **Queue Drainage**: Redis `agent_tasks` drained from 3 → 0.
- **Scan Success**: `scan_merchants` execution confirmed in logs; `last_scan_time` verified as `2026-03-14T19:41:25Z`.
- **Pipeline Fix**: `fix_pipeline` executed diagnostics and returned `health_status: degraded`.
- **Sales Cycle**: `run_sales_cycle` executed logic and confirmed `blocked_low_distress: 1`.

## 8. Final Known-Good System State
- **API**: Healthy (`/health` status 200).
- **Scheduler**: Healthy (0% idle CPU, firing discovery cycles).
- **Worker**: Healthy (Processing background tasks correctly).
- **Core State**: Queue depth 0; metrics populated; `operator_state` consistent with DB.
- **Safety**: Gmail approval mode (`GMAIL_CHANNEL_MODE=approval_required`) and outreach rules remain UNCHANGED.

## 9. Remaining Non-Outage Issue
- `contact_discovery_stalled`: The diagnostic pipeline identifies that while the system is functional, contact discovery for unattributed merchants is currently not producing net-new leads.
