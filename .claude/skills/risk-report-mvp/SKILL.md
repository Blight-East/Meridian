---
name: risk-report-mvp
description: Ship the merchant-facing free "Processor Risk Report" — the acquisition wedge. Phase 2. Use to convert the existing backend pipeline into a product surface.
phase: 2
status: draft
blast_radius: medium (new API route + frontend page, opt-in traffic)
parallelizable: with phase 1 (frontend decoupled)
---

# Free Processor Risk Report — the acquisition wedge

## Why

Audit diagnosis: **you don't have a product, you have a signal pipeline
stapled to an operator approval inbox.** Paying customers need to see
the output of the pipeline *themselves*. The cheapest way to test
whether merchants want this is a free, single-shot report.

Form → backend runs existing pipeline on the domain → 1-page PDF back
within 60s. Email capture on the form gates the report. Nurture to
$99 Monitor tier.

This is the **smallest thing that proves the business**. Without it,
everything else in the roadmap is speculation.

## Blast radius

- **Backend:** new FastAPI route `POST /api/risk-report` on
  `agent-flux-api`. New helper `runtime/product/risk_report.py` that
  orchestrates the existing ingest + classify + rank pipeline against
  a single domain.
- **Frontend:** new page on `/Users/ct/payflux-prod/payflux-site/` at
  `/risk-report`. Simple form (domain + email). Renders PDF inline or
  downloads.
- **Data:** new table `risk_report_requests` for the request log +
  email capture + rate limiting.
- Existing pipeline: read-only. This skill does **not** alter
  classifier, scorer, or approval-gate logic.

## Preconditions

1. Phase 0 complete (rate limiter wired; otherwise this endpoint could
   be abused for free compute).
2. Provider adapter (PR #6) merged — this feature will be LLM-heavy
   for summaries.
3. Email service configured for nurture (Mailchimp, Customer.io,
   or SendGrid — pick one during planning).

## Design

### Endpoint

```
POST /api/risk-report
Content-Type: application/json
{
  "domain": "example.com",
  "email": "operator@example.com",
  "captcha_token": "..."
}

-> 202 Accepted
{
  "request_id": "uuid",
  "status_url": "/api/risk-report/uuid/status"
}
```

Polling endpoint `/api/risk-report/{id}/status`:
```
{ "status": "queued" | "running" | "done" | "error", "report_url": "/api/risk-report/{id}/pdf" }
```

### Pipeline

On request:
1. Rate-limit by IP (5/hour) and by email domain (2/hour).
2. Enqueue a task `generate_risk_report` in Redis.
3. Worker picks up → runs existing signal ingestion *scoped to this
   domain*, classifies, scores, generates PDF via
   `runtime/product/pdf_renderer.py` (reportlab or weasyprint).
4. PDF cached in S3 (or local storage MVP) with request_id.
5. Email with download link sent to operator. Operator added to
   nurture list in email service.

### PDF contents (1 page, opinionated)

- Header: "Processor Risk Report — example.com — 2026-04-23"
- Risk tier: Low / Elevated / High (based on existing scoring)
- 3 signals found (or "No recent signals — worth monitoring
  preemptively")
- Peer comparison: 3 anonymized merchants in similar risk tier
- Next steps: "Consider continuous monitoring — [CTA link to Monitor
  tier]"
- Footer: legal disclaimer per CLAUDE.md messaging constraints —
  "pattern detection and evidence-backed alerts," not "we predict
  Stripe action."

### Captcha

hCaptcha or Turnstile on the form. Required — this endpoint will be
scraped instantly without it.

## Steps

1. **Migration `014_risk_report_requests.sql`:**
   ```sql
   CREATE TABLE risk_report_requests (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     domain TEXT NOT NULL,
     email TEXT NOT NULL,
     ip_hash TEXT,
     status TEXT NOT NULL DEFAULT 'queued',
     report_path TEXT,
     error_message TEXT,
     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     completed_at TIMESTAMPTZ
   );
   CREATE INDEX ON risk_report_requests (email, created_at DESC);
   CREATE INDEX ON risk_report_requests (status, created_at);
   ```
   → **verify**: table created.

2. **Backend skeleton.** `runtime/api/risk_report_routes.py` registering
   POST / status / PDF endpoints. Captcha verification middleware.
   → **verify**: curl the endpoint locally with a fake captcha stub;
   returns 202.

3. **Pipeline wrapper.** `runtime/product/risk_report.py`:
   - Input: domain.
   - Steps: (a) targeted ingest from web/forums for this domain, (b)
     classify via existing classifier, (c) score via existing scorer,
     (d) find peer cohort (3 anonymized merchants with similar
     industry + tier).
   - Output: dict passed to PDF renderer.
   - **Critical:** use the same classifier + scorer as the main
     pipeline. No LLM free-text summary that could make predictive
     claims violating CLAUDE.md messaging constraints.
   → **verify**: unit test with known-domain → expected tier.

4. **PDF renderer.** `runtime/product/pdf_renderer.py` using
   weasyprint (HTML → PDF, easier to style) or reportlab (more
   control, less pretty). Recommend weasyprint for MVP.
   Layout from design above.
   → **verify**: generates 1-page PDF, renders correctly on Preview +
   Chrome + Acrobat.

5. **Worker task.** Register `generate_risk_report` in the worker's
   TOOLS dispatch (post-946eb69 registry). Emit telemetry events at
   start + end.
   → **verify**: enqueue a task manually via Redis; worker picks up,
   report lands in storage, email fires.

6. **Email integration.** Choose one:
   - Mailchimp (cheap, easy, lists).
   - Customer.io (better for drip, API-first).
   - SendGrid (simplest transactional).
   Wire into `runtime/product/risk_report.py` completion hook.
   → **verify**: test run sends email to a test address.

7. **Frontend page.** On `payflux-site`:
   - Route `/risk-report` with form (domain, email, captcha).
   - Submit handler → POST backend, poll status, show result inline
     or trigger PDF download.
   - Meta tags + Open Graph for sharing.
   → **verify**: end-to-end test from the actual frontend → PDF in
   hand.

8. **Analytics.** At minimum: track form submission + report
   generation + email-link click. Connect to PostHog (or existing
   analytics on payflux-site).
   → **verify**: dashboard shows event stream.

9. **Legal review.** Before launching publicly, have a lawyer (or
   lawyer-in-a-box service) review the PDF disclaimer + site copy.
   CLAUDE.md messaging constraints are the starting point, not the
   endpoint.
   → **verify**: legal sign-off documented in
   `docs/legal/risk-report-disclaimer.md`.

10. **Soft launch.** Link from PayFlux site header. Target: 100
    reports in 14 days, 20% email-capture follow-up rate.
    → **verify**: analytics dashboard hits targets.

## Rollback

- Disable frontend route. Backend stays; unreachable is fine.
- Revert worker task registration.
- `risk_report_requests` table stays (audit trail).

## Exit criterion

- 100 real reports generated (not tests).
- At least 20 emails confirmed valid + opted in to nurture.
- 5+ inbound asks about continuous monitoring (paid tier interest
  signal).
- No legal/messaging-constraint incidents.
- Report p95 generation time < 90s (critical — longer than that,
  people abandon the page).

## Notes

- **Keep the MVP boring.** No AI chat, no personalization, no "interactive
  dashboard." A form, a PDF, an email. Ship in 2 weeks. Iterate based
  on conversion data.
- **Rate limiting is NOT optional.** Without it, a single scraper
  burns your LLM credits in a day.
- **The PDF is marketing copy as much as it is a report.** Hire a
  designer for the template. The content is generated; the aesthetic
  sells the paid tier.
- **Don't auto-create accounts.** Email capture is enough. The paid
  signup flow is a separate phase (post-this-skill) once you see
  inbound interest.
- **CLAUDE.md messaging constraints are sacred.** No claim that
  "Stripe will freeze you." Phrasing is "pattern detection,"
  "evidence-backed risk indicators," "peer cohort comparison." Get
  legal to sign off on the exact wording.
