-- 008_meridian_pipeline_stabilization.sql
-- Additive schema repair for Meridian pipeline drift.
-- Safe to re-run. Never drops or rewrites tables.

BEGIN;

SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE IF EXISTS qualified_leads
    ADD COLUMN IF NOT EXISTS lead_priority DOUBLE PRECISION DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lifecycle_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS merchant_name TEXT,
    ADD COLUMN IF NOT EXISTS merchant_website TEXT,
    ADD COLUMN IF NOT EXISTS industry TEXT,
    ADD COLUMN IF NOT EXISTS location TEXT,
    ADD COLUMN IF NOT EXISTS investigation_notes TEXT;

ALTER TABLE IF EXISTS merchants
    ADD COLUMN IF NOT EXISTS urgency_score DOUBLE PRECISION DEFAULT 0;

UPDATE merchant_opportunities mo
SET status = mapped.desired_status
FROM (
    SELECT
        ooa.opportunity_id,
        CASE
            WHEN COALESCE(ooa.status, '') IN ('sent', 'replied', 'follow_up_needed', 'won', 'lost', 'ignored')
                OR COALESCE(ooa.approval_state, '') = 'sent'
                OR ooa.sent_at IS NOT NULL
                THEN 'outreach_sent'
            WHEN COALESCE(ooa.status, '') IN ('awaiting_approval', 'draft_ready')
                OR COALESCE(ooa.approval_state, '') IN ('approval_required', 'approved')
                THEN 'outreach_pending'
            ELSE NULL
        END AS desired_status
    FROM opportunity_outreach_actions ooa
) AS mapped
WHERE mapped.opportunity_id = mo.id
  AND mapped.desired_status IS NOT NULL
  AND COALESCE(mo.status, 'pending_review') NOT IN ('rejected', 'converted')
  AND COALESCE(mo.status, 'pending_review') IS DISTINCT FROM mapped.desired_status;

COMMIT;
