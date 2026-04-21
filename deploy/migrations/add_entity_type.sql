-- Entity Taxonomy Migration
-- Adds entity_type columns to signals and qualified_leads tables
-- Run once before deploying the entity taxonomy module

ALTER TABLE signals ADD COLUMN IF NOT EXISTS entity_type TEXT DEFAULT 'unknown';
ALTER TABLE signals ADD COLUMN IF NOT EXISTS classification TEXT DEFAULT NULL;
ALTER TABLE qualified_leads ADD COLUMN IF NOT EXISTS entity_type TEXT DEFAULT 'unknown';
