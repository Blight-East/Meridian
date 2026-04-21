-- Merchant Identity Resolution Migration
-- Extends existing merchants and merchant_signals tables

-- Add missing columns to merchants
ALTER TABLE merchants ADD COLUMN IF NOT EXISTS normalized_domain TEXT;
ALTER TABLE merchants ADD COLUMN IF NOT EXISTS detected_from TEXT;
CREATE INDEX IF NOT EXISTS idx_merchants_normalized_domain ON merchants(normalized_domain);

-- Add missing columns to merchant_signals
ALTER TABLE merchant_signals ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add merchant_id to signals for direct linkage
ALTER TABLE signals ADD COLUMN IF NOT EXISTS merchant_id INTEGER;
