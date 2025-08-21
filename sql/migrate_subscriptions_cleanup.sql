-- Migration: Clean up subscriptions table
-- Changes:
-- 1. Rename 'status' column to 'is_active' and convert to boolean (0/1)
-- 2. Remove 'created_date_time' column
-- 3. Remove 'owner_id' column
-- 4. Remove 'owner_tenant_id' column
-- 5. Remove 'owner_type' column

-- Step 1: Create new table with updated schema
CREATE TABLE IF NOT EXISTS subscriptions_new (
    tenant_id TEXT(50) NOT NULL,
    subscription_id TEXT(255) NOT NULL,
    commerce_subscription_id TEXT(255),
    sku_id TEXT(255) NOT NULL,
    sku_part_number TEXT(100),
    is_active INTEGER NOT NULL DEFAULT 1,  -- Changed from status TEXT to is_active INTEGER
    is_trial INTEGER NOT NULL DEFAULT 0,
    total_licenses INTEGER NOT NULL DEFAULT 0,
    next_lifecycle_date_time TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, subscription_id)
);

-- Step 2: Copy data from old table to new table, converting status to is_active
INSERT INTO subscriptions_new (
    tenant_id,
    subscription_id,
    commerce_subscription_id,
    sku_id,
    sku_part_number,
    is_active,  -- Convert status to boolean
    is_trial,
    total_licenses,
    next_lifecycle_date_time,
    created_at,
    last_updated
)
SELECT 
    tenant_id,
    subscription_id,
    commerce_subscription_id,
    sku_id,
    sku_part_number,
    CASE 
        WHEN status = 'Enabled' THEN 1
        ELSE 0
    END as is_active,
    is_trial,
    total_licenses,
    next_lifecycle_date_time,
    created_at,
    last_updated
FROM subscriptions;

-- Step 3: Drop old table
DROP TABLE subscriptions;

-- Step 4: Rename new table to original name
ALTER TABLE subscriptions_new RENAME TO subscriptions;

-- Step 5: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant ON subscriptions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_sku ON subscriptions(sku_id);
