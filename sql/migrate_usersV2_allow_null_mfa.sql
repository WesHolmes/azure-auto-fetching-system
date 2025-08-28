-- Migration to allow NULL values in is_mfa_compliant column for non-premium users
-- This allows us to distinguish between non-premium users (NULL) and premium users with actual MFA data (0 or 1)

-- First, create a temporary table with the new schema
CREATE TABLE usersV2_new (
    user_id TEXT(50),
    tenant_id TEXT(50) NOT NULL,
    user_principal_name TEXT(255) NOT NULL,
    primary_email TEXT(255) NOT NULL,
    display_name TEXT(255),
    department TEXT(100),
    job_title TEXT(100),
    office_location TEXT(100),
    mobile_phone TEXT(50),
    account_type TEXT(50),
    account_enabled INTEGER NOT NULL DEFAULT 1,
    is_global_admin INTEGER NOT NULL DEFAULT 0,
    is_mfa_compliant INTEGER, -- Removed NOT NULL constraint to allow NULL for non-premium users
    license_count INTEGER NOT NULL DEFAULT 0,
    group_count INTEGER NOT NULL DEFAULT 0,
    last_sign_in_date TEXT, -- ISO datetime format
    last_password_change TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, tenant_id)
);

-- Copy existing data, converting 0 values to NULL for non-premium users (this will be handled by application logic)
INSERT INTO usersV2_new 
SELECT 
    user_id,
    tenant_id,
    user_principal_name,
    primary_email,
    display_name,
    department,
    job_title,
    office_location,
    mobile_phone,
    account_type,
    account_enabled,
    is_global_admin,
    is_mfa_compliant, -- Keep existing values for now, will be updated by application logic
    license_count,
    group_count,
    last_sign_in_date,
    last_password_change,
    created_at,
    last_updated
FROM usersV2;

-- Drop the old table and rename the new one
DROP TABLE usersV2;
ALTER TABLE usersV2_new RENAME TO usersV2;

-- Recreate the index
CREATE INDEX idx_usersV2_tenant ON usersV2(tenant_id);

-- Update existing records to set is_mfa_compliant to NULL (this represents non-premium users)
-- Note: This is a placeholder - the actual logic will be handled by the application
-- when it determines tenant premium status during sync
