-- Migration to allow NULL values in is_mfa_compliant column
-- This fixes the issue where non-premium users couldn't have NULL MFA compliance values

-- Create a temporary table with the new schema
CREATE TABLE usersV2_temp (
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
    is_mfa_compliant INTEGER DEFAULT 0,  -- Removed NOT NULL constraint to allow NULL values
    license_count INTEGER NOT NULL DEFAULT 0,
    group_count INTEGER NOT NULL DEFAULT 0,
    last_sign_in_date TEXT, -- ISO datetime format
    last_password_change TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, tenant_id)
);

-- Copy all existing data to the temporary table
INSERT INTO usersV2_temp 
SELECT * FROM usersV2;

-- Drop the old table
DROP TABLE usersV2;

-- Rename the temporary table to the original name
ALTER TABLE usersV2_temp RENAME TO usersV2;

-- Recreate indexes if any exist
-- (Add any specific indexes that were on the original table)

PRAGMA table_info(usersV2);  -- Verify the new schema
