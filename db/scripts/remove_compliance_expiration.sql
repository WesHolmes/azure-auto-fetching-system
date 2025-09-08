-- Migration script to remove compliance_expiration_date column from azure_devices table

-- Step 1: Create new azure_devices table without compliance_expiration_date
CREATE TABLE IF NOT EXISTS azure_devices_reordered (
    tenant_id TEXT(50) NOT NULL,
    device_id TEXT(255) NOT NULL,
    device_name TEXT(255),
    model TEXT(100),
    operating_system TEXT(100),
    os_version TEXT(100),
    device_ownership TEXT(50),
    is_compliant INTEGER DEFAULT 0,
    is_managed INTEGER DEFAULT 0,
    manufacturer TEXT(100),
    
    -- Additional fields from Azure AD API:
    account_enabled INTEGER DEFAULT 1,
    device_version TEXT(50),
    is_rooted INTEGER DEFAULT 0,
    mdm_app_id TEXT(255),
    profile_type TEXT(50),
    trust_type TEXT(50),
    on_premises_sync_enabled INTEGER DEFAULT 0,
    on_premises_last_sync_date TEXT, -- ISO datetime format
    last_sign_in_date TEXT, -- Moved to third-to-last position
    
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, device_id)
);

-- Step 2: Copy data from existing azure_devices table (excluding compliance_expiration_date)
INSERT INTO azure_devices_reordered (
    tenant_id, device_id, device_name, model, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer,
    account_enabled, device_version, is_rooted, mdm_app_id, profile_type, 
    trust_type, on_premises_sync_enabled, on_premises_last_sync_date, 
    last_sign_in_date, created_at, last_updated
)
SELECT 
    tenant_id, device_id, device_name, model, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer,
    account_enabled, device_version, is_rooted, mdm_app_id, profile_type, 
    trust_type, on_premises_sync_enabled, on_premises_last_sync_date, 
    last_sign_in_date, created_at, last_updated
FROM azure_devices;

-- Step 3: Drop the old azure_devices table
DROP TABLE IF EXISTS azure_devices;

-- Step 4: Rename the reordered table
ALTER TABLE azure_devices_reordered RENAME TO azure_devices;

-- Step 5: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_azure_devices_tenant ON azure_devices(tenant_id);
