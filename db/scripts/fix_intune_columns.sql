-- Fix script to add back serial_number, is_encrypted, and enrolled_date to intune_devices
-- These should be kept for Intune devices but removed from Azure devices

-- Step 1: Create new intune_devices table with the correct columns
CREATE TABLE IF NOT EXISTS intune_devices_fixed (
    tenant_id TEXT(50) NOT NULL,
    device_id TEXT(255) NOT NULL,
    device_name TEXT(255),
    model TEXT(100),
    serial_number TEXT(100),  -- Keep for Intune devices
    operating_system TEXT(100),
    os_version TEXT(100),
    device_ownership TEXT(50),
    is_compliant INTEGER DEFAULT 0,
    is_managed INTEGER DEFAULT 0,
    manufacturer TEXT(100),
    total_storage_gb REAL, -- Storage in GB for proper sorting
    free_storage_gb REAL,  -- Storage in GB for proper sorting
    compliance_state TEXT(50),
    is_encrypted INTEGER DEFAULT 0,  -- Keep for Intune devices
    last_sign_in_date TEXT, -- ISO datetime format
    enrolled_date TEXT, -- Keep for Intune devices
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, device_id)
);

-- Step 2: Copy data from intune_devices to intune_devices_fixed
-- We'll set default values for the missing columns since they were removed
INSERT INTO intune_devices_fixed (
    tenant_id, device_id, device_name, model, serial_number, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer, total_storage_gb,
    free_storage_gb, compliance_state, is_encrypted, last_sign_in_date, enrolled_date,
    created_at, last_updated
)
SELECT 
    tenant_id, device_id, device_name, model, 'N/A' as serial_number, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer, total_storage_gb,
    free_storage_gb, compliance_state, 0 as is_encrypted, last_sign_in_date, NULL as enrolled_date,
    created_at, last_updated
FROM intune_devices;

-- Step 3: Drop the old intune_devices table
DROP TABLE IF EXISTS intune_devices;

-- Step 4: Rename the fixed table
ALTER TABLE intune_devices_fixed RENAME TO intune_devices;

-- Step 5: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_intune_devices_tenant ON intune_devices(tenant_id);
