-- Migration script to remove unnecessary columns from device tables
-- This script will:
-- 1. Remove physical_memory_gb from intune_devices table
-- 2. Remove total_storage, free_storage, physical_memory, serial_number, is_encrypted, enrolled_date from azure_devices table

-- Step 1: Create new intune_devices table without physical_memory_gb
CREATE TABLE IF NOT EXISTS intune_devices_new (
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
    total_storage_gb REAL, -- Keep storage for Intune devices
    free_storage_gb REAL,  -- Keep storage for Intune devices
    compliance_state TEXT(50),
    last_sign_in_date TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, device_id)
);

-- Step 2: Copy data from intune_devices to intune_devices_new (excluding physical_memory_gb)
INSERT INTO intune_devices_new (
    tenant_id, device_id, device_name, model, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer, total_storage_gb,
    free_storage_gb, compliance_state, last_sign_in_date, created_at, last_updated
)
SELECT 
    tenant_id, device_id, device_name, model, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer, total_storage_gb,
    free_storage_gb, compliance_state, last_sign_in_date, created_at, last_updated
FROM intune_devices;

-- Step 3: Create new azure_devices table without unnecessary columns
CREATE TABLE IF NOT EXISTS azure_devices_new (
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
    compliance_state TEXT(50),
    last_sign_in_date TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, device_id)
);

-- Step 4: Copy data from azure_devices to azure_devices_new (excluding removed columns)
INSERT INTO azure_devices_new (
    tenant_id, device_id, device_name, model, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer, compliance_state,
    last_sign_in_date, created_at, last_updated
)
SELECT 
    tenant_id, device_id, device_name, model, operating_system, os_version,
    device_ownership, is_compliant, is_managed, manufacturer, compliance_state,
    last_sign_in_date, created_at, last_updated
FROM azure_devices;

-- Step 5: Drop old tables
DROP TABLE IF EXISTS intune_devices;
DROP TABLE IF EXISTS azure_devices;

-- Step 6: Rename new tables
ALTER TABLE intune_devices_new RENAME TO intune_devices;
ALTER TABLE azure_devices_new RENAME TO azure_devices;

-- Step 7: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_intune_devices_tenant ON intune_devices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_azure_devices_tenant ON azure_devices(tenant_id);
