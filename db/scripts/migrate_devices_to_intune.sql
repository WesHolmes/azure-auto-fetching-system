-- Migration script to separate Azure and Intune devices
-- This script will:
-- 1. Create azure_devices table
-- 2. Rename devices table to intune_devices
-- 3. Remove device_type column from intune_devices
-- 4. Move Azure devices to azure_devices table

-- Step 1: Create azure_devices table
CREATE TABLE IF NOT EXISTS azure_devices (
    tenant_id TEXT(50) NOT NULL,
    device_id TEXT(255) NOT NULL,
    device_name TEXT(255),
    model TEXT(100),
    serial_number TEXT(100),
    operating_system TEXT(100),
    os_version TEXT(100),
    device_ownership TEXT(50),
    is_compliant INTEGER DEFAULT 0,
    is_managed INTEGER DEFAULT 0,
    manufacturer TEXT(100),
    total_storage TEXT(50),
    free_storage TEXT(50),
    physical_memory TEXT(50),
    compliance_state TEXT(50),
    is_encrypted INTEGER DEFAULT 0,
    last_sign_in_date TEXT, -- ISO datetime format
    enrolled_date TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, device_id)
);

-- Step 2: Move Azure devices to azure_devices table
INSERT INTO azure_devices (
    tenant_id, device_id, device_name, model, serial_number,
    operating_system, os_version, device_ownership, is_compliant,
    is_managed, manufacturer, total_storage, free_storage,
    physical_memory, compliance_state, is_encrypted,
    last_sign_in_date, enrolled_date, created_at, last_updated
)
SELECT 
    tenant_id, device_id, device_name, model, serial_number,
    operating_system, os_version, device_ownership, is_compliant,
    is_managed, manufacturer, total_storage, free_storage,
    physical_memory, compliance_state, is_encrypted,
    last_sign_in_date, enrolled_date, created_at, last_updated
FROM devices 
WHERE device_type = 'Azure';

-- Step 3: Create intune_devices table (without device_type column)
CREATE TABLE IF NOT EXISTS intune_devices (
    tenant_id TEXT(50) NOT NULL,
    device_id TEXT(255) NOT NULL,
    device_name TEXT(255),
    model TEXT(100),
    serial_number TEXT(100),
    operating_system TEXT(100),
    os_version TEXT(100),
    device_ownership TEXT(50),
    is_compliant INTEGER DEFAULT 0,
    is_managed INTEGER DEFAULT 0,
    manufacturer TEXT(100),
    total_storage_gb REAL, -- Changed to REAL for proper sorting
    free_storage_gb REAL,  -- Changed to REAL for proper sorting
    physical_memory_gb REAL, -- Changed to REAL for proper sorting
    compliance_state TEXT(50),
    is_encrypted INTEGER DEFAULT 0,
    last_sign_in_date TEXT, -- ISO datetime format
    enrolled_date TEXT, -- ISO datetime format
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, device_id)
);

-- Step 4: Move Intune devices to intune_devices table
-- Note: We'll need to convert storage values to GB and handle NULL values
INSERT INTO intune_devices (
    tenant_id, device_id, device_name, model, serial_number,
    operating_system, os_version, device_ownership, is_compliant,
    is_managed, manufacturer, total_storage_gb, free_storage_gb,
    physical_memory_gb, compliance_state, is_encrypted,
    last_sign_in_date, enrolled_date, created_at, last_updated
)
SELECT 
    tenant_id, device_id, device_name, model, serial_number,
    operating_system, os_version, device_ownership, is_compliant,
    is_managed, manufacturer, 
    -- Convert storage values to GB (simplified conversion for existing data)
    CASE 
        WHEN total_storage = 'N/A' OR total_storage IS NULL THEN NULL
        WHEN total_storage LIKE '%TB%' THEN CAST(REPLACE(REPLACE(total_storage, ' TB', ''), ' ', '') AS REAL) * 1024
        WHEN total_storage LIKE '%GB%' THEN CAST(REPLACE(REPLACE(total_storage, ' GB', ''), ' ', '') AS REAL)
        WHEN total_storage LIKE '%MB%' THEN CAST(REPLACE(REPLACE(total_storage, ' MB', ''), ' ', '') AS REAL) / 1024
        WHEN total_storage LIKE '%KB%' THEN CAST(REPLACE(REPLACE(total_storage, ' KB', ''), ' ', '') AS REAL) / (1024 * 1024)
        ELSE NULL
    END as total_storage_gb,
    CASE 
        WHEN free_storage = 'N/A' OR free_storage IS NULL THEN NULL
        WHEN free_storage LIKE '%TB%' THEN CAST(REPLACE(REPLACE(free_storage, ' TB', ''), ' ', '') AS REAL) * 1024
        WHEN free_storage LIKE '%GB%' THEN CAST(REPLACE(REPLACE(free_storage, ' GB', ''), ' ', '') AS REAL)
        WHEN free_storage LIKE '%MB%' THEN CAST(REPLACE(REPLACE(free_storage, ' MB', ''), ' ', '') AS REAL) / 1024
        WHEN free_storage LIKE '%KB%' THEN CAST(REPLACE(REPLACE(free_storage, ' KB', ''), ' ', '') AS REAL) / (1024 * 1024)
        ELSE NULL
    END as free_storage_gb,
    CASE 
        WHEN physical_memory = 'N/A' OR physical_memory IS NULL THEN NULL
        WHEN physical_memory LIKE '%TB%' THEN CAST(REPLACE(REPLACE(physical_memory, ' TB', ''), ' ', '') AS REAL) * 1024
        WHEN physical_memory LIKE '%GB%' THEN CAST(REPLACE(REPLACE(physical_memory, ' GB', ''), ' ', '') AS REAL)
        WHEN physical_memory LIKE '%MB%' THEN CAST(REPLACE(REPLACE(physical_memory, ' MB', ''), ' ', '') AS REAL) / 1024
        WHEN physical_memory LIKE '%KB%' THEN CAST(REPLACE(REPLACE(physical_memory, ' KB', ''), ' ', '') AS REAL) / (1024 * 1024)
        ELSE NULL
    END as physical_memory_gb,
    compliance_state, is_encrypted,
    last_sign_in_date, enrolled_date, created_at, last_updated
FROM devices 
WHERE device_type = 'Intune';

-- Step 5: Create indexes for new tables
CREATE INDEX IF NOT EXISTS idx_azure_devices_tenant ON azure_devices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_intune_devices_tenant ON intune_devices(tenant_id);

-- Step 6: Drop the old devices table
DROP TABLE IF EXISTS devices;

-- Step 7: Update user_devicesV2 table to reference intune_devices
-- Note: This will need to be handled in the application code as well
