-- Migration to add missing is_active column to user_groupsV2 table
-- This column is needed for tracking active vs inactive group memberships

-- Add the is_active column with default value 1 (active)
ALTER TABLE user_groupsV2 ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;

-- Update existing records to ensure they are marked as active
-- (This is actually not needed since DEFAULT 1 handles it, but good for clarity)
UPDATE user_groupsV2 SET is_active = 1 WHERE is_active IS NULL;

-- Verify the column was added successfully
-- SELECT sql FROM sqlite_master WHERE type='table' AND name='user_groupsV2';
