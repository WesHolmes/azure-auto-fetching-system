-- Migration script to add office_location and mobile_phone columns to usersV2 table
-- Run this script on existing databases to add the new columns

-- Add office_location column after job_title
ALTER TABLE usersV2 ADD COLUMN office_location TEXT(100);

-- Add mobile_phone column after office_location
ALTER TABLE usersV2 ADD COLUMN mobile_phone TEXT(50);

-- Update existing records to have NULL values for the new columns
-- (This is automatic in SQLite when adding columns)
