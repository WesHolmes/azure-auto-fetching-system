import logging
from typing import List, Dict, Any
from api.sql.base import SQLBaseConnector

logger = logging.getLogger(__name__)
""" SQL TABLE SCHEMA
-- Create the backup_policies table with simplified schema
CREATE TABLE [dbo].[backup_policies] (
    [backup_id] INT NOT NULL,
    [backup_datetime] DATETIME NOT NULL,
    [company_name] NVARCHAR(255) NOT NULL,
    [device_name] NVARCHAR(255) NOT NULL,
    [device_type] NVARCHAR(100) NULL,
    [days_since_last_good_result] DECIMAL(10, 2) NULL,
    [days_since_last_result] DECIMAL(10, 2) NULL,
    [days_in_status] DECIMAL(10, 2) NULL,
    [is_verified] BIT NOT NULL DEFAULT 0,
    [backup_result] NVARCHAR(50) NULL,
    [backup_type] NVARCHAR(100) NULL,
    [backup_policy_name] NVARCHAR(255) NULL,
    [is_retired] BIT NOT NULL DEFAULT 0,
    [last_updated] DATETIME NULL,
    PRIMARY KEY CLUSTERED ([backup_id], [backup_datetime])
);
GO

CREATE NONCLUSTERED INDEX IX_backup_company_name ON dbo.backup_policies(company_name);
GO

CREATE NONCLUSTERED INDEX IX_backup_datetime ON dbo.backup_policies(backup_datetime DESC);
GO

-- Verify the structure
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'backup_policies'
ORDER BY ORDINAL_POSITION;
GO


"""

class BackupRadarSQL(SQLBaseConnector):
    """SQL operations for backup radar data"""

    def upsert_backup_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Upsert backup records into the database using MERGE statement.
        Returns the number of records processed.
        """
        if not records:
            logger.warning("No backup records to upsert")
            return 0

        # SQL MERGE statement for upsert operation
        merge_query = """
        MERGE dbo.backup_policies AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source
            (company_name, device_name, device_type, days_since_last_good_result,
             days_since_last_result, days_in_status, is_verified, backup_result,
             backup_id, backup_type, backup_policy_name, backup_datetime, last_updated, is_retired)
        ON target.backup_id = source.backup_id
           AND target.backup_datetime = source.backup_datetime
        WHEN MATCHED THEN
            UPDATE SET
                company_name = source.company_name,
                device_name = source.device_name,
                device_type = source.device_type,
                days_since_last_good_result = source.days_since_last_good_result,
                days_since_last_result = source.days_since_last_result,
                days_in_status = source.days_in_status,
                is_verified = source.is_verified,
                backup_result = source.backup_result,
                backup_type = source.backup_type,
                backup_policy_name = source.backup_policy_name,
                last_updated = source.last_updated,
                is_retired = source.is_retired
        WHEN NOT MATCHED THEN
            INSERT (company_name, device_name, device_type, days_since_last_good_result,
                    days_since_last_result, days_in_status, is_verified, backup_result,
                    backup_id, backup_type, backup_policy_name, backup_datetime, last_updated, is_retired)
            VALUES (source.company_name, source.device_name, source.device_type,
                    source.days_since_last_good_result, source.days_since_last_result,
                    source.days_in_status, source.is_verified, source.backup_result,
                    source.backup_id, source.backup_type, source.backup_policy_name,
                    source.backup_datetime, source.last_updated, source.is_retired);
        """

        # Convert records to tuples for batch execution
        params_list = [
            (
                record['company_name'],
                record['device_name'],
                record.get('device_type'),
                record['days_since_last_good_result'],
                record['days_since_last_result'],
                record['days_in_status'],
                record['is_verified'],
                record['backup_result'],
                record['backup_id'],
                record['backup_type'],
                record['backup_policy_name'],
                record['backup_datetime'],
                record['last_updated'],
                record.get('is_retired', False)
            )
            for record in records
        ]

        try:
            # Use batch execution for better performance
            processed = self.execute_batch(merge_query, params_list)
            logger.info(f"Successfully upserted {processed} backup records")
            return processed
        except Exception as e:
            logger.error(f"Failed to upsert backup records: {str(e)}")
            raise