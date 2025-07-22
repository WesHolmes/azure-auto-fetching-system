import logging
from typing import List, Dict, Any
from api.sql.base import SQLBaseConnector

logger = logging.getLogger(__name__)

""" SQL TABLE SCHEMA
-- Tenants table schema for ConnectWise company information
-- This table stores company details from ConnectWise Manage and Automate

CREATE TABLE [dbo].[tenants] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [connectwise_company_id] INT NOT NULL,
    [company_name] NVARCHAR(255) NULL,
    [company_identifier] NVARCHAR(100) NULL,
    [automate_company_id] NVARCHAR(50) NULL,
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_tenants_connectwise_company_id 
ON dbo.tenants(connectwise_company_id);
GO

CREATE NONCLUSTERED INDEX IX_tenants_automate_company_id 
ON dbo.tenants(automate_company_id);
GO
"""


class ConnectWiseSQL(SQLBaseConnector):
    """SQL operations for ConnectWise data"""

    def get_distinct_company_ids(self) -> List[int]:
        """Get distinct ConnectWise company IDs from tenants table."""
        query = """
        SELECT DISTINCT connectwise_company_id
        FROM dbo.tenants
        WHERE connectwise_company_id IS NOT NULL
        ORDER BY connectwise_company_id
        """

        results = self.execute_query(query)
        return [row['connectwise_company_id'] for row in results]

    def update_company_info(self, company_id: int, company_name: str, company_identifier: str) -> bool:
        """Update company_name and company_identifier for a specific ConnectWise company ID."""
        query = """
        UPDATE dbo.tenants
        SET company_name = ?,
            company_identifier = ?,
            updated_at = GETDATE()
        WHERE connectwise_company_id = ?
        """

        try:
            affected_rows = self.execute_update(query, (company_name, company_identifier, company_id))
            if affected_rows > 0:
                logger.debug(f"Updated {affected_rows} rows for company ID {company_id}")
            return affected_rows > 0
        except Exception as e:
            logger.error(f"Failed to update company ID {company_id}: {str(e)}")
            return False
    
    def update_automate_company_id(self, connectwise_company_id: int, automate_company_id: str) -> bool:
        """Update automate_company_id for a specific ConnectWise company ID."""
        query = """
        UPDATE dbo.tenants
        SET automate_company_id = ?,
            updated_at = GETDATE()
        WHERE connectwise_company_id = ?
        """

        try:
            affected_rows = self.execute_update(query, (automate_company_id, connectwise_company_id))
            if affected_rows > 0:
                logger.debug(f"Updated Automate ID to {automate_company_id} for CW company ID {connectwise_company_id}")
            return affected_rows > 0
        except Exception as e:
            logger.error(f"Failed to update Automate ID for CW company ID {connectwise_company_id}: {str(e)}")
            return False

    def update_company_info_batch(self, updates: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch update company information with optimized performance."""
        if not updates:
            return {'updated': 0, 'failed': 0}

        query = """
        UPDATE dbo.tenants
        SET company_name = ?,
            company_identifier = ?,
            updated_at = GETDATE()
        WHERE connectwise_company_id = ?
        """

        # Convert to list of tuples for executemany
        params_list = [
            (update['company_name'], update['company_identifier'], update['company_id'])
            for update in updates
        ]

        try:
            # Use batch execution for better performance
            total_updated = self.execute_batch(query, params_list)
            logger.info(f"Batch updated {total_updated} rows for {len(updates)} companies")
            return {'updated': total_updated, 'failed': 0}

        except Exception as e:
            logger.error(f"Batch update failed: {str(e)}")
            # Fall back to individual updates if batch fails
            updated = 0
            failed = 0
            for update in updates:
                if self.update_company_info(
                    update['company_id'],
                    update['company_name'],
                    update['company_identifier']
                ):
                    updated += 1
                else:
                    failed += 1

            return {'updated': updated, 'failed': failed}