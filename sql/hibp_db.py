import logging
from datetime import datetime
from typing import List, Dict, Optional
from sql.base import SQLBaseConnector

logger = logging.getLogger(__name__)


class HIBPDB(SQLBaseConnector):
    def create_tables(self):
        """Create user_breaches table if it doesn't exist."""
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='user_breaches' AND xtype='U')
        BEGIN
            CREATE TABLE user_breaches (
                -- Composite primary key columns
                tenant_id NVARCHAR(250) NOT NULL,
                user_principal_name NVARCHAR(250) NOT NULL,
                breach_name NVARCHAR(250) NOT NULL,

                -- User info
                user_id NVARCHAR(250) NOT NULL,

                -- Breach details
                breach_title NVARCHAR(250) NULL,
                breach_date DATE NULL,
                data_classes NVARCHAR(MAX) NULL,

                -- Breach flags
                is_verified BIT NOT NULL DEFAULT 0,
                is_sensitive BIT NOT NULL DEFAULT 0,
                is_spam_list BIT NOT NULL DEFAULT 0,

                -- Calculated field
                password_reset_required BIT NOT NULL DEFAULT 0,

                -- Metadata
                last_updated DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),

                -- Composite primary key
                CONSTRAINT PK_user_breaches PRIMARY KEY (tenant_id, user_principal_name, breach_name)
            );

            -- Index for faster lookups by tenant
            CREATE INDEX IX_user_breaches_tenant_id ON user_breaches(tenant_id);

            -- Index for finding users who need password resets
            CREATE INDEX IX_user_breaches_password_reset ON user_breaches(tenant_id, password_reset_required)
            WHERE password_reset_required = 1;
        END
        """

        try:
            self.execute_update(create_table_query)
            logger.info("user_breaches table ready")
        except Exception as e:
            logger.error(f"Failed to create user_breaches table: {str(e)}")
            raise

    def upsert_breach(self, breach_data: Dict) -> bool:
        """Upsert a single breach record."""
        merge_query = """
        MERGE user_breaches AS target
        USING (SELECT ? AS tenant_id, ? AS user_principal_name, ? AS breach_name) AS source
        ON target.tenant_id = source.tenant_id
           AND target.user_principal_name = source.user_principal_name
           AND target.breach_name = source.breach_name
        WHEN MATCHED THEN
            UPDATE SET
                user_id = ?,
                breach_title = ?,
                breach_date = ?,
                data_classes = ?,
                is_verified = ?,
                is_sensitive = ?,
                is_spam_list = ?,
                password_reset_required = ?,
                last_updated = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT (tenant_id, user_principal_name, breach_name, user_id, breach_title,
                    breach_date, data_classes, is_verified, is_sensitive, is_spam_list,
                    password_reset_required)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        params = (
            # Source params
            breach_data["tenant_id"],
            breach_data["user_principal_name"],
            breach_data["breach_name"],
            # Update params
            breach_data["user_id"],
            breach_data.get("breach_title"),
            breach_data.get("breach_date"),
            breach_data.get("data_classes"),
            breach_data.get("is_verified", False),
            breach_data.get("is_sensitive", False),
            breach_data.get("is_spam_list", False),
            breach_data.get("password_reset_required", False),
            # Insert params
            breach_data["tenant_id"],
            breach_data["user_principal_name"],
            breach_data["breach_name"],
            breach_data["user_id"],
            breach_data.get("breach_title"),
            breach_data.get("breach_date"),
            breach_data.get("data_classes"),
            breach_data.get("is_verified", False),
            breach_data.get("is_sensitive", False),
            breach_data.get("is_spam_list", False),
            breach_data.get("password_reset_required", False),
        )

        try:
            self.execute_update(merge_query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to upsert breach: {str(e)}")
            return False

    def get_user_breaches(self, tenant_id: str, user_principal_name: str) -> List[Dict]:
        """Get all breaches for a specific user (excluding NO_BREACH sentinel)."""
        query = """
        SELECT * FROM user_breaches
        WHERE tenant_id = ? AND user_principal_name = ?
        AND breach_name != 'NO_BREACH'
        ORDER BY breach_date DESC
        """
        return self.execute_query(query, (tenant_id, user_principal_name))

    def get_user_last_checked(
        self, tenant_id: str, user_principal_name: str
    ) -> Optional[datetime]:
        """Get when we last checked this user for breaches."""
        query = """
        SELECT MAX(last_updated) as last_checked
        FROM user_breaches
        WHERE tenant_id = ? AND user_principal_name = ?
        """
        results = self.execute_query(query, (tenant_id, user_principal_name))
        if results and results[0]["last_checked"]:
            return results[0]["last_checked"]
        return None

    def get_recently_checked_users(self, tenant_id: str, days: int = 7) -> set:
        """Get set of users checked within the last N days"""
        query = """
        SELECT DISTINCT user_principal_name
        FROM user_breaches
        WHERE tenant_id = ?
        AND last_updated >= DATEADD(day, -?, GETUTCDATE())
        """
        results = self.execute_query(query, (tenant_id, days))
        return {row["user_principal_name"] for row in results}

    def get_tenant_breach_summary(self, tenant_id: str) -> Dict:
        """Get breach summary statistics for a tenant."""
        query = """
        SELECT
            COUNT(DISTINCT user_principal_name) as affected_users,
            COUNT(*) as total_breaches,
            SUM(CASE WHEN password_reset_required = 1 THEN 1 ELSE 0 END) as password_resets_needed
        FROM user_breaches
        WHERE tenant_id = ?
        AND breach_name != 'NO_BREACH'
        """
        results = self.execute_query(query, (tenant_id,))
        return (
            results[0]
            if results
            else {"affected_users": 0, "total_breaches": 0, "password_resets_needed": 0}
        )

    def bulk_update_password_reset_status(
        self, tenant_id: str, user_password_dates: Dict[str, str]
    ) -> int:
        """Update password_reset_required for all users based on their current password dates."""
        if not user_password_dates:
            return 0

        # Build case statement for bulk update
        case_conditions = []
        params = []

        for upn, pwd_date in user_password_dates.items():
            if pwd_date:
                case_conditions.append(
                    f"WHEN user_principal_name = ? AND breach_date > ? THEN 1"
                )
                params.extend([upn, pwd_date])

        if not case_conditions:
            return 0

        query = f"""
        UPDATE user_breaches
        SET password_reset_required = CASE
            {" ".join(case_conditions)}
            ELSE 0
        END,
        last_updated = GETUTCDATE()
        WHERE tenant_id = ?
        AND user_principal_name IN ({",".join(["?" for _ in user_password_dates])})
        AND breach_name != 'NO_BREACH'
        """

        # Add parameters in the order they appear in the query
        params.append(tenant_id)  # For WHERE tenant_id = ?
        params.extend(user_password_dates.keys())  # For IN clause

        return self.execute_update(query, tuple(params))
