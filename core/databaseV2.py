import logging
import os
import sqlite3


logger = logging.getLogger(__name__)


def get_connection():
    """Get database connection"""
    path = os.getenv("DATABASE_PATH", "./data/graph_sync.db")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return sqlite3.connect(path)


def init_schema():
    """Initialize database schema - simple and clean"""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Users table V2
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS usersV2 (
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
                is_mfa_compliant INTEGER NOT NULL DEFAULT 0,
                license_count INTEGER NOT NULL DEFAULT 0,
                group_count INTEGER NOT NULL DEFAULT 0,
                last_sign_in_date TEXT, -- ISO datetime format
                last_password_change TEXT, -- ISO datetime format
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (user_id, tenant_id)
            )
        """
        )

        # Licenses table (tenant-level)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS licenses (
                tenant_id TEXT,
                license_id TEXT,
                license_display_name TEXT,
                license_partnumber TEXT,
                status TEXT,
                total_count INTEGER DEFAULT 0,
                consumed_count INTEGER DEFAULT 0,
                warning_count INTEGER DEFAULT 0,
                suspended_count INTEGER DEFAULT 0,
                monthly_cost REAL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (tenant_id, license_id)
            )
        """
        )

        # User licenses table V2
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_licensesV2 (
                user_id TEXT(50) NOT NULL,
                tenant_id TEXT(50) NOT NULL,
                license_id TEXT(255) NOT NULL,
                user_principal_name TEXT(255) NOT NULL,
                license_display_name TEXT(255) NOT NULL,
                license_partnumber TEXT(100),
                is_active INTEGER NOT NULL DEFAULT 1,
                unassigned_date TEXT, -- ISO datetime format
                monthly_cost REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (user_id, tenant_id, license_id)
            )
        """
        )

        # Roles table (role definitions)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS roles (
                tenant_id TEXT,
                role_id TEXT,
                role_display_name TEXT,
                role_description TEXT,
                member_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (tenant_id, role_id)
            )
        """
        )

        # User roles table V2
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_rolesV2 (
                user_id TEXT(50) NOT NULL,
                tenant_id TEXT(50) NOT NULL,
                role_id TEXT(255) NOT NULL,
                user_principal_name TEXT(255) NOT NULL,
                role_display_name TEXT(255) NOT NULL,
                role_description TEXT(500),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (user_id, tenant_id, role_id)
            )
        """
        )

        # Groups table (tenant-level group definitions)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                tenant_id TEXT(50) NOT NULL,
                group_id TEXT(255) NOT NULL,
                group_display_name TEXT(255) NOT NULL,
                group_description TEXT(500),
                group_type TEXT(100) NOT NULL,
                mail_enabled INTEGER NOT NULL DEFAULT 0,
                security_enabled INTEGER NOT NULL DEFAULT 1,
                mail_nickname TEXT(100),
                visibility TEXT(50) DEFAULT 'Private',
                member_count INTEGER NOT NULL DEFAULT 0,
                owner_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (tenant_id, group_id)
            )
        """
        )

        # User groups table V2 (user-group assignments)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_groupsV2 (
                user_id TEXT(50) NOT NULL,
                tenant_id TEXT(50) NOT NULL,
                group_id TEXT(255) NOT NULL,
                user_principal_name TEXT(255) NOT NULL,
                group_display_name TEXT(255) NOT NULL,
                group_type TEXT(100) NOT NULL,
                membership_type TEXT(50) DEFAULT 'Member',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (user_id, tenant_id, group_id)
            )
        """
        )

        # Basic indexes only - V2 tables
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_usersV2_tenant ON usersV2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_tenant ON licenses(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_licensesV2_tenant ON user_licensesV2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_roles_tenant ON roles(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_rolesV2_tenant ON user_rolesV2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_groups_tenant ON groups(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_groups_type ON groups(group_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_groupsV2_tenant ON user_groupsV2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_groupsV2_user ON user_groupsV2(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_groupsV2_group ON user_groupsV2(group_id)")

        conn.commit()
        logger.info("Database schema initialized")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to initialize schema: {str(e)}")
        raise
    finally:
        conn.close()


def upsert_many(table, records):
    """Insert or update multiple records"""
    if not records:
        return 0

    conn = get_connection()
    cursor = conn.cursor()

    try:
        columns = list(records[0].keys())
        placeholders = ",".join(["?" for _ in columns])
        query = f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"

        inserted = 0
        for record in records:
            values = [record.get(col) for col in columns]
            cursor.execute(query, values)
            inserted += 1

        conn.commit()
        return inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert records: {str(e)}")
        raise
    finally:
        conn.close()


def query(sql, params=None):
    """Execute a SELECT query"""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        raise
    finally:
        conn.close()


def execute_query(sql, params=None):
    """Execute an INSERT/UPDATE/DELETE query"""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        conn.commit()
        return cursor.rowcount

    except Exception as e:
        conn.rollback()
        logger.error(f"Execute query failed: {str(e)}")
        raise
    finally:
        conn.close()


# Schema will be initialized when sync functions are called
init_schema()
