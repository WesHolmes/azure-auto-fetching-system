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
        # Users table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT,
                tenant_id TEXT,
                display_name TEXT,
                user_principal_name TEXT,
                mail TEXT,
                account_enabled BOOLEAN,
                user_type TEXT,
                department TEXT,
                job_title TEXT,
                last_sign_in TEXT,
                is_mfa_compliant BOOLEAN DEFAULT 0,
                license_count INTEGER DEFAULT 0,
                group_count INTEGER DEFAULT 0,
                is_admin BOOLEAN DEFAULT 0,
                synced_at TEXT,
                PRIMARY KEY (id, tenant_id)
            )
        """
        )

        # Service principals table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS service_principals (
                id TEXT,
                tenant_id TEXT,
                app_id TEXT,
                display_name TEXT,
                publisher_name TEXT,
                service_principal_type TEXT,
                owners TEXT,
                credential_exp_date TEXT,
                credential_type TEXT,
                enabled_sp BOOLEAN DEFAULT 0,
                last_sign_in TEXT,
                synced_at TEXT,
                PRIMARY KEY (id, tenant_id)
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
                last_update TEXT,
                PRIMARY KEY (tenant_id, license_id)
            )
        """
        )

        # User licenses table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_licenses (
                tenant_id TEXT,
                user_id TEXT,
                license_id TEXT,
                user_principal_name TEXT,
                is_active INTEGER DEFAULT 1,
                assigned_date TEXT,
                unassigned_date TEXT,
                license_display_name TEXT,
                license_partnumber TEXT,
                monthly_cost REAL DEFAULT 0,
                last_update TEXT,
                PRIMARY KEY (tenant_id, user_id, license_id)
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
                PRIMARY KEY (tenant_id, role_id)
            )
        """
        )

        # User roles table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_roles (
                tenant_id TEXT,
                user_id TEXT,
                role_id TEXT,
                user_principal_name TEXT,
                role_display_name TEXT,
                role_description TEXT,
                assigned_date TEXT,
                synced_at TEXT,
                PRIMARY KEY (tenant_id, user_id, role_id)
            )
        """
        )

        # Policies table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS policies (
                id TEXT,
                tenant_id TEXT,
                display_name TEXT,
                state BOOLEAN,
                created_date TEXT,
                modified_date TEXT,
                synced_at TEXT,
                PRIMARY KEY (id, tenant_id)
            )
        """
        )

        # Policy users table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS policy_users (
                tenant_id TEXT,
                user_id TEXT,
                policy_id TEXT,
                user_principal_name TEXT,
                synced_at TEXT,
                PRIMARY KEY (tenant_id, user_id, policy_id)
            )
        """
        )

        # Policy applications table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS policy_applications (
                tenant_id TEXT,
                application_id TEXT,
                policy_id TEXT,
                application_name TEXT,
                synced_at TEXT,
                PRIMARY KEY (tenant_id, application_id, policy_id)
            )
        """
        )

        # Basic indexes only
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_licenses_tenant ON licenses(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_licenses_tenant ON user_licenses(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_roles_tenant ON roles(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_roles_tenant ON user_roles(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_policies_tenant ON policies(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_users_tenant ON policy_users(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_users_policy ON policy_users(policy_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_applications_tenant ON policy_applications(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_applications_policy ON policy_applications(policy_id)")

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


# Initialize schema when module loads
init_schema()
