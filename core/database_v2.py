import logging
import os
import sqlite3


logger = logging.getLogger(__name__)


def get_v2_connection():
    """Get V2 database connection - simplified like V1"""
    path = os.getenv("DATABASE_V2_PATH", "./data/graph_sync_v2.db")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return sqlite3.connect(path)


def init_v2_schema():
    """Initialize V2 database schema - simplified like V1"""
    conn = get_v2_connection()
    cursor = conn.cursor()

    try:
        # Applications_v2 table (renamed from service_principals with enhanced columns)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS applications_v2 (
                id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                app_id TEXT NOT NULL,
                display_name TEXT,
                app_display_name TEXT,
                service_principal_type TEXT,
                account_enabled INTEGER,
                sign_in_audience TEXT,
                app_owner_organization_id TEXT,
                app_role_assignment_required INTEGER,
                key_credentials TEXT,
                password_credentials TEXT,
                app_roles TEXT,
                oauth2_permission_scopes TEXT,
                tags TEXT,
                created_date TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT,
                last_sign_in TEXT,
                PRIMARY KEY (id, tenant_id)
            )
        """)

        # Policies_v2 table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS policies_v2 (
                tenant_id TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                display_name TEXT,
                is_active INTEGER,
                created_date TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT,
                PRIMARY KEY (tenant_id, policy_id)
            )
        """)

        # User_policies_v2 table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_policies_v2 (
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                user_principal_name TEXT,
                policy_name TEXT,
                last_updated TEXT,
                PRIMARY KEY (tenant_id, user_id, policy_id)
            )
        """)

        # Application_policies_v2 table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS application_policies_v2 (
                tenant_id TEXT NOT NULL,
                application_id TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                application_name TEXT,
                policy_name TEXT,
                last_updated TEXT,
                PRIMARY KEY (tenant_id, application_id, policy_id)
            )
        """)

        # Devices_v2 table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS devices_v2 (
                id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                device_name TEXT,
                managed_device_name TEXT,
                user_id TEXT,
                user_principal_name TEXT,
                device_type TEXT,
                operating_system TEXT,
                os_version TEXT,
                compliance_state TEXT,
                managed_device_owner_type TEXT,
                enrollment_type TEXT,
                management_state TEXT,
                is_encrypted INTEGER,
                is_supervised INTEGER,
                azure_ad_device_id TEXT,
                serial_number TEXT,
                manufacturer TEXT,
                model TEXT,
                last_contact_date_time TEXT,
                enrollment_date_time TEXT,
                created_date TEXT NOT NULL DEFAULT (datetime('now')),
                last_updated TEXT,
                PRIMARY KEY (id, tenant_id)
            )
        """)

        # Create indexes for v2 tables
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_applications_tenant ON applications_v2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_applications_app_id ON applications_v2(app_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_policies_tenant ON policies_v2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_user_policies_tenant ON user_policies_v2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_application_policies_tenant ON application_policies_v2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_devices_tenant ON devices_v2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_devices_user ON devices_v2(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_v2_devices_compliance ON devices_v2(compliance_state)")

        conn.commit()
        logger.info("V2 database schema initialized successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to initialize V2 schema: {str(e)}")
        raise
    finally:
        conn.close()


def upsert_many_v2(table, records):
    """V2 bulk upsert function - simplified like V1"""
    if not records:
        return 0

    conn = get_v2_connection()
    cursor = conn.cursor()

    try:
        inserted_count = 0
        updated_count = 0

        for record in records:
            # Check if record already exists
            if table in ["devices_v2", "applications_v2", "policies_v2"]:
                # For tables with created_date, check if record exists using primary key
                if table == "devices_v2":
                    cursor.execute(
                        "SELECT id, tenant_id FROM devices_v2 WHERE id = ? AND tenant_id = ?", (record.get("id"), record.get("tenant_id"))
                    )
                elif table == "applications_v2":
                    cursor.execute(
                        "SELECT id, tenant_id FROM applications_v2 WHERE id = ? AND tenant_id = ?",
                        (record.get("id"), record.get("tenant_id")),
                    )
                elif table == "policies_v2":
                    cursor.execute(
                        "SELECT tenant_id, policy_id FROM policies_v2 WHERE tenant_id = ? AND policy_id = ?",
                        (record.get("tenant_id"), record.get("policy_id")),
                    )

                existing_record = cursor.fetchone()

                if existing_record:
                    # Record exists - update without changing created_date
                    if "created_date" in record:
                        del record["created_date"]  # Don't update created_date

                    columns = list(record.keys())
                    placeholders = ", ".join(["?" for _ in columns])
                    column_names = ", ".join(columns)
                    values = [record.get(col) for col in columns]

                    if table == "devices_v2":
                        sql = f"UPDATE {table} SET {', '.join([f'{col} = ?' for col in columns])} WHERE id = ? AND tenant_id = ?"
                        values.extend([record.get("id"), record.get("tenant_id")])
                    elif table == "applications_v2":
                        sql = f"UPDATE {table} SET {', '.join([f'{col} = ?' for col in columns])} WHERE id = ? AND tenant_id = ?"
                        values.extend([record.get("id"), record.get("tenant_id")])
                    elif table == "policies_v2":
                        sql = f"UPDATE {table} SET {', '.join([f'{col} = ?' for col in columns])} WHERE tenant_id = ? AND policy_id = ?"
                        values.extend([record.get("tenant_id"), record.get("policy_id")])

                    cursor.execute(sql, values)
                    updated_count += cursor.rowcount
                else:
                    # Record doesn't exist - insert with created_date
                    if "created_date" not in record:
                        from datetime import datetime

                        record["created_date"] = datetime.now().isoformat()

                    columns = list(record.keys())
                    placeholders = ", ".join(["?" for _ in columns])
                    column_names = ", ".join(columns)
                    values = [record.get(col) for col in columns]

                    sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
                    cursor.execute(sql, values)
                    inserted_count += cursor.rowcount
            else:
                # For other tables, use the original INSERT OR REPLACE logic
                columns = list(record.keys())
                placeholders = ", ".join(["?" for _ in columns])
                column_names = ", ".join(columns)
                values = [record.get(col) for col in columns]

                sql = f"INSERT OR REPLACE INTO {table} ({column_names}) VALUES ({placeholders})"
                cursor.execute(sql, values)
                inserted_count += cursor.rowcount

        conn.commit()
        logger.info(f"V2 Successfully processed {len(records)} records to {table}: {inserted_count} inserted, {updated_count} updated")
        return inserted_count + updated_count

    except Exception as e:
        conn.rollback()
        logger.error(f"V2 Failed to upsert records to {table}: {str(e)}")
        raise
    finally:
        conn.close()


def query_v2(sql, params=None):
    """V2 SELECT query function - simplified like V1"""
    conn = get_v2_connection()
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
        logger.error(f"V2 Query failed: {str(e)}")
        raise
    finally:
        conn.close()


def execute_v2(sql, params=None):
    """V2 INSERT/UPDATE/DELETE query function - simplified like V1"""
    conn = get_v2_connection()
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
        logger.error(f"V2 Execute query failed: {str(e)}")
        raise
    finally:
        conn.close()


# Initialize V2 schema when module loads (like V1)
init_v2_schema()
