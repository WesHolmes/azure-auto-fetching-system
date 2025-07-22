import os
import sqlite3
import logging

logger = logging.getLogger(__name__)

def get_connection():
    path = os.getenv('DATABASE_PATH', './data/graph_sync.db')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return sqlite3.connect(path)

def init_schema():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Users table
    cursor.execute("""
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
            is_mfa_compliant BOOLEAN DEFAULT 0,     -- New column
            license_count INTEGER DEFAULT 0,         -- New column
            group_count INTEGER DEFAULT 0,           -- New column
            synced_at TEXT,
            PRIMARY KEY (id, tenant_id)
        )
    """)
    
    # Service principals table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS service_principals (
            id TEXT,
            tenant_id TEXT,
            app_id TEXT,
            display_name TEXT,
            publisher_name TEXT,
            service_principal_type TEXT,
            synced_at TEXT,
            PRIMARY KEY (id, tenant_id)
        )
    """)
    
    # Basic indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sp_tenant ON service_principals(tenant_id)")
    
    conn.commit()
    conn.close()

def migrate_add_columns():
    """Add new columns to existing users table if they don't exist"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # check existing columns
        cursor.execute("PRAGMA table_info(users)")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        # add new columns if they don't exist
        new_columns = [
            ("is_mfa_compliant", "BOOLEAN DEFAULT 0"),
            ("license_count", "INTEGER DEFAULT 0"),
            ("group_count", "INTEGER DEFAULT 0")
        ]
        
        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                logger.info(f"Adding column {col_name} to users table")
                cursor.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
                print(f"Added column: {col_name}")
        
        conn.commit()
        logger.info("Column migration completed")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        conn.close()

def upsert_many(table, records):
    if not records:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    columns = list(records[0].keys())
    placeholders = ','.join(['?' for _ in columns])
    query = f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    
    for record in records:
        values = [record.get(col) for col in columns]
        cursor.execute(query, values)
    
    conn.commit()
    conn.close()

def query(sql, params=None):
    """Execute a SELECT query and return results as list of dicts"""
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

# init. schema on import
init_schema()

# run migration to add new columns
try:
    migrate_add_columns()
except Exception as e:
    print(f"Migration warning: {str(e)}")
