import os
import sqlite3

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

# Initialize schema on import
init_schema()