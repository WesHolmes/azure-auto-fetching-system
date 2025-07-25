import os
import sqlite3
import os
import aiosqlite
import asyncio
from typing import List, Dict, Any

DATABASE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "graph_sync.db")

def init_database():
    """Initialize database schema"""
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Create service_principals table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS service_principals (
            id TEXT,
            tenant_id TEXT,
            app_id TEXT,
            display_name TEXT,
            service_principal_type TEXT,
            owners TEXT,
            expired_credentials BOOLEAN,
            has_credentials BOOLEAN,
            enabled_sp BOOLEAN,
            last_sign_in TEXT,
            synced_at TEXT,
            PRIMARY KEY (id, tenant_id)
        )
    """)
    
    # Create users table
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
    
    # Create useful indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sp_tenant ON service_principals(tenant_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sp_app_id ON service_principals(app_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sp_credentials ON service_principals(has_credentials, expired_credentials)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_upn ON users(user_principal_name)")
    
    conn.commit()
    conn.close()
    print(f"✅ Database initialized: {DATABASE_PATH}")

async def init_database_async():
    """Async version of database initialization"""
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    async with aiosqlite.connect(DATABASE_PATH) as conn:
        # Create service_principals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS service_principals (
                id TEXT,
                tenant_id TEXT,
                app_id TEXT,
                display_name TEXT,
                service_principal_type TEXT,
                owners TEXT,
                expired_credentials BOOLEAN,
                has_credentials BOOLEAN,
                enabled_sp BOOLEAN,
                last_sign_in TEXT,
                synced_at TEXT,
                PRIMARY KEY (id, tenant_id)
            )
        """)
        
        # Create users table
        await conn.execute("""
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
        
        # Create useful indexes
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sp_tenant ON service_principals(tenant_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sp_app_id ON service_principals(app_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sp_credentials ON service_principals(has_credentials, expired_credentials)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_upn ON users(user_principal_name)")
        
        await conn.commit()
        print(f"✅ Async database initialized: {DATABASE_PATH}")

def get_db():
    # Initialize database if it doesn't exist
    if not os.path.exists(DATABASE_PATH):
        init_database()
    return sqlite3.connect(DATABASE_PATH)

# Backward compatibility alias
def get_connection():
    return get_db()

def upsert_many(table_name, records):
    """Synchronous bulk upsert for better performance"""
    if not records:
        return

    conn = get_db()
    
    # Get first record to determine column names
    columns = list(records[0].keys())
    placeholders = ', '.join(['?' for _ in columns])
    
    # Create ON CONFLICT clause for upsert
    conflict_columns = ['id', 'tenant_id'] if 'tenant_id' in columns else ['id']
    conflict_clause = ', '.join(conflict_columns)
    update_columns = [col for col in columns if col not in conflict_columns]
    update_clause = ', '.join([f"{col} = excluded.{col}" for col in update_columns])
    
    sql = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    VALUES ({placeholders})
    ON CONFLICT({conflict_clause}) DO UPDATE SET
    {update_clause}
    """
    
    # Convert records to tuples
    data = [tuple(record[col] for col in columns) for record in records]
    
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, data)
        conn.commit()
        print(f"DEBUG: Upserted {len(records)} records to {table_name}")
    except Exception as e:
        print(f"ERROR: Database upsert failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


# Async database operations
async def get_async_db():
    """Get async database connection with initialization check"""
    # Initialize database if it doesn't exist
    if not os.path.exists(DATABASE_PATH):
        await init_database_async()
    return await aiosqlite.connect(DATABASE_PATH)

async def upsert_many_async(table_name: str, records: List[Dict[str, Any]]):
    """Async bulk upsert for better performance"""
    if not records:
        return

    # Ensure database is initialized
    if not os.path.exists(DATABASE_PATH):
        await init_database_async()

    async with aiosqlite.connect(DATABASE_PATH) as conn:
        # Get first record to determine column names
        columns = list(records[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        
        # Create ON CONFLICT clause for upsert
        conflict_columns = ['id', 'tenant_id'] if 'tenant_id' in columns else ['id']
        conflict_clause = ', '.join(conflict_columns)
        update_columns = [col for col in columns if col not in conflict_columns]
        update_clause = ', '.join([f"{col} = excluded.{col}" for col in update_columns])
        
        sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT({conflict_clause}) DO UPDATE SET
        {update_clause}
        """
        
        # Convert records to tuples
        data = [tuple(record[col] for col in columns) for record in records]
        
        try:
            await conn.executemany(sql, data)
            await conn.commit()
            print(f"DEBUG: Async upserted {len(records)} records to {table_name}")
        except Exception as e:
            print(f"ERROR: Async database upsert failed: {e}")
            await conn.rollback()
            raise

async def execute_query_async(query: str, params: tuple = None) -> List[Dict[str, Any]]:
    """Execute async query and return results as list of dictionaries"""
    # Ensure database is initialized
    if not os.path.exists(DATABASE_PATH):
        await init_database_async()
    
    async with aiosqlite.connect(DATABASE_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(query, params or ()) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]