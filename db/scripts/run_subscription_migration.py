#!/usr/bin/env python3
"""
Migration script to update subscriptions table schema:
1. Change 'status' column to 'is_active' (boolean 0/1)
2. Remove 'created_date_time' column
3. Remove 'owner_id' column
4. Remove 'owner_tenant_id' column
5. Remove 'owner_type' column
"""

from pathlib import Path
import sys


# Add parent directory to path to import core modules
sys.path.append(str(Path(__file__).parent.parent))

from db.db_client import get_connection


def run_migration():
    """Run the subscription table migration"""
    db_path = Path(__file__).parent / "data" / "sqlite.db"

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return False

    print(f"Running migration on database: {db_path}")

    # Read the migration SQL
    migration_file = Path(__file__).parent / "migrate_subscriptions_cleanup.sql"
    if not migration_file.exists():
        print(f"Migration file not found: {migration_file}")
        return False

    with open(migration_file) as f:
        migration_sql = f.read()

    # Split into individual statements
    statements = [stmt.strip() for stmt in migration_sql.split(";") if stmt.strip()]

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Check if subscriptions table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='subscriptions'")
        if not cursor.fetchone():
            print("Subscriptions table does not exist. Creating with new schema...")
            # Just create the new table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    tenant_id TEXT(50) NOT NULL,
                    subscription_id TEXT(255) NOT NULL,
                    commerce_subscription_id TEXT(255),
                    sku_id TEXT(255) NOT NULL,
                    sku_part_number TEXT(100),
                    is_active INTEGER NOT NULL DEFAULT 1,
                    is_trial INTEGER NOT NULL DEFAULT 0,
                    total_licenses INTEGER NOT NULL DEFAULT 0,
                    next_lifecycle_date_time TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (tenant_id, subscription_id)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant ON subscriptions(tenant_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_sku ON subscriptions(sku_id)")
            print("New subscriptions table created successfully!")
            return True

        # Check current table structure
        cursor.execute("PRAGMA table_info(subscriptions)")
        columns = {col[1]: col[2] for col in cursor.fetchall()}

        print("Current table structure:")
        for col_name, col_type in columns.items():
            print(f"  {col_name}: {col_type}")

        # Check if migration is already applied
        if "is_active" in columns and "status" not in columns:
            print("Migration already applied - table has 'is_active' column")
            return True

        print("\nStarting migration...")

        # Execute migration statements
        for i, statement in enumerate(statements, 1):
            if statement.strip():
                print(f"Executing statement {i}: {statement[:50]}...")
                cursor.execute(statement)

        # Verify the new structure
        cursor.execute("PRAGMA table_info(subscriptions)")
        new_columns = {col[1]: col[2] for col in cursor.fetchall()}

        print("\nNew table structure:")
        for col_name, col_type in new_columns.items():
            print(f"  {col_name}: {col_type}")

        # Check if migration was successful
        if "is_active" in new_columns and "status" not in new_columns:
            print("\n✅ Migration completed successfully!")
            return True
        else:
            print("\n❌ Migration failed - table structure not as expected")
            return False

    except Exception as e:
        print(f"❌ Migration failed with error: {e}")
        conn.rollback()
        return False
    finally:
        conn.commit()
        conn.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
