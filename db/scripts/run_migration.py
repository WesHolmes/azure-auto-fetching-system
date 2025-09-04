#!/usr/bin/env python3
"""
Database migration script to add office_location and mobile_phone columns to usersV2 table,
and create groups and user_groupsV2 tables.
Run this script to update existing databases with the new schema.
"""

import os
from pathlib import Path
import sqlite3
import sys


def run_users_migration(db_path):
    """Run the migration to add new columns to usersV2 table"""

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print(f"Running users migration on: {db_path}")

        # Check if columns already exist
        cursor.execute("PRAGMA table_info(usersV2)")
        columns = [col[1] for col in cursor.fetchall()]

        if "office_location" in columns and "mobile_phone" in columns:
            print("Users migration already completed - columns exist")
            return True

        # Add office_location column
        if "office_location" not in columns:
            print("Adding office_location column...")
            cursor.execute("ALTER TABLE usersV2 ADD COLUMN office_location TEXT(100)")
            print("✓ Added office_location column")
        else:
            print("office_location column already exists")

        # Add mobile_phone column
        if "mobile_phone" not in columns:
            print("Adding mobile_phone column...")
            cursor.execute("ALTER TABLE usersV2 ADD COLUMN mobile_phone TEXT(50)")
            print("✓ Added mobile_phone column")
        else:
            print("mobile_phone column already exists")

        # Commit changes
        conn.commit()
        print("✓ Users migration completed successfully")

        # Verify the new schema
        cursor.execute("PRAGMA table_info(usersV2)")
        columns = [col[1] for col in cursor.fetchall()]
        print(f"Current usersV2 columns: {', '.join(columns)}")

        return True

    except Exception as e:
        print(f"Users migration failed: {str(e)}")
        return False
    finally:
        if "conn" in locals():
            conn.close()


def run_groups_migration(db_path):
    """Run the migration to add groups and user_groupsV2 tables"""

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print(f"Running groups migration on: {db_path}")

        # Check if groups table already exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='groups'")
        groups_exists = cursor.fetchone() is not None

        # Check if user_groupsV2 table already exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_groupsV2'")
        user_groups_exists = cursor.fetchone() is not None

        if groups_exists and user_groups_exists:
            print("Groups migration already completed - tables exist")
            return True

        # Create groups table
        if not groups_exists:
            print("Creating groups table...")
            cursor.execute("""
                CREATE TABLE groups (
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
            """)
            print("✓ Created groups table")
        else:
            print("groups table already exists")

        # Create user_groupsV2 table
        if not user_groups_exists:
            print("Creating user_groupsV2 table...")
            cursor.execute("""
                CREATE TABLE user_groupsV2 (
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
            """)
            print("✓ Created user_groupsV2 table")
        else:
            print("user_groupsV2 table already exists")

        # Create indexes
        print("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_groups_tenant ON groups(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_groups_type ON groups(group_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_groupsV2_tenant ON user_groupsV2(tenant_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_groupsV2_user ON user_groupsV2(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_groupsV2_group ON user_groupsV2(group_id)")
        print("✓ Created all indexes")

        # Commit changes
        conn.commit()
        print("✓ Groups migration completed successfully")

        return True

    except Exception as e:
        print(f"Groups migration failed: {str(e)}")
        return False
    finally:
        if "conn" in locals():
            conn.close()


def run_migration(db_path):
    """Run all migrations in sequence"""

    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return False

    print(f"Starting migrations for: {db_path}")
    print("=" * 50)

    # Run users migration
    users_success = run_users_migration(db_path)
    print()

    # Run groups migration
    groups_success = run_groups_migration(db_path)
    print()

    # Overall result
    if users_success and groups_success:
        print("=" * 50)
        print("✓ ALL MIGRATIONS COMPLETED SUCCESSFULLY")
        return True
    else:
        print("=" * 50)
        print("❌ SOME MIGRATIONS FAILED")
        return False


def main():
    """Main function to run all migrations"""

    # Look for database files in common locations
    possible_paths = [
        "sql/data/users.db",
        "sql/data/tenant_data.db",
        "sql/data/graph_sync.db",
        "data/users.db",
        "data/tenant_data.db",
        "data/graph_sync.db",
        "users.db",
        "tenant_data.db",
        "graph_sync.db",
    ]

    # Also check current directory for .db files
    current_dir = Path(".")
    db_files = list(current_dir.glob("*.db"))
    possible_paths.extend([str(f) for f in db_files])

    print("Looking for database files...")

    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found database: {path}")
            if run_migration(path):
                print(f"Successfully migrated: {path}")
            else:
                print(f"Failed to migrate: {path}")
            print()

    if not any(os.path.exists(p) for p in possible_paths):
        print("No database files found in common locations.")
        print("Please specify the database path manually:")
        print("python run_migration.py <database_path>")

        if len(sys.argv) > 1:
            db_path = sys.argv[1]
            if run_migration(db_path):
                print(f"Successfully migrated: {db_path}")
            else:
                print(f"Failed to migrate: {db_path}")
        else:
            print("No database path specified.")


if __name__ == "__main__":
    main()
