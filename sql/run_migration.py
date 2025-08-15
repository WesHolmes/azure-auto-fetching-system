#!/usr/bin/env python3
"""
Database migration script to add office_location and mobile_phone columns to usersV2 table.
Run this script to update existing databases with the new schema.
"""

import os
from pathlib import Path
import sqlite3
import sys


def run_migration(db_path):
    """Run the migration to add new columns to usersV2 table"""

    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print(f"Connected to database: {db_path}")

        # Check if columns already exist
        cursor.execute("PRAGMA table_info(usersV2)")
        columns = [col[1] for col in cursor.fetchall()]

        if "office_location" in columns and "mobile_phone" in columns:
            print("Migration already completed - columns exist")
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
        print("✓ Migration completed successfully")

        # Verify the new schema
        cursor.execute("PRAGMA table_info(usersV2)")
        columns = [col[1] for col in cursor.fetchall()]
        print(f"Current columns: {', '.join(columns)}")

        return True

    except Exception as e:
        print(f"Migration failed: {str(e)}")
        return False
    finally:
        if "conn" in locals():
            conn.close()


def main():
    """Main function to run migration"""

    # Look for database files in common locations
    possible_paths = ["data/users.db", "data/tenant_data.db", "users.db", "tenant_data.db"]

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


if __name__ == "__main__":
    main()
