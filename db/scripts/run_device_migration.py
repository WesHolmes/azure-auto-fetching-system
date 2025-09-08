#!/usr/bin/env python3
"""
Migration script to separate Azure and Intune devices
This script will:
1. Create azure_devices table
2. Rename devices table to intune_devices
3. Remove device_type column from intune_devices
4. Move Azure devices to azure_devices table
5. Convert storage values to consistent GB format
"""

import logging
import os
import sys


# Add the parent directory to the path so we can import db_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_connection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_migration():
    """Run the device migration"""
    logger.info("Starting device migration...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Read and execute the migration SQL
        migration_sql_path = os.path.join(os.path.dirname(__file__), "migrate_devices_to_intune.sql")

        with open(migration_sql_path) as f:
            migration_sql = f.read()

        # Split the SQL into individual statements and execute them
        statements = [stmt.strip() for stmt in migration_sql.split(";") if stmt.strip()]

        for i, statement in enumerate(statements):
            if statement:
                logger.info(f"Executing statement {i + 1}/{len(statements)}")
                cursor.execute(statement)

        conn.commit()
        logger.info("Migration completed successfully!")

        # Verify the migration
        cursor.execute("SELECT COUNT(*) FROM azure_devices")
        azure_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM intune_devices")
        intune_count = cursor.fetchone()[0]

        logger.info("Migration results:")
        logger.info(f"  Azure devices: {azure_count}")
        logger.info(f"  Intune devices: {intune_count}")

        # Show sample data
        cursor.execute(
            "SELECT device_name, physical_memory_gb, total_storage_gb, free_storage_gb, enrolled_date FROM intune_devices LIMIT 3"
        )
        sample_devices = cursor.fetchall()

        logger.info("Sample Intune devices after migration:")
        for device in sample_devices:
            logger.info(f"  {device[0]}: Memory={device[1]}GB, Total={device[2]}GB, Free={device[3]}GB, Enrolled={device[4]}")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_migration()
