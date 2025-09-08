#!/usr/bin/env python3
"""
Migration script to add additional Azure device fields
This script will add the recommended fields from the Azure AD devices API
"""

import logging
import os
import sys


# Add the parent directory to the path so we can import db_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_connection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_azure_fields_migration():
    """Run the Azure device fields migration"""
    logger.info("Adding additional Azure device fields...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Read and execute the migration SQL
        migration_sql_path = os.path.join(os.path.dirname(__file__), "add_azure_device_fields.sql")

        with open(migration_sql_path) as f:
            migration_sql = f.read()

        # Split the SQL into individual statements and execute them
        statements = [stmt.strip() for stmt in migration_sql.split(";") if stmt.strip()]

        for i, statement in enumerate(statements):
            if statement:
                logger.info(f"Executing statement {i + 1}/{len(statements)}")
                cursor.execute(statement)

        conn.commit()
        logger.info("Azure device fields migration completed successfully!")

        # Verify the migration
        cursor.execute("SELECT COUNT(*) FROM azure_devices")
        azure_count = cursor.fetchone()[0]

        logger.info("Migration results:")
        logger.info(f"  Azure devices: {azure_count}")

        # Show sample data structure
        cursor.execute("PRAGMA table_info(azure_devices)")
        azure_columns = cursor.fetchall()
        logger.info("Azure devices table columns (updated):")
        for col in azure_columns:
            logger.info(f"  {col[1]} ({col[2]})")

    except Exception as e:
        logger.error(f"Azure fields migration failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_azure_fields_migration()
