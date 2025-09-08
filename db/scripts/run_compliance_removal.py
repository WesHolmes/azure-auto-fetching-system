#!/usr/bin/env python3
"""
Migration script to remove compliance_state column and reorder last_sign_in_date
Move last_sign_in_date to third-to-last position (left of created_at)
"""

import logging
import os
import sys


# Add the parent directory to the path so we can import db_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_connection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_compliance_removal():
    """Run the compliance state removal and column reorder migration"""
    logger.info("Removing compliance_state column and reordering Azure devices table...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Read and execute the migration SQL
        migration_sql_path = os.path.join(os.path.dirname(__file__), "remove_compliance_reorder_azure.sql")

        with open(migration_sql_path) as f:
            migration_sql = f.read()

        # Split the SQL into individual statements and execute them
        statements = [stmt.strip() for stmt in migration_sql.split(";") if stmt.strip()]

        for i, statement in enumerate(statements):
            if statement:
                logger.info(f"Executing statement {i + 1}/{len(statements)}")
                cursor.execute(statement)

        conn.commit()
        logger.info("Compliance state removal and column reorder completed successfully!")

        # Verify the changes
        cursor.execute("SELECT COUNT(*) FROM azure_devices")
        azure_count = cursor.fetchone()[0]

        logger.info("Migration results:")
        logger.info(f"  Azure devices: {azure_count}")

        # Show sample data structure
        cursor.execute("PRAGMA table_info(azure_devices)")
        azure_columns = cursor.fetchall()
        logger.info("Azure devices table columns (after changes):")
        for col in azure_columns:
            logger.info(f"  {col[1]} ({col[2]})")

        # Verify compliance_state is removed
        column_names = [col[1] for col in azure_columns]
        if "compliance_state" in column_names:
            logger.warning("⚠️  compliance_state column still exists!")
        else:
            logger.info("✅ compliance_state column successfully removed")

        # Verify last_sign_in_date position
        last_sign_in_index = None
        created_at_index = None
        for i, col in enumerate(azure_columns):
            if col[1] == "last_sign_in_date":
                last_sign_in_index = i
            elif col[1] == "created_at":
                created_at_index = i

        if last_sign_in_index is not None and created_at_index is not None:
            if last_sign_in_index == created_at_index - 1:
                logger.info("✅ last_sign_in_date is correctly positioned left of created_at")
            else:
                logger.warning(f"⚠️  last_sign_in_date is at position {last_sign_in_index}, created_at at {created_at_index}")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_compliance_removal()
