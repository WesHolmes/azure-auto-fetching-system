#!/usr/bin/env python3
"""
Migration script to reorder columns in azure_devices table
Move last_sign_in_date before created_at for better visibility
"""

import logging
import os
import sys


# Add the parent directory to the path so we can import db_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_connection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_column_reorder():
    """Run the column reorder migration"""
    logger.info("Reordering Azure devices table columns...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Read and execute the reorder SQL
        reorder_sql_path = os.path.join(os.path.dirname(__file__), "reorder_azure_columns.sql")

        with open(reorder_sql_path) as f:
            reorder_sql = f.read()

        # Split the SQL into individual statements and execute them
        statements = [stmt.strip() for stmt in reorder_sql.split(";") if stmt.strip()]

        for i, statement in enumerate(statements):
            if statement:
                logger.info(f"Executing statement {i + 1}/{len(statements)}")
                cursor.execute(statement)

        conn.commit()
        logger.info("Column reorder completed successfully!")

        # Verify the reorder
        cursor.execute("SELECT COUNT(*) FROM azure_devices")
        azure_count = cursor.fetchone()[0]

        logger.info("Reorder results:")
        logger.info(f"  Azure devices: {azure_count}")

        # Show sample data structure
        cursor.execute("PRAGMA table_info(azure_devices)")
        azure_columns = cursor.fetchall()
        logger.info("Azure devices table columns (reordered):")
        for col in azure_columns:
            logger.info(f"  {col[1]} ({col[2]})")

    except Exception as e:
        logger.error(f"Column reorder failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_column_reorder()
