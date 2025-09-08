#!/usr/bin/env python3
"""
Fix script to add back serial_number, is_encrypted, and enrolled_date to intune_devices
These should be kept for Intune devices but removed from Azure devices
"""

import logging
import os
import sys


# Add the parent directory to the path so we can import db_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_connection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_intune_fix():
    """Run the intune columns fix"""
    logger.info("Fixing Intune devices table columns...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Read and execute the fix SQL
        fix_sql_path = os.path.join(os.path.dirname(__file__), "fix_intune_columns.sql")

        with open(fix_sql_path) as f:
            fix_sql = f.read()

        # Split the SQL into individual statements and execute them
        statements = [stmt.strip() for stmt in fix_sql.split(";") if stmt.strip()]

        for i, statement in enumerate(statements):
            if statement:
                logger.info(f"Executing statement {i + 1}/{len(statements)}")
                cursor.execute(statement)

        conn.commit()
        logger.info("Intune columns fix completed successfully!")

        # Verify the fix
        cursor.execute("SELECT COUNT(*) FROM intune_devices")
        intune_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM azure_devices")
        azure_count = cursor.fetchone()[0]

        logger.info("Fix results:")
        logger.info(f"  Intune devices: {intune_count}")
        logger.info(f"  Azure devices: {azure_count}")

        # Show sample data structure
        cursor.execute("PRAGMA table_info(intune_devices)")
        intune_columns = cursor.fetchall()
        logger.info("Intune devices table columns (fixed):")
        for col in intune_columns:
            logger.info(f"  {col[1]} ({col[2]})")

        cursor.execute("PRAGMA table_info(azure_devices)")
        azure_columns = cursor.fetchall()
        logger.info("Azure devices table columns:")
        for col in azure_columns:
            logger.info(f"  {col[1]} ({col[2]})")

    except Exception as e:
        logger.error(f"Intune fix failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_intune_fix()
