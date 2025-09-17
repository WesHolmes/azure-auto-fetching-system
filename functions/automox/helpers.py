from datetime import datetime
import logging
from typing import Any

import pytz

from db.db_client import init_schema, upsert_many
from shared.amx_api import AutomoxApi, AutomoxError
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def transform_organization_data(org_data: dict[str, Any]) -> dict[str, Any]:
    """
    Transform raw organization data from Automox API into database format.

    Args:
        org_data: Raw organization data from Automox API

    Returns:
        Transformed organization data for database storage
    """
    # Extract name string - format is "DisplayName:ConnectwiseID"
    name_str = org_data.get("name", "Unknown:0")

    if ":" in name_str:
        display_name, connectwise_id_str = name_str.split(":", 1)
        try:
            connectwise_id = int(connectwise_id_str)
        except (ValueError, TypeError):
            connectwise_id = 0
    else:
        display_name = name_str
        connectwise_id = 0

    return {
        "organization_id": org_data.get("id"),
        "connectwise_id": connectwise_id,
        "display_name": display_name,
        "device_count": org_data.get("device_count"),
        "created_at": datetime.now(pytz.UTC).isoformat(),
        "last_updated": datetime.now(pytz.UTC).isoformat(),
    }


def sync_automox_organizations() -> dict[str, Any]:
    """
    Sync Automox organizations data to database.

    Returns:
        Dictionary containing sync results and statistics
    """
    logger.info("Starting Automox organizations sync")
    start_time = datetime.now(pytz.UTC)

    try:
        # Initialize database schema
        init_schema()

        # Get organizations from Automox API
        with AutomoxApi() as api:
            logger.info("Fetching organizations from Automox API")
            orgs_data = api.get_all_organizations()

            if not orgs_data:
                logger.warning("No organizations found in Automox API response")
                return {
                    "status": "success",
                    "organizations_synced": 0,
                    "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
                    "message": "No organizations found to sync",
                }

            logger.info(f"Found {len(orgs_data)} organizations in Automox API")

            # Transform data for database
            transformed_orgs = []
            for org in orgs_data:
                try:
                    transformed_org = transform_organization_data(org)
                    transformed_orgs.append(transformed_org)
                except Exception as e:
                    logger.error(f"Error transforming organization data: {e}")
                    continue

            if not transformed_orgs:
                logger.error("No valid organizations to sync after transformation")
                return {
                    "status": "error",
                    "error": "No valid organizations to sync after transformation",
                    "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
                }

            # Insert/update organizations in database
            logger.info(f"Syncing {len(transformed_orgs)} organizations to database")
            upsert_many("amx_orgs", transformed_orgs)

            duration = (datetime.now(pytz.UTC) - start_time).total_seconds()
            logger.info(f"Successfully synced {len(transformed_orgs)} organizations in {duration:.2f}s")

            return {
                "status": "success",
                "organizations_synced": len(transformed_orgs),
                "duration_seconds": duration,
                "message": f"Successfully synced {len(transformed_orgs)} organizations",
            }

    except AutomoxError as e:
        error_msg = f"Automox API error: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {"status": "error", "error": error_msg, "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds()}

    except Exception as e:
        error_msg = f"Unexpected error during Automox organizations sync: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {"status": "error", "error": error_msg, "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds()}


def get_organization_statistics() -> dict[str, Any]:
    """
    Get statistics about synced organizations.

    Returns:
        Dictionary containing organization statistics
    """
    try:
        from db.db_client import get_connection

        conn = get_connection()
        cursor = conn.cursor()

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM amx_orgs")
        total_orgs = cursor.fetchone()[0]

        # Get organizations with devices
        cursor.execute("SELECT COUNT(*) FROM amx_orgs WHERE device_count > 0")
        orgs_with_devices = cursor.fetchone()[0]

        # Get total device count
        cursor.execute("SELECT SUM(device_count) FROM amx_orgs WHERE device_count IS NOT NULL")
        total_devices = cursor.fetchone()[0] or 0

        # Get latest sync time
        cursor.execute("SELECT MAX(last_updated) FROM amx_orgs")
        latest_sync = cursor.fetchone()[0]

        conn.close()

        return {
            "total_organizations": total_orgs,
            "organizations_with_devices": orgs_with_devices,
            "total_devices": total_devices,
            "latest_sync": latest_sync,
        }

    except Exception as e:
        logger.error(f"Error getting organization statistics: {e}")
        return {"total_organizations": 0, "organizations_with_devices": 0, "total_devices": 0, "latest_sync": None, "error": str(e)}
