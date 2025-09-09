from datetime import datetime
import logging

import azure.functions as func

from functions.backup_radar.helpers import sync_backup_radar_data
from shared.graph_client import get_tenants
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def timer_backup_radar_sync(timer: func.TimerRequest) -> None:
    """
    Scheduled Backup Radar sync function - runs daily to sync backup data for all tenants.
    Fetches both active and retired backups from Backup Radar API and stores in database.
    """
    if timer.past_due:
        logger.info("The timer is past due!")

    logger.info("Starting scheduled Backup Radar sync for all tenants")
    start_time = datetime.now()

    try:
        # Get all tenants
        tenants = get_tenants()
        logger.info(f"Found {len(tenants)} tenants for Backup Radar sync")

        # Sync backup data for all tenants
        result = sync_backup_radar_data(tenants, days_back=7)

        # Log results
        if result["status"] == "success":
            logger.info(
                f"Backup Radar sync completed successfully: "
                f"{result['total_active_backups']} active, "
                f"{result['total_retired_backups']} retired backups "
                f"in {result['duration_seconds']:.2f}s"
            )

            # Log any errors if present
            if "errors" in result:
                error_summary = result["errors"]
                logger.warning(
                    f"Backup Radar sync completed with {error_summary['total_errors']} errors: "
                    f"{error_summary['api_errors']} API errors, "
                    f"{error_summary['data_errors']} data processing errors"
                )
        else:
            logger.error(f"Backup Radar sync failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        error_msg = f"Backup Radar timer function failed: {clean_error_message(str(e))}"
        logger.error(error_msg)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Backup Radar sync failed after {duration:.2f}s: {error_msg}")
