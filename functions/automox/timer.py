from datetime import datetime
import logging

import azure.functions as func

from functions.automox.helpers import sync_automox_devices, sync_automox_organizations, sync_automox_packages
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def timer_amx_org_sync(timer: func.TimerRequest) -> None:
    """
    Scheduled Automox organizations sync function - runs daily to sync organization data.
    Fetches organization data from Automox API and stores in database.
    """
    if timer.past_due:
        logger.info("The timer is past due!")

    logger.info("Starting scheduled Automox organizations sync")
    start_time = datetime.now()

    try:
        # Sync organization data
        result = sync_automox_organizations()

        # Log results
        if result["status"] == "success":
            logger.info(
                f"Automox organizations sync completed successfully: "
                f"{result['organizations_synced']} organizations synced "
                f"in {result['duration_seconds']:.2f}s"
            )
        else:
            logger.error(f"Automox organizations sync failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        error_msg = f"Automox organizations timer function failed: {clean_error_message(str(e))}"
        logger.error(error_msg)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Automox organizations sync failed after {duration:.2f}s: {error_msg}")


def timer_amx_devices_sync(timer: func.TimerRequest) -> None:
    """
    Scheduled Automox devices sync function - runs daily to sync device data.
    Fetches device data from Automox API for all organizations and stores in database.
    """
    if timer.past_due:
        logger.info("The timer is past due!")

    logger.info("Starting scheduled Automox devices sync")
    start_time = datetime.now()

    try:
        # Sync device data
        result = sync_automox_devices()

        # Log results
        if result["status"] == "success":
            logger.info(
                f"Automox devices sync completed successfully: "
                f"{result['devices_synced']} devices synced across "
                f"{result['organizations_processed']} organizations "
                f"in {result['duration_seconds']:.2f}s"
            )
        else:
            logger.error(f"Automox devices sync failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        error_msg = f"Automox devices timer function failed: {clean_error_message(str(e))}"
        logger.error(error_msg)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Automox devices sync failed after {duration:.2f}s: {error_msg}")


def timer_amx_packages_sync(timer: func.TimerRequest) -> None:
    """
    Scheduled Automox packages sync function - runs daily to sync package data.
    Fetches package data from Automox API for all organizations and stores in database.
    """
    if timer.past_due:
        logger.info("The timer is past due!")

    logger.info("Starting scheduled Automox packages sync")
    start_time = datetime.now()

    try:
        # Sync package data
        result = sync_automox_packages()

        # Log results
        if result["status"] == "success":
            logger.info(
                f"Automox packages sync completed successfully: "
                f"{result['packages_synced']} packages synced across "
                f"{result['organizations_processed']} organizations "
                f"in {result['duration_seconds']:.2f}s"
            )
        else:
            logger.error(f"Automox packages sync failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        error_msg = f"Automox packages timer function failed: {clean_error_message(str(e))}"
        logger.error(error_msg)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Automox packages sync failed after {duration:.2f}s: {error_msg}")
