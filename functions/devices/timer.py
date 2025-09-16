from datetime import datetime
import logging

import azure.functions as func

from functions.devices.helpers import sync_azure_devices, sync_intune_devices
from shared.graph_client import get_tenants
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def timer_devices_sync(timer: func.TimerRequest) -> None:
    """V2 Device sync using new database schema with concurrent processing - syncs both Intune and Azure devices"""
    if timer.past_due:
        logger.info("The timer is past due!")

    logger.info("Starting device sync V2 for all tenants (Intune + Azure)")
    start_time = datetime.now()

    tenants = get_tenants()
    total_intune_devices = 0
    total_azure_devices = 0
    total_relationships = 0
    intune_results = []
    azure_results = []

    for tenant in tenants:
        tenant_id = tenant["tenant_id"]
        tenant_name = tenant["display_name"]
        logger.info(f"Starting device sync for {tenant_name}")

        # Sync Intune devices
        try:
            logger.info(f"Syncing Intune devices for {tenant_name}")
            intune_result = sync_intune_devices(tenant_id, tenant_name)
            intune_results.append(intune_result)

            if intune_result["status"] == "success":
                total_intune_devices += intune_result.get("devices_synced", 0)
                total_relationships += intune_result.get("relationships_synced", 0)
                logger.info(
                    f"✓ Intune {tenant_name}: {intune_result.get('devices_synced', 0)} devices, {intune_result.get('relationships_synced', 0)} relationships synced"
                )
            else:
                logger.error(f"✗ Intune {tenant_name}: {intune_result.get('error', 'Unknown error')}")

        except Exception as e:
            error_msg = clean_error_message(str(e), tenant_name=tenant_name)
            logger.error(f"✗ Intune {tenant_name}: {error_msg}")
            intune_results.append(
                {
                    "status": "error",
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_name,
                    "error": str(e),
                }
            )

        # Sync Azure devices
        try:
            logger.info(f"Syncing Azure devices for {tenant_name}")
            azure_result = sync_azure_devices(tenant_id, tenant_name)
            azure_results.append(azure_result)

            if azure_result["status"] == "success":
                total_azure_devices += azure_result.get("devices_synced", 0)
                total_relationships += azure_result.get("relationships_synced", 0)
                logger.info(
                    f"✓ Azure {tenant_name}: {azure_result.get('devices_synced', 0)} devices, {azure_result.get('relationships_synced', 0)} relationships synced"
                )
            else:
                logger.error(f"✗ Azure {tenant_name}: {azure_result.get('error', 'Unknown error')}")

        except Exception as e:
            error_msg = clean_error_message(str(e), tenant_name=tenant_name)
            logger.error(f"✗ Azure {tenant_name}: {error_msg}")
            azure_results.append(
                {
                    "status": "error",
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_name,
                    "error": str(e),
                }
            )

    duration = (datetime.now() - start_time).total_seconds()
    total_devices = total_intune_devices + total_azure_devices
    logger.info(
        f"Device sync V2 completed: {total_intune_devices} Intune devices, {total_azure_devices} Azure devices, {total_relationships} relationships across {len(tenants)} tenants in {duration:.1f}s"
    )

    # Log summary of results
    successful_intune = [r for r in intune_results if r.get("status") == "success"]
    failed_intune = [r for r in intune_results if r.get("status") != "success"]
    successful_azure = [r for r in azure_results if r.get("status") == "success"]
    failed_azure = [r for r in azure_results if r.get("status") != "success"]

    logger.info(f"Intune sync summary: {len(successful_intune)} successful, {len(failed_intune)} failed")
    logger.info(f"Azure sync summary: {len(successful_azure)} successful, {len(failed_azure)} failed")

    if failed_intune:
        logger.warning(f"Failed Intune tenants: {[t['tenant_name'] for t in failed_intune]}")
    if failed_azure:
        logger.warning(f"Failed Azure tenants: {[t['tenant_name'] for t in failed_azure]}")
