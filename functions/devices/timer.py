from datetime import datetime
import logging

import azure.functions as func

from functions.devices.helpers import sync_devices
from shared.graph_client import get_tenants
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def timer_devices_sync(timer: func.TimerRequest) -> None:
    """V2 Device sync using new database schema with concurrent processing"""
    if timer.past_due:
        logger.info("The timer is past due!")

    logger.info("Starting device sync V2 for all tenants")
    start_time = datetime.now()

    tenants = get_tenants()
    total_devices = 0
    total_relationships = 0
    results = []

    for tenant in tenants:
        tenant_id = tenant["tenant_id"]
        tenant_name = tenant["display_name"]
        logger.info(f"Starting device sync for {tenant_name}")

        try:
            result = sync_devices(tenant_id, tenant_name)
            results.append(result)

            if result["status"] == "success":
                total_devices += result.get("devices_synced", 0)
                total_relationships += result.get("relationships_synced", 0)
                logger.info(
                    f"✓ V2 {tenant_name}: {result.get('devices_synced', 0)} devices, {result.get('relationships_synced', 0)} relationships synced"
                )
            else:
                logger.error(f"✗ V2 {tenant_name}: {result.get('error', 'Unknown error')}")

        except Exception as e:
            error_msg = clean_error_message(str(e), tenant_name=tenant_name)
            logger.error(f"✗ V2 {tenant_name}: {error_msg}")
            results.append(
                {
                    "status": "error",
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_name,
                    "error": str(e),
                }
            )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"Device sync V2 completed: {total_devices} devices, {total_relationships} relationships across {len(tenants)} tenants in {duration:.1f}s"
    )

    # Log summary of results
    successful_tenants = [r for r in results if r.get("status") == "success"]
    failed_tenants = [r for r in results if r.get("status") != "success"]

    logger.info(f"Device sync summary: {len(successful_tenants)} successful, {len(failed_tenants)} failed")

    if failed_tenants:
        logger.warning(f"Failed tenants: {[t['tenant_name'] for t in failed_tenants]}")
