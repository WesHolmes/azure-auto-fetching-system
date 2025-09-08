from datetime import datetime
import logging

import azure.functions as func

from db.db_client import query
from functions.devices.helpers import sync_devices
from shared.graph_client import get_tenants
from shared.utils import clean_error_message, create_error_response, create_success_response


logger = logging.getLogger(__name__)


def http_devices_sync(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint for manual device sync"""
    try:
        logger.info("Starting manual device sync via HTTP request")
        start_time = datetime.now()

        # Get tenant_id from query parameters if specified
        tenant_id = req.params.get("tenant_id")

        if tenant_id:
            # Sync specific tenant
            logger.info(f"Syncing devices for specific tenant: {tenant_id}")
            tenants = [t for t in get_tenants() if t["tenant_id"] == tenant_id]

            if not tenants:
                return func.HttpResponse(f"Tenant {tenant_id} not found", status_code=404)
        else:
            # Sync all tenants
            logger.info("Syncing devices for all tenants")
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
                        f"✓ {tenant_name}: {result.get('devices_synced', 0)} devices, {result.get('relationships_synced', 0)} relationships synced"
                    )
                else:
                    logger.error(f"✗ {tenant_name}: {result.get('error', 'Unknown error')}")

            except Exception as e:
                error_msg = clean_error_message(str(e), tenant_name=tenant_name)
                logger.error(f"✗ {tenant_name}: {error_msg}")
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "error": str(e),
                    }
                )

        duration = (datetime.now() - start_time).total_seconds()

        # Prepare response
        successful_tenants = [r for r in results if r.get("status") == "success"]
        failed_tenants = [r for r in results if r.get("status") != "success"]

        response_data = {
            "status": "completed",
            "total_devices": total_devices,
            "total_relationships": total_relationships,
            "tenants_processed": len(tenants),
            "successful_tenants": len(successful_tenants),
            "failed_tenants": len(failed_tenants),
            "duration_seconds": duration,
            "results": results,
        }

        logger.info(
            f"Device sync completed: {total_devices} devices, {total_relationships} relationships across {len(tenants)} tenants in {duration:.1f}s"
        )

        return func.HttpResponse(func.HttpResponse.json(response_data), status_code=200, headers={"Content-Type": "application/json"})

    except Exception as e:
        error_msg = clean_error_message(str(e), "Device sync HTTP request failed")
        logger.error(error_msg)

        return func.HttpResponse(
            func.HttpResponse.json({"status": "error", "error": str(e), "message": "Device sync failed"}),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


def get_devices(req: func.HttpRequest) -> func.HttpResponse:
    """Get devices for a specific tenant"""
    try:
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        devices_query = """
            SELECT d.*,
                   COUNT(DISTINCT ud.user_id) as user_count,
                   SUM(CASE WHEN ud.relationship_type = 'owner' THEN 1 ELSE 0 END) as owner_count
            FROM intune_devices d
            LEFT JOIN user_devicesV2 ud ON d.tenant_id = ud.tenant_id AND d.device_id = ud.device_id
            WHERE d.tenant_id = ?
            GROUP BY d.device_id, d.tenant_id
            ORDER BY d.device_name
        """

        devices = query(devices_query, (tenant_id,))

        return create_success_response(
            data={"devices": devices, "count": len(devices)},
            tenant_id=tenant_id,
            operation="get_devices",
            message=f"Retrieved {len(devices)} devices",
        )

    except Exception as e:
        logger.error(f"Error retrieving devices for tenant {tenant_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve devices: {str(e)}", 500)
