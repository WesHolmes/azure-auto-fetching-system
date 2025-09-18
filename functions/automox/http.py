import logging

import azure.functions as func

from functions.automox.helpers import (
    get_device_statistics,
    get_organization_statistics,
    sync_automox_devices,
    sync_automox_organizations,
)
from shared.utils import create_error_response, create_success_response


logger = logging.getLogger(__name__)


def http_amx_orgs_sync(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to manually sync Automox organizations.

    GET /api/amx/orgs/sync - Sync organizations from Automox API
    """
    try:
        logger.info("Starting manual Automox organizations sync")

        # Sync organization data
        result = sync_automox_organizations()

        if result["status"] == "success":
            return create_success_response(
                data={"organizations_synced": result["organizations_synced"], "duration_seconds": result["duration_seconds"]},
                tenant_id="automox",
                tenant_name="Automox",
                operation="sync_organizations",
                message=result["message"],
            )
        else:
            return create_error_response(
                error_message=result["error"], status_code=500, tenant_id="automox", tenant_name="Automox", operation="sync_organizations"
            )

    except Exception as e:
        error_msg = f"Error during manual organizations sync: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="sync_organizations"
        )


def http_amx_orgs_stats(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to get Automox organizations statistics.

    GET /api/amx/orgs/stats - Get organization statistics
    """
    try:
        logger.info("Fetching Automox organizations statistics")

        # Get statistics
        stats = get_organization_statistics()

        return create_success_response(
            data=stats,
            tenant_id="automox",
            tenant_name="Automox",
            operation="get_statistics",
            message="Organization statistics retrieved successfully",
        )

    except Exception as e:
        error_msg = f"Error fetching organization statistics: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="get_statistics"
        )


def http_amx_orgs_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to list all Automox organizations.

    GET /api/amx/orgs - List all organizations
    """
    try:
        from db.db_client import get_connection

        logger.info("Fetching all Automox organizations")

        conn = get_connection()
        cursor = conn.cursor()

        # Get all organizations
        cursor.execute("""
            SELECT organization_id, connectwise_id, display_name, device_count, 
                   created_at, last_updated
            FROM amx_orgs 
            ORDER BY display_name
        """)

        columns = [description[0] for description in cursor.description]
        organizations = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()

        return create_success_response(
            data=organizations,
            tenant_id="automox",
            tenant_name="Automox",
            operation="list_organizations",
            message=f"Retrieved {len(organizations)} organizations",
        )

    except Exception as e:
        error_msg = f"Error fetching organizations list: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="list_organizations"
        )


def http_amx_devices_sync(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to manually sync Automox devices.

    POST /api/amx/devices/sync - Sync devices from Automox API
    """
    try:
        logger.info("Starting manual Automox devices sync")

        # Sync device data
        result = sync_automox_devices()

        if result["status"] == "success":
            return create_success_response(
                data={
                    "devices_synced": result["devices_synced"],
                    "organizations_processed": result["organizations_processed"],
                    "duration_seconds": result["duration_seconds"],
                },
                tenant_id="automox",
                tenant_name="Automox",
                operation="sync_devices",
                message=result["message"],
            )
        else:
            return create_error_response(
                error_message=result["error"], status_code=500, tenant_id="automox", tenant_name="Automox", operation="sync_devices"
            )

    except Exception as e:
        error_msg = f"Error during manual devices sync: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="sync_devices"
        )


def http_amx_devices_stats(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to get Automox devices statistics.

    GET /api/amx/devices/stats - Get device statistics
    """
    try:
        logger.info("Fetching Automox devices statistics")

        # Get statistics
        stats = get_device_statistics()

        return create_success_response(
            data=stats,
            tenant_id="automox",
            tenant_name="Automox",
            operation="get_device_statistics",
            message="Device statistics retrieved successfully",
        )

    except Exception as e:
        error_msg = f"Error fetching device statistics: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="get_device_statistics"
        )


def http_amx_devices_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to list all Automox devices.

    GET /api/amx/devices - List all devices
    """
    try:
        from db.db_client import get_connection

        logger.info("Fetching all Automox devices")

        conn = get_connection()
        cursor = conn.cursor()

        # Get all devices with organization info and device details
        cursor.execute("""
            SELECT d.organization_id, d.device_id, d.display_name, 
                   d.agent_version, d.connected, d.is_compliant, d.pending_patches, d.needs_reboot,
                   d.created_at, d.last_updated, o.display_name as org_name,
                   dd.os_family, dd.os_name, dd.os_version, dd.os_version_id, dd.serial_number,
                   dd.model, dd.vendor, dd.version, dd.mdm_server, dd.mdm_profile_installed,
                   dd.secure_token_account, dd.last_logged_in_user, dd.last_process_time,
                   dd.last_disconnect_time, dd.is_delayed_by_user, dd.needs_attention,
                   dd.is_compatible, dd.create_time
            FROM amx_devices d
            LEFT JOIN amx_orgs o ON d.organization_id = o.organization_id
            LEFT JOIN amx_device_details dd ON d.organization_id = dd.organization_id AND d.device_id = dd.device_id
            ORDER BY o.display_name, d.display_name
        """)

        columns = [description[0] for description in cursor.description]
        devices = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()

        return create_success_response(
            data=devices,
            tenant_id="automox",
            tenant_name="Automox",
            operation="list_devices",
            message=f"Retrieved {len(devices)} devices",
        )

    except Exception as e:
        error_msg = f"Error fetching devices list: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="list_devices"
        )
