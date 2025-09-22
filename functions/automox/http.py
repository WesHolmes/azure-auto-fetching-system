import logging

import azure.functions as func

from functions.automox.helpers import (
    get_device_statistics,
    get_organization_statistics,
    get_package_statistics,
    sync_automox_devices,
    sync_automox_organizations,
    sync_automox_packages,
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


def http_amx_packages_sync(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger for manual Automox packages sync.
    Manually triggers the sync of packages data from Automox API.
    """
    logger.info("Manual Automox packages sync requested")

    try:
        # Sync packages data
        result = sync_automox_packages()

        if result["status"] == "success":
            logger.info(f"Packages sync completed: {result['packages_synced']} packages synced")
            return create_success_response(
                data=result,
                tenant_id="automox",
                tenant_name="Automox",
                operation="sync_packages",
                message=f"Successfully synced {result['packages_synced']} packages",
            )
        else:
            error_msg = result.get("message", "Unknown error during packages sync")
            logger.error(f"Packages sync failed: {error_msg}")
            return create_error_response(
                error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="sync_packages"
            )

    except Exception as e:
        error_msg = f"Error during packages sync: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="sync_packages"
        )


def http_amx_packages_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to list all Automox packages.
    Returns all packages with organization and device information.
    """
    try:
        from db.db_client import get_connection

        logger.info("Fetching all Automox packages")

        conn = get_connection()
        cursor = conn.cursor()

        # Get all packages with organization and device info
        cursor.execute("""
            SELECT p.organization_id, p.device_id, p.package_id, p.software_id,
                   p.display_name, p.name, p.package_version_id, p.version, p.repo,
                   p.installed, p.ignored, p.group_ignored, p.deferred_until,
                   p.group_deferred_until, p.requires_reboot, p.severity, p.cve_score,
                   p.cves, p.is_managed, p.impact, p.os_name, p.os_version,
                   p.scheduled_at, p.created_at, p.last_updated,
                   o.display_name as org_name, d.display_name as device_name
            FROM amx_packages p
            LEFT JOIN amx_orgs o ON p.organization_id = o.organization_id
            LEFT JOIN amx_devices d ON p.organization_id = d.organization_id AND p.device_id = d.device_id
            ORDER BY o.display_name, d.display_name, p.display_name
        """)

        columns = [description[0] for description in cursor.description]
        packages = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()

        return create_success_response(
            data=packages,
            tenant_id="automox",
            tenant_name="Automox",
            operation="list_packages",
            message=f"Retrieved {len(packages)} packages",
        )

    except Exception as e:
        error_msg = f"Error fetching packages list: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="list_packages"
        )


def http_amx_packages_stats(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to get Automox packages statistics.
    Returns comprehensive statistics about packages data.
    """
    try:
        logger.info("Fetching Automox packages statistics")

        # Get package statistics
        stats = get_package_statistics()

        return create_success_response(
            data=stats,
            tenant_id="automox",
            tenant_name="Automox",
            operation="packages_stats",
            message="Packages statistics retrieved successfully",
        )

    except Exception as e:
        error_msg = f"Error fetching packages statistics: {str(e)}"
        logger.error(error_msg)
        return create_error_response(
            error_message=error_msg, status_code=500, tenant_id="automox", tenant_name="Automox", operation="packages_stats"
        )
