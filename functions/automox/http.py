import logging

import azure.functions as func

from functions.automox.helpers import get_organization_statistics, sync_automox_organizations
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
