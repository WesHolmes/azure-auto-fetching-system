import logging

import azure.functions as func

from db.db_client import query
from shared.error_reporting import categorize_sync_errors
from shared.graph_client import get_tenants
from shared.utils import create_error_response, create_success_response

from .helpers import sync_rolesV2


# HTTP FUNCTIONS
def http_sync_roles(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for manual role sync"""
    try:
        logging.info("Starting manual role sync")
        tenants = get_tenants()
        tenant_ids = [tenant["tenant_id"] for tenant in tenants]
        result = sync_rolesV2(tenant_ids)

        if result["status"] == "completed":
            successful_tenants = result["successful_tenants"]
            failed_tenants = result["failed_tenants"]
            total_roles = result["total_roles_synced"]
            total_role_assignments = result["total_role_assignments_synced"]

            if failed_tenants > 0:
                categorize_sync_errors(result["results"], "Role")

            response_msg = f"Role sync completed: {total_roles} roles, {total_role_assignments} role assignments synced across {successful_tenants} tenants"
            if failed_tenants > 0:
                response_msg += f" ({failed_tenants} tenants failed)"

            return create_success_response(
                data={
                    "total_roles": total_roles,
                    "total_role_assignments": total_role_assignments,
                    "successful_tenants": successful_tenants,
                    "failed_tenants": failed_tenants,
                },
                tenant_id="multi_tenant",
                tenant_name="all_tenants",
                operation="role_sync_http",
                message=response_msg,
            )
        else:
            error_msg = f"Role sync failed: {result.get('error', 'Unknown error')}"
            logging.error(error_msg)
            return create_error_response(error_message=error_msg, status_code=500)
    except Exception as e:
        error_msg = f"Role sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


def get_roles(req: func.HttpRequest) -> func.HttpResponse:
    """Get roles for a specific tenant"""
    try:
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        roles_query = """
            SELECT r.*,
                   COUNT(DISTINCT ur.user_id) as assigned_count
            FROM roles r
            LEFT JOIN user_rolesV2 ur ON r.tenant_id = ur.tenant_id AND r.role_id = ur.role_id
            WHERE r.tenant_id = ?
            GROUP BY r.role_id, r.tenant_id
            ORDER BY r.role_display_name
        """

        roles = query(roles_query, (tenant_id,))

        return create_success_response(
            data={"roles": roles, "count": len(roles)}, tenant_id=tenant_id, operation="get_roles", message=f"Retrieved {len(roles)} roles"
        )

    except Exception as e:
        logging.error(f"Error retrieving roles for tenant {tenant_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve roles: {str(e)}", 500)
