import logging

import azure.functions as func

from db.db_client import query
from shared.error_reporting import categorize_sync_errors
from shared.graph_client import get_tenants
from shared.utils import clean_error_message, create_error_response, create_success_response

from .helpers import sync_groups


# HTTP FUNCTIONS
def http_group_sync(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for manual group sync"""
    try:
        logging.info("Starting manual group sync")
        tenants = get_tenants()
        results = []

        for tenant in tenants:
            try:
                result = sync_groups(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(
                        f" {tenant['display_name']}: {result['groups_synced']} groups, {result.get('user_groups_synced', 0)} user memberships synced"
                    )
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "groups_synced": result["groups_synced"],
                            "user_groups_synced": result.get("user_groups_synced", 0),
                        }
                    )
                else:
                    logging.error(f" {tenant['display_name']}: {result['error']}")
                    results.append(
                        {
                            "status": "error",
                            "tenant_id": tenant["tenant_id"],
                            "error": result.get("error", "Unknown error"),
                        }
                    )
            except Exception as e:
                logging.error(clean_error_message(str(e), tenant["display_name"]))
                results.append({"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)})

        failed_count = len([r for r in results if r["status"] == "error"])
        if failed_count > 0:
            categorize_sync_errors(results, "Groups HTTP")

        total_groups = sum(r.get("groups_synced", 0) for r in results if r["status"] == "completed")
        total_user_groups = sum(r.get("user_groups_synced", 0) for r in results if r["status"] == "completed")

        return create_success_response(
            data={"total_groups": total_groups, "total_user_groups": total_user_groups, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="groups_sync_http",
            message=f"Synced {total_groups} groups and {total_user_groups} user-group assignments across {len(tenants)} tenants",
        )
    except Exception as e:
        error_msg = f"Group sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


def get_groups(req: func.HttpRequest) -> func.HttpResponse:
    """Get groups for a specific tenant"""
    try:
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        groups_query = """
            SELECT g.*,
                   COUNT(DISTINCT ug.user_id) as member_count,
                   SUM(CASE WHEN ug.is_active = 1 THEN 1 ELSE 0 END) as active_members
            FROM groups g
            LEFT JOIN user_groupsV2 ug ON g.tenant_id = ug.tenant_id AND g.group_id = ug.group_id
            WHERE g.tenant_id = ?
            GROUP BY g.group_id, g.tenant_id
            ORDER BY g.display_name
        """

        groups = query(groups_query, (tenant_id,))

        return create_success_response(
            data={"groups": groups, "count": len(groups)},
            tenant_id=tenant_id,
            operation="get_groups",
            message=f"Retrieved {len(groups)} groups",
        )

    except Exception as e:
        logging.error(f"Error retrieving groups for tenant {tenant_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve groups: {str(e)}", 500)
