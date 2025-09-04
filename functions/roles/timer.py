import logging

import azure.functions as func

from db.db_client import query
from shared.error_reporting import categorize_sync_errors
from shared.graph_client import get_tenants

from .helpers import sync_rolesV2


logger = logging.getLogger(__name__)


# TIMER FUNCTIONS
def timer_roles_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for role sync across all tenants"""
    if timer.past_due:
        logging.info("Role sync V2 timer is past due!")

    logging.info("Starting scheduled role sync V2")
    tenants = get_tenants()
    tenant_ids = [tenant["tenant_id"] for tenant in tenants]

    result = sync_rolesV2(tenant_ids)

    if result["status"] == "completed":
        logging.info(
            f"  V2 Role sync completed: {result['total_roles_synced']} roles, {result['total_role_assignments_synced']} role assignments across {result['successful_tenants']} tenants"
        )
        if result["failed_tenants"] > 0:
            categorize_sync_errors(result["results"], "Role V2")
    else:
        logging.error(f"  V2 Role sync failed: {result.get('error', 'Unknown error')}")


def get_roles_analysis(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for roles analysis across all tenants"""
    if timer.past_due:
        logging.warning("Roles analysis timer is past due!")

    logging.info("Starting scheduled roles analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing roles for tenant: {tenant_name}")

            # Query role data for this tenant
            total_roles_query = "SELECT COUNT(*) as count FROM roles WHERE tenant_id = ?"
            total_roles_result = query(total_roles_query, (tenant_id,))

            total_assignments_query = "SELECT COUNT(*) as count FROM user_rolesV2 WHERE tenant_id = ?"
            total_assignments_result = query(total_assignments_query, (tenant_id,))

            users_with_roles_query = "SELECT COUNT(DISTINCT user_id) as count FROM user_rolesV2 WHERE tenant_id = ?"
            users_with_roles_result = query(users_with_roles_query, (tenant_id,))

            admin_roles_query = "SELECT COUNT(*) as count FROM roles WHERE tenant_id = ? AND (role_display_name LIKE '%Admin%' OR role_display_name LIKE '%Administrator%')"
            admin_roles_result = query(admin_roles_query, (tenant_id,))

            multi_role_users_query = "SELECT COUNT(*) as count FROM (SELECT user_id FROM user_rolesV2 WHERE tenant_id = ? GROUP BY user_id HAVING COUNT(role_id) > 1)"
            multi_role_users_result = query(multi_role_users_query, (tenant_id,))

            # Calculate metrics
            total_roles = total_roles_result[0]["count"] if total_roles_result else 0
            total_assignments = total_assignments_result[0]["count"] if total_assignments_result else 0
            users_with_roles = users_with_roles_result[0]["count"] if users_with_roles_result else 0
            admin_roles = admin_roles_result[0]["count"] if admin_roles_result else 0
            multi_role_users = multi_role_users_result[0]["count"] if multi_role_users_result else 0

            # Generate optimization actions
            actions = []
            if admin_roles > 0:
                actions.append(f"Review {admin_roles} admin roles for security")

            if multi_role_users > 0:
                actions.append(f"Review {multi_role_users} users with multiple roles")

            if total_assignments > 0 and users_with_roles > 0:
                avg_roles_per_user = total_assignments / users_with_roles
                if avg_roles_per_user > 2:
                    actions.append(f"High role density: {avg_roles_per_user:.1f} roles per user")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_roles": total_roles,
                "total_assignments": total_assignments,
                "users_with_roles": users_with_roles,
                "admin_roles": admin_roles,
                "multi_role_users": multi_role_users,
                "actions": actions,
            }

            logging.info(f"✓ {tenant_name}: {total_roles} roles, {users_with_roles} users, {admin_roles} admin roles")
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Roles analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Roles analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_roles_all = sum(r.get("total_roles", 0) for r in results if r["status"] == "completed")
    total_assignments_all = sum(r.get("total_assignments", 0) for r in results if r["status"] == "completed")
    total_users_all = sum(r.get("users_with_roles", 0) for r in results if r["status"] == "completed")
    total_admin_roles_all = sum(r.get("admin_roles", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_roles_all} roles, {total_assignments_all} assignments, {total_users_all} users, {total_admin_roles_all} admin roles"
    )
