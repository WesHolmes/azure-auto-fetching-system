import logging

import azure.functions as func

from db.db_client import query
from shared.error_reporting import categorize_sync_errors
from shared.graph_client import get_tenants
from shared.utils import clean_error_message

from .helpers import sync_groups


logger = logging.getLogger(__name__)


# TIMER FUNCTIONS
def timer_groups_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for group sync across all tenants"""
    if timer.past_due:
        logging.info("Group sync V2 timer is past due!")

    logging.info("Starting scheduled group sync V2")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_groups(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(
                    f" V2 {tenant['display_name']}: {result['groups_synced']} groups synced, {result.get('user_groups_synced', 0)} user memberships synced"
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
                logging.error(f" V2 {tenant['display_name']}: {result['error']}")
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
        categorize_sync_errors(results, "Group V2")


def get_groups_analysis(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for groups analysis across all tenants"""
    if timer.past_due:
        logging.warning("Groups analysis timer is past due!")

    logging.info("Starting scheduled groups analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing groups for tenant: {tenant_name}")

            # Query group data for this tenant
            total_groups_query = "SELECT COUNT(*) as count FROM groups WHERE tenant_id = ?"
            total_groups_result = query(total_groups_query, (tenant_id,))

            total_members_query = "SELECT COUNT(*) as count FROM user_groupsV2 WHERE tenant_id = ?"
            total_members_result = query(total_members_query, (tenant_id,))

            active_members_query = "SELECT COUNT(*) as count FROM user_groupsV2 WHERE tenant_id = ? AND is_active = 1"
            active_members_result = query(active_members_query, (tenant_id,))

            security_groups_query = "SELECT COUNT(*) as count FROM groups WHERE tenant_id = ? AND security_enabled = 1"
            security_groups_result = query(security_groups_query, (tenant_id,))

            mail_enabled_groups_query = "SELECT COUNT(*) as count FROM groups WHERE tenant_id = ? AND mail_enabled = 1"
            mail_enabled_groups_result = query(mail_enabled_groups_query, (tenant_id,))

            # Calculate metrics
            total_groups = total_groups_result[0]["count"] if total_groups_result else 0
            total_members = total_members_result[0]["count"] if total_members_result else 0
            active_members = active_members_result[0]["count"] if active_members_result else 0
            security_groups = security_groups_result[0]["count"] if security_groups_result else 0
            mail_enabled_groups = mail_enabled_groups_result[0]["count"] if mail_enabled_groups_result else 0

            # Generate optimization actions
            actions = []
            if total_members > 0 and active_members < total_members:
                inactive_count = total_members - active_members
                actions.append(f"Review {inactive_count} inactive group memberships")

            if security_groups > 0:
                actions.append(f"Monitor {security_groups} security groups")

            if mail_enabled_groups > 0:
                actions.append(f"Review {mail_enabled_groups} mail-enabled groups")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_groups": total_groups,
                "total_members": total_members,
                "active_members": active_members,
                "security_groups": security_groups,
                "mail_enabled_groups": mail_enabled_groups,
                "actions": actions,
            }

            logging.info(f"✓ {tenant_name}: {total_groups} groups, {active_members}/{total_members} active members")
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Groups analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Groups analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_groups_all = sum(r.get("total_groups", 0) for r in results if r["status"] == "completed")
    total_members_all = sum(r.get("total_members", 0) for r in results if r["status"] == "completed")
    total_security_groups_all = sum(r.get("security_groups", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_groups_all} groups, {total_members_all} members, {total_security_groups_all} security groups"
    )
