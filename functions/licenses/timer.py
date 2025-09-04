import logging

import azure.functions as func

from db.db_client import query
from shared.error_reporting import categorize_sync_errors
from shared.graph_client import get_tenants
from shared.utils import clean_error_message

from .helpers import sync_licenses_v2, sync_subscriptions


logger = logging.getLogger(__name__)


# TIMER FUNCTIONS
def timer_licenses_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for license sync across all tenants"""
    if timer.past_due:
        logging.warning("License sync V2 timer is past due!")

    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_licenses_v2(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(f" V2 {tenant['display_name']}: {result['licenses_synced']} licenses synced")
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "licenses_synced": result["licenses_synced"],
                        "user_licenses_synced": result.get("user_licenses_replaced", 0),
                        "inactive_licenses_updated": result.get("inactive_licenses_updated", 0),
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
        categorize_sync_errors(results, "License V2")


def timer_subscriptions_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for subscription sync across all tenants"""
    if timer.past_due:
        logging.info("Subscription sync V2 timer is past due!")

    logging.info("Starting scheduled subscription sync V2")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_subscriptions(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(f" V2 {tenant['display_name']}: {result['subscriptions_synced']} subscriptions synced")
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "subscriptions_synced": result["subscriptions_synced"],
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
        categorize_sync_errors(results, "Subscription V2")


def get_licenses_analysis(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for licenses analysis across all tenants"""
    if timer.past_due:
        logging.warning("Licenses analysis timer is past due!")

    logging.info("Starting scheduled licenses analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing licenses for tenant: {tenant_name}")

            # Query license data for this tenant
            total_licenses_query = "SELECT COUNT(DISTINCT license_display_name) as count FROM licenses WHERE tenant_id = ?"
            total_licenses_result = query(total_licenses_query, (tenant_id,))

            total_assignments_query = "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ?"
            total_assignments_result = query(total_assignments_query, (tenant_id,))

            active_assignments_query = "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 1"
            active_assignments_result = query(active_assignments_query, (tenant_id,))

            total_cost_query = "SELECT SUM(monthly_cost) as total_cost FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 1"
            total_cost_result = query(total_cost_query, (tenant_id,))

            # Calculate metrics
            total_licenses = total_licenses_result[0]["count"] if total_licenses_result else 0
            total_assignments = total_assignments_result[0]["count"] if total_assignments_result else 0
            active_assignments = active_assignments_result[0]["count"] if active_assignments_result else 0
            total_cost = total_cost_result[0]["total_cost"] if total_cost_result and total_cost_result[0]["total_cost"] else 0

            # Generate optimization actions
            actions = []
            if total_assignments > 0 and active_assignments < total_assignments:
                inactive_count = total_assignments - active_assignments
                actions.append(f"Review {inactive_count} inactive license assignments")

            if total_cost > 0:
                actions.append(f"Monthly cost: ${total_cost:.2f}")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_licenses": total_licenses,
                "total_assignments": total_assignments,
                "active_assignments": active_assignments,
                "total_monthly_cost": total_cost,
                "actions": actions,
            }

            logging.info(f"✓ {tenant_name}: {total_licenses} licenses, {active_assignments}/{total_assignments} active assignments")
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    successful_count = len([r for r in results if r["status"] == "completed"])
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Licenses analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Licenses analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_licenses_all = sum(r.get("total_licenses", 0) for r in results if r["status"] == "completed")
    total_assignments_all = sum(r.get("total_assignments", 0) for r in results if r["status"] == "completed")
    total_cost_all = sum(r.get("total_monthly_cost", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_licenses_all} licenses, {total_assignments_all} assignments, ${total_cost_all:.2f} monthly cost"
    )
