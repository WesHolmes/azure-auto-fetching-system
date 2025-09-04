import logging

import azure.functions as func

from db.db_client import query
from shared.error_reporting import categorize_sync_errors
from shared.graph_beta_client import GraphBetaClient
from shared.graph_client import get_tenants
from shared.utils import clean_error_message, create_error_response, create_success_response

from .helpers import sync_licenses_v2, sync_subscriptions


def get_licenses(req: func.HttpRequest) -> func.HttpResponse:
    """Get licenses for a specific tenant"""
    try:
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        licenses_query = """
            SELECT l.*,
                   COUNT(DISTINCT ul.user_id) as assigned_count,
                   SUM(CASE WHEN ul.is_active = 1 THEN 1 ELSE 0 END) as active_assignments
            FROM licenses l
            LEFT JOIN user_licensesV2 ul ON l.tenant_id = ul.tenant_id AND l.license_display_name = ul.license_display_name
            WHERE l.tenant_id = ?
            GROUP BY l.license_display_name, l.tenant_id
            ORDER BY l.license_display_name
        """

        licenses = query(licenses_query, (tenant_id,))

        return create_success_response(
            data={"licenses": licenses, "count": len(licenses)},
            tenant_id=tenant_id,
            operation="get_licenses",
            message=f"Retrieved {len(licenses)} licenses",
        )

    except Exception as e:
        logging.error(f"Error retrieving licenses for tenant {tenant_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve licenses: {str(e)}", 500)


def get_subscriptions(req: func.HttpRequest) -> func.HttpResponse:
    """Get subscriptions for a specific tenant"""
    try:
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        subscriptions_query = """
            SELECT * FROM subscriptions
            WHERE tenant_id = ?
            ORDER BY subscription_display_name
        """

        subscriptions = query(subscriptions_query, (tenant_id,))

        return create_success_response(
            data={"subscriptions": subscriptions, "count": len(subscriptions)},
            tenant_id=tenant_id,
            operation="get_subscriptions",
            message=f"Retrieved {len(subscriptions)} subscriptions",
        )

    except Exception as e:
        logging.error(f"Error retrieving subscriptions for tenant {tenant_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve subscriptions: {str(e)}", 500)


def get_tenant_subscription_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant subscription data"""
    # Returns structured response with subscription optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"Subscriptions API request for tenant: {tenant_id}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # single Graph API call - much faster
        graph_client = GraphBetaClient(tenant_id)
        tenant_details = graph_client.get_tenant_details(tenant_id)

        # handle fact get_tenant_details returns a list
        if tenant_details and len(tenant_details) > 0:
            tenant_name = tenant_details[0].get("displayName", tenant_id)
        else:
            tenant_name = tenant_id

        logging.info(f"Processing subscription data for tenant: {tenant_name}")

        # grab subscription data
        # basic subscription counts
        total_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ?"
        total_subscriptions_result = query(total_subscriptions_query, (tenant_id,))

        active_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ? AND is_active = 1"
        active_subscriptions_result = query(active_subscriptions_query, (tenant_id,))

        # trial subscriptions count
        trial_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ? AND is_trial = 1"
        trial_subscriptions_result = query(trial_subscriptions_query, (tenant_id,))

        # expiring soon subscriptions (within 30 days)
        expiring_soon_query = """
        SELECT COUNT(*) as count FROM subscriptions
        WHERE tenant_id = ? AND next_lifecycle_date_time IS NOT NULL
        AND date(next_lifecycle_date_time) <= date('now', '+30 days')
        """
        expiring_soon_result = query(expiring_soon_query, (tenant_id,))

        # calculate metrics
        total_subscriptions = total_subscriptions_result[0]["count"] if total_subscriptions_result else 0
        active_subscriptions = active_subscriptions_result[0]["count"] if active_subscriptions_result else 0
        trial_subscriptions = trial_subscriptions_result[0]["count"] if trial_subscriptions_result else 0
        expiring_soon = expiring_soon_result[0]["count"] if expiring_soon_result else 0
        inactive_subscriptions = total_subscriptions - active_subscriptions

        # fetch actual subscription data for the data field
        subscriptions_query = """
            SELECT
                subscription_id,
                commerce_subscription_id,
                sku_id,
                sku_part_number,
                is_active,
                is_trial,
                total_licenses,
                next_lifecycle_date_time,
                created_at,
                last_updated
            FROM subscriptions
            WHERE tenant_id = ?
            ORDER BY sku_part_number
        """
        subscriptions_result = query(subscriptions_query, (tenant_id,))

        # transform subscription data for frontend consumption
        subscriptions_data = []
        for subscription in subscriptions_result:
            subscriptions_data.append(
                {
                    "subscription_id": subscription["subscription_id"],
                    "commerce_subscription_id": subscription["commerce_subscription_id"],
                    "sku_id": subscription["sku_id"],
                    "sku_part_number": subscription["sku_part_number"],
                    "is_active": bool(subscription["is_active"]),
                    "is_trial": bool(subscription["is_trial"]),
                    "total_licenses": subscription["total_licenses"],
                    "next_lifecycle_date_time": subscription["next_lifecycle_date_time"],
                    "created_at": subscription["created_at"],
                    "last_updated": subscription["last_updated"],
                }
            )

        # generate subscription optimization actions
        actions = []

        # action 1: trial subscriptions
        if trial_subscriptions > 0:
            actions.append(
                {
                    "title": "Review Trial Subscriptions",
                    "description": f"{trial_subscriptions} trial subscriptions - evaluate before expiration",
                    "action": "review",
                }
            )

        # action 2: expiring subscriptions
        if expiring_soon > 0:
            actions.append(
                {
                    "title": "Renew Expiring Subscriptions",
                    "description": f"{expiring_soon} subscriptions expiring within 30 days",
                    "action": "renew",
                }
            )

        # action 3: inactive subscriptions
        if inactive_subscriptions > 0:
            actions.append(
                {
                    "title": "Review Inactive Subscriptions",
                    "description": f"{inactive_subscriptions} inactive subscriptions - consider cancellation",
                    "action": "review",
                }
            )

        # action 4: license optimization
        total_licenses = sum(sub.get("total_licenses", 0) for sub in subscriptions_data)
        if total_licenses > 0:
            # Check if there are unused licenses (this would require user_licensesV2 data)
            actions.append(
                {
                    "title": "Optimize License Usage",
                    "description": f"{total_licenses} total licenses - review utilization",
                    "action": "optimize",
                }
            )

        # build response structure using utility function
        return create_success_response(
            data=subscriptions_data,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="get_tenant_subscription_by_id",
            metrics={
                "total_subscriptions": total_subscriptions,
                "active_subscriptions": active_subscriptions,
                "inactive_subscriptions": inactive_subscriptions,
                "trial_subscriptions": trial_subscriptions,
                "expiring_soon": expiring_soon,
                "total_licenses": total_licenses,
            },
            actions=actions,
        )

    except Exception as e:
        error_msg = f"Error retrieving subscription data: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


# HTTP FUNCTIONS
def http_licenses_sync(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for manual license sync"""
    try:
        logging.info("Starting manual license sync")
        tenants = get_tenants()
        total_licenses = 0
        total_assignments = 0
        results = []

        for tenant in tenants:
            try:
                result = sync_licenses_v2(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(
                        f" {tenant['display_name']}: {result['licenses_synced']} licenses, {result.get('user_licenses_replaced', 0)} user assignments replaced"
                    )
                    total_licenses += result["licenses_synced"]
                    total_assignments += result["user_licenses_replaced"]
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "licenses_synced": result["licenses_synced"],
                            "user_licenses_synced": result["user_licenses_replaced"],
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
            categorize_sync_errors(results, "License HTTP")

        return create_success_response(
            data={"total_licenses": total_licenses, "total_assignments": total_assignments, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="license_sync_http",
            message=f"Synced {total_licenses} licenses and {total_assignments} user assignments across {len(tenants)} tenants",
        )
    except Exception as e:
        error_msg = f"License sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


def http_subscription_sync(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for manual subscription sync"""
    try:
        logging.info("Starting manual subscription sync")
        tenants = get_tenants()
        results = []

        for tenant in tenants:
            try:
                result = sync_subscriptions(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(f" {tenant['display_name']}: {result['subscriptions_synced']} subscriptions synced")
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "subscriptions_synced": result["subscriptions_synced"],
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
            categorize_sync_errors(results, "Subscriptions HTTP")

        total_subscriptions = sum(r.get("subscriptions_synced", 0) for r in results if r["status"] == "completed")

        return create_success_response(
            data={"total_subscriptions": total_subscriptions, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="subscriptions_sync_http",
            message=f"Synced {total_subscriptions} subscriptions across {len(tenants)} tenants",
        )
    except Exception as e:
        error_msg = f"Subscription sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)
