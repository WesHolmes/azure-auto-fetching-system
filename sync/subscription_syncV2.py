from datetime import datetime
import logging

from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient
from sql.databaseV2 import init_schema, query, upsert_many
from utils.http import clean_error_message


logger = logging.getLogger(__name__)


def fetch_beta_tenant_subscriptions(tenant_id):
    """Fetch tenant-level subscription information"""
    try:
        logger.info(f"Fetching tenant subscriptions for {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        # Get all subscriptions with detailed information
        subscriptions = graph.get(
            "/directory/subscriptions",
            select=[
                "id",
                "commerceSubscriptionId",
                "skuId",
                "skuPartNumber",
                "status",
                "isTrial",
                "totalLicenses",
                "nextLifecycleDateTime",
            ],
        )

        logger.info(f"Found {len(subscriptions) if subscriptions else 0} subscriptions")
        return subscriptions

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch subscriptions")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)

        # Log the detailed error for debugging
        if hasattr(e, "response") and hasattr(e.response, "text"):
            logger.error(f"Response body: {e.response.text}")
        # Return empty list but continue processing
        return []


def fetch_v1_tenant_subscriptions(tenant_id):
    """Fetch tenant-level subscription information from v1.0 endpoint"""
    try:
        logger.info(f"Fetching tenant subscriptions for {tenant_id} using v1.0 endpoint")
        graph = GraphClient(tenant_id)

        # Get all subscriptions with basic information from v1.0 endpoint
        subscriptions = graph.get(
            "/directory/subscriptions",
            select=[
                "id",
                "commerceSubscriptionId",
                "skuId",
                "skuPartNumber",
                "status",
                "isTrial",
                "totalLicenses",
                "nextLifecycleDateTime",
            ],
        )

        logger.info(f"Found {len(subscriptions) if subscriptions else 0} subscriptions from v1.0 endpoint")
        return subscriptions

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch subscriptions from v1.0 endpoint")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)

        # Log the detailed error for debugging
        if hasattr(e, "response") and hasattr(e.response, "text"):
            logger.error(f"Response body: {e.response.text}")
        # Return empty list but continue processing
        return []


def sync_subscriptions(tenant_id, tenant_name):
    """Sync tenant subscriptions"""

    # Initialize database schema
    init_schema()

    try:
        logger.info(f"Starting subscription sync for {tenant_name}")

        # First, detect tenant capability by attempting to fetch signin activity
        # This determines if the tenant is premium and can access advanced features
        try:
            graph = GraphBetaClient(tenant_id)
            test_user = graph.get("/users", select=["id", "userPrincipalName"], top=1)

            if test_user:
                # Try to fetch signin activity for the first user to test premium capabilities
                try:
                    user_id = test_user[0]["id"]
                    signin_activity = graph.get(f"/users/{user_id}/signInActivity")
                    is_premium = True
                    logger.info(f"Tenant {tenant_id} is premium - using beta endpoint")
                except Exception:
                    # Signin activity not accessible, tenant is not premium
                    is_premium = False
                    logger.info(f"Tenant {tenant_id} is not premium - using v1.0 endpoint")
            else:
                logger.warning(f"No users found in tenant {tenant_id} for capability testing")
                is_premium = False

        except Exception as capability_error:
            logger.warning(f"Could not determine tenant capability for {tenant_id}: {str(capability_error)}")
            is_premium = False

        # Now fetch tenant subscriptions using the appropriate endpoint based on tenant capability
        if is_premium:
            # Premium tenant - use beta endpoint for advanced features
            tenant_subscriptions = fetch_beta_tenant_subscriptions(tenant_id)
        else:
            # Non-premium tenant - use v1.0 endpoint for basic features
            tenant_subscriptions = fetch_v1_tenant_subscriptions(tenant_id)

        if tenant_subscriptions:
            # Transform and store subscriptions
            subscription_records = []
            for subscription in tenant_subscriptions:
                subscription_data = {
                    "tenant_id": tenant_id,
                    "subscription_id": subscription.get("id"),
                    "commerce_subscription_id": subscription.get("commerceSubscriptionId"),
                    "sku_id": subscription.get("skuId"),
                    "sku_part_number": subscription.get("skuPartNumber"),
                    "is_active": 1 if subscription.get("status") == "Enabled" else 0,
                    "is_trial": 1 if subscription.get("isTrial", False) else 0,
                    "total_licenses": subscription.get("totalLicenses", 0),
                    "next_lifecycle_date_time": subscription.get("nextLifecycleDateTime"),
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                }
                subscription_records.append(subscription_data)

            if subscription_records:
                upsert_many("subscriptions", subscription_records)
                logger.info(f"Stored {len(subscription_records)} subscriptions")

        # Count total subscriptions after sync
        total_subscriptions = query(
            """
            SELECT COUNT(*) as total
            FROM subscriptions 
            WHERE tenant_id = ?
        """,
            (tenant_id,),
        )[0]["total"]

        # Log final summary
        logger.info(f"=== SUBSCRIPTION SYNC SUMMARY FOR {tenant_name} ===")
        logger.info(f"Subscriptions processed: {len(tenant_subscriptions) if tenant_subscriptions else 0}")
        logger.info(f"Subscriptions stored: {len(subscription_records) if 'subscription_records' in locals() else 0}")
        logger.info(f"Total subscriptions in database: {total_subscriptions}")
        logger.info("=" * 50)

        return {
            "status": "success",
            "subscriptions_synced": total_subscriptions,
        }

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), tenant_name=tenant_name)
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for {tenant_name}: {str(e)}", exc_info=True)

        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }


def get_tenant_subscriptions(tenant_id):
    """Get all subscriptions for a specific tenant"""
    try:
        subscriptions = query(
            """
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
        """,
            (tenant_id,),
        )
        return subscriptions
    except Exception as e:
        logger.error(f"Failed to get subscriptions for tenant {tenant_id}: {str(e)}")
        return []


def get_subscription_by_sku(tenant_id, sku_id):
    """Get a specific subscription by SKU ID"""
    try:
        subscription = query(
            """
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
            WHERE tenant_id = ? AND sku_id = ?
        """,
            (tenant_id, sku_id),
        )
        return subscription[0] if subscription else None
    except Exception as e:
        logger.error(f"Failed to get subscription {sku_id} for tenant {tenant_id}: {str(e)}")
        return None
