from datetime import datetime
import logging

from core.databaseV2 import init_schema, query, upsert_many
from core.graph_beta_client import GraphBetaClient


logger = logging.getLogger(__name__)


def fetch_tenant_subscriptions(tenant_id):
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
        # Clean up error message for better console readability
        if "401 Unauthorized" in str(e):
            error_msg = "✗ Failed to fetch subscriptions: Authentication failed (401 Unauthorized)"
        elif "403 Forbidden" in str(e):
            error_msg = "✗ Failed to fetch subscriptions: Access denied (403 Forbidden)"
        elif "404 Not Found" in str(e):
            error_msg = "✗ Failed to fetch subscriptions: Resource not found (404)"
        elif "500 Internal Server Error" in str(e):
            error_msg = "✗ Failed to fetch subscriptions: Server error (500)"
        else:
            error_msg = f"✗ Failed to fetch subscriptions: {str(e)}"

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

        # Fetch tenant subscriptions
        tenant_subscriptions = fetch_tenant_subscriptions(tenant_id)

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
        # Clean up error message for better console readability
        if "401 Unauthorized" in str(e):
            error_msg = f"✗ {tenant_name}: Authentication failed (401 Unauthorized)"
        elif "403 Forbidden" in str(e):
            error_msg = f"✗ {tenant_name}: Access denied (403 Forbidden)"
        elif "404 Not Found" in str(e):
            error_msg = f"✗ {tenant_name}: Resource not found (404)"
        elif "500 Internal Server Error" in str(e):
            error_msg = f"✗ {tenant_name}: Server error (500)"
        else:
            error_msg = f"✗ {tenant_name}: {str(e)}"

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
