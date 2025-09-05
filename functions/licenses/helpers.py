from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import UTC, datetime, timedelta
import logging
import os

from db.db_client import execute_query, get_connection, init_schema, query, upsert_many
from shared.graph_beta_client import GraphBetaClient
from shared.graph_client import GraphClient
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def estimate_license_cost(sku_part_number: str) -> float:
    """Estimate monthly cost for common Microsoft license SKUs"""
    sku_costs = {
        "ENTERPRISEPACK": 22.00,  # E3
        "ENTERPRISEPREMIUM": 35.00,  # E5
        "EXCHANGESTANDARD": 4.00,  # Exchange Online Plan 1
        "EXCHANGEENTERPRISE": 8.00,  # Exchange Online Plan 2
        "SPB": 12.50,  # Microsoft 365 Business Standard
        "SMB_BUSINESS_ESSENTIALS": 6.00,  # Business Basic
        "SMB_BUSINESS_PREMIUM": 22.00,  # Business Premium
        "STANDARDWOFFPACK": 12.50,  # E1
        "POWER_BI_PRO": 10.00,
        "EMS": 10.60,  # Enterprise Mobility + Security E3
        "EMSPREMIUM": 16.40,  # Enterprise Mobility + Security E5
        "VISIOCLIENT": 15.00,
        "PROJECTPREMIUM": 55.00,
        "TEAMS_EXPLORATORY": 0.00,
        "FLOW_FREE": 0.00,
        "WINDOWS_STORE": 7.00,
        "DEVELOPERPACK": 19.00,
        "STREAM": 3.00,
    }

    if not sku_part_number:
        return 15.00

    sku_upper = sku_part_number.upper()
    for sku_pattern, cost in sku_costs.items():
        if sku_pattern in sku_upper:
            return cost
    return 15.00  # Default estimate


def get_sku_display_name(sku_part_number: str, license_id: str) -> str:
    """Get friendly display name for SKU using CSV data"""
    try:
        csv_path = os.path.join(os.path.dirname(__file__), "../data/product-plan-names.csv")
        if not os.path.exists(csv_path):
            return sku_part_number if sku_part_number else "Unknown License"

        with open(csv_path, encoding="utf-8") as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for row in csv_reader:
                if row.get("String_Id", "").upper() == sku_part_number.upper() and row.get("GUID", "").lower() == license_id.lower():
                    return row.get("Product_Display_Name", sku_part_number)

            csvfile.seek(0)
            next(csv_reader)

            for row in csv_reader:
                if row.get("String_Id", "").upper() == sku_part_number.upper():
                    return row.get("Product_Display_Name", sku_part_number)

        return sku_part_number if sku_part_number else "Unknown License"
    except Exception as e:
        logger.warning(f"Error reading CSV file for SKU lookup: {str(e)}")
        return sku_part_number if sku_part_number else "Unknown License"


def detect_tenant_capabilities(tenant_id):
    """Detect if tenant has premium capabilities by testing signin activity access"""
    try:
        graph = GraphBetaClient(tenant_id)
        test_user = graph.get("/users", select=["id", "userPrincipalName"], top=1)

        if test_user:
            try:
                user_id = test_user[0]["id"]
                graph.get(f"/users/{user_id}/signInActivity")
                logger.info(f"Tenant {tenant_id} is premium - using beta endpoint")
                return True
            except Exception:
                logger.info(f"Tenant {tenant_id} is not premium - using v1.0 endpoint")
                return False
        else:
            logger.warning(f"No users found in tenant {tenant_id} for capability testing")
            return False
    except Exception as e:
        logger.warning(f"Could not determine tenant capability for {tenant_id}: {str(e)}")
        return False


def fetch_tenant_licenses(tenant_id, use_beta=True):
    """Fetch tenant-level license information"""
    try:
        logger.info(f"Fetching tenant licenses for {tenant_id}")

        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        return graph.get("/subscribedSkus")
    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch licenses")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def fetch_users_with_licenses(tenant_id, use_beta=True):
    """Fetch users with license assignments"""
    try:
        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        return graph.get(
            "/users",
            select=["id", "userPrincipalName", "assignedLicenses", "displayName", "accountEnabled"],
            top=999,
        )
    except Exception as e:
        logger.warning(f"Failed to fetch users with licenses: {str(e)}")
        return []


def fetch_user_license_details_batch(tenant_id, user, use_beta=True):
    """Fetch detailed license information for a user (for concurrent processing)"""
    user_id = user.get("id")
    try:
        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        license_details = graph.get(
            f"/users/{user_id}/licenseDetails",
            select=["skuId", "skuPartNumber", "servicePlans"],
        )

        return user_id, license_details or []
    except Exception as e:
        logger.warning(f"Failed to fetch license details for user {user_id}: {str(e)}")
        return user_id, []


def fetch_tenant_subscriptions(tenant_id, use_beta=True):
    """Fetch tenant-level subscription information"""
    try:
        logger.info(f"Fetching tenant subscriptions for {tenant_id}")

        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        return graph.get(
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
    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch subscriptions")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def sync_licenses_v2(tenant_id, tenant_name):
    """Sync both tenant licenses and user license assignments"""
    init_schema()

    try:
        logger.info(f"Starting license sync for {tenant_name}")

        # Detect tenant capabilities
        is_premium = detect_tenant_capabilities(tenant_id)

        # Fetch tenant licenses
        tenant_licenses = fetch_tenant_licenses(tenant_id, is_premium)

        # Create lookup dictionary for tenant licenses
        license_lookup = {}
        if tenant_licenses:
            license_records = []
            for lic in tenant_licenses:
                prepaid_units = lic.get("prepaidUnits", {})
                total_units = prepaid_units.get("enabled", 0) + prepaid_units.get("lockedOut", 0)
                sku_part_number = lic.get("skuPartNumber", "")

                license_data = {
                    "tenant_id": tenant_id,
                    "license_id": lic.get("skuId"),
                    "license_display_name": get_sku_display_name(sku_part_number, lic.get("skuId")),
                    "license_partnumber": sku_part_number,
                    "status": "active" if lic.get("capabilityStatus") == "Enabled" else "inactive",
                    "total_count": total_units,
                    "consumed_count": lic.get("consumedUnits", 0),
                    "warning_count": prepaid_units.get("warning", 0),
                    "suspended_count": prepaid_units.get("suspended", 0),
                    "monthly_cost": estimate_license_cost(sku_part_number),
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                }
                license_records.append(license_data)
                license_lookup[lic.get("skuId")] = license_data

            if license_records:
                upsert_many("licenses", license_records)
                logger.info(f"Stored {len(license_records)} tenant licenses")

        # Fetch users with licenses
        all_users = fetch_users_with_licenses(tenant_id, is_premium)

        user_license_records = []
        users_with_licenses = 0

        # Filter users with licenses for concurrent processing
        users_with_licenses_list = [user for user in all_users if user.get("assignedLicenses")]
        users_with_licenses = len(users_with_licenses_list)

        if users_with_licenses_list:
            logger.info(f"Starting concurrent processing of {users_with_licenses} users with licenses...")

            # Use ThreadPoolExecutor to fetch license details concurrently
            with ThreadPoolExecutor(max_workers=15) as executor:
                future_to_user = {
                    executor.submit(fetch_user_license_details_batch, tenant_id, user, is_premium): user
                    for user in users_with_licenses_list
                }

                processed_count = 0
                license_details_lookup = {}

                for future in as_completed(future_to_user):
                    user = future_to_user[future]
                    try:
                        user_id, detailed_licenses = future.result()
                        license_details_lookup[user_id] = {lic["skuId"]: lic for lic in detailed_licenses}
                    except Exception as e:
                        logger.warning(f"Failed to process license details for user {user.get('id', 'unknown')}: {str(e)}")
                        continue

                    processed_count += 1
                    if processed_count % 50 == 0 or processed_count == users_with_licenses:
                        logger.info(f"Processed {processed_count}/{users_with_licenses} users with licenses...")

            # Now process all users with their license details
            for user in users_with_licenses_list:
                user_id = user.get("id")
                upn = user.get("userPrincipalName")
                assigned_licenses = user.get("assignedLicenses", [])
                user_account_enabled = user.get("accountEnabled", True)
                license_detail_lookup = license_details_lookup.get(user_id, {})

                for assigned_license in assigned_licenses:
                    sku_id = assigned_license.get("skuId")
                    license_info = license_lookup.get(sku_id, {})
                    user_license_detail = license_detail_lookup.get(sku_id, {})

                    sku_part_number = user_license_detail.get("skuPartNumber") or license_info.get("license_partnumber") or "UNKNOWN"

                    # Determine license activity status
                    is_license_active = 1
                    if not user_account_enabled:
                        is_license_active = 0
                    else:
                        # Check user activity from database
                        user_activity = query(
                            "SELECT last_sign_in_date FROM usersV2 WHERE user_id = ? AND tenant_id = ?",
                            (user_id, tenant_id),
                        )

                        if user_activity:
                            last_sign_in = user_activity[0].get("last_sign_in_date")
                            if last_sign_in:
                                try:
                                    last_signin_date = datetime.fromisoformat(last_sign_in)
                                    cutoff_date = datetime.now(UTC) - timedelta(days=90)
                                    if last_signin_date < cutoff_date:
                                        is_license_active = 0
                                except Exception:
                                    pass
                            else:
                                is_license_active = 0

                    user_license_record = {
                        "tenant_id": tenant_id,
                        "user_id": user_id,
                        "license_id": sku_id,
                        "user_principal_name": upn,
                        "is_active": is_license_active,
                        "unassigned_date": None,
                        "license_display_name": get_sku_display_name(sku_part_number, sku_id),
                        "license_partnumber": sku_part_number,
                        "monthly_cost": estimate_license_cost(sku_part_number),
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                    }
                    user_license_records.append(user_license_record)

        # Store user licenses using DELETE + INSERT approach
        if user_license_records:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_licensesV2 WHERE tenant_id = ?", (tenant_id,))
            conn.commit()
            conn.close()

            upsert_many("user_licensesV2", user_license_records)
            logger.info(f"Replaced and stored {len(user_license_records)} user license assignments")

        # Update inactive licenses for disabled users
        existing_license_users = query(
            "SELECT DISTINCT user_id, user_principal_name FROM user_licensesV2 WHERE tenant_id = ?",
            (tenant_id,),
        )

        current_user_ids = {user.get("id") for user in all_users if user.get("assignedLicenses")}
        users_to_check = [u["user_id"] for u in existing_license_users if u["user_id"] not in current_user_ids]

        if users_to_check:
            user_status_query = f"""
                SELECT user_id, account_enabled, user_principal_name
                FROM usersV2
                WHERE user_id IN ({",".join(["?" for _ in users_to_check])}) AND tenant_id = ?
            """
            user_statuses = query(user_status_query, users_to_check + [tenant_id])

            for user_status in user_statuses:
                if not user_status["account_enabled"]:
                    execute_query(
                        """UPDATE user_licensesV2
                           SET is_active = 0, unassigned_date = ?, last_updated = ?
                           WHERE user_id = ? AND tenant_id = ? AND is_active = 1""",
                        (datetime.now().isoformat(), datetime.now().isoformat(), user_status["user_id"], tenant_id),
                    )

        return {
            "status": "success",
            "licenses_synced": len(license_records) if "license_records" in locals() else 0,
            "user_licenses_replaced": len(user_license_records),
            "users_with_licenses_processed": users_with_licenses,
            "inactive_licenses_updated": len(users_to_check) if users_to_check else 0,
        }

    except Exception as e:
        error_msg = clean_error_message(str(e), tenant_name=tenant_name)
        logger.error(error_msg)
        logger.debug(f"Full error details for {tenant_name}: {str(e)}", exc_info=True)
        return {"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)}


def sync_subscriptions(tenant_id, tenant_name):
    """Sync tenant subscriptions with optimized processing"""
    init_schema()

    try:
        logger.info(f"Starting subscription sync for {tenant_name}")
        start_time = datetime.now()

        # Detect tenant capabilities
        is_premium = detect_tenant_capabilities(tenant_id)

        # Fetch tenant subscriptions
        tenant_subscriptions = fetch_tenant_subscriptions(tenant_id, is_premium)

        if not tenant_subscriptions:
            logger.warning(f"No subscriptions found for tenant {tenant_id}")
            return {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "subscriptions_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        logger.info(f"Processing {len(tenant_subscriptions)} subscriptions...")

        # Process subscriptions with better error handling
        subscription_records = []
        processed_count = 0
        failed_count = 0

        for subscription in tenant_subscriptions:
            try:
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
                processed_count += 1

                # Log progress every 100 subscriptions
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count}/{len(tenant_subscriptions)} subscriptions...")

            except Exception as e:
                failed_count += 1
                logger.warning(f"Failed to process subscription {subscription.get('id', 'unknown')}: {str(e)}")
                continue

        # Store subscriptions using DELETE + INSERT approach for better performance
        if subscription_records:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM subscriptions WHERE tenant_id = ?", (tenant_id,))
            conn.commit()
            conn.close()

            upsert_many("subscriptions", subscription_records)
            logger.info(f"Stored {len(subscription_records)} subscriptions")

        # Count total subscriptions after sync
        total_subscriptions = query("SELECT COUNT(*) as total FROM subscriptions WHERE tenant_id = ?", (tenant_id,))[0]["total"]

        duration = (datetime.now() - start_time).total_seconds()

        # Log final summary
        logger.info(f"=== SUBSCRIPTION SYNC SUMMARY FOR {tenant_name} ===")
        logger.info(f"Subscriptions processed: {processed_count}")
        logger.info(f"Subscriptions failed: {failed_count}")
        logger.info(f"Subscriptions stored: {len(subscription_records)}")
        logger.info(f"Total subscriptions in database: {total_subscriptions}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 50)

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "subscriptions_synced": total_subscriptions,
            "subscriptions_processed": processed_count,
            "subscriptions_failed": failed_count,
            "duration_seconds": duration,
        }

    except Exception as e:
        error_msg = clean_error_message(str(e), tenant_name=tenant_name)
        logger.error(error_msg)
        logger.debug(f"Full error details for {tenant_name}: {str(e)}", exc_info=True)
        return {"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)}
