import csv
from datetime import UTC, datetime, timedelta
import logging
import os

from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient
from sql.databaseV2 import execute_query, init_schema, query, upsert_many
from utils.http import clean_error_message


logger = logging.getLogger(__name__)


def fetch_beta_tenant_licenses(tenant_id):
    """Fetch tenant-level license information"""
    try:
        logger.info(f"Fetching tenant licenses for {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        # Test with no top parameter to see if that's the issue
        licenses = graph.get("/subscribedSkus")

        return licenses

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch licenses")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)

        # Log the detailed error for debugging
        if hasattr(e, "response") and hasattr(e.response, "text"):
            logger.error(f"Response body: {e.response.text}")
        # Return empty list but continue processing
        return []


def fetch_v1_tenant_licenses(tenant_id):
    """Fetch tenant-level license information from v1.0 endpoint"""
    try:
        logger.info(f"Fetching tenant licenses for {tenant_id} using v1.0 endpoint")
        graph = GraphClient(tenant_id)

        # v1.0 endpoint for subscribed SKUs
        licenses = graph.get("/subscribedSkus")

        return licenses

    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch licenses from v1.0 endpoint")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def fetch_beta_user_license_details(tenant_id, user_id):
    """Fetch detailed license information for a specific user"""
    try:
        graph = GraphBetaClient(tenant_id)
        license_details = graph.get(
            f"/users/{user_id}/licenseDetails",
            select=["skuId", "skuPartNumber", "servicePlans"],
        )
        return license_details
    except Exception as e:
        logger.warning(f"Failed to fetch license details for user {user_id}: {str(e)}")
        return []


def fetch_v1_user_license_details(tenant_id, user_id):
    """Fetch detailed license information for a specific user from v1.0 endpoint"""
    try:
        graph = GraphClient(tenant_id)
        license_details = graph.get(
            f"/users/{user_id}/licenseDetails",
            select=["skuId", "skuPartNumber", "servicePlans"],
        )
        return license_details
    except Exception as e:
        logger.warning(f"Failed to fetch license details for user {user_id} from v1.0 endpoint: {str(e)}")
        return []


def fetch_beta_users_with_licenses(tenant_id):
    """Fetch users with license assignments from beta endpoint"""
    try:
        graph = GraphBetaClient(tenant_id)
        all_users = graph.get(
            "/users",
            select=[
                "id",
                "userPrincipalName",
                "assignedLicenses",
                "displayName",
                "accountEnabled",
            ],
            top=999,
        )
        return all_users
    except Exception as e:
        logger.warning(f"Failed to fetch users with licenses from beta endpoint: {str(e)}")
        return []


def fetch_v1_users_with_licenses(tenant_id):
    """Fetch users with license assignments from v1.0 endpoint"""
    try:
        graph = GraphClient(tenant_id)
        all_users = graph.get(
            "/users",
            select=[
                "id",
                "userPrincipalName",
                "assignedLicenses",
                "displayName",
                "accountEnabled",
            ],
            top=999,
        )
        return all_users
    except Exception as e:
        logger.warning(f"Failed to fetch users with licenses from v1.0 endpoint: {str(e)}")
        return []


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
    """Get friendly display name for SKU using CSV data based on SKU part number and license ID"""
    try:
        # Path to the CSV file
        csv_path = os.path.join(os.path.dirname(__file__), "..", "sql", "data", "product-plan-names.csv")

        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found at {csv_path}, falling back to SKU part number")
            return sku_part_number if sku_part_number else "Unknown License"

        # Read CSV and look for matching SKU part number and license ID
        with open(csv_path, encoding="utf-8") as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for row in csv_reader:
                # Check if both String_Id (SKU part number) and GUID (license ID) match
                if row.get("String_Id", "").upper() == sku_part_number.upper() and row.get("GUID", "").lower() == license_id.lower():
                    return row.get("Product_Display_Name", sku_part_number)

            # If no exact match found, try to find by SKU part number only
            csvfile.seek(0)  # Reset file pointer
            next(csv_reader)  # Skip header row

            for row in csv_reader:
                if row.get("String_Id", "").upper() == sku_part_number.upper():
                    return row.get("Product_Display_Name", sku_part_number)

        # If no match found in CSV, return the SKU part number
        return sku_part_number if sku_part_number else "Unknown License"

    except Exception as e:
        logger.warning(f"Error reading CSV file for SKU lookup: {str(e)}, falling back to SKU part number")
        return sku_part_number if sku_part_number else "Unknown License"


def sync_licenses_v2(tenant_id, tenant_name):
    """Sync both tenant licenses and user license assignments"""

    # Initialize database schema
    init_schema()

    try:
        logger.info(f"Starting license sync for {tenant_name}")

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

        # Now fetch tenant licenses using the appropriate endpoint based on tenant capability
        if is_premium:
            # Premium tenant - use beta endpoint for advanced features
            tenant_licenses = fetch_beta_tenant_licenses(tenant_id)
        else:
            # Non-premium tenant - use v1.0 endpoint for basic features
            tenant_licenses = fetch_v1_tenant_licenses(tenant_id)

        # Create lookup dictionary for tenant licenses
        license_lookup = {}
        if tenant_licenses:
            # Transform and store tenant licenses
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

        # Fetch ALL users with licenses using the appropriate endpoint based on tenant capability
        logger.info(f"Fetching user license assignments for {tenant_id}")

        if is_premium:
            # Premium tenant - use beta endpoint for advanced features
            all_users = fetch_beta_users_with_licenses(tenant_id)
        else:
            # Non-premium tenant - use v1.0 endpoint for basic features
            all_users = fetch_v1_users_with_licenses(tenant_id)

        user_license_records = []
        users_with_licenses = 0

        for user in all_users:
            user_id = user.get("id")
            upn = user.get("userPrincipalName")
            assigned_licenses = user.get("assignedLicenses", [])
            # is_active should be per-license, not per-user
            user_account_enabled = user.get("accountEnabled", True)

            if assigned_licenses:
                users_with_licenses += 1

                # Get detailed license info for this user using the appropriate endpoint
                if is_premium:
                    detailed_licenses = fetch_beta_user_license_details(tenant_id, user_id)
                else:
                    detailed_licenses = fetch_v1_user_license_details(tenant_id, user_id)
                license_detail_lookup = {lic["skuId"]: lic for lic in detailed_licenses}

                for assigned_license in assigned_licenses:
                    sku_id = assigned_license.get("skuId")

                    # Get details from tenant license lookup or user's detailed licenses
                    license_info = license_lookup.get(sku_id, {})
                    user_license_detail = license_detail_lookup.get(sku_id, {})

                    # Use SKU part number from user details if available
                    sku_part_number = user_license_detail.get("skuPartNumber") or license_info.get("license_partnumber") or "UNKNOWN"

                    # Determine if license should be considered active
                    # Consider inactive if:
                    # 1. Account is disabled, OR
                    # 2. Account is enabled but user hasn't signed in for 90+ days
                    is_license_active = 1

                    if not user_account_enabled:
                        # Account is disabled
                        is_license_active = 0
                    else:
                        # Account is enabled, check activity
                        # Get user's last sign-in from users table
                        user_activity = query(
                            """
                            SELECT last_sign_in_date 
                            FROM usersV2 
                            WHERE user_id = ? AND tenant_id = ?
                        """,
                            (user_id, tenant_id),
                        )

                        if user_activity:
                            last_sign_in = user_activity[0].get("last_sign_in_date")
                            if last_sign_in:
                                try:
                                    last_signin_date = datetime.fromisoformat(last_sign_in)
                                    cutoff_date = datetime.now(UTC) - timedelta(days=90)

                                    if last_signin_date < cutoff_date:
                                        # User hasn't signed in for 90+ days - consider license inactive
                                        is_license_active = 0
                                except Exception:
                                    # If we can't parse the date, assume active
                                    pass
                            else:
                                # Never signed in - consider license inactive
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

        # Store user licenses using DELETE + INSERT approach (same as role/group sync)
        if user_license_records:
            # Clear existing user license records for this tenant first
            from sql.databaseV2 import get_connection

            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute("DELETE FROM user_licensesV2 WHERE tenant_id = ?", (tenant_id,))
            conn.commit()
            conn.close()

            # Insert fresh user license records
            upsert_many("user_licensesV2", user_license_records)
            logger.info(f"Replaced and stored {len(user_license_records)} user license assignments from {users_with_licenses} users")

        # Check for users who previously had licenses but no longer have assignments
        # This catches users who were disabled and had their licenses removed by Microsoft
        existing_license_users = query(
            """
            SELECT DISTINCT user_id, user_principal_name 
            FROM user_licensesV2 
            WHERE tenant_id = ?
        """,
            (tenant_id,),
        )

        current_user_ids = {user.get("id") for user in all_users if user.get("assignedLicenses")}

        # Find users who had licenses before but don't now (likely disabled)
        users_to_check = []
        for existing_user in existing_license_users:
            if existing_user["user_id"] not in current_user_ids:
                users_to_check.append(existing_user["user_id"])

        # For these users, check if they're now inactive and mark their licenses accordingly
        if users_to_check:
            user_status_query = f"""
                SELECT user_id, account_enabled, user_principal_name
                FROM usersV2 
                WHERE user_id IN ({",".join(["?" for _ in users_to_check])})
                AND tenant_id = ?
            """
            user_statuses = query(user_status_query, users_to_check + [tenant_id])

            for user_status in user_statuses:
                if not user_status["account_enabled"]:  # User is now inactive
                    # Update their existing license records to mark as inactive
                    execute_query(
                        """
                        UPDATE user_licensesV2 
                        SET is_active = 0, 
                            unassigned_date = ?,
                            last_updated = ?
                        WHERE user_id = ? AND tenant_id = ? AND is_active = 1
                    """,
                        (
                            datetime.now().isoformat(),
                            datetime.now().isoformat(),
                            user_status["user_id"],
                            tenant_id,
                        ),
                    )
                    logger.info(f"Marked licenses as inactive for disabled user: {user_status['user_principal_name']}")

        return {
            "status": "success",
            "licenses_synced": len(license_records) if "license_records" in locals() else 0,
            "user_licenses_replaced": len(user_license_records),
            "inactive_licenses_updated": len(users_to_check) if users_to_check else 0,
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
