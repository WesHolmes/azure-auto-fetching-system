from datetime import UTC, datetime, timedelta
import logging

from core.databaseV2 import execute_query, init_schema, query, upsert_many
from core.graph_beta_client import GraphBetaClient


logger = logging.getLogger(__name__)


def fetch_tenant_licenses(tenant_id):
    """Fetch tenant-level license information"""
    try:
        logger.info(f"Fetching tenant licenses for {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        # Test with no top parameter to see if that's the issue
        licenses = graph.get("/subscribedSkus")

        return licenses

    except Exception as e:
        logger.error(f"Failed to fetch tenant licenses: {str(e)}")
        # Log the detailed error for debugging
        if hasattr(e, "response") and hasattr(e.response, "text"):
            logger.error(f"Response body: {e.response.text}")
        # Return empty list but continue processing
        return []


def fetch_user_license_details(tenant_id, user_id):
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


def get_sku_display_name(sku_part_number: str) -> str:
    """Get friendly display name for SKU"""
    sku_names = {
        "ENTERPRISEPACK": "Office 365 E3",
        "ENTERPRISEPREMIUM": "Office 365 E5",
        "EXCHANGESTANDARD": "Exchange Online Plan 1",
        "EXCHANGEENTERPRISE": "Exchange Online Plan 2",
        "SPB": "Microsoft 365 Business Standard",
        "SMB_BUSINESS_ESSENTIALS": "Microsoft 365 Business Basic",
        "SMB_BUSINESS_PREMIUM": "Microsoft 365 Business Premium",
        "STANDARDWOFFPACK": "Office 365 E1",
        "POWER_BI_PRO": "Power BI Pro",
        "EMS": "Enterprise Mobility + Security E3",
        "EMSPREMIUM": "Enterprise Mobility + Security E5",
        "VISIOCLIENT": "Visio Online Plan 2",
        "PROJECTPREMIUM": "Project Online Premium",
        "TEAMS_EXPLORATORY": "Teams Exploratory",
        "FLOW_FREE": "Power Automate Free",
        "WINDOWS_STORE": "Windows Store for Business",
        "DEVELOPERPACK": "Office 365 E3 Developer",
        "STREAM": "Microsoft Stream",
    }

    if not sku_part_number:
        return "Unknown License"

    sku_upper = sku_part_number.upper()
    for sku_pattern, display_name in sku_names.items():
        if sku_pattern in sku_upper:
            return display_name

    # Return the SKU part number if no friendly name found
    return sku_part_number


def sync_licenses(tenant_id, tenant_name):
    """Sync both tenant licenses and user license assignments"""

    # Initialize database schema
    init_schema()

    try:
        logger.info(f"Starting license sync for {tenant_name}")

        # First, try to fetch tenant licenses
        tenant_licenses = fetch_tenant_licenses(tenant_id)

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
                    "license_display_name": get_sku_display_name(sku_part_number),
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

        # Fetch ALL users with licenses (not just active ones)
        logger.info(f"Fetching user license assignments for {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        # Get all users, not filtered - include accountEnabled to detect inactive users
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

                # Get detailed license info for this user
                detailed_licenses = fetch_user_license_details(tenant_id, user_id)
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
                        "license_display_name": get_sku_display_name(sku_part_number),
                        "license_partnumber": sku_part_number,
                        "monthly_cost": estimate_license_cost(sku_part_number),
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                    }
                    user_license_records.append(user_license_record)

        # Store user licenses
        if user_license_records:
            upsert_many("user_licensesV2", user_license_records)
            logger.info(f"Stored {len(user_license_records)} user license assignments from {users_with_licenses} users")

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
            "user_licenses_synced": len(user_license_records),
            "inactive_licenses_updated": len(users_to_check) if users_to_check else 0,
        }

    except Exception as e:
        logger.error(f"License sync failed for {tenant_name}: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }
