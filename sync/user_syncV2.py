from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from core.databaseV2 import init_schema, upsert_many
from core.graph_beta_client import GraphBetaClient


logger = logging.getLogger(__name__)


def fetch_users(tenant_id):
    """Fetch users from Graph API"""
    try:
        logger.info(f"Starting user fetch for tenant {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        users = graph.get(
            "/users",
            select=[
                "id",
                "displayName",
                "userPrincipalName",
                "mail",
                "accountEnabled",
                "userType",
                "department",
                "jobTitle",
                "officeLocation",
                "mobilePhone",
                "signInActivity",
                "createdDateTime",
                "assignedLicenses",
                "lastPasswordChangeDateTime",
            ],
            expand="manager($select=id,displayName)",
            top=999,
        )

        logger.info(f"Successfully fetched {len(users)} users for tenant {tenant_id}")
        return users

    except Exception as e:
        logger.error(f"Failed to fetch users for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def fetch_user_groups(tenant_id, user_id):
    """Check if user is admin"""
    try:
        graph = GraphBetaClient(tenant_id)
        groups = graph.get(f"/users/{user_id}/memberOf", select=["id", "displayName"])

        # check for admin roles
        admin_keywords = ["admin", "administrator", "global"]
        is_admin = any(any(keyword in group.get("displayName", "").lower() for keyword in admin_keywords) for group in groups)
        return is_admin, len(groups)

    except Exception as e:
        logger.debug(f"Failed to fetch groups for user {user_id}: {str(e)}")
        return False, 0


def fetch_user_mfa_status(tenant_id):
    """Fetch MFA registration details for all users"""
    try:
        logger.info(f"Fetching MFA status for tenant {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        mfa_details = graph.get(
            "/reports/authenticationMethods/userRegistrationDetails",
            select=[
                "id",
                "userPrincipalName",
                "isMfaRegistered",
                "isMfaCapable",
                "methodsRegistered",
            ],
        )

        # conv. to lookup dictionary
        mfa_lookup = {item["id"]: item for item in mfa_details}
        logger.info(f"Successfully fetched MFA status for {len(mfa_lookup)} users")
        return mfa_lookup

    except Exception as e:
        logger.warning(f"Could not fetch MFA data for tenant {tenant_id}: {str(e)}")
        return {}


def fetch_user_groups_batch(tenant_id, user_ids):
    """Fetch groups for multiple users concurrently"""
    results = {}

    def fetch_single_user_groups(user_id):
        try:
            return user_id, fetch_user_groups(tenant_id, user_id)
        except Exception as e:
            logger.debug(f"Failed to fetch groups for user {user_id}: {str(e)}")
            return user_id, (False, 0)

    # Use ThreadPoolExecutor to fetch groups concurrently
    max_workers = 20  # Limit concurrent requests to avoid rate limiting

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all group fetch tasks
        future_to_user = {executor.submit(fetch_single_user_groups, user_id): user_id for user_id in user_ids}

        # Process completed tasks
        for future in as_completed(future_to_user):
            try:
                user_id, result = future.result()
                results[user_id] = result
            except Exception as e:
                user_id = future_to_user[future]
                logger.error(f"Failed to process groups for user {user_id}: {e}")
                results[user_id] = (False, 0)

    return results


def transform_user_records(users, tenant_id, mfa_lookup):
    """Transform Graph API users to database records"""
    records = []
    license_records = []  # Add this back
    start_time = datetime.now()

    logger.info(f"Starting transformation of {len(users)} users")

    # First, collect all user IDs that need group fetching
    user_ids = [user.get("id") for user in users]

    # Fetch all user groups concurrently
    logger.info("Fetching group memberships for all users concurrently...")
    group_results = fetch_user_groups_batch(tenant_id, user_ids)
    logger.info(f"Completed group fetching in {(datetime.now() - start_time).total_seconds():.1f}s")

    for i, user in enumerate(users, 1):
        user_id = user.get("id")
        display_name = user.get("displayName", "Unknown")
        upn = user.get("userPrincipalName")
        account_enabled = user.get("accountEnabled", True)

        if i % 50 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = i / elapsed
            eta = (len(users) - i) / rate
            logger.info(f"Processing user {i}/{len(users)} - Elapsed: {elapsed:.1f}s, ETA: {eta:.1f}s")

        try:
            # Get last sign-in
            signin_activity = user.get("signInActivity", {})
            last_sign_in = signin_activity.get("lastSignInDateTime", None)

            # get license count
            assigned_licenses = user.get("assignedLicenses", [])
            license_count = len(assigned_licenses)
            is_active_license = 1 if account_enabled else 0

            # get mfa details
            mfa_data = mfa_lookup.get(user_id, {})
            is_mfa_registered = mfa_data.get("isMfaRegistered", False)

            # Get group count and admin status from pre-fetched results
            is_admin, group_count = group_results.get(user_id, (False, 0))

            # Process assigned licenses for user_licenses table
            if assigned_licenses:
                try:
                    for assigned_license in assigned_licenses:
                        sku_id = assigned_license.get("skuId")
                        if sku_id:
                            # We'll populate display name and part number in license_sync
                            user_license_record = {
                                "user_id": user_id,
                                "tenant_id": tenant_id,
                                "license_id": sku_id,
                                "user_principal_name": upn,
                                "license_display_name": "Pending Sync",  # Will be updated by license sync
                                "license_partnumber": "Pending Sync",  # Will be updated by license sync
                                "is_active": is_active_license,
                                "unassigned_date": None,
                                "monthly_cost": 15.00,  # Default, will be updated
                                "last_updated": datetime.now().isoformat(),
                            }
                            license_records.append(user_license_record)
                except Exception as e:
                    logger.warning(f"Could not process licenses for user {user_id}: {str(e)}")

            # Handle primary_email (required field)
            primary_email = user.get("mail") or upn or "unknown@domain.com"

            # Get password change date
            last_password_change = user.get("lastPasswordChangeDateTime")

            # Get created date
            created_at = user.get("createdDateTime") or datetime.now().isoformat()

            record = {
                "user_id": user_id,
                "tenant_id": tenant_id,
                "user_principal_name": upn,
                "primary_email": primary_email,
                "display_name": display_name,
                "department": user.get("department"),
                "job_title": user.get("jobTitle"),
                "office_location": user.get("officeLocation"),
                "mobile_phone": user.get("mobilePhone"),
                "account_type": user.get("userType"),
                "account_enabled": 1 if user.get("accountEnabled") else 0,
                "is_global_admin": 1 if is_admin else 0,
                "is_mfa_compliant": 1 if is_mfa_registered else 0,
                "license_count": license_count,
                "group_count": group_count,
                "last_sign_in_date": last_sign_in,
                "last_password_change": last_password_change,
                "created_at": created_at,
                "last_updated": datetime.now().isoformat(),
            }
            records.append(record)

        except Exception as e:
            logger.error(f"Failed to process user {display_name}: {str(e)}")
            # Add basic record
            primary_email = user.get("mail") or upn or "unknown@domain.com"
            created_at = user.get("createdDateTime") or datetime.now().isoformat()

            basic_record = {
                "user_id": user_id,
                "tenant_id": tenant_id,
                "user_principal_name": upn,
                "primary_email": primary_email,
                "display_name": display_name,
                "department": user.get("department"),
                "job_title": user.get("jobTitle"),
                "office_location": user.get("officeLocation"),
                "mobile_phone": user.get("mobilePhone"),
                "account_type": user.get("userType"),
                "account_enabled": 1 if user.get("accountEnabled") else 0,
                "is_global_admin": 0,
                "is_mfa_compliant": 0,
                "license_count": 0,
                "group_count": 0,
                "last_sign_in_date": None,
                "last_password_change": user.get("lastPasswordChangeDateTime"),
                "created_at": created_at,
                "last_updated": datetime.now().isoformat(),
            }
            records.append(basic_record)

    logger.info(f"Transformation complete: {len(records)} users, {len(license_records)} licenses")
    return records, license_records  # Return both values


def sync_users(tenant_id, tenant_name):
    """Orchestrate user synchronization with enrichment"""
    start_time = datetime.now()
    logger.info(f"Starting user sync for {tenant_name} (tenant_id: {tenant_id})")

    # Initialize database schema
    init_schema()

    try:
        # fetch all data
        users = fetch_users(tenant_id)

        if not users:
            logger.warning(f"No users found for {tenant_name}")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "users_synced": 0,
                "user_licenses_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # fetch MFA status (optional enrichment)
        mfa_lookup = fetch_user_mfa_status(tenant_id)

        # transform data
        user_records, user_license_records = transform_user_records(users, tenant_id, mfa_lookup)

        # store in database with error handling
        users_stored = 0
        user_licenses_stored = 0

        try:
            if user_records:
                users_stored = upsert_many("usersV2", user_records)
                logger.info(f"Stored {users_stored} users for {tenant_name}")
        except Exception as e:
            logger.error(f"Failed to store users for {tenant_name}: {str(e)}", exc_info=True)
            raise

        try:
            if user_license_records:
                user_licenses_stored = upsert_many("user_licensesV2", user_license_records)
                logger.info(f"Stored {user_licenses_stored} user licenses for {tenant_name}")
        except Exception as e:
            logger.error(
                f"Failed to store user licenses for {tenant_name}: {str(e)}",
                exc_info=True,
            )
            # Don't raise here - users were stored successfully

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed user sync for {tenant_name}: {users_stored} users, {user_licenses_stored} user licenses in {duration:.1f}s")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "users_synced": users_stored,
            "user_licenses_synced": user_licenses_stored,
            "duration_seconds": duration,
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"User sync failed for {tenant_name} after {duration:.1f}s: {str(e)}"
        logger.error(error_msg, exc_info=True)

        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
            "duration_seconds": duration,
        }
