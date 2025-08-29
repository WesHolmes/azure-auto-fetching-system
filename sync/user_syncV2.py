from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient
from sql.databaseV2 import init_schema, upsert_many
from utils.http import clean_error_message


logger = logging.getLogger(__name__)

# if v1.0 is detected, we set premium specific attributes to nulls or none for v1.0 endpoints
# be careful, bc v1.0 might give diff. resp. struct. compared to beta, so we need to handle that
"""before fetching users, deter. if user is premium or not
and to deter. we use a simple fetch funct. where we fetch a single user from v1.0 endpoint and selecting for the signin activity
if error comes from query^, then they are not premium, and can only access v1.0 endpoints"""


def fetch_beta_users(tenant_id):
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
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch users")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def _test_tenant_capability(graph, graph_beta, tenant_id):
    """Helper function to test tenant capability for premium features"""
    try:
        # Test with a single user to check if signin activity is accessible
        test_user = graph.get("/users", select=["id", "userPrincipalName"], top=1)
        if not test_user:
            logger.warning(f"No users found in tenant {tenant_id} for capability testing")
            return False

        # Try to fetch signin activity for the first user (BETA endpoint)
        user_id = test_user[0]["id"]
        try:
            graph_beta.get(f"/users/{user_id}/signInActivity", select=["lastSignInDateTime"])
            logger.info(f"Tenant {tenant_id} is PREMIUM - beta signin activity accessible")
            return True
        except Exception:
            # Fallback: test if we can access beta MFA data (another beta-only feature)
            try:
                # Test if we can access MFA registration details (beta endpoint)
                graph_beta.get("/reports/authenticationMethods/userRegistrationDetails", select=["id"], top=1)
                logger.info(f"Tenant {tenant_id} is PREMIUM - beta MFA data accessible")
                return True
            except Exception:
                logger.info(f"Tenant {tenant_id} is NOT PREMIUM - no beta features accessible")
                return False
    except Exception as capability_error:
        logger.warning(f"Could not determine tenant capability for {tenant_id}: {str(capability_error)}")
        return False


def fetch_v1_users(tenant_id):
    """Fetch users from Graph API v1.0 endpoint with tenant capability detection"""
    try:
        logger.info(f"Starting user fetch for tenant {tenant_id} using v1.0 endpoint")
        graph = GraphClient(tenant_id)
        graph_beta = GraphBetaClient(tenant_id)

        # Test tenant capability
        is_premium = _test_tenant_capability(graph, graph_beta, tenant_id)

        # Fetch users with appropriate attributes based on capability
        if is_premium:
            # Premium tenant - can access advanced attributes (including beta features)
            users = graph_beta.get(
                "/users",
                select=[
                    "id",
                    "displayName",
                    "userPrincipalName",
                    "mail",
                    "accountEnabled",
                    "givenName",
                    "surname",
                    "jobTitle",
                    "department",
                    "officeLocation",
                    "businessPhones",
                    "mobilePhone",
                    "preferredLanguage",
                    "usageLocation",
                    "signInActivity",
                    "createdDateTime",
                    "assignedLicenses",
                    "lastPasswordChangeDateTime",
                ],
            )
        else:
            # Non-premium tenant - can still access v1.0 properties like department, jobTitle, etc.
            # Only MFA and signin activity are restricted (beta-only features)
            users = graph.get(
                "/users",
                select=[
                    "id",
                    "displayName",
                    "userPrincipalName",
                    "mail",
                    "accountEnabled",
                    "jobTitle",
                    "department",
                    "officeLocation",
                    "mobilePhone",
                    "userType",
                    "createdDateTime",
                    "assignedLicenses",
                    "lastPasswordChangeDateTime",
                ],
            )

        logger.info(f"Successfully fetched {len(users)} users from v1.0 endpoint for tenant {tenant_id}")

        # Add capability flag to each user record for downstream processing
        for user in users:
            user["_tenant_premium"] = is_premium

        return users, is_premium

    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch users from v1.0 endpoint")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
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


def transform_user_records(users, tenant_id, mfa_lookup, is_premium=True):
    """Transform Graph API users to database records"""
    records = []
    start_time = datetime.now()

    logger.info(f"Starting transformation of {len(users)} users for {'premium' if is_premium else 'non-premium'} tenant")

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
            # Get last sign-in based on tenant premium status
            if is_premium:
                # Premium tenant - use actual signin data
                signin_activity = user.get("signInActivity", {})
                last_sign_in = signin_activity.get("lastSignInDateTime", None)

                # If no signin activity date, set to default (1900-01-01)
                if last_sign_in is None:
                    last_sign_in = "1900-01-01"
            else:
                # Non-premium tenant - set to NULL (no access to signin data)
                last_sign_in = None

            # get license count
            assigned_licenses = user.get("assignedLicenses", [])
            license_count = len(assigned_licenses)
            is_active_license = 1 if account_enabled else 0

            # get mfa details based on tenant premium status
            if is_premium:
                # Premium tenant (beta) - use actual MFA data from beta endpoint
                mfa_data = mfa_lookup.get(user_id, {})
                if mfa_data:
                    # We have MFA data from beta endpoint
                    is_mfa_registered = mfa_data.get("isMfaRegistered", False)
                    is_mfa_compliant = 1 if is_mfa_registered else 0
                else:
                    # No MFA data found for this user, default to 0 (not compliant)
                    is_mfa_compliant = 0
            else:
                # Non-premium tenant (v1.0 only) - set MFA compliance to NULL (no access to MFA data)
                is_mfa_compliant = None

            # Get group count and admin status from pre-fetched results
            is_admin, group_count = group_results.get(user_id, (False, 0))

            # License processing moved to license_syncV2 for cleaner separation of concerns
            # License sync will handle all user_license records completely

            # Handle primary_email (required field)
            primary_email = user.get("mail") or upn or "unknown@domain.com"

            # Get password change date
            last_password_change = user.get("lastPasswordChangeDateTime")

            # Get created date
            created_at = user.get("createdDateTime") or datetime.now().isoformat()

            # Handle user properties - both premium and non-premium tenants can access these via v1.0
            # Only MFA compliance and signin activity are restricted to premium tenants
            department = user.get("department") or "Unassigned"
            job_title = user.get("jobTitle") or "Not Specified"
            office_location = user.get("officeLocation") or "Remote"
            mobile_phone = user.get("mobilePhone") or "Not Provided"
            account_type = user.get("userType") or "Member"

            record = {
                "user_id": user_id,
                "tenant_id": tenant_id,
                "user_principal_name": upn,
                "primary_email": primary_email,
                "display_name": display_name,
                "department": department,
                "job_title": job_title,
                "office_location": office_location,
                "mobile_phone": mobile_phone,
                "account_type": account_type,
                "account_enabled": 1 if user.get("accountEnabled") else 0,
                "is_global_admin": 1 if is_admin else 0,
                "is_mfa_compliant": is_mfa_compliant,  # Now uses the variable that can be NULL for non-premium
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

            # Handle user properties for basic record - both premium and non-premium tenants can access these
            department = user.get("department") or "Unassigned"
            job_title = user.get("jobTitle") or "Not Specified"
            office_location = user.get("officeLocation") or "Remote"
            mobile_phone = user.get("mobilePhone") or "Not Provided"
            account_type = user.get("userType") or "Member"

            basic_record = {
                "user_id": user_id,
                "tenant_id": tenant_id,
                "user_principal_name": upn,
                "primary_email": primary_email,
                "display_name": display_name,
                "department": department,
                "job_title": job_title,
                "office_location": office_location,
                "mobile_phone": mobile_phone,
                "account_type": account_type,
                "account_enabled": 1 if user.get("accountEnabled") else 0,
                "is_global_admin": 0,
                "is_mfa_compliant": None if not is_premium else 0,  # NULL for v1.0 tenants, 0 for beta tenants with error
                "license_count": 0,
                "group_count": 0,
                "last_sign_in_date": None if not is_premium else "1900-01-01",  # NULL for v1.0 tenants, default for beta tenants with error
                "last_password_change": user.get("lastPasswordChangeDateTime"),
                "created_at": created_at,
                "last_updated": datetime.now().isoformat(),
            }
            records.append(basic_record)

    logger.info(f"Transformation complete: {len(records)} users")
    return records  # Only return user records, license sync handles licenses


def sync_users(tenant_id, tenant_name):
    """Orchestrate user synchronization with enrichment"""
    start_time = datetime.now()
    logger.info(f"Starting user sync for {tenant_name} (tenant_id: {tenant_id})")

    # Initialize database schema
    init_schema()

    try:
        # Use the template pattern: fetch users and determine tenant capability in one call
        # fetch_v1_users handles tenant capability detection and returns both users and is_premium flag
        try:
            users, is_premium = fetch_v1_users(tenant_id)

            # Now fetch MFA status based on tenant capability
            if is_premium:
                # Premium tenant - fetch MFA data
                mfa_lookup = fetch_user_mfa_status(tenant_id)
            else:
                # Non-premium tenant - skip MFA fetch (no access to MFA data)
                mfa_lookup = {}

        except Exception as e:
            logger.error(f"Failed to check tenant capability for {tenant_name}: {str(e)}", exc_info=True)
            # Fallback to v1.0 workflow - assume non-premium
            is_premium = False
            users, _ = fetch_v1_users(tenant_id)
            mfa_lookup = {}

        if not users:
            logger.warning(f"No users found for {tenant_name}")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "users_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # transform data with premium status flag
        user_records = transform_user_records(users, tenant_id, mfa_lookup, is_premium)

        # store in database with error handling
        users_stored = 0

        try:
            if user_records:
                users_stored = upsert_many("usersV2", user_records)
                logger.info(f"Stored {users_stored} users for {tenant_name}")
        except Exception as e:
            logger.error(f"Failed to store users for {tenant_name}: {str(e)}", exc_info=True)
            raise

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed user sync for {tenant_name}: {users_stored} users in {duration:.1f}s")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "users_synced": users_stored,
            "duration_seconds": duration,
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()

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
            "duration_seconds": duration,
        }
