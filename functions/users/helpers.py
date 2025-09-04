from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from db.db_client import execute_query, init_schema, query, upsert_many
from shared.graph_beta_client import GraphBetaClient
from shared.graph_client import GraphClient
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


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


def transform_user_records(users, tenant_id, mfa_lookup, is_premium=None):
    """Transform Graph API users to database records"""
    # is_premium = True
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
            department = user.get("department") or "N/A"
            job_title = user.get("jobTitle") or "N/A"
            office_location = user.get("officeLocation") or "N/A"
            mobile_phone = user.get("mobilePhone") or "N/A"
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
            department = user.get("department") or "N/A"
            job_title = user.get("jobTitle") or "N/A"
            office_location = user.get("officeLocation") or "N/A"
            mobile_phone = user.get("mobilePhone") or "N/A"
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


def calculate_inactive_users(tenant_id: str, days: int = 90) -> dict[str, Any]:
    """
    calculate inactive users based on last sign-in activity
    analyzes user activity patterns and potential license cost savings

    returns:
        dict: with analysis results and potential savings
    """
    try:
        logger.info(f"starting inactive users analysis for tenant {tenant_id}")

        # calculate the cutoff date for determining inactive users
        cutoff_date = datetime.now(UTC) - timedelta(days=days)
        logger.debug(f"cutoff date set to {cutoff_date}")

        # query users from database - using sqlite parameterized queries
        query_sql = """
        SELECT
            user_id, display_name, user_principal_name, account_enabled,
            last_sign_in_date, license_count, is_global_admin
        FROM usersV2
        WHERE tenant_id = ? AND account_enabled = 1
        """

        # execute database query with proper parameterization
        users = query(query_sql, (tenant_id,))
        logger.info(f"retrieved {len(users)} active users from database")

        # initialize lists to categorize users by activity status
        inactive_users = []
        active_users = []
        never_signed_in = []

        # process each user to determine activity status
        for user in users:
            if user["last_sign_in_date"]:
                # parse the last sign-in timestamp
                last_signin = datetime.fromisoformat(user["last_sign_in_date"])

                # check if user is inactive based on cutoff date
                if last_signin < cutoff_date:
                    days_inactive = (datetime.now(UTC) - last_signin).days

                    # add to inactive users with potential savings calculation
                    inactive_users.append(
                        {
                            "user_id": user["user_id"],
                            "display_name": user["display_name"],
                            "user_principal_name": user["user_principal_name"],
                            "days_inactive": days_inactive,
                            "license_count": user.get("license_count", 0),
                        }
                    )
                else:
                    # user is active - signed in within threshold
                    active_users.append(user)
            else:
                # user has never signed in - potential cleanup candidate
                never_signed_in.append(user)

        # Calculate actual potential cost savings using real license costs
        inactive_user_ids = [u["user_id"] for u in inactive_users]
        if inactive_user_ids:
            # Get actual monthly costs for inactive users' licenses
            placeholders = ",".join(["?" for _ in inactive_user_ids])
            inactive_cost_query = f"""
            SELECT SUM(monthly_cost) as total_cost
            FROM user_licensesV2
            WHERE user_id IN ({placeholders}) AND tenant_id = ?
            """
            cost_result = query(inactive_cost_query, inactive_user_ids + [tenant_id])
            monthly_savings = cost_result[0]["total_cost"] if cost_result and cost_result[0]["total_cost"] else 0
        else:
            monthly_savings = 0

        logger.info(
            f"analysis complete: {len(inactive_users)} inactive, {len(active_users)} active, {len(never_signed_in)} never signed in"
        )

        # prepare comprehensive result object
        result = {
            "tenant_id": tenant_id,
            "analysis_date": datetime.now(UTC).isoformat(),
            "threshold_days": days,
            "inactive_count": len(inactive_users),
            "active_count": len(active_users),
            "never_signed_in_count": len(never_signed_in),
            "potential_monthly_savings": monthly_savings,
            "utilization_rate": round((len(active_users) / len(users)) * 100, 2) if users else 0,
            "inactive_users": inactive_users[:10],  # top 10 for summary report
        }

        return result

    except Exception as e:
        logger.error(f"error calculating inactive users: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}


def calculate_mfa_compliance(tenant_id: str) -> dict[str, Any]:
    """
    calculate multi-factor authentication compliance across users
    identifies security risks from non-mfa users, especially admins

    returns:
        dictionary with mfa compliance metrics and risk assessment
    """
    try:
        logger.info(f"starting mfa compliance analysis for tenant {tenant_id}")

        # query users with mfa registration status
        query_sql = """
        SELECT
            user_id, display_name, user_principal_name,
            is_mfa_compliant, is_global_admin, account_enabled
        FROM usersV2
        WHERE tenant_id = ? AND account_enabled = 1
        """

        # execute parameterized query
        users = query(query_sql, (tenant_id,))
        # logger.info(f"analyzing mfa status for {len(users)} active users")

        # initialize lists for compliance categorization
        compliant = []
        non_compliant = []
        admin_non_compliant = []

        # categorize users by mfa compliance status
        for user in users:
            if user.get("is_mfa_compliant", False):
                # user has mfa enabled - compliant
                compliant.append(user)
            else:
                # user does not have mfa - non-compliant
                non_compliant.append(user)

                # check if non-compliant user is an admin - high security risk
                if user.get("is_global_admin", False):
                    admin_non_compliant.append(user)

        # calculate compliance metrics
        total_users = len(users)
        compliance_rate = (len(compliant) / total_users * 100) if total_users > 0 else 0

        # logger.info(f"mfa compliance rate: {compliance_rate:.1f}% ({len(compliant)}/{total_users})")
        # logger.warning(f"critical: {len(admin_non_compliant)} admin users without mfa")

        # prepare comprehensive compliance report
        result = {
            "tenant_id": tenant_id,
            "analysis_date": datetime.now(UTC).isoformat(),
            "total_users": total_users,
            "mfa_enabled": len(compliant),
            "non_compliant": len(non_compliant),
            "compliance_rate": round(compliance_rate, 1),
            "admin_non_compliant": len(admin_non_compliant),
            "risk_level": "high" if admin_non_compliant else ("medium" if non_compliant else "low"),
            "critical_users": admin_non_compliant[:10],  # top 10 admin users without mfa - security priority
        }

        return result

    except Exception as e:
        logger.error(f"error calculating mfa compliance: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}


def calculate_license_optimization(tenant_id: str) -> dict[str, Any]:
    """
    analyze license usage patterns and identify optimization opportunities
    helps reduce costs by identifying unused or underutilized licenses

    args:
        tenant_id: microsoft tenant identifier

    returns:
        dictionary with license usage analysis and cost optimization recommendations
    """
    try:
        # logger.info(f"starting license optimization analysis for tenant {tenant_id}")

        # simplified query without license table dependency
        # focuses on user activity patterns to estimate license utilization
        query_sql = """
        SELECT
            user_id, display_name, user_principal_name, last_sign_in_date,
            account_enabled, account_type, license_count
        FROM usersV2
        WHERE tenant_id = ? AND account_enabled = 1
        """

        # execute query to get user activity data
        users = query(query_sql, (tenant_id,))
        # logger.info(f"analyzing license optimization for {len(users)} active users")

        # categorize users by usage patterns for license optimization
        active_users = 0
        inactive_users = 0
        never_signed_in = 0
        guest_users = 0

        # 90-day inactivity threshold for license optimization
        cutoff_date = datetime.now(UTC) - timedelta(days=90)

        # analyze each user's activity pattern
        for user in users:
            # count guest users (may not need paid licenses)
            if user.get("account_type") == "Guest":
                guest_users += 1
                continue

            if user["last_sign_in_date"]:
                # parse last sign-in date
                last_signin = datetime.fromisoformat(user["last_sign_in_date"])

                if last_signin >= cutoff_date:
                    # user is active - license is being utilized
                    active_users += 1
                else:
                    # user is inactive - potential license optimization candidate
                    inactive_users += 1
            else:
                # user never signed in - license potentially wasted
                never_signed_in += 1

        # Calculate optimization metrics using actual license costs
        total_paid_users = len(users) - guest_users
        underutilized_licenses = inactive_users + never_signed_in
        utilization_rate = (active_users / total_paid_users * 100) if total_paid_users > 0 else 0

        # Calculate actual cost savings using real license costs from database
        # Get actual monthly costs for underutilized licenses
        underutilized_cost_query = """
        SELECT SUM(ul.monthly_cost) as total_cost
        FROM usersV2 u
        INNER JOIN user_licensesV2 ul ON u.user_id = ul.user_id
        WHERE u.tenant_id = ?
        AND u.account_enabled = 1
        AND (u.last_sign_in_date IS NULL OR datetime(u.last_sign_in_date) < datetime('now', '-90 days'))
        """

        cost_result = query(underutilized_cost_query, (tenant_id,))
        actual_monthly_savings = cost_result[0]["total_cost"] if cost_result and cost_result[0]["total_cost"] else 0
        actual_annual_savings = actual_monthly_savings * 12

        # Fallback estimate if no cost data available
        if actual_monthly_savings == 0 and underutilized_licenses > 0:
            estimated_monthly_savings = underutilized_licenses * 15  # Fallback estimate
            estimated_annual_savings = estimated_monthly_savings * 12
        else:
            estimated_monthly_savings = actual_monthly_savings
            estimated_annual_savings = actual_annual_savings

        # logger.info(f"license utilization: {utilization_rate:.1f}% ({active_users}/{total_paid_users})")
        # logger.info(f"potential monthly savings: ${estimated_monthly_savings}")

        # prepare comprehensive optimization report
        result = {
            "tenant_id": tenant_id,
            "analysis_date": datetime.now(UTC).isoformat(),
            "total_users": len(users),
            "total_paid_users": total_paid_users,
            "active_users": active_users,
            "inactive_users": inactive_users,
            "never_signed_in": never_signed_in,
            "guest_users": guest_users,
            "utilization_rate": round(utilization_rate, 1),
            "underutilized_licenses": underutilized_licenses,
            "estimated_monthly_savings": estimated_monthly_savings,
            "estimated_annual_savings": estimated_annual_savings,
            "optimization_score": round(utilization_rate, 0),  # simple score based on utilization
        }

        return result

    except Exception as e:
        logger.error(f"error calculating license optimization: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}


def fix_inactive_user_licenses(tenant_id: str) -> dict[str, Any]:
    try:
        logger.info(f"Starting retroactive license fix for tenant {tenant_id}")

        # Find inactive users who still have active license records
        query_sql = """
        SELECT DISTINCT u.user_id, u.user_principal_name, u.account_enabled
        FROM usersV2 u
        INNER JOIN user_licensesV2 ul ON u.user_id = ul.user_id
        WHERE u.tenant_id = ?
        AND u.account_enabled = 0
        AND ul.is_active = 1
        """

        inactive_users_with_active_licenses = query(query_sql, (tenant_id,))

        if not inactive_users_with_active_licenses:
            logger.info("No inactive users with active licenses found")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "users_updated": 0,
                "message": "No inactive users with active licenses found",
            }

        # Update their license records to mark as inactive
        updated_count = 0
        for user in inactive_users_with_active_licenses:
            rows_updated = execute_query(
                """
                UPDATE user_licensesV2
                SET is_active = 0,
                    unassigned_date = ?,
                    last_updated = ?
                WHERE user_id = ? AND tenant_id = ? AND is_active = 1
            """,
                (
                    datetime.now(UTC).isoformat(),
                    datetime.now(UTC).isoformat(),
                    user["user_id"],
                    tenant_id,
                ),
            )

            if rows_updated > 0:
                updated_count += 1
                logger.info(f"Marked {rows_updated} licenses as inactive for user: {user['user_principal_name']}")

        logger.info(f"Fixed licenses for {updated_count} inactive users")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "users_updated": updated_count,
            "licenses_marked_inactive": sum(
                query(
                    "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 0",
                    (tenant_id,),
                )[0].values()
            ),
        }

    except Exception as e:
        logger.error(f"Error fixing inactive user licenses: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}
