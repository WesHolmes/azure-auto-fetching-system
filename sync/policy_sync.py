import logging
from datetime import datetime
from core.graph_client import GraphClient
from core.database import upsert_many, query

logger = logging.getLogger(__name__)


def fetch_conditional_access_policies(tenant_id):
    """Fetch conditional access policies from Microsoft Graph"""
    try:
        logger.info(f"Fetching conditional access policies for tenant {tenant_id}")

        client = GraphClient(tenant_id)

        # Fetch policies with relevant fields including conditions for user assignments
        policies = client.get(
            "/policies/conditionalAccessPolicies",
            select=[
                "id",
                "displayName",
                "state",
                "createdDateTime",
                "modifiedDateTime",
                "conditions",
            ],
        )

        logger.info(f"Retrieved {len(policies)} conditional access policies")
        return policies

    except Exception as e:
        logger.error(f"Failed to fetch conditional access policies: {str(e)}")
        raise


def transform_conditional_access_policies(policies, tenant_id):
    """Transform Graph API policies to database records"""
    records = []

    logger.info(f"Transforming {len(policies)} conditional access policies")

    for policy in policies:
        record = {
            "id": policy.get("id"),
            "tenant_id": tenant_id,
            "display_name": policy.get("displayName"),
            "state": policy.get("state") == "enabled",
            "created_date": policy.get("createdDateTime"),
            "modified_date": policy.get("modifiedDateTime"),
            "synced_at": datetime.now().isoformat(),
        }

        records.append(record)

    logger.info(f"Transformed {len(records)} policy records")
    return records


def fetch_tenant_users(tenant_id):
    """Fetch all users from Microsoft Graph for the tenant"""
    try:
        logger.info(f"Fetching all users from Microsoft Graph for tenant {tenant_id}")

        client = GraphClient(tenant_id)

        # Fetch all users with basic details needed for policy mapping
        users = client.get(
            "/users",
            select=[
                "id",
                "displayName",
                "userPrincipalName",
                "accountEnabled",
                "userType",
            ],
            top=999,
        )

        # Create lookup dictionaries
        user_lookup = {}
        for user in users:
            user_id = user.get("id")
            if user_id:
                user_lookup[user_id] = {
                    "id": user_id,
                    "displayName": user.get("displayName", ""),
                    "userPrincipalName": user.get("userPrincipalName", ""),
                    "accountEnabled": user.get("accountEnabled", True),
                    "userType": user.get("userType", "Member"),
                }

        logger.info(
            f"Successfully fetched {len(user_lookup)} users from Microsoft Graph"
        )
        return user_lookup

    except Exception as e:
        logger.error(f"Failed to fetch users from Microsoft Graph: {str(e)}")
        return {}


def fetch_tenant_applications(tenant_id):
    """Fetch all applications/service principals from Microsoft Graph for the tenant"""
    try:
        logger.info(
            f"Fetching all applications from Microsoft Graph for tenant {tenant_id}"
        )

        client = GraphClient(tenant_id)

        # Fetch all service principals with basic details needed for policy mapping
        applications = client.get(
            "/servicePrincipals",
            select=[
                "id",
                "appId",
                "displayName",
                "servicePrincipalType",
                "accountEnabled",
            ],
            top=999,
        )

        # Create lookup dictionaries using both service principal ID and app ID
        app_lookup = {}
        for app in applications:
            sp_id = app.get("id")
            app_id = app.get("appId")
            display_name = app.get("displayName", "")

            # Store by service principal ID (used in policy conditions)
            if sp_id:
                app_lookup[sp_id] = {
                    "id": sp_id,
                    "appId": app_id,
                    "displayName": display_name,
                    "servicePrincipalType": app.get("servicePrincipalType", ""),
                    "accountEnabled": app.get("accountEnabled", True),
                }

            # Also store by app ID if different from service principal ID
            if app_id and app_id != sp_id:
                app_lookup[app_id] = {
                    "id": sp_id,  # Keep original service principal ID
                    "appId": app_id,
                    "displayName": display_name,
                    "servicePrincipalType": app.get("servicePrincipalType", ""),
                    "accountEnabled": app.get("accountEnabled", True),
                }

        logger.info(
            f"Successfully fetched {len(applications)} applications from Microsoft Graph"
        )
        return app_lookup

    except Exception as e:
        logger.error(f"Failed to fetch applications from Microsoft Graph: {str(e)}")
        return {}


def resolve_policy_users(tenant_id, policy, tenant_users):
    """Resolve which users are assigned to a specific policy"""
    policy_id = policy.get("id")
    policy_name = policy.get("displayName", "Unknown Policy")
    conditions = policy.get("conditions", {})
    users_condition = conditions.get("users", {})

    # Get included and excluded users
    include_users = users_condition.get("includeUsers", [])
    exclude_users = set(users_condition.get("excludeUsers", []))

    logger.info(
        f"Processing policy '{policy_name}' - includeUsers: {include_users}, excludeUsers: {list(exclude_users)}"
    )

    assigned_users = set()

    # Process include users
    for user_spec in include_users:
        if user_spec == "All":
            # Include all users in the tenant
            assigned_users.update(tenant_users.keys())
            logger.info(
                f"Policy '{policy_name}' applies to ALL users ({len(tenant_users)} users)"
            )
        elif user_spec == "GuestsOrExternalUsers":
            # Include guest/external users
            guest_users = {
                uid
                for uid, user in tenant_users.items()
                if user.get("userType") == "Guest"
            }
            assigned_users.update(guest_users)
            logger.info(
                f"Policy '{policy_name}' applies to {len(guest_users)} guest users"
            )
        elif user_spec in tenant_users:
            # Specific user ID
            assigned_users.add(user_spec)
            user_name = tenant_users[user_spec].get("displayName", "Unknown")
            logger.info(f"Policy '{policy_name}' applies to specific user: {user_name}")
        else:
            # Could be a group ID or role - for now log and skip
            logger.warning(
                f"Policy '{policy_name}' has unhandled includeUsers spec: {user_spec}"
            )

    # Remove excluded users
    excluded_count = len(assigned_users & exclude_users)
    assigned_users = assigned_users - exclude_users

    if excluded_count > 0:
        logger.info(f"Policy '{policy_name}' excluded {excluded_count} users")

    logger.info(f"Policy '{policy_name}' final assignment: {len(assigned_users)} users")

    return assigned_users


def resolve_policy_applications(tenant_id, policy, tenant_applications):
    """Resolve which applications are assigned to a specific policy"""
    policy_id = policy.get("id")
    policy_name = policy.get("displayName", "Unknown Policy")
    conditions = policy.get("conditions", {})
    applications_condition = conditions.get("applications", {})

    # Get included and excluded applications
    include_applications = applications_condition.get("includeApplications", [])
    exclude_applications = set(applications_condition.get("excludeApplications", []))

    logger.info(
        f"Processing policy '{policy_name}' - includeApplications: {include_applications}, excludeApplications: {list(exclude_applications)}"
    )

    assigned_applications = set()

    # Process include applications
    for app_spec in include_applications:
        if app_spec == "All":
            # Include all applications in the tenant
            assigned_applications.update(tenant_applications.keys())
            logger.info(
                f"Policy '{policy_name}' applies to ALL applications ({len(tenant_applications)} applications)"
            )
        elif app_spec in tenant_applications:
            # Specific application ID (could be service principal ID or app ID)
            assigned_applications.add(app_spec)
            app_name = tenant_applications[app_spec].get("displayName", "Unknown")
            logger.info(
                f"Policy '{policy_name}' applies to specific application: {app_name}"
            )
        else:
            # Could be an unrecognized application ID or special value - log and skip
            logger.warning(
                f"Policy '{policy_name}' has unhandled includeApplications spec: {app_spec}"
            )

    # Remove excluded applications
    excluded_count = len(assigned_applications & exclude_applications)
    assigned_applications = assigned_applications - exclude_applications

    if excluded_count > 0:
        logger.info(f"Policy '{policy_name}' excluded {excluded_count} applications")

    logger.info(
        f"Policy '{policy_name}' final application assignment: {len(assigned_applications)} applications"
    )

    return assigned_applications


def create_policy_user_records(tenant_id, policies, tenant_users):
    """Create policy-user mapping records for database storage"""
    policy_user_records = []
    synced_at = datetime.now().isoformat()

    try:
        logger.info(f"Creating policy-user mappings for {len(policies)} policies")

        for policy in policies:
            policy_id = policy.get("id")
            if not policy_id:
                continue

            # Resolve which users this policy applies to
            assigned_user_ids = resolve_policy_users(tenant_id, policy, tenant_users)

            # Create database records for each user-policy assignment
            for user_id in assigned_user_ids:
                user_info = tenant_users.get(user_id, {})

                policy_user_records.append(
                    {
                        "tenant_id": tenant_id,
                        "user_id": user_id,
                        "policy_id": policy_id,
                        "user_principal_name": user_info.get("userPrincipalName", ""),
                        "synced_at": synced_at,
                    }
                )

        logger.info(
            f"Created {len(policy_user_records)} policy-user assignment records"
        )
        return policy_user_records

    except Exception as e:
        logger.error(f"Failed to create policy-user records: {str(e)}")
        return []


def create_policy_application_records(tenant_id, policies, tenant_applications):
    """Create policy-application mapping records for database storage"""
    policy_application_records = []
    synced_at = datetime.now().isoformat()

    try:
        logger.info(
            f"Creating policy-application mappings for {len(policies)} policies"
        )

        for policy in policies:
            policy_id = policy.get("id")
            if not policy_id:
                continue

            # Resolve which applications this policy applies to
            assigned_application_ids = resolve_policy_applications(
                tenant_id, policy, tenant_applications
            )

            # Create database records for each application-policy assignment
            for app_id in assigned_application_ids:
                app_info = tenant_applications.get(app_id, {})

                policy_application_records.append(
                    {
                        "tenant_id": tenant_id,
                        "application_id": app_info.get(
                            "id", app_id
                        ),  # Use service principal ID
                        "policy_id": policy_id,
                        "application_name": app_info.get("displayName", ""),
                        "synced_at": synced_at,
                    }
                )

        logger.info(
            f"Created {len(policy_application_records)} policy-application assignment records"
        )
        return policy_application_records

    except Exception as e:
        logger.error(f"Failed to create policy-application records: {str(e)}")
        return []


def sync_conditional_access_policies(tenant_id, tenant_name):
    """Synchronize conditional access policies for a single tenant"""
    try:
        logger.info(
            f"Starting conditional access policy sync for {tenant_name} ({tenant_id})"
        )
        start_time = datetime.now()

        # Fetch data from Microsoft Graph
        policies = fetch_conditional_access_policies(tenant_id)

        if not policies:
            logger.warning(f"No conditional access policies found for {tenant_name}")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "policies_synced": 0,
                "policy_users_synced": 0,
                "policy_applications_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Transform to database format
        records = transform_conditional_access_policies(policies, tenant_id)

        # Store policies in database
        policies_stored = upsert_many("policies", records)

        # Fetch tenant users from Microsoft Graph
        tenant_users = fetch_tenant_users(tenant_id)

        # Create and store policy-user assignments
        policy_user_records = create_policy_user_records(
            tenant_id, policies, tenant_users
        )
        policy_users_stored = 0

        if policy_user_records:
            policy_users_stored = upsert_many("policy_users", policy_user_records)
            logger.info(
                f"Stored {policy_users_stored} policy-user assignments for {tenant_name}"
            )

        # Fetch tenant applications from Microsoft Graph
        tenant_applications = fetch_tenant_applications(tenant_id)

        # Create and store policy-application assignments
        policy_application_records = create_policy_application_records(
            tenant_id, policies, tenant_applications
        )
        policy_applications_stored = 0

        if policy_application_records:
            policy_applications_stored = upsert_many(
                "policy_applications", policy_application_records
            )
            logger.info(
                f"Stored {policy_applications_stored} policy-application assignments for {tenant_name}"
            )

        sync_duration = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"Successfully synced {policies_stored} conditional access policies, {policy_users_stored} user assignments, and {policy_applications_stored} application assignments for {tenant_name}"
        )

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "policies_synced": policies_stored,
            "policy_users_synced": policy_users_stored,
            "policy_applications_synced": policy_applications_stored,
            "duration_seconds": sync_duration,
        }

    except Exception as e:
        logger.error(
            f"Conditional access policy sync failed for {tenant_name}: {str(e)}"
        )
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "policies_synced": 0,
            "policy_users_synced": 0,
            "policy_applications_synced": 0,
            "error": str(e),
        }
