from datetime import datetime
import logging

from core.database_v2 import upsert_many_v2
from core.graph_client import GraphClient


logger = logging.getLogger(__name__)


def fetch_conditional_access_policies_v2(tenant_id):
    """Fetch conditional access policies from Microsoft Graph for V2"""
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


def transform_conditional_access_policies_v2(policies, tenant_id):
    """Transform Graph API policies to V2 database records"""
    records = []

    logger.info(f"Transforming {len(policies)} conditional access policies for V2")

    for policy in policies:
        record = {
            "tenant_id": tenant_id,
            "policy_id": policy.get("id"),
            "display_name": policy.get("displayName"),
            "is_active": 1 if policy.get("state") == "enabled" else 0,
            "last_updated": datetime.now().isoformat(),
        }

        records.append(record)

    logger.info(f"Transformed {len(records)} policy records for V2")
    return records


def fetch_tenant_users_v2(tenant_id):
    """Fetch all users from Microsoft Graph for the tenant - V2 version"""
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

        return user_lookup

    except Exception as e:
        logger.error(f"Failed to fetch users from Microsoft Graph: {str(e)}")
        return {}


def fetch_tenant_applications_v2(tenant_id):
    """Fetch all applications/service principals from Microsoft Graph for the tenant - V2 version"""
    try:
        logger.info(f"Fetching all applications from Microsoft Graph for tenant {tenant_id}")

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

        return app_lookup

    except Exception as e:
        logger.error(f"Failed to fetch applications from Microsoft Graph: {str(e)}")
        return {}


def resolve_policy_users_v2(tenant_id, policy, tenant_users):
    """Resolve which users are assigned to a specific policy - V2 version"""
    policy_id = policy.get("id")
    policy_name = policy.get("displayName", "Unknown Policy")
    conditions = policy.get("conditions", {})
    users_condition = conditions.get("users", {})

    # Get included and excluded users
    include_users = users_condition.get("includeUsers", [])
    exclude_users = set(users_condition.get("excludeUsers", []))

    assigned_users = set()

    # Process include users
    for user_spec in include_users:
        if user_spec == "All":
            assigned_users.update(tenant_users.keys())
        elif user_spec == "GuestsOrExternalUsers":
            guest_users = {uid for uid, user in tenant_users.items() if user.get("userType") == "Guest"}
            assigned_users.update(guest_users)
        elif user_spec in tenant_users:
            assigned_users.add(user_spec)
        else:
            logger.warning(f"Policy '{policy_name}' has unhandled includeUsers spec: {user_spec}")

    # Remove excluded users
    assigned_users = assigned_users - exclude_users

    logger.info(f"Policy '{policy_name}' assigned to {len(assigned_users)} users")

    return assigned_users


def resolve_policy_applications_v2(tenant_id, policy, tenant_applications):
    """Resolve which applications are assigned to a specific policy - V2 version"""
    policy_id = policy.get("id")
    policy_name = policy.get("displayName", "Unknown Policy")
    conditions = policy.get("conditions", {})
    applications_condition = conditions.get("applications", {})

    # Get included and excluded applications
    include_applications = applications_condition.get("includeApplications", [])
    exclude_applications = set(applications_condition.get("excludeApplications", []))

    assigned_applications = set()

    # Process include applications
    for app_spec in include_applications:
        if app_spec == "All":
            assigned_applications.update(tenant_applications.keys())
        elif app_spec in tenant_applications:
            assigned_applications.add(app_spec)
        else:
            logger.warning(f"Policy '{policy_name}' has unhandled includeApplications spec: {app_spec}")

    # Remove excluded applications
    assigned_applications = assigned_applications - exclude_applications

    logger.info(f"Policy '{policy_name}' assigned to {len(assigned_applications)} applications")

    return assigned_applications


def create_policy_user_records_v2(tenant_id, policies, tenant_users):
    """Create policy-user mapping records for V2 database storage"""
    policy_user_records = []
    last_updated = datetime.now().isoformat()

    try:
        for policy in policies:
            policy_id = policy.get("id")
            policy_name = policy.get("displayName")
            if not policy_id:
                continue

            # Resolve which users this policy applies to
            assigned_user_ids = resolve_policy_users_v2(tenant_id, policy, tenant_users)

            # Create V2 database records for each user-policy assignment
            for user_id in assigned_user_ids:
                user_info = tenant_users.get(user_id, {})

                policy_user_records.append(
                    {
                        "tenant_id": tenant_id,
                        "user_id": user_id,
                        "policy_id": policy_id,
                        "user_principal_name": user_info.get("userPrincipalName"),
                        "policy_name": policy_name,
                        "last_updated": last_updated,
                    }
                )

        return policy_user_records

    except Exception as e:
        logger.error(f"Failed to create V2 policy-user records: {str(e)}")
        return []


def create_policy_application_records_v2(tenant_id, policies, tenant_applications):
    """Create policy-application mapping records for V2 database storage"""
    policy_application_records = []
    last_updated = datetime.now().isoformat()

    try:
        for policy in policies:
            policy_id = policy.get("id")
            policy_name = policy.get("displayName")
            if not policy_id:
                continue

            # Resolve which applications this policy applies to
            assigned_application_ids = resolve_policy_applications_v2(tenant_id, policy, tenant_applications)

            # Create V2 database records for each application-policy assignment
            for app_id in assigned_application_ids:
                app_info = tenant_applications.get(app_id, {})

                policy_application_records.append(
                    {
                        "tenant_id": tenant_id,
                        "application_id": app_info.get("id", app_id),  # Use service principal ID
                        "policy_id": policy_id,
                        "application_name": app_info.get("displayName"),
                        "policy_name": policy_name,
                        "last_updated": last_updated,
                    }
                )

        return policy_application_records

    except Exception as e:
        logger.error(f"Failed to create V2 policy-application records: {str(e)}")
        return []


def sync_conditional_access_policies_v2(tenant_id, tenant_name):
    """Synchronize conditional access policies for a single tenant using V2 schema"""
    try:
        logger.info(f"Starting V2 conditional access policy sync for {tenant_name} ({tenant_id})")
        start_time = datetime.now()

        # Fetch data from Microsoft Graph
        policies = fetch_conditional_access_policies_v2(tenant_id)

        if not policies:
            logger.warning(f"No conditional access policies found for {tenant_name}")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "policies_synced": 0,
                "user_policies_synced": 0,
                "application_policies_synced": 0,
                "sync_time_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Transform to V2 database format
        policy_records = transform_conditional_access_policies_v2(policies, tenant_id)

        # Store policies in V2 database
        policies_stored = upsert_many_v2("policies_v2", policy_records)

        # Fetch tenant users from Microsoft Graph
        tenant_users = fetch_tenant_users_v2(tenant_id)

        # Create and store policy-user assignments in V2
        policy_user_records = create_policy_user_records_v2(tenant_id, policies, tenant_users)
        user_policies_stored = 0

        if policy_user_records:
            user_policies_stored = upsert_many_v2("user_policies_v2", policy_user_records)

        # Fetch tenant applications from Microsoft Graph
        tenant_applications = fetch_tenant_applications_v2(tenant_id)

        # Create and store policy-application assignments in V2
        policy_application_records = create_policy_application_records_v2(tenant_id, policies, tenant_applications)
        application_policies_stored = 0

        if policy_application_records:
            application_policies_stored = upsert_many_v2("application_policies_v2", policy_application_records)

        sync_duration = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"V2 policies sync completed for {tenant_name}: {policies_stored} policies, {user_policies_stored} user assignments, {application_policies_stored} app assignments in {sync_duration:.2f}s"
        )

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "policies_synced": policies_stored,
            "user_policies_synced": user_policies_stored,
            "application_policies_synced": application_policies_stored,
            "sync_time_seconds": sync_duration,
        }

    except Exception as e:
        logger.error(f"V2 conditional access policy sync failed for {tenant_name}: {str(e)}")
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "policies_synced": 0,
            "user_policies_synced": 0,
            "application_policies_synced": 0,
            "error": str(e),
        }
