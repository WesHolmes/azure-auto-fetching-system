from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import logging

from core.database_v2 import upsert_many_v2
from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient


logger = logging.getLogger(__name__)


def fetch_owners_for_app(graph, app_id):
    """Fetch owners for a single application/service principal"""
    try:
        owners = graph.get(f"/servicePrincipals/{app_id}/owners")
        return ",".join([owner.get("displayName", owner.get("id", "")) for owner in owners]) if owners else None
    except Exception as e:
        logger.warning(f"Failed to get owners for application {app_id}: {e}")
        return None


def fetch_applications_with_owners(tenant_id):
    """Fetch all service principals (applications) and their owners from the tenant"""
    graph = GraphClient(tenant_id)

    # Fetch service principals with enhanced fields for V2
    service_principals = graph.get(
        "/servicePrincipals",
        select=[
            "id",
            "appId",
            "displayName",
            "servicePrincipalType",
            "accountEnabled",
            "signInAudience",
            "appOwnerOrganizationId",
            "appRoleAssignmentRequired",
            "keyCredentials",
            "passwordCredentials",
            "appRoles",
            "oauth2PermissionScopes",
            "tags",
        ],
    )

    logger.info(f"Found {len(service_principals)} applications for tenant {tenant_id}")

    # Use ThreadPoolExecutor to fetch owners concurrently
    max_workers = 20  # Limit concurrent requests to avoid rate limiting

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all owner fetch tasks
        future_to_app = {executor.submit(fetch_owners_for_app, graph, sp["id"]): sp for sp in service_principals}

        # Process completed tasks
        for future in as_completed(future_to_app):
            sp = future_to_app[future]
            try:
                sp["owners"] = future.result()
            except Exception as e:
                logger.error(f"Failed to process owners for application {sp['id']}: {e}")
                sp["owners"] = None

    return service_principals


def fetch_signin_data_v2(tenant_id):
    """Fetch last sign-in data for service principals"""
    try:
        graph_beta = GraphBetaClient(tenant_id)

        # Fetch service principal sign-in activities from the reports endpoint
        signin_activities = graph_beta.get(
            "/reports/servicePrincipalSignInActivities",
        )

        # Create lookup dictionary with appId as key and last sign-in as value
        signin_lookup = {}
        for activity in signin_activities:
            app_id = activity.get("appId")
            last_signin_activity = activity.get("lastSignInActivity", {})
            last_signin = last_signin_activity.get("lastSignInDateTime")

            if app_id and last_signin:
                signin_lookup[app_id] = last_signin

        logger.info(f"Fetched sign-in data for {len(signin_lookup)} applications")
        return signin_lookup

    except Exception as e:
        logger.warning(f"Failed to fetch sign-in data for tenant {tenant_id}: {e}")
        return {}


def _json_or_none(data):
    """Helper to convert data to JSON string or None if empty"""
    return json.dumps(data) if data else None


def transform_applications_v2(applications, tenant_id, signin_lookup):
    """Transform applications into V2 database records"""
    records = []
    current_time = datetime.now().isoformat()

    for app in applications:
        record = {
            "id": app["id"],
            "tenant_id": tenant_id,
            "app_id": app["appId"],
            "display_name": app.get("displayName"),
            "app_display_name": app.get("displayName"),
            "service_principal_type": app.get("servicePrincipalType"),
            "account_enabled": 1 if app.get("accountEnabled", False) else 0,
            "sign_in_audience": app.get("signInAudience"),
            "app_owner_organization_id": app.get("appOwnerOrganizationId"),
            "app_role_assignment_required": 1 if app.get("appRoleAssignmentRequired", False) else 0,
            "key_credentials": _json_or_none(app.get("keyCredentials")),
            "password_credentials": _json_or_none(app.get("passwordCredentials")),
            "app_roles": _json_or_none(app.get("appRoles")),
            "oauth2_permission_scopes": _json_or_none(app.get("oauth2PermissionScopes")),
            "tags": _json_or_none(app.get("tags")),
            "last_updated": current_time,
            "last_sign_in": signin_lookup.get(str(app["appId"])),
        }
        records.append(record)

    return records


def sync_applications_v2(tenant_id, tenant_name):
    """Synchronize applications for a single tenant using V2 schema"""
    try:
        logger.info(f"Starting V2 applications sync for tenant {tenant_name} ({tenant_id})")
        start_time = datetime.now()

        # Fetch data from Microsoft Graph
        applications = fetch_applications_with_owners(tenant_id)
        signin_lookup = fetch_signin_data_v2(tenant_id)

        # Transform to database format
        records = transform_applications_v2(applications, tenant_id, signin_lookup)

        # Store in V2 database
        upsert_many_v2("applications_v2", records)

        sync_duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"V2 applications sync completed for {tenant_name}: {len(records)} applications in {sync_duration:.2f}s")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "applications_synced": len(records),
            "sync_time_seconds": sync_duration,
        }

    except Exception as e:
        logger.error(f"V2 applications sync failed for {tenant_name}: {str(e)}")
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }
