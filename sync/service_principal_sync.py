from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.graph_client import GraphClient
from core.graph_beta_client import GraphBetaClient
from core.database import upsert_many
import logging

logger = logging.getLogger(__name__)


def fetch_owners_for_sp(graph, sp_id):
    """Fetch owners for a single service principal"""
    try:
        owners = graph.get(f"/servicePrincipals/{sp_id}/owners")
        return (
            ",".join(
                [owner.get("displayName", owner.get("id", "")) for owner in owners]
            )
            if owners
            else None
        )
    except Exception as e:
        logger.warning(f"Failed to get owners for SP {sp_id}: {e}")
        return None


def fetch_service_principals_with_owners(tenant_id):
    """Fetch all service principals and their owners from the tenant"""
    graph = GraphClient(tenant_id)

    service_principals = graph.get(
        "/servicePrincipals",
        select=[
            "id",
            "appId",
            "displayName",
            "servicePrincipalType",
            "accountEnabled",
            "passwordCredentials",
            "keyCredentials",
        ],
    )

    logger.info(
        f"Found {len(service_principals)} service principals for tenant {tenant_id}"
    )

    # Use ThreadPoolExecutor to fetch owners concurrently
    max_workers = 20  # Limit concurrent requests to avoid rate limiting

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all owner fetch tasks
        future_to_sp = {
            executor.submit(fetch_owners_for_sp, graph, sp["id"]): sp
            for sp in service_principals
        }

        # Process completed tasks
        for future in as_completed(future_to_sp):
            sp = future_to_sp[future]
            try:
                sp["owners"] = future.result()
            except Exception as e:
                logger.error(f"Failed to process owners for SP {sp['id']}: {e}")
                sp["owners"] = None

    return service_principals


def fetch_signin_data(tenant_id):
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

        logger.info(f"Fetched sign-in data for {len(signin_lookup)} service principals")
        return signin_lookup

    except Exception as e:
        logger.warning(f"Failed to fetch sign-in data for tenant {tenant_id}: {e}")
        return {}


def get_credential_info(password_creds, key_creds):
    """Get credential expiration date and type information"""
    earliest_expiry = None
    credential_types = []

    # Check password credentials
    if password_creds:
        credential_types.append("password")
        for cred in password_creds:
            end_datetime = cred.get("endDateTime")
            if end_datetime:
                try:
                    expiry = datetime.fromisoformat(end_datetime.replace("Z", "+00:00"))
                    if earliest_expiry is None or expiry < earliest_expiry:
                        earliest_expiry = expiry
                except ValueError:
                    continue

    # Check key credentials (certificates)
    if key_creds:
        credential_types.append("certificate")
        for cred in key_creds:
            end_datetime = cred.get("endDateTime")
            if end_datetime:
                try:
                    expiry = datetime.fromisoformat(end_datetime.replace("Z", "+00:00"))
                    if earliest_expiry is None or expiry < earliest_expiry:
                        earliest_expiry = expiry
                except ValueError:
                    continue

    # Determine credential type string
    if len(credential_types) == 2:
        credential_type = "both"
    elif len(credential_types) == 1:
        credential_type = credential_types[0]
    else:
        credential_type = None

    return earliest_expiry.isoformat() if earliest_expiry else None, credential_type


def transform_service_principals(service_principals, tenant_id, signin_lookup):
    """Transform service principals into database records"""
    records = []

    for sp in service_principals:
        password_creds = sp.get("passwordCredentials", [])
        key_creds = sp.get("keyCredentials", [])

        # Get credential expiration and type info
        credential_exp_date, credential_type = get_credential_info(
            password_creds, key_creds
        )

        record = {
            "id": sp["id"],
            "tenant_id": tenant_id,
            "app_id": sp["appId"],
            "display_name": sp["displayName"],
            "service_principal_type": sp["servicePrincipalType"],
            "owners": sp.get("owners"),
            "credential_exp_date": credential_exp_date,
            "credential_type": credential_type,
            "enabled_sp": sp.get("accountEnabled", False),
            "last_sign_in": signin_lookup.get(str(sp["appId"])),
            "synced_at": datetime.now().isoformat(),
        }
        records.append(record)

    return records


def sync_service_principals(tenant_id, tenant_name):
    """Synchronize service principals for a single tenant"""
    try:
        logger.info(f"Starting sync for tenant {tenant_name} ({tenant_id})")
        start_time = datetime.now()

        # Fetch data from Microsoft Graph
        service_principals = fetch_service_principals_with_owners(tenant_id)
        signin_lookup = fetch_signin_data(tenant_id)

        # Transform to database format
        records = transform_service_principals(
            service_principals, tenant_id, signin_lookup
        )

        # Store in database
        upsert_many("service_principals", records)

        sync_duration = (datetime.now() - start_time).total_seconds()

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "service_principals_synced": len(records),
            "sync_time_seconds": sync_duration,
        }

    except Exception as e:
        logger.error(f"Service principal sync failed for {tenant_name}: {str(e)}")
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }


if __name__ == "__main__":
    """Test sync functionality by running directly"""
    logger.info("Running Service Principal Sync Module")
    logger.info("=" * 50)

    from core.tenant_manager import get_tenants

    tenants = get_tenants()
    logger.info(f"Found {len(tenants)} tenant(s)")

    if not tenants:
        logger.error("No tenants configured. Please check your tenant configuration.")
        exit(1)

    logger.info("\nRunning Service Principal Sync...")
    start_time = datetime.now()
    results = []

    # Process each tenant sequentially
    for tenant in tenants:
        result = sync_service_principals(tenant["tenant_id"], tenant["name"])
        results.append(result)

        if result["status"] == "success":
            logger.info(
                f"✓ {tenant['name']}: {result['service_principals_synced']} SPs synced in {result['sync_time_seconds']:.2f}s"
            )
        else:
            logger.error(f"✗ {tenant['name']}: {result['error']}")

    total_time = (datetime.now() - start_time).total_seconds()
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "error"]
    total_synced = sum(r.get("service_principals_synced", 0) for r in successful)

    logger.info("\nResults:")
    logger.info(f"   Successful: {len(successful)}/{len(tenants)} tenants")
    logger.info(f"   Failed: {len(failed)} tenants")
    logger.info(f"   Total synced: {total_synced} service principals")
    logger.info(f"   Total time: {total_time:.2f} seconds")

    if failed:
        logger.warning("\nFailed tenants:")
        for result in failed:
            logger.warning(f"   - {result['tenant_name']}: {result['error']}")

    logger.info("\nSync completed!")
