from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient
from sql.databaseV2 import init_schema, upsert_many
from utils.http import clean_error_message


logger = logging.getLogger(__name__)


def fetch_beta_directory_roles(tenant_id):
    """Fetch directory roles from Graph API"""
    try:
        logger.info(f"Starting directory roles fetch for tenant {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        roles = graph.get(
            "/directoryRoles",
            select=["id", "displayName", "description", "deletedDateTime"],
        )

        logger.info(f"Successfully fetched {len(roles)} directory roles for tenant {tenant_id}")
        return roles

    except Exception as e:
        logger.error(
            f"Failed to fetch directory roles for tenant {tenant_id}: {str(e)}",
            exc_info=True,
        )
        raise


def fetch_v1_directory_roles(tenant_id):
    """Fetch directory roles from Graph API v1.0 endpoint"""
    try:
        logger.info(f"Starting directory roles fetch for tenant {tenant_id} using v1.0 endpoint")
        graph = GraphClient(tenant_id)

        roles = graph.get(
            "/directoryRoles",
            select=["id", "displayName", "description", "deletedDateTime"],
        )

        logger.info(f"Successfully fetched {len(roles)} directory roles from v1.0 endpoint for tenant {tenant_id}")
        return roles

    except Exception as e:
        logger.error(
            f"Failed to fetch directory roles from v1.0 endpoint for tenant {tenant_id}: {str(e)}",
            exc_info=True,
        )
        raise


def fetch_beta_role_members(tenant_id, role_id):
    """Fetch members of a specific directory role"""
    try:
        graph = GraphBetaClient(tenant_id)

        members = graph.get(
            f"/directoryRoles/{role_id}/members",
            select=["id", "displayName", "userPrincipalName", "userType"],
        )

        return members

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch role members")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for role {role_id} in tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def fetch_v1_role_members(tenant_id, role_id):
    """Fetch members of a specific directory role from v1.0 endpoint"""
    try:
        graph = GraphClient(tenant_id)

        members = graph.get(
            f"/directoryRoles/{role_id}/members",
            select=["id", "displayName", "userPrincipalName", "userType"],
        )

        return members

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch role members from v1.0 endpoint")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for role {role_id} in tenant {tenant_id} from v1.0 endpoint: {str(e)}", exc_info=True)
        return []


def transform_role_data(roles, tenant_id, is_premium=False):
    """Transform role data for database storage"""
    try:
        logger.info(f"Transforming {len(roles)} roles for tenant {tenant_id}")

        role_records = []
        user_role_records = []

        # use ThreadPoolExecutor to fetch role members concurrently using the appropriate endpoint
        with ThreadPoolExecutor(max_workers=10) as executor:
            # submit all role member fetch tasks using the appropriate endpoint based on tenant capability
            if is_premium:
                future_to_role = {executor.submit(fetch_beta_role_members, tenant_id, role["id"]): role for role in roles if role.get("id")}
            else:
                future_to_role = {executor.submit(fetch_v1_role_members, tenant_id, role["id"]): role for role in roles if role.get("id")}

            # process completed futures
            for future in as_completed(future_to_role):
                role = future_to_role[future]
                try:
                    members = future.result()

                    # Count user members for this role
                    user_members = [m for m in members if m.get("@odata.type") == "#microsoft.graph.user"]
                    member_count = len(user_members)

                    # Create role record
                    role_record = {
                        "tenant_id": tenant_id,
                        "role_id": role.get("id"),
                        "role_display_name": role.get("displayName"),
                        "role_description": role.get("description"),
                        "member_count": member_count,
                        "created_at": datetime.utcnow().isoformat(),
                        "last_updated": datetime.utcnow().isoformat(),
                    }
                    role_records.append(role_record)

                    # Process each user member of this role
                    for member in user_members:
                        user_role_record = {
                            "user_id": member.get("id"),
                            "tenant_id": tenant_id,
                            "role_id": role.get("id"),
                            "user_principal_name": member.get("userPrincipalName"),
                            "role_display_name": role.get("displayName"),
                            "role_description": role.get("description"),
                            "created_at": datetime.utcnow().isoformat(),
                            "last_updated": datetime.utcnow().isoformat(),
                        }
                        user_role_records.append(user_role_record)

                except Exception as e:
                    logger.error(f"Failed to process role {role.get('displayName', 'Unknown')}: {str(e)}")
                    continue

        logger.info(f"Transformed {len(role_records)} roles and {len(user_role_records)} user role assignments for tenant {tenant_id}")
        return role_records, user_role_records

    except Exception as e:
        logger.error(f"Failed to transform role data for tenant {tenant_id}: {str(e)}")
        raise


def sync_roles(tenant_id):
    """Main function to sync directory roles and their assignments"""

    # Initialize database schema
    init_schema()

    try:
        logger.info(f"Starting role sync for tenant {tenant_id}")
        start_time = datetime.utcnow()

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

        # Now fetch directory roles using the appropriate endpoint based on tenant capability
        if is_premium:
            # Premium tenant - use beta endpoint for advanced features
            roles = fetch_beta_directory_roles(tenant_id)
        else:
            # Non-premium tenant - use v1.0 endpoint for basic features
            roles = fetch_v1_directory_roles(tenant_id)

        if not roles:
            logger.warning(f"No directory roles found for tenant {tenant_id}")
            return {
                "status": "completed",
                "tenant_id": tenant_id,
                "roles_synced": 0,
                "user_roles_synced": 0,
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            }

        # Transform and get role data using the appropriate endpoint based on tenant capability
        role_records, user_role_records = transform_role_data(roles, tenant_id, is_premium)

        # Store in database
        from sql.databaseV2 import get_connection

        conn = get_connection()
        cursor = conn.cursor()

        # Clear existing records for this tenant first
        cursor.execute("DELETE FROM roles WHERE tenant_id = ?", (tenant_id,))
        cursor.execute("DELETE FROM user_rolesV2 WHERE tenant_id = ?", (tenant_id,))
        conn.commit()
        conn.close()

        # Insert new role records
        if role_records:
            upsert_many("roles", role_records)
            logger.info(f"Successfully stored {len(role_records)} roles")

        # Insert new user role assignments
        if user_role_records:
            upsert_many("user_rolesV2", user_role_records)
            logger.info(f"Successfully stored {len(user_role_records)} user role assignments")

        if not role_records and not user_role_records:
            logger.warning(f"No role data to store for tenant {tenant_id}")

        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"Role sync completed for tenant {tenant_id} in {duration:.2f} seconds")

        return {
            "status": "completed",
            "tenant_id": tenant_id,
            "roles_synced": len(role_records) if role_records else 0,
            "user_roles_synced": len(user_role_records) if user_role_records else 0,
            "duration_seconds": duration,
        }

    except Exception as e:
        duration = (datetime.utcnow() - start_time).total_seconds()

        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), f"Tenant {tenant_id}")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)

        return {
            "status": "error",
            "tenant_id": tenant_id,
            "error": str(e),
            "duration_seconds": duration,
        }


def sync_rolesV2(tenant_ids):
    """Sync roles for multiple tenants concurrently"""
    try:
        logger.info(f"Starting role sync for {len(tenant_ids)} tenants")
        start_time = datetime.utcnow()

        results = []

        # Use ThreadPoolExecutor for concurrent tenant processing
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_tenant = {executor.submit(sync_roles, tenant_id): tenant_id for tenant_id in tenant_ids}

            for future in as_completed(future_to_tenant):
                tenant_id = future_to_tenant[future]
                try:
                    result = future.result()
                    results.append(result)

                    if result["status"] == "completed":
                        logger.info(
                            f"  Tenant {tenant_id}: {result['roles_synced']} roles, {result['user_roles_synced']} role assignments synced"
                        )
                    else:
                        logger.error(f"  Tenant {tenant_id}: {result.get('error', 'Unknown error')}")

                except Exception as e:
                    logger.error(f"  Tenant {tenant_id}: {str(e)}")
                    results.append(
                        {
                            "status": "error",
                            "tenant_id": tenant_id,
                            "error": str(e),
                            "duration_seconds": 0,
                        }
                    )

        duration = (datetime.utcnow() - start_time).total_seconds()

        # Summary
        successful = [r for r in results if r["status"] == "completed"]
        failed = [r for r in results if r["status"] == "error"]
        total_roles = sum(r.get("roles_synced", 0) for r in successful)
        total_role_assignments = sum(r.get("user_roles_synced", 0) for r in successful)

        logger.info(f"Role sync summary: {len(successful)} successful, {len(failed)} failed")
        logger.info(f"Total roles synced: {total_roles}")
        logger.info(f"Total role assignments synced: {total_role_assignments}")
        logger.info(f"Total duration: {duration:.2f} seconds")

        return {
            "status": "completed",
            "total_tenants": len(tenant_ids),
            "successful_tenants": len(successful),
            "failed_tenants": len(failed),
            "total_roles_synced": total_roles,
            "total_role_assignments_synced": total_role_assignments,
            "duration_seconds": duration,
            "results": results,
        }

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Multi-tenant role sync")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for multi-tenant role sync: {str(e)}", exc_info=True)

        return {
            "status": "error",
            "error": str(e),
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
        }
