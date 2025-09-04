from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from db.db_client import get_connection, init_schema, upsert_many
from shared.graph_beta_client import GraphBetaClient
from shared.graph_client import GraphClient
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def detect_tenant_capabilities(tenant_id):
    """Detect if tenant has premium capabilities by testing signin activity access"""
    try:
        graph = GraphBetaClient(tenant_id)
        test_user = graph.get("/users", select=["id", "userPrincipalName"], top=1)

        if test_user:
            try:
                user_id = test_user[0]["id"]
                graph.get(f"/users/{user_id}/signInActivity")
                logger.info(f"Tenant {tenant_id} is premium - using beta endpoint")
                return True
            except Exception:
                logger.info(f"Tenant {tenant_id} is not premium - using v1.0 endpoint")
                return False
        else:
            logger.warning(f"No users found in tenant {tenant_id} for capability testing")
            return False
    except Exception as e:
        logger.warning(f"Could not determine tenant capability for {tenant_id}: {str(e)}")
        return False


def fetch_directory_roles(tenant_id, use_beta=True):
    """Fetch directory roles from Graph API"""
    try:
        logger.info(f"Starting directory roles fetch for tenant {tenant_id}")

        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        roles = graph.get(
            "/directoryRoles",
            select=["id", "displayName", "description", "deletedDateTime"],
        )

        logger.info(f"Successfully fetched {len(roles)} directory roles for tenant {tenant_id}")
        return roles
    except Exception as e:
        logger.error(f"Failed to fetch directory roles for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def fetch_role_members(tenant_id, role_id, use_beta=True):
    """Fetch members of a specific directory role"""
    try:
        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        members = graph.get(
            f"/directoryRoles/{role_id}/members",
            select=["id", "displayName", "userPrincipalName", "userType"],
        )
        return members
    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch role members")
        logger.error(error_msg)
        logger.debug(f"Full error details for role {role_id} in tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def transform_role_data(roles, tenant_id, use_beta=True):
    """Transform role data for database storage"""
    try:
        logger.info(f"Transforming {len(roles)} roles for tenant {tenant_id}")

        role_records = []
        user_role_records = []

        # Use ThreadPoolExecutor to fetch role members concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_role = {
                executor.submit(fetch_role_members, tenant_id, role["id"], use_beta): role for role in roles if role.get("id")
            }

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

        logger.info(f"Transformed {len(role_records)} roles and {len(user_role_records)} user role assignments")
        return role_records, user_role_records
    except Exception as e:
        logger.error(f"Failed to transform role data for tenant {tenant_id}: {str(e)}")
        raise


def sync_roles(tenant_id):
    """Main function to sync directory roles and their assignments"""
    init_schema()

    try:
        logger.info(f"Starting role sync for tenant {tenant_id}")
        start_time = datetime.utcnow()

        # Detect tenant capabilities
        is_premium = detect_tenant_capabilities(tenant_id)

        # Fetch directory roles
        roles = fetch_directory_roles(tenant_id, is_premium)

        if not roles:
            logger.warning(f"No directory roles found for tenant {tenant_id}")
            return {
                "status": "completed",
                "tenant_id": tenant_id,
                "roles_synced": 0,
                "user_roles_synced": 0,
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            }

        # Transform and get role data
        role_records, user_role_records = transform_role_data(roles, tenant_id, is_premium)

        # Store in database using DELETE + INSERT approach
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM roles WHERE tenant_id = ?", (tenant_id,))
        cursor.execute("DELETE FROM user_rolesV2 WHERE tenant_id = ?", (tenant_id,))
        conn.commit()
        conn.close()

        # Insert new records
        if role_records:
            upsert_many("roles", role_records)
            logger.info(f"Successfully stored {len(role_records)} roles")

        if user_role_records:
            upsert_many("user_rolesV2", user_role_records)
            logger.info(f"Successfully stored {len(user_role_records)} user role assignments")

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
        error_msg = clean_error_message(str(e), f"Tenant {tenant_id}")
        logger.error(error_msg)
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
        error_msg = clean_error_message(str(e), "Multi-tenant role sync")
        logger.error(error_msg)
        logger.debug(f"Full error details for multi-tenant role sync: {str(e)}", exc_info=True)

        return {
            "status": "error",
            "error": str(e),
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
        }
