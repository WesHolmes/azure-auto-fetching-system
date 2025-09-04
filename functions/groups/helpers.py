from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from db.db_client import get_connection, init_schema, query, upsert_many
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


def fetch_tenant_groups(tenant_id, use_beta=True):
    """Fetch tenant-level group information"""
    try:
        logger.info(f"Fetching tenant groups for {tenant_id}")

        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        groups = graph.get(
            "/groups",
            select=["id", "displayName", "description", "groupTypes", "mailEnabled", "securityEnabled", "mailNickname", "visibility"],
            top=999,
        )

        logger.info(f"Successfully fetched {len(groups) if groups else 0} groups for tenant {tenant_id}")
        return groups
    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch groups")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def fetch_group_members_and_owners(tenant_id, group, use_beta=True):
    """Fetch both members and owners for a specific group (for concurrent processing)"""
    group_id = group.get("id")
    try:
        if use_beta:
            graph = GraphBetaClient(tenant_id)
        else:
            graph = GraphClient(tenant_id)

        # Fetch members and owners concurrently for this group
        members = graph.get(f"/groups/{group_id}/members", select=["id", "userPrincipalName", "displayName"])
        owners = graph.get(f"/groups/{group_id}/owners", select=["id", "userPrincipalName", "displayName"])

        return members or [], owners or []
    except Exception as e:
        logger.warning(f"Failed to fetch members/owners for group {group_id}: {str(e)}")
        return [], []


def determine_group_type(group_types):
    """Determine the group type based on groupTypes array"""
    if not group_types:
        return "Security"

    group_types_lower = [gt.lower() for gt in group_types]

    if "unified" in group_types_lower:
        return "Microsoft 365"
    elif "dynamicmembership" in group_types_lower:
        return "Dynamic"
    elif "mailenabled" in group_types_lower and "securityenabled" in group_types_lower:
        return "Mail-Enabled Security"
    elif "mailenabled" in group_types_lower:
        return "Mail-Enabled"
    else:
        return "Security"


def transform_group_data(groups, tenant_id, use_beta=True):
    """Transform group data for database storage using concurrent processing"""
    try:
        logger.info(f"Transforming {len(groups)} groups for tenant {tenant_id}")

        group_records = []
        user_group_records = []

        # Use ThreadPoolExecutor to fetch group members and owners concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_group = {
                executor.submit(fetch_group_members_and_owners, tenant_id, group, use_beta): group for group in groups if group.get("id")
            }

            processed_count = 0
            total_groups = len(future_to_group)

            for future in as_completed(future_to_group):
                group = future_to_group[future]
                try:
                    members, owners = future.result()

                    group_types = group.get("groupTypes", [])
                    group_type = determine_group_type(group_types)

                    # Count user members and owners (users have userPrincipalName, service principals don't)
                    user_members = [m for m in members if m.get("userPrincipalName")]
                    user_owners = [o for o in owners if o.get("userPrincipalName")]

                    member_count = len(user_members)
                    owner_count = len(user_owners)

                    # Create group record
                    group_record = {
                        "tenant_id": tenant_id,
                        "group_id": group.get("id"),
                        "group_display_name": group.get("displayName", "Unknown Group"),
                        "group_description": group.get("description"),
                        "group_type": group_type,
                        "mail_enabled": 1 if group.get("mailEnabled", False) else 0,
                        "security_enabled": 1 if group.get("securityEnabled", True) else 0,
                        "mail_nickname": group.get("mailNickname"),
                        "visibility": group.get("visibility", "Private"),
                        "member_count": member_count,
                        "owner_count": owner_count,
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                    }
                    group_records.append(group_record)

                    # Process each user member of this group
                    for member in user_members:
                        user_group_record = {
                            "user_id": member.get("id"),
                            "tenant_id": tenant_id,
                            "group_id": group.get("id"),
                            "user_principal_name": member.get("userPrincipalName"),
                            "group_display_name": group.get("displayName", "Unknown Group"),
                            "group_type": group_type,
                            "membership_type": "Member",
                            "created_at": datetime.now().isoformat(),
                            "last_updated": datetime.now().isoformat(),
                        }
                        user_group_records.append(user_group_record)

                    # Process each user owner of this group
                    for owner in user_owners:
                        # Check if user is already in user_group_records as a member
                        existing_record = next(
                            (
                                record
                                for record in user_group_records
                                if record["user_id"] == owner.get("id") and record["group_id"] == group.get("id")
                            ),
                            None,
                        )

                        if existing_record:
                            # Update existing record to mark as owner
                            existing_record["membership_type"] = "Owner"
                        else:
                            # Create new record for owner
                            user_group_record = {
                                "user_id": owner.get("id"),
                                "tenant_id": tenant_id,
                                "group_id": group.get("id"),
                                "user_principal_name": owner.get("userPrincipalName"),
                                "group_display_name": group.get("displayName", "Unknown Group"),
                                "group_type": group_type,
                                "membership_type": "Owner",
                                "created_at": datetime.now().isoformat(),
                                "last_updated": datetime.now().isoformat(),
                            }
                            user_group_records.append(user_group_record)

                except Exception as e:
                    logger.warning(f"Failed to process group {group.get('id', 'unknown')}: {str(e)}")
                    continue

                processed_count += 1
                if processed_count % 20 == 0 or processed_count == total_groups:
                    logger.info(f"Processed {processed_count}/{total_groups} groups...")

        logger.info(f"Transformed {len(group_records)} groups and {len(user_group_records)} user group assignments")
        return group_records, user_group_records
    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to transform groups")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def sync_groups(tenant_id, tenant_name):
    """Main function to sync groups and their assignments"""
    init_schema()

    try:
        logger.info(f"Starting group sync for {tenant_name}")
        start_time = datetime.now()

        # Detect tenant capabilities
        is_premium = detect_tenant_capabilities(tenant_id)

        # Fetch tenant groups
        groups = fetch_tenant_groups(tenant_id, is_premium)

        if not groups:
            logger.warning(f"No groups found for tenant {tenant_id}")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "groups_synced": 0,
                "user_groups_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Transform and get group data using concurrent processing
        logger.info(f"Starting concurrent transformation of {len(groups)} groups...")
        group_records, user_group_records = transform_group_data(groups, tenant_id, is_premium)

        # Store in database using DELETE + INSERT approach
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM groups WHERE tenant_id = ?", (tenant_id,))
        cursor.execute("DELETE FROM user_groupsV2 WHERE tenant_id = ?", (tenant_id,))
        conn.commit()
        conn.close()

        # Insert new records
        if group_records:
            upsert_many("groups", group_records)
            logger.info(f"Stored {len(group_records)} groups")

        if user_group_records:
            upsert_many("user_groupsV2", user_group_records)
            logger.info(f"Stored {len(user_group_records)} user group assignments")

        # Count totals after sync
        total_groups = query("SELECT COUNT(*) as total FROM groups WHERE tenant_id = ?", (tenant_id,))[0]["total"]
        total_memberships = query("SELECT COUNT(*) as total FROM user_groupsV2 WHERE tenant_id = ?", (tenant_id,))[0]["total"]

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Group sync completed for {tenant_name} in {duration:.2f} seconds")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "groups_synced": total_groups,
            "user_groups_synced": total_memberships,
            "duration_seconds": duration,
        }

    except Exception as e:
        error_msg = clean_error_message(str(e), tenant_name=tenant_name)
        logger.error(error_msg)
        logger.debug(f"Full error details for {tenant_name}: {str(e)}", exc_info=True)

        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }


def get_user_groups(tenant_id, user_id):
    """Get all groups for a specific user"""
    try:
        groups = query(
            """SELECT g.group_id, g.group_display_name, g.group_description, g.group_type,
                      ug.membership_type, ug.created_at as joined_date
               FROM user_groupsV2 ug
               JOIN groups g ON ug.group_id = g.group_id AND ug.tenant_id = g.tenant_id
               WHERE ug.user_id = ? AND ug.tenant_id = ?
               ORDER BY g.group_display_name""",
            (user_id, tenant_id),
        )
        return groups
    except Exception as e:
        logger.error(f"Failed to get groups for user {user_id}: {str(e)}")
        return []


def get_group_members(tenant_id, group_id):
    """Get all members of a specific group"""
    try:
        members = query(
            """SELECT u.user_id, u.user_principal_name, u.display_name, u.department, u.job_title,
                      ug.membership_type, ug.created_at as joined_date
               FROM user_groupsV2 ug
               JOIN usersV2 u ON ug.user_id = u.user_id AND ug.tenant_id = u.tenant_id
               WHERE ug.group_id = ? AND ug.tenant_id = ?
               ORDER BY u.display_name""",
            (group_id, tenant_id),
        )
        return members
    except Exception as e:
        logger.error(f"Failed to get members for group {group_id}: {str(e)}")
        return []
