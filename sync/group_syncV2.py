from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from core.databaseV2 import execute_query, init_schema, query, upsert_many
from core.graph_beta_client import GraphBetaClient


logger = logging.getLogger(__name__)


def fetch_tenant_groups(tenant_id):
    """Fetch tenant-level group information"""
    try:
        logger.info(f"Fetching tenant groups for {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        # Get all groups with detailed information
        groups = graph.get(
            "/groups",
            select=[
                "id",
                "displayName",
                "description",
                "groupTypes",
                "mailEnabled",
                "securityEnabled",
                "mailNickname",
                "visibility",
            ],
        )

        return groups

    except Exception as e:
        logger.error(f"Failed to fetch tenant groups: {str(e)}")
        # Log the detailed error for debugging
        if hasattr(e, "response") and hasattr(e.response, "text"):
            logger.error(f"Response body: {e.response.text}")
        # Return empty list but continue processing
        return []


def fetch_group_members(tenant_id, group_id):
    """Fetch detailed member information for a specific group"""
    try:
        logger.info(f"Fetching members for group {group_id}")
        graph = GraphBetaClient(tenant_id)
        members = graph.get(f"/groups/{group_id}/members", select=["id", "userPrincipalName", "displayName", "@odata.type"])
        logger.info(f"Group {group_id}: Found {len(members) if members else 0} members")
        return members
    except Exception as e:
        logger.warning(f"Failed to fetch members for group {group_id}: {str(e)}")
        return []


def fetch_group_owners(tenant_id, group_id):
    """Fetch detailed owner information for a specific group"""
    try:
        logger.info(f"Fetching owners for group {group_id}")
        graph = GraphBetaClient(tenant_id)
        owners = graph.get(f"/groups/{group_id}/owners", select=["id", "userPrincipalName", "displayName", "@odata.type"])
        logger.info(f"Group {group_id}: Found {len(owners) if owners else 0} owners")
        return owners
    except Exception as e:
        logger.warning(f"Failed to fetch owners for group {group_id}: {str(e)}")
        return []


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


def sync_groups(tenant_id, tenant_name):
    """Sync both tenant groups and user group assignments"""

    # Initialize database schema
    init_schema()

    try:
        logger.info(f"Starting group sync for {tenant_name}")

        # First, try to fetch tenant groups
        tenant_groups = fetch_tenant_groups(tenant_id)

        # Create lookup dictionary for tenant groups
        group_lookup = {}
        if tenant_groups:
            # Transform and store tenant groups
            group_records = []
            for group in tenant_groups:
                group_types = group.get("groupTypes", [])
                group_type = determine_group_type(group_types)

                # Member and owner counts will be calculated from actual data
                member_count = 0
                owner_count = 0

                group_data = {
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
                group_records.append(group_data)
                group_lookup[group.get("id")] = group_data

            if group_records:
                upsert_many("groups", group_records)
                logger.info(f"Stored {len(group_records)} tenant groups")

        # Now sync user-group assignments
        user_group_records = []
        groups_with_members = 0

        # Use ThreadPoolExecutor to fetch group members concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all group member fetch tasks
            future_to_group = {
                executor.submit(fetch_group_members, tenant_id, group["id"]): group for group in tenant_groups if group.get("id")
            }

            # Process completed futures
            for future in as_completed(future_to_group):
                group = future_to_group[future]
                try:
                    members = future.result()
                    group_id = group.get("id")
                    group_display_name = group.get("displayName", "Unknown Group")
                    group_types = group.get("groupTypes", [])
                    group_type = determine_group_type(group_types)

                    if members:
                        groups_with_members += 1

                        # Process each user member of this group
                        for member in members:
                            # Only process user objects, skip service principals
                            if member.get("@odata.type") == "#microsoft.graph.user":
                                user_group_record = {
                                    "user_id": member.get("id"),
                                    "tenant_id": tenant_id,
                                    "group_id": group_id,
                                    "user_principal_name": member.get("userPrincipalName"),
                                    "group_display_name": group_display_name,
                                    "group_type": group_type,
                                    "membership_type": "Member",  # Default to Member
                                    "created_at": datetime.now().isoformat(),
                                    "last_updated": datetime.now().isoformat(),
                                }
                                user_group_records.append(user_group_record)

                except Exception as e:
                    logger.error(f"Failed to process group {group.get('id', 'Unknown')}: {str(e)}")
                    continue

        # Now fetch and process group owners
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all group owner fetch tasks
            future_to_group_owners = {
                executor.submit(fetch_group_owners, tenant_id, group["id"]): group for group in tenant_groups if group.get("id")
            }

            # Process completed futures for owners
            for future in as_completed(future_to_group_owners):
                group = future_to_group_owners[future]
                try:
                    owners = future.result()
                    group_id = group.get("id")
                    group_display_name = group.get("displayName", "Unknown Group")
                    group_types = group.get("groupTypes", [])
                    group_type = determine_group_type(group_types)

                    # Process each user owner of this group
                    for owner in owners:
                        # Only process user objects, skip service principals
                        if owner.get("@odata.type") == "#microsoft.graph.user":
                            # Check if user is already in user_group_records as a member
                            existing_record = next(
                                (
                                    record
                                    for record in user_group_records
                                    if record["user_id"] == owner.get("id") and record["group_id"] == group_id
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
                                    "group_id": group_id,
                                    "user_principal_name": owner.get("userPrincipalName"),
                                    "group_display_name": group_display_name,
                                    "group_type": group_type,
                                    "membership_type": "Owner",
                                    "created_at": datetime.now().isoformat(),
                                    "last_updated": datetime.now().isoformat(),
                                }
                                user_group_records.append(user_group_record)

                except Exception as e:
                    logger.error(f"Failed to process group owners for {group.get('id', 'Unknown')}: {str(e)}")
                    continue

        # Store user group assignments
        if user_group_records:
            upsert_many("user_groupsV2", user_group_records)
            logger.info(f"Stored {len(user_group_records)} user group assignments from {groups_with_members} groups")

        # Count total memberships after sync (including existing ones)
        total_memberships = query(
            """
            SELECT COUNT(*) as total
            FROM user_groupsV2 
            WHERE tenant_id = ?
        """,
            (tenant_id,),
        )[0]["total"]

        # Update group member/owner counts in groups table
        update_group_counts(tenant_id)

        # Update group counts in usersV2 table
        update_user_group_counts(tenant_id)

        # Check for users who previously had groups but no longer have assignments
        # This catches users who were disabled and had their group memberships removed
        existing_group_users = query(
            """
            SELECT DISTINCT user_id, user_principal_name 
            FROM user_groupsV2 
            WHERE tenant_id = ?
        """,
            (tenant_id,),
        )

        current_user_ids = {record["user_id"] for record in user_group_records}

        # Find users who had groups before but don't now (likely disabled)
        users_to_check = []
        for existing_user in existing_group_users:
            if existing_user["user_id"] not in current_user_ids:
                users_to_check.append(existing_user["user_id"])

        # For these users, check if they're now inactive and mark their group memberships accordingly
        if users_to_check:
            user_status_query = f"""
                SELECT user_id, account_enabled, user_principal_name
                FROM usersV2 
                WHERE user_id IN ({",".join(["?" for _ in users_to_check])})
                AND tenant_id = ?
            """
            user_statuses = query(user_status_query, users_to_check + [tenant_id])

            for user_status in user_statuses:
                if not user_status["account_enabled"]:  # User is now inactive
                    # Remove their group memberships since they're disabled
                    execute_query(
                        """
                        DELETE FROM user_groupsV2 
                        WHERE user_id = ? AND tenant_id = ?
                    """,
                        (user_status["user_id"], tenant_id),
                    )
                    logger.info(f"Removed group memberships for disabled user: {user_status['user_principal_name']}")

        return {
            "status": "success",
            "groups_synced": len(group_records) if "group_records" in locals() else 0,
            "user_groups_synced": total_memberships,
            "inactive_users_cleaned": len(users_to_check) if users_to_check else 0,
        }

    except Exception as e:
        logger.error(f"Group sync failed for {tenant_name}: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }


def update_user_group_counts(tenant_id):
    """Update the group_count field in usersV2 table based on current group memberships"""
    try:
        logger.info(f"Updating group counts for users in tenant {tenant_id}")

        # Get current group counts for all users
        group_counts = query(
            """
            SELECT user_id, COUNT(*) as group_count
            FROM user_groupsV2 
            WHERE tenant_id = ?
            GROUP BY user_id
        """,
            (tenant_id,),
        )

        # Update each user's group count
        for user_count in group_counts:
            execute_query(
                """
                UPDATE usersV2 
                SET group_count = ?, last_updated = ?
                WHERE user_id = ? AND tenant_id = ?
            """,
                (
                    user_count["group_count"],
                    datetime.now().isoformat(),
                    user_count["user_id"],
                    tenant_id,
                ),
            )

        # Set group_count to 0 for users with no groups
        execute_query(
            """
            UPDATE usersV2 
            SET group_count = 0, last_updated = ?
            WHERE tenant_id = ? AND user_id NOT IN (
                SELECT DISTINCT user_id FROM user_groupsV2 WHERE tenant_id = ?
            )
        """,
            (
                datetime.now().isoformat(),
                tenant_id,
                tenant_id,
            ),
        )

        logger.info(f"Updated group counts for users in tenant {tenant_id}")

    except Exception as e:
        logger.error(f"Failed to update user group counts for tenant {tenant_id}: {str(e)}")


def update_group_counts(tenant_id):
    """Update the member_count and owner_count fields in groups table based on actual data"""
    try:
        logger.info(f"Updating group member/owner counts for tenant {tenant_id}")

        # Get member counts for each group
        member_counts = query(
            """
            SELECT group_id, COUNT(*) as member_count
            FROM user_groupsV2 
            WHERE tenant_id = ? AND membership_type IN ('Member', 'Owner')
            GROUP BY group_id
        """,
            (tenant_id,),
        )

        # Get owner counts for each group
        owner_counts = query(
            """
            SELECT group_id, COUNT(*) as owner_count
            FROM user_groupsV2 
            WHERE tenant_id = ? AND membership_type = 'Owner'
            GROUP BY group_id
        """,
            (tenant_id,),
        )

        # Create lookup dictionaries
        member_count_lookup = {mc["group_id"]: mc["member_count"] for mc in member_counts}
        owner_count_lookup = {oc["group_id"]: oc["owner_count"] for oc in owner_counts}

        # Update each group's counts
        for group_id in set(member_count_lookup.keys()) | set(owner_count_lookup.keys()):
            member_count = member_count_lookup.get(group_id, 0)
            owner_count = owner_count_lookup.get(group_id, 0)

            execute_query(
                """
                UPDATE groups 
                SET member_count = ?, owner_count = ?, last_updated = ?
                WHERE group_id = ? AND tenant_id = ?
            """,
                (
                    member_count,
                    owner_count,
                    datetime.now().isoformat(),
                    group_id,
                    tenant_id,
                ),
            )

        logger.info(f"Updated group member/owner counts for tenant {tenant_id}")

    except Exception as e:
        logger.error(f"Failed to update group counts for tenant {tenant_id}: {str(e)}")


def get_user_groups(tenant_id, user_id):
    """Get all groups for a specific user"""
    try:
        groups = query(
            """
            SELECT 
                g.group_id,
                g.group_display_name,
                g.group_description,
                g.group_type,
                ug.membership_type,
                ug.created_at as joined_date
            FROM user_groupsV2 ug
            JOIN groups g ON ug.group_id = g.group_id AND ug.tenant_id = g.tenant_id
            WHERE ug.user_id = ? AND ug.tenant_id = ?
            ORDER BY g.group_display_name
        """,
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
            """
            SELECT 
                u.user_id,
                u.user_principal_name,
                u.display_name,
                u.department,
                u.job_title,
                ug.membership_type,
                ug.created_at as joined_date
            FROM user_groupsV2 ug
            JOIN usersV2 u ON ug.user_id = u.user_id AND ug.tenant_id = u.tenant_id
            WHERE ug.group_id = ? AND ug.tenant_id = ?
            ORDER BY u.display_name
        """,
            (group_id, tenant_id),
        )
        return members
    except Exception as e:
        logger.error(f"Failed to get members for group {group_id}: {str(e)}")
        return []
