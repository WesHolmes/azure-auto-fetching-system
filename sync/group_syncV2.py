from datetime import datetime
import logging

from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient
from sql.databaseV2 import init_schema, upsert_many
from utils.http import clean_error_message


logger = logging.getLogger(__name__)


def fetch_beta_tenant_groups(tenant_id):
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
            top=999,
        )

        logger.info(f"Successfully fetched {len(groups) if groups else 0} groups for tenant {tenant_id}")
        return groups

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to fetch groups")
        logger.error(error_msg)

        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)

        # Log the detailed error for debugging
        if hasattr(e, "response") and hasattr(e.response, "text"):
            logger.error(f"Response body: {e.response.text}")
        # Return empty list but continue processing
        return []


def fetch_v1_tenant_groups(tenant_id):
    """Fetch tenant-level group information from v1.0 endpoint"""
    try:
        logger.info(f"Fetching tenant groups for {tenant_id} using v1.0 endpoint")
        graph = GraphClient(tenant_id)

        # Get all groups with basic information from v1.0 endpoint
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
            top=999,
        )

        logger.info(f"Successfully fetched {len(groups) if groups else 0} groups from v1.0 endpoint for tenant {tenant_id}")
        return groups

    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch groups from v1.0 endpoint")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


def fetch_beta_group_members(tenant_id, group_id):
    """Fetch detailed member information for a specific group"""
    try:
        # logger.info(f"Fetching members for group {group_id}")
        graph = GraphBetaClient(tenant_id)
        members = graph.get(f"/groups/{group_id}/members", select=["id", "userPrincipalName", "displayName"])
        # logger.info(f"Group {group_id}: Found {len(members) if members else 0} members")
        return members
    except Exception as e:
        logger.warning(f"Failed to fetch members for group {group_id}: {str(e)}")
        return []


def fetch_v1_group_members(tenant_id, group_id):
    """Fetch detailed member information for a specific group from v1.0 endpoint"""
    try:
        # logger.info(f"Fetching members for group {group_id} from v1.0 endpoint")
        graph = GraphClient(tenant_id)
        members = graph.get(f"/groups/{group_id}/members", select=["id", "userPrincipalName", "displayName"])
        # logger.info(f"Group {group_id}: Found {len(members) if members else 0} members from v1.0 endpoint")
        return members
    except Exception as e:
        logger.warning(f"Failed to fetch members for group {group_id} from v1.0 endpoint: {str(e)}")
        return []


def fetch_beta_group_owners(tenant_id, group_id):
    """Fetch detailed owner information for a specific group"""
    try:
        # logger.info(f"Fetching owners for group {group_id}")
        graph = GraphBetaClient(tenant_id)
        owners = graph.get(f"/groups/{group_id}/owners", select=["id", "userPrincipalName", "displayName"])
        # logger.info(f"Group {group_id}: Found {len(owners) if owners else 0} owners")
        return owners
    except Exception as e:
        logger.warning(f"Failed to fetch owners for group {group_id}: {str(e)}")
        return []


def fetch_v1_group_owners(tenant_id, group_id):
    """Fetch detailed owner information for a specific group from v1.0 endpoint"""
    try:
        # logger.info(f"Fetching owners for group {group_id} from v1.0 endpoint")
        graph = GraphClient(tenant_id)
        owners = graph.get(f"/groups/{group_id}/owners", select=["id", "userPrincipalName", "displayName"])
        # logger.info(f"Group {group_id}: Found {len(owners) if owners else 0} owners from v1.0 endpoint")
        return owners
    except Exception as e:
        logger.warning(f"Failed to fetch owners for group {group_id} from v1.0 endpoint: {str(e)}")
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


def transform_group_data(groups, tenant_id, is_premium=False):
    """Transform group data for database storage"""
    try:
        logger.info(f"Transforming {len(groups)} groups for tenant {tenant_id}")

        group_records = []
        user_group_records = []

        for group in groups:
            group_types = group.get("groupTypes", [])
            group_type = determine_group_type(group_types)

            # Fetch members and owners for this group using the appropriate endpoint
            if is_premium:
                members = fetch_beta_group_members(tenant_id, group.get("id"))
                owners = fetch_beta_group_owners(tenant_id, group.get("id"))
            else:
                members = fetch_v1_group_members(tenant_id, group.get("id"))
                owners = fetch_v1_group_owners(tenant_id, group.get("id"))

            # Count user members and owners (users have userPrincipalName, service principals don't)
            user_members = [m for m in members if m.get("userPrincipalName")]
            user_owners = [o for o in owners if o.get("userPrincipalName")]

            # Log what we found for debugging
            total_members = len(members) if members else 0
            total_owners = len(owners) if owners else 0
            logger.debug(
                f"Group {group.get('id')}: {total_members} total members, {len(user_members)} users; {total_owners} total owners, {len(user_owners)} users"
            )

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

        logger.info(f"Transformed {len(group_records)} groups and {len(user_group_records)} user group assignments for tenant {tenant_id}")
        return group_records, user_group_records

    except Exception as e:
        # Use helper function for clean error messages
        error_msg = clean_error_message(str(e), "Failed to transform groups")
        logger.error(error_msg)
        # Log full error details at debug level for troubleshooting
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def sync_groups(tenant_id, tenant_name):
    """Main function to sync groups and their assignments"""

    # Initialize database schema
    init_schema()

    try:
        logger.info(f"Starting group sync for {tenant_name}")
        start_time = datetime.now()

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

        # Now fetch tenant groups using the appropriate endpoint based on tenant capability
        if is_premium:
            # Premium tenant - use beta endpoint for advanced features
            groups = fetch_beta_tenant_groups(tenant_id)
        else:
            # Non-premium tenant - use v1.0 endpoint for basic features
            groups = fetch_v1_tenant_groups(tenant_id)

        if not groups:
            logger.warning(f"No groups found for tenant {tenant_id}")
            return {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "groups_synced": 0,
                "user_groups_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Transform and get group data using the appropriate endpoint based on tenant capability
        group_records, user_group_records = transform_group_data(groups, tenant_id, is_premium)

        # Store in database
        from sql.databaseV2 import get_connection

        conn = get_connection()
        cursor = conn.cursor()

        # Clear existing records for this tenant first
        cursor.execute("DELETE FROM groups WHERE tenant_id = ?", (tenant_id,))
        cursor.execute("DELETE FROM user_groupsV2 WHERE tenant_id = ?", (tenant_id,))
        conn.commit()
        conn.close()

        # Insert new group records
        if group_records:
            upsert_many("groups", group_records)
            logger.info(f"Stored {len(group_records)} groups")

        # Insert new user group records
        if user_group_records:
            upsert_many("user_groupsV2", user_group_records)
            logger.info(f"Stored {len(user_group_records)} user group assignments")

        # Count total groups and memberships after sync
        from sql.databaseV2 import query

        total_groups = query(
            """
            SELECT COUNT(*) as total
            FROM groups 
            WHERE tenant_id = ?
        """,
            (tenant_id,),
        )[0]["total"]

        total_memberships = query(
            """
            SELECT COUNT(*) as total
            FROM user_groupsV2 
            WHERE tenant_id = ?
        """,
            (tenant_id,),
        )[0]["total"]

        duration = (datetime.now() - start_time).total_seconds()

        # Log final summary
        logger.info(f"=== GROUP SYNC SUMMARY FOR {tenant_name} ===")
        logger.info(f"Groups processed: {len(groups)}")
        logger.info(f"Groups stored: {len(group_records)}")
        logger.info(f"User group assignments stored: {len(user_group_records)}")
        logger.info(f"Total groups in database: {total_groups}")
        logger.info(f"Total memberships in database: {total_memberships}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 50)

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "groups_synced": total_groups,
            "user_groups_synced": total_memberships,
            "duration_seconds": duration,
        }

    except Exception as e:
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
        }


def get_user_groups(tenant_id, user_id):
    """Get all groups for a specific user"""
    try:
        from sql.databaseV2 import query

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
        from sql.databaseV2 import query

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
