"""Users domain - HTTP and Timer triggers for user-related operations"""

import logging

import azure.functions as func

from db.db_client import execute_query, query
from shared.error_reporting import categorize_sync_errors
from shared.graph_beta_client import GraphBetaClient
from shared.graph_client import get_tenants
from shared.utils import clean_error_message, create_bulk_operation_response, create_error_response, create_success_response

from .helpers import sync_users


# HTTP SYNC FUNCTIONS
def http_users_sync(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for manual user sync"""
    try:
        logging.info("Starting manual user sync V2")
        tenants = get_tenants()
        total_users = 0
        results = []

        for tenant in tenants:
            try:
                result = sync_users(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(f"✓ {tenant['display_name']}: {result['users_synced']} users synced")
                    total_users += result["users_synced"]
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "users_synced": result["users_synced"],
                        }
                    )
                else:
                    logging.error(f"✗ {tenant['display_name']}: {result['error']}")
                    results.append(
                        {
                            "status": "error",
                            "tenant_id": tenant["tenant_id"],
                            "error": result.get("error", "Unknown error"),
                        }
                    )
            except Exception as e:
                logging.error(clean_error_message(str(e), tenant["display_name"]))
                results.append({"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)})

        failed_count = len([r for r in results if r["status"] == "error"])
        if failed_count > 0:
            categorize_sync_errors(results, "User V2 HTTP")

        return create_success_response(
            data={"total_users": total_users, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="user_sync_v2_http",
            message=f"Synced {total_users} users across {len(tenants)} tenants",
        )
    except Exception as e:
        error_msg = f"User sync V2 failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


# USER MANAGEMENT FUNCTIONS
def get_user(req: func.HttpRequest) -> func.HttpResponse:
    """Get individual user details"""
    try:
        user_id = req.route_params.get("user_id")
        tenant_id = req.params.get("tenant_id")

        if not user_id:
            return create_error_response("User ID is required", 400)
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        user_query = """
            SELECT u.*,
                   GROUP_CONCAT(DISTINCT ul.license_display_name) as licenses,
                   GROUP_CONCAT(DISTINCT r.role_display_name) as roles,
                   GROUP_CONCAT(DISTINCT g.display_name) as groups
            FROM usersV2 u
            LEFT JOIN user_licensesV2 ul ON u.tenant_id = ul.tenant_id AND u.user_id = ul.user_id AND ul.is_active = 1
            LEFT JOIN user_rolesV2 ur ON u.tenant_id = ur.tenant_id AND u.user_id = ur.user_id
            LEFT JOIN roles r ON ur.tenant_id = r.tenant_id AND ur.role_id = r.role_id
            LEFT JOIN user_groupsV2 ug ON u.tenant_id = ug.tenant_id AND u.user_id = ug.user_id AND ug.is_active = 1
            LEFT JOIN groups g ON ug.tenant_id = g.tenant_id AND ug.group_id = g.group_id
            WHERE u.user_id = ? AND u.tenant_id = ?
            GROUP BY u.user_id, u.tenant_id
        """

        user_result = query(user_query, (user_id, tenant_id))

        if not user_result:
            return create_error_response("User not found", 404)

        user = user_result[0]
        user["licenses"] = user["licenses"].split(",") if user["licenses"] else []
        user["roles"] = user["roles"].split(",") if user["roles"] else []
        user["groups"] = user["groups"].split(",") if user["groups"] else []

        return create_success_response(data=user, tenant_id=tenant_id, operation="get_user", message=f"Retrieved user {user_id}")

    except Exception as e:
        logging.error(f"Error retrieving user {user_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve user: {str(e)}", 500)


def get_users(req: func.HttpRequest) -> func.HttpResponse:
    """Get list of users for a tenant"""
    try:
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response("Tenant ID is required", 400)

        users_query = """
            SELECT u.user_id, u.display_name, u.user_principal_name, u.account_enabled,
                   u.created_date_time, u.last_sign_in_date_time, u.last_non_interactive_sign_in_date_time,
                   u.is_mfa_registered, u.strong_authentication_methods,
                   COUNT(DISTINCT ul.license_display_name) as license_count,
                   COUNT(DISTINCT ur.role_id) as role_count,
                   COUNT(DISTINCT ug.group_id) as group_count
            FROM usersV2 u
            LEFT JOIN user_licensesV2 ul ON u.tenant_id = ul.tenant_id AND u.user_id = ul.user_id AND ul.is_active = 1
            LEFT JOIN user_rolesV2 ur ON u.tenant_id = ur.tenant_id AND u.user_id = ur.user_id
            LEFT JOIN user_groupsV2 ug ON u.tenant_id = ug.tenant_id AND u.user_id = ug.user_id AND ug.is_active = 1
            WHERE u.tenant_id = ?
            GROUP BY u.user_id, u.tenant_id
            ORDER BY u.display_name
        """

        users = query(users_query, (tenant_id,))

        return create_success_response(
            data={"users": users, "count": len(users)}, tenant_id=tenant_id, operation="get_users", message=f"Retrieved {len(users)} users"
        )

    except Exception as e:
        logging.error(f"Error retrieving users for tenant {tenant_id}: {str(e)}")
        return create_error_response(f"Failed to retrieve users: {str(e)}", 500)


def edit_user(req: func.HttpRequest) -> func.HttpResponse:
    """Edit user properties"""
    try:
        body = req.get_json()
        if not body:
            return create_error_response("Request body is required", 400)

        user_id = body.get("user_id")
        tenant_id = body.get("tenant_id")

        if not user_id or not tenant_id:
            return create_error_response("user_id and tenant_id are required", 400)

        # Build update fields dynamically
        update_fields = []
        params = []

        editable_fields = [
            "display_name",
            "given_name",
            "surname",
            "job_title",
            "department",
            "office_location",
            "mobile_phone",
            "business_phones",
            "account_enabled",
        ]

        for field in editable_fields:
            if field in body:
                update_fields.append(f"{field} = ?")
                params.append(body[field])

        if not update_fields:
            return create_error_response("No valid fields to update", 400)

        params.extend([user_id, tenant_id])
        update_query = f"UPDATE usersV2 SET {', '.join(update_fields)} WHERE user_id = ? AND tenant_id = ?"

        execute_query(update_query, params)

        return create_success_response(
            data={"updated_fields": list(body.keys())}, tenant_id=tenant_id, operation="edit_user", message=f"Updated user {user_id}"
        )

    except Exception as e:
        logging.error(f"Error updating user: {str(e)}")
        return create_error_response(f"Failed to update user: {str(e)}", 500)


def disable_user(req: func.HttpRequest) -> func.HttpResponse:
    """Disable a user account"""
    try:
        user_id = req.route_params.get("user_id")
        tenant_id = req.params.get("tenant_id")

        if not user_id or not tenant_id:
            return create_error_response("user_id and tenant_id are required", 400)

        # Update database first
        execute_query("UPDATE usersV2 SET account_enabled = 0 WHERE user_id = ? AND tenant_id = ?", (user_id, tenant_id))

        # Update via Graph API
        client = GraphBetaClient(tenant_id)
        client.patch_user(user_id, {"accountEnabled": False})

        return create_success_response(
            data={"user_id": user_id, "disabled": True}, tenant_id=tenant_id, operation="disable_user", message=f"Disabled user {user_id}"
        )

    except Exception as e:
        logging.error(f"Error disabling user {user_id}: {str(e)}")
        return create_error_response(f"Failed to disable user: {str(e)}", 500)


def reset_user_password(req: func.HttpRequest) -> func.HttpResponse:
    """Reset user password"""
    try:
        user_id = req.route_params.get("user_id")
        tenant_id = req.params.get("tenant_id")

        if not user_id or not tenant_id:
            return create_error_response("user_id and tenant_id are required", 400)

        body = req.get_json()
        temp_password = body.get("temporary_password", "TempPass123!")
        force_change = body.get("force_change_password_next_sign_in", True)

        # Reset password via Graph API
        client = GraphBetaClient(tenant_id)
        client.patch_user(user_id, {"passwordProfile": {"password": temp_password, "forceChangePasswordNextSignIn": force_change}})

        return create_success_response(
            data={
                "user_id": user_id,
                "password_reset": True,
                "temporary_password": temp_password,
                "must_change_on_next_signin": force_change,
            },
            tenant_id=tenant_id,
            operation="reset_user_password",
            message=f"Reset password for user {user_id}",
        )

    except Exception as e:
        logging.error(f"Error resetting password for user {user_id}: {str(e)}")
        return create_error_response(f"Failed to reset password: {str(e)}", 500)


def create_user(req: func.HttpRequest) -> func.HttpResponse:
    """Create a new user"""
    try:
        body = req.get_json()
        if not body:
            return create_error_response("Request body is required", 400)

        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return create_error_response("tenant_id is required", 400)

        # Required fields for user creation
        required_fields = ["displayName", "userPrincipalName", "mailNickname"]
        for field in required_fields:
            if field not in body:
                return create_error_response(f"{field} is required", 400)

        # Create user via Graph API
        client = GraphBetaClient(tenant_id)

        user_data = {
            "displayName": body["displayName"],
            "userPrincipalName": body["userPrincipalName"],
            "mailNickname": body["mailNickname"],
            "accountEnabled": body.get("accountEnabled", True),
            "passwordProfile": {
                "password": body.get("password", "TempPass123!"),
                "forceChangePasswordNextSignIn": body.get("forceChangePasswordNextSignIn", True),
            },
        }

        # Add optional fields
        optional_fields = ["givenName", "surname", "jobTitle", "department", "officeLocation"]
        for field in optional_fields:
            if field in body:
                user_data[field] = body[field]

        created_user = client.create_user(user_data)

        return create_success_response(
            data=created_user,
            tenant_id=tenant_id,
            operation="create_user",
            message=f"Created user {created_user.get('userPrincipalName', 'unknown')}",
        )

    except Exception as e:
        logging.error(f"Error creating user: {str(e)}")
        return create_error_response(f"Failed to create user: {str(e)}", 500)


def delete_user(req: func.HttpRequest) -> func.HttpResponse:
    """Delete a user"""
    try:
        user_id = req.route_params.get("user_id")
        tenant_id = req.params.get("tenant_id")

        if not user_id or not tenant_id:
            return create_error_response("user_id and tenant_id are required", 400)

        # Delete from Graph API first
        client = GraphBetaClient(tenant_id)
        client.delete_user(user_id)

        # Remove from database
        execute_query("DELETE FROM usersV2 WHERE user_id = ? AND tenant_id = ?", (user_id, tenant_id))
        execute_query("DELETE FROM user_licensesV2 WHERE user_id = ? AND tenant_id = ?", (user_id, tenant_id))
        execute_query("DELETE FROM user_rolesV2 WHERE user_id = ? AND tenant_id = ?", (user_id, tenant_id))
        execute_query("DELETE FROM user_groupsV2 WHERE user_id = ? AND tenant_id = ?", (user_id, tenant_id))

        return create_success_response(
            data={"user_id": user_id, "deleted": True}, tenant_id=tenant_id, operation="delete_user", message=f"Deleted user {user_id}"
        )

    except Exception as e:
        logging.error(f"Error deleting user {user_id}: {str(e)}")
        return create_error_response(f"Failed to delete user: {str(e)}", 500)


def bulk_disable_users(req: func.HttpRequest) -> func.HttpResponse:
    """Bulk disable multiple users"""
    try:
        body = req.get_json()
        if not body:
            return create_error_response("Request body is required", 400)

        user_ids = body.get("user_ids", [])
        tenant_id = body.get("tenant_id")

        if not user_ids or not tenant_id:
            return create_error_response("user_ids and tenant_id are required", 400)

        results = []
        client = GraphBetaClient(tenant_id)

        for user_id in user_ids:
            try:
                # Update database
                execute_query("UPDATE usersV2 SET account_enabled = 0 WHERE user_id = ? AND tenant_id = ?", (user_id, tenant_id))

                # Update via Graph API
                client.patch_user(user_id, {"accountEnabled": False})

                results.append({"user_id": user_id, "status": "success"})

            except Exception as e:
                results.append({"user_id": user_id, "status": "error", "error": str(e)})

        successful = len([r for r in results if r["status"] == "success"])

        return create_bulk_operation_response(
            results=results, tenant_id=tenant_id, operation="bulk_disable_users", message=f"Disabled {successful}/{len(user_ids)} users"
        )

    except Exception as e:
        logging.error(f"Error in bulk disable users: {str(e)}")
        return create_error_response(f"Bulk disable failed: {str(e)}", 500)
