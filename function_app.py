from datetime import datetime, timedelta
import json
import logging

import azure.functions as func

from analysis.user_analysis import (
    calculate_inactive_users,
    calculate_license_optimization,
    calculate_mfa_compliance,
)
from core.graph_beta_client import GraphBetaClient
from core.tenant_manager import get_tenants
from sql.databaseV2 import execute_query, query, upsert_many
from sync.group_syncV2 import sync_groups
from sync.license_syncV2 import sync_licenses_v2
from sync.role_syncV2 import sync_rolesV2
from sync.subscription_syncV2 import sync_subscriptions
from sync.user_syncV2 import sync_users as sync_users_v2
from utils.error_reporting import aggregate_recent_sync_errors, categorize_sync_errors
from utils.http import clean_error_message, create_bulk_operation_response, create_error_response, create_success_response


# from sync.hibp_sync import sync_hibp_breaches

app = func.FunctionApp()


# TIMER TRIGGERS (Scheduled Functions)


@app.schedule(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def users_syncV2(timer: func.TimerRequest) -> None:
    """V2 User sync using new database schema"""
    if timer.past_due:
        logging.warning("User sync V2 timer is past due!")

    tenants = get_tenants()
    tenants.reverse()  # Process in reverse order
    results = []

    for tenant in tenants:
        try:
            result = sync_users_v2(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(f"✓ V2 {tenant['display_name']}: {result['users_synced']} users synced")
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "users_synced": result["users_synced"],
                        "user_licenses_synced": result.get("user_licenses_replaced", 0),
                    }
                )

                # Run analysis after successful sync
                try:
                    inactive_result = calculate_inactive_users(tenant["tenant_id"])
                    logging.info(f"  Inactive users: {inactive_result.get('inactive_count', 0)}")

                    mfa_result = calculate_mfa_compliance(tenant["tenant_id"])
                    logging.info(f"  MFA compliance: {mfa_result.get('compliance_rate', 0)}%")

                except Exception as e:
                    logging.error(f"Analysis error: {str(e)}")

            else:
                logging.error(f"✗ V2 {tenant['display_name']}: {result['error']}")
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

    # Use centralized error reporting
    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "User V2")


@app.schedule(schedule="15 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def licenses_syncV2(timer: func.TimerRequest) -> None:
    """V2 License sync using new database schema"""
    if timer.past_due:
        logging.warning("License sync V2 timer is past due!")

    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_licenses_v2(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(f"✓ V2 {tenant['display_name']}: {result['licenses_synced']} licenses synced")
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "licenses_synced": result["licenses_synced"],
                        "user_licenses_synced": result.get("user_licenses_replaced", 0),
                        "inactive_licenses_updated": result.get("inactive_licenses_updated", 0),
                    }
                )
            else:
                logging.error(f"✗ V2 {tenant['display_name']}: {result['error']}")
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
        categorize_sync_errors(results, "License V2")


@app.schedule(schedule="0 */2 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def role_syncV2(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for role sync using new database schema"""
    if timer.past_due:
        logging.info("Role sync V2 timer is past due!")

    logging.info("Starting scheduled role sync V2")
    tenants = get_tenants()
    tenant_ids = [tenant["tenant_id"] for tenant in tenants]

    result = sync_rolesV2(tenant_ids)

    if result["status"] == "completed":
        logging.info(
            f"  V2 Role sync completed: {result['total_roles_synced']} roles, {result['total_role_assignments_synced']} role assignments across {result['successful_tenants']} tenants"
        )
        if result["failed_tenants"] > 0:
            categorize_sync_errors(result["results"], "Role V2")
    else:
        logging.error(f"  V2 Role sync failed: {result.get('error', 'Unknown error')}")


@app.schedule(schedule="30 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def group_syncV2(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for group sync using new database schema"""
    if timer.past_due:
        logging.info("Group sync V2 timer is past due!")

    logging.info("Starting scheduled group sync V2")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_groups(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ V2 {tenant['display_name']}: {result['groups_synced']} groups synced, {result.get('user_groups_synced', 0)} user memberships synced"
                )
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "groups_synced": result["groups_synced"],
                        "user_groups_synced": result.get("user_groups_synced", 0),
                    }
                )
            else:
                logging.error(f"✗ V2 {tenant['display_name']}: {result['error']}")
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
        categorize_sync_errors(results, "Group V2")


@app.schedule(schedule="45 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def subscription_syncV2(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for subscription sync using new database schema"""
    if timer.past_due:
        logging.info("Subscription sync V2 timer is past due!")

    logging.info("Starting scheduled subscription sync V2")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_subscriptions(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(f"✓ V2 {tenant['display_name']}: {result['subscriptions_synced']} subscriptions synced")
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "subscriptions_synced": result["subscriptions_synced"],
                    }
                )
            else:
                logging.error(f"✗ V2 {tenant['display_name']}: {result['error']}")
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
        categorize_sync_errors(results, "Subscription V2")


# HTTP TRIGGERS (Manual Endpoints)


@app.route(route="sync/usersV2", methods=["POST"])
def user_sync_v2_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint for user synchronization across all tenants"""
    try:
        logging.info("Starting manual user sync V2")
        from sync.user_syncV2 import sync_users as sync_users_v2

        tenants = get_tenants()
        total_users = 0
        results = []

        for tenant in tenants:
            try:
                result = sync_users_v2(tenant["tenant_id"], tenant["display_name"])
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

        # Use centralized error reporting (same pattern as other sync functions)
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


@app.route(route="sync/licenses", methods=["POST"])
def license_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint for license synchronization across all tenants"""
    try:
        logging.info("Starting manual license sync")
        from sync.license_syncV2 import sync_licenses_v2

        tenants = get_tenants()
        total_licenses = 0
        total_assignments = 0
        results = []

        for tenant in tenants:
            try:
                result = sync_licenses_v2(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(
                        f"✓ {tenant['display_name']}: {result['licenses_synced']} licenses, {result.get('user_licenses_replaced', 0)} user assignments replaced"
                    )
                    total_licenses += result["licenses_synced"]
                    total_assignments += result["user_licenses_replaced"]
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "licenses_synced": result["licenses_synced"],
                            "user_licenses_synced": result["user_licenses_replaced"],
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

        # Use centralized error reporting (same pattern as other sync functions)
        failed_count = len([r for r in results if r["status"] == "error"])
        if failed_count > 0:
            categorize_sync_errors(results, "License HTTP")

        return create_success_response(
            data={"total_licenses": total_licenses, "total_assignments": total_assignments, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="license_sync_http",
            message=f"Synced {total_licenses} licenses and {total_assignments} user assignments across {len(tenants)} tenants",
        )

    except Exception as e:
        error_msg = f"License sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.route(route="sync/roles", methods=["POST"])
def role_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint for role synchronization across all tenants"""
    try:
        logging.info("Starting manual role sync")

        tenants = get_tenants()
        tenant_ids = [tenant["tenant_id"] for tenant in tenants]

        result = sync_rolesV2(tenant_ids)

        if result["status"] == "completed":
            successful_tenants = result["successful_tenants"]
            failed_tenants = result["failed_tenants"]
            total_roles = result["total_roles_synced"]
            total_role_assignments = result["total_role_assignments_synced"]

            if failed_tenants > 0:
                categorize_sync_errors(result["results"], "Role")

            response_msg = f"Role sync completed: {total_roles} roles, {total_role_assignments} role assignments synced across {successful_tenants} tenants"
            if failed_tenants > 0:
                response_msg += f" ({failed_tenants} tenants failed)"

            return create_success_response(
                data={
                    "total_roles": total_roles,
                    "total_role_assignments": total_role_assignments,
                    "successful_tenants": successful_tenants,
                    "failed_tenants": failed_tenants,
                },
                tenant_id="multi_tenant",
                tenant_name="all_tenants",
                operation="role_sync_http",
                message=response_msg,
            )
        else:
            error_msg = f"Role sync failed: {result.get('error', 'Unknown error')}"
            logging.error(error_msg)
            return create_error_response(error_message=error_msg, status_code=500)

    except Exception as e:
        error_msg = f"Role sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.route(route="sync/groups", methods=["POST"])
def groups_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint for group synchronization across all tenants"""
    try:
        logging.info("Starting manual group sync")
        from sync.group_syncV2 import sync_groups

        tenants = get_tenants()
        results = []

        for tenant in tenants:
            try:
                result = sync_groups(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(
                        f"✓ {tenant['display_name']}: {result['groups_synced']} groups, {result.get('user_groups_synced', 0)} user memberships synced"
                    )
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "groups_synced": result["groups_synced"],
                            "user_groups_synced": result.get("user_groups_synced", 0),
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

        # Use centralized error reporting (same pattern as other sync functions)
        failed_count = len([r for r in results if r["status"] == "error"])
        if failed_count > 0:
            categorize_sync_errors(results, "Groups HTTP")

        total_groups = sum(r.get("groups_synced", 0) for r in results if r["status"] == "completed")
        total_user_groups = sum(r.get("user_groups_synced", 0) for r in results if r["status"] == "completed")

        return create_success_response(
            data={"total_groups": total_groups, "total_user_groups": total_user_groups, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="groups_sync_http",
            message=f"Synced {total_groups} groups and {total_user_groups} user-group assignments across {len(tenants)} tenants",
        )

    except Exception as e:
        error_msg = f"Group sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.route(route="sync/subscriptions", methods=["POST"])
def subscriptions_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint for subscription synchronization across all tenants"""
    try:
        logging.info("Starting manual subscription sync")
        tenants = get_tenants()
        results = []

        for tenant in tenants:
            try:
                result = sync_subscriptions(tenant["tenant_id"], tenant["display_name"])
                if result["status"] == "success":
                    logging.info(f"✓ {tenant['display_name']}: {result['subscriptions_synced']} subscriptions synced")
                    results.append(
                        {
                            "status": "completed",
                            "tenant_id": tenant["tenant_id"],
                            "subscriptions_synced": result["subscriptions_synced"],
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

        # Use centralized error reporting (same pattern as other sync functions)
        failed_count = len([r for r in results if r["status"] == "error"])
        if failed_count > 0:
            categorize_sync_errors(results, "Subscriptions HTTP")

        total_subscriptions = sum(r.get("subscriptions_synced", 0) for r in results if r["status"] == "completed")

        return create_success_response(
            data={"total_subscriptions": total_subscriptions, "tenants_processed": len(tenants)},
            tenant_id="multi_tenant",
            tenant_name="all_tenants",
            operation="subscriptions_sync_http",
            message=f"Synced {total_subscriptions} subscriptions across {len(tenants)} tenants",
        )

    except Exception as e:
        error_msg = f"Subscription sync failed: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.route(route="tenant/users/{user_id}", methods=["GET"])
def get_tenant_user_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single user data by user_id"""
    # Returns individual user details for a specific user in a specific tenant

    try:
        # extract user_id from URL path
        user_id = req.route_params.get("user_id")

        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"User by ID API request for user: {user_id} in tenant: {tenant_id}")

        if not user_id:
            return create_error_response(error_message="user_id is required in URL path", status_code=400)

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # Single Graph API call - much faster
        graph_client = GraphBetaClient(tenant_id)
        tenant_details = graph_client.get_tenant_details(tenant_id)

        # Handle the fact that get_tenant_details returns a list
        if tenant_details and len(tenant_details) > 0:
            tenant_name = tenant_details[0].get("displayName", tenant_id)
        else:
            tenant_name = tenant_id

        logging.info(f"Processing user data for user {user_id} in tenant: {tenant_name}")

        # fetch specific user data
        user_query = """
            SELECT 
                user_id,
                user_principal_name,
                primary_email,
                display_name,
                department,
                job_title,
                office_location,
                mobile_phone,
                account_type,
                account_enabled,
                is_global_admin,
                is_mfa_compliant,
                license_count,
                group_count,
                last_sign_in_date,
                last_password_change,
                created_at,
                last_updated
            FROM usersV2 
            WHERE tenant_id = ? AND user_id = ?
        """
        user_result = query(user_query, (tenant_id, user_id))

        if not user_result:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"User {user_id} not found in tenant {tenant_id}"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        user = user_result[0]

        # transform user data for frontend consumption
        user_data = {
            "user_id": user["user_id"],
            "user_principal_name": user["user_principal_name"],
            "primary_email": user["primary_email"],
            "display_name": user["display_name"],
            "department": user["department"],
            "job_title": user["job_title"],
            "office_location": user["office_location"],
            "mobile_phone": user["mobile_phone"],
            "account_type": user["account_type"],
            "account_enabled": bool(user["account_enabled"]),
            "is_global_admin": bool(user["is_global_admin"]),
            "is_mfa_compliant": bool(user["is_mfa_compliant"]),
            "license_count": user["license_count"],
            "group_count": user["group_count"],
            "last_sign_in_date": user["last_sign_in_date"],
            "last_password_change": user["last_password_change"],
            "created_at": user["created_at"],
            "last_updated": user["last_updated"],
        }

        # build response structure using utility function
        return create_success_response(
            data=user_data, tenant_id=tenant_id, tenant_name=tenant_name, operation="get_tenant_user_by_id", resource_id=user_id
        )

    except Exception as e:
        error_msg = f"Error retrieving user data: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.route(route="tenant/users", methods=["GET"])
def get_tenant_users(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant user data"""
    # Returns structured response with user optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"Users API request for tenant: {tenant_id}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # single Graph API call - much faster
        graph_client = GraphBetaClient(tenant_id)
        tenant_details = graph_client.get_tenant_details(tenant_id)

        # handle fact get_tenant_details returns a list
        if tenant_details and len(tenant_details) > 0:
            tenant_name = tenant_details[0].get("displayName", tenant_id)
        else:
            tenant_name = tenant_id

        logging.info(f"Processing user data for tenant: {tenant_name}")

        # grab user data
        # basic user counts
        total_users_query = "SELECT COUNT(*) as count FROM usersV2 WHERE tenant_id = ?"
        total_users_result = query(total_users_query, (tenant_id,))

        active_users_query = "SELECT COUNT(*) as count FROM usersV2 WHERE tenant_id = ? AND account_enabled = 1"
        active_users_result = query(active_users_query, (tenant_id,))

        # admin users count
        admin_users_query = "SELECT COUNT(DISTINCT user_id) as count FROM user_rolesV2 WHERE tenant_id = ?"
        admin_users_result = query(admin_users_query, (tenant_id,))

        # never signed in users
        never_signed_in_query = (
            "SELECT COUNT(*) as count FROM usersV2 WHERE tenant_id = ? AND last_sign_in_date IS NULL AND account_enabled = 1"
        )
        never_signed_in_result = query(never_signed_in_query, (tenant_id,))

        # grab analysis data
        mfa_result = calculate_mfa_compliance(tenant_id)
        inactive_result = calculate_inactive_users(tenant_id)

        # calculate metrics
        total_users = total_users_result[0]["count"] if total_users_result else 0
        active_users = active_users_result[0]["count"] if active_users_result else 0
        inactive_users = inactive_result.get("inactive_count", 0)  # Use correct inactive count from analysis
        admin_users = admin_users_result[0]["count"] if admin_users_result else 0
        never_signed_in = never_signed_in_result[0]["count"] if never_signed_in_result else 0

        # fetch actual user data for the data field
        users_query = """
            SELECT 
                user_id,
                user_principal_name,
                primary_email,
                display_name,
                department,
                job_title,
                office_location,
                mobile_phone,
                account_type,
                account_enabled,
                is_global_admin,
                is_mfa_compliant,
                license_count,
                group_count,
                last_sign_in_date,
                last_password_change,
                created_at,
                last_updated
            FROM usersV2 
            WHERE tenant_id = ? 
            ORDER BY display_name
        """
        users_result = query(users_query, (tenant_id,))

        # transform user data for frontend consumption
        users_data = []
        for user in users_result:
            users_data.append(
                {
                    "user_id": user["user_id"],
                    "user_principal_name": user["user_principal_name"],
                    "primary_email": user["primary_email"],
                    "display_name": user["display_name"],
                    "department": user["department"],
                    "job_title": user["job_title"],
                    "office_location": user["office_location"],
                    "mobile_phone": user["mobile_phone"],
                    "account_type": user["account_type"],
                    "account_enabled": bool(user["account_enabled"]),
                    "is_global_admin": bool(user["is_global_admin"]),
                    "is_mfa_compliant": bool(user["is_mfa_compliant"]),
                    "license_count": user["license_count"],
                    "group_count": user["group_count"],
                    "last_sign_in_date": user["last_sign_in_date"],
                    "last_password_change": user["last_password_change"],
                    "created_at": user["created_at"],
                    "last_updated": user["last_updated"],
                }
            )

        # generate user optimization actions
        actions = []

        # action 1: never signed in users
        if never_signed_in > 0:
            actions.append(
                {
                    "title": "Review Unused Accounts",
                    "description": f"{never_signed_in} users have never signed in - consider deactivating",
                    "action": "review",
                }
            )

        # action 2: inactive users
        if inactive_users > 0:
            actions.append(
                {
                    "title": "Review Inactive Users",
                    "description": f"{inactive_users} inactive user accounts - verify if still needed",
                    "action": "review",
                }
            )

        # action 3: MFA non-compliance
        non_compliant_users = mfa_result.get("non_compliant", 0)
        if non_compliant_users > 0:
            actions.append(
                {
                    "title": "Enable MFA for Users",
                    "description": f"{non_compliant_users} users without MFA enabled - security risk",
                    "action": "secure",
                }
            )

        # action 4: admin MFA compliance
        admin_non_compliant = mfa_result.get("admin_non_compliant", 0)
        if admin_non_compliant > 0:
            actions.append(
                {
                    "title": "Secure Admin Accounts",
                    "description": f"{admin_non_compliant} admin users without MFA - critical security risk",
                    "action": "secure",
                }
            )

        # build response structure using utility function
        return create_success_response(
            data=users_data,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="get_tenant_users",
            metrics={
                "total_users": total_users,
                "active_users": active_users,
                "inactive_users": inactive_users,
                "admin_users": admin_users,
                "never_signed_in_users": never_signed_in,
                "mfa_compliance_rate": mfa_result.get("compliance_rate", 0),
                "mfa_enabled_users": mfa_result.get("mfa_enabled", 0),
                "risk_level": mfa_result.get("risk_level", "unknown"),
            },
            actions=actions,
        )

    except Exception as e:
        error_msg = f"Error retrieving user data: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({"success": False, "error": error_msg}), status_code=500, headers={"Content-Type": "application/json"}
        )


@app.route(route="tenant/users/edit", methods=["PATCH"])
def edit_user(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP PATCH endpoint to edit an existing user account"""

    try:
        # Parse request body
        try:
            request_data = req.get_json()
        except Exception as e:
            return create_error_response(error_message=f"Invalid JSON in request body: {str(e)}", status_code=400)

        # Extract and validate required fields
        tenant_id = request_data.get("tenant_id")
        user_id = request_data.get("user_id")
        user_updates = request_data.get("user_updates", {})

        if not user_id:
            return create_error_response(error_message="user_id is required in request body", status_code=400)

        if not tenant_id:
            return create_error_response(error_message="tenant_id is required", status_code=400)

        if not user_updates:
            return create_error_response(error_message="user_updates object is required", status_code=400)

        # Add detailed logging for debugging
        logging.info("=== USER EDIT DEBUG LOG ===")
        logging.info(f"Editing user ID: {user_id}")
        logging.info(f"Tenant ID: {tenant_id}")
        logging.info(f"User updates received: {user_updates}")
        logging.info(f"All update keys: {list(user_updates.keys())}")

        # Check if user exists and belongs to the specified tenant
        existing_user_query = "SELECT * FROM usersV2 WHERE user_id = ? AND tenant_id = ?"
        existing_user_result = query(existing_user_query, (user_id, tenant_id))

        if not existing_user_result:
            return create_error_response(error_message=f"User {user_id} not found in tenant {tenant_id}", status_code=404)

        existing_user = existing_user_result[0]
        logging.info(f"Found existing user: {existing_user['display_name']}")

        # Prepare update data for Graph API (only include fields that are being updated)
        graph_update_data = {}

        # Map frontend field names to Graph API field names
        field_mapping = {
            "displayName": "displayName",
            "department": "department",
            "jobTitle": "jobTitle",
            "officeLocation": "officeLocation",
            "mobilePhone": "mobilePhone",
        }

        for frontend_field, graph_field in field_mapping.items():
            if frontend_field in user_updates and user_updates[frontend_field] is not None:
                graph_update_data[graph_field] = user_updates[frontend_field]
                logging.info(f"Will update {graph_field}: '{user_updates[frontend_field]}'")

        if not graph_update_data:
            return create_error_response(error_message="No valid fields to update", status_code=400)

        # Update user via Graph API
        graph_client = GraphBetaClient(tenant_id)
        logging.info(f"Updating user {user_id} via Graph API with data: {graph_update_data}")

        try:
            update_result = graph_client.update_user(user_id, graph_update_data)
        except Exception as graph_error:
            error_msg = f"Graph API error: {str(graph_error)}"
            logging.error(f"Failed to update user via Graph API: {error_msg}")
            return create_error_response(error_message=f"Failed to update user: {error_msg}", status_code=500)

        if update_result.get("status") != "success":
            error_msg = update_result.get("error", "Unknown error updating user")
            logging.error(f"Failed to update user via Graph API: {error_msg}")
            return create_error_response(error_message=f"Failed to update user: {error_msg}", status_code=500)

        # Update local database
        update_fields = []
        update_values = []

        # Map Graph API fields to database fields
        db_field_mapping = {
            "displayName": "display_name",
            "department": "department",
            "jobTitle": "job_title",
            "officeLocation": "office_location",
            "mobilePhone": "mobile_phone",
        }

        for graph_field, db_field in db_field_mapping.items():
            if graph_field in graph_update_data:
                update_fields.append(f"{db_field} = ?")
                update_values.append(graph_update_data[graph_field])

        # Add timestamp and user_id for WHERE clause
        update_values.append(datetime.now().isoformat())
        update_values.append(user_id)
        update_values.append(tenant_id)

        # Build and execute update query
        update_query = f"""
            UPDATE usersV2 
            SET {", ".join(update_fields)}, last_updated = ?
            WHERE user_id = ? AND tenant_id = ?
        """

        try:
            execute_query(update_query, update_values)
            logging.info(f"Successfully updated user {user_id} in database")
        except Exception as db_error:
            logging.warning(f"Graph API update succeeded but local DB update failed for user {user_id}: {str(db_error)}")
            # Note: user is updated in Graph but local DB might be out of sync

        # Fetch updated user data for response
        updated_user_query = "SELECT * FROM usersV2 WHERE user_id = ? AND tenant_id = ?"
        updated_user_result = query(updated_user_query, (user_id, tenant_id))

        if updated_user_result:
            updated_user = updated_user_result[0]
            # Transform to frontend-friendly format
            response_user_data = {
                "user_id": updated_user["user_id"],
                "user_principal_name": updated_user["user_principal_name"],
                "primary_email": updated_user["primary_email"],
                "display_name": updated_user["display_name"],
                "department": updated_user["department"],
                "job_title": updated_user["job_title"],
                "office_location": updated_user["office_location"],
                "mobile_phone": updated_user["mobile_phone"],
                "account_type": updated_user["account_type"],
                "account_enabled": bool(updated_user["account_enabled"]),
                "is_global_admin": bool(updated_user["is_global_admin"]),
                "is_mfa_compliant": bool(updated_user["is_mfa_compliant"]),
                "license_count": updated_user["license_count"],
                "group_count": updated_user["group_count"],
                "last_sign_in_date": updated_user["last_sign_in_date"],
                "last_password_change": updated_user["last_password_change"],
                "created_at": updated_user["created_at"],
                "last_updated": updated_user["last_updated"],
            }
        else:
            response_user_data = None

        return create_success_response(
            data=response_user_data,
            tenant_id=tenant_id,
            tenant_name=tenant_id,  # Use tenant_id as fallback since we don't have tenant_name
            operation="edit_user",
            message=f"User {existing_user['display_name']} updated successfully",
            updated_fields=list(graph_update_data.keys()),
        )

    except Exception as e:
        error_msg = f"Error updating user: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.schedule(schedule="0 25 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def get_tenant_licenses(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for licenses analysis across all tenants"""
    if timer.past_due:
        logging.warning("Licenses analysis timer is past due!")

    logging.info("Starting scheduled licenses analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing licenses for tenant: {tenant_name}")

            # Query license data for this tenant
            total_licenses_query = "SELECT COUNT(DISTINCT license_display_name) as count FROM licenses WHERE tenant_id = ?"
            total_licenses_result = query(total_licenses_query, (tenant_id,))

            total_assignments_query = "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ?"
            total_assignments_result = query(total_assignments_query, (tenant_id,))

            active_assignments_query = "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 1"
            active_assignments_result = query(active_assignments_query, (tenant_id,))

            total_cost_query = "SELECT SUM(monthly_cost) as total_cost FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 1"
            total_cost_result = query(total_cost_query, (tenant_id,))

            # Calculate metrics
            total_licenses = total_licenses_result[0]["count"] if total_licenses_result else 0
            total_assignments = total_assignments_result[0]["count"] if total_assignments_result else 0
            active_assignments = active_assignments_result[0]["count"] if active_assignments_result else 0
            total_cost = total_cost_result[0]["total_cost"] if total_cost_result and total_cost_result[0]["total_cost"] else 0

            # Generate optimization actions
            actions = []
            if total_assignments > 0 and active_assignments < total_assignments:
                inactive_count = total_assignments - active_assignments
                actions.append(f"Review {inactive_count} inactive license assignments")

            if total_cost > 0:
                actions.append(f"Monthly cost: ${total_cost:.2f}")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_licenses": total_licenses,
                "total_assignments": total_assignments,
                "active_assignments": active_assignments,
                "total_monthly_cost": total_cost,
                "actions": actions,
            }

            logging.info(f"✓ {tenant_name}: {total_licenses} licenses, {active_assignments}/{total_assignments} active assignments")
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    successful_count = len([r for r in results if r["status"] == "completed"])
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Licenses analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Licenses analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_licenses_all = sum(r.get("total_licenses", 0) for r in results if r["status"] == "completed")
    total_assignments_all = sum(r.get("total_assignments", 0) for r in results if r["status"] == "completed")
    total_cost_all = sum(r.get("total_monthly_cost", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_licenses_all} licenses, {total_assignments_all} assignments, ${total_cost_all:.2f} monthly cost"
    )


@app.route(route="tenant/licenses", methods=["GET"])
def get_tenant_licenses_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant license analysis"""
    # Returns structured response with license optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"Licenses analysis API request for tenant: {tenant_id}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # single Graph API call - much faster (same pattern as get_tenant_subscriptions)
        graph_client = GraphBetaClient(tenant_id)
        tenant_details = graph_client.get_tenant_details(tenant_id)

        # handle fact get_tenant_details returns a list
        if tenant_details and len(tenant_details) > 0:
            tenant_name = tenant_details[0].get("displayName", tenant_id)
        else:
            tenant_name = tenant_id

        logging.info(f"Processing license analysis for tenant: {tenant_name}")

        # grab license data - simplified queries like get_tenant_subscriptions
        # basic license counts
        total_licenses_query = "SELECT COUNT(DISTINCT license_display_name) as count FROM licenses WHERE tenant_id = ?"
        total_licenses_result = query(total_licenses_query, (tenant_id,))

        # total license assignments
        total_assignments_query = "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ?"
        total_assignments_result = query(total_assignments_query, (tenant_id,))

        # active license assignments
        active_assignments_query = "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 1"
        active_assignments_result = query(active_assignments_query, (tenant_id,))

        # total monthly cost for active licenses
        total_cost_query = "SELECT SUM(monthly_cost) as total_cost FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 1"
        total_cost_result = query(total_cost_query, (tenant_id,))

        # grab license optimization data
        license_optimization = calculate_license_optimization(tenant_id)

        # fetch actual license data for the data field (same pattern as subscriptions)
        licenses_query = """
            SELECT 
            license_display_name,
            license_partnumber,
            monthly_cost,
            status
        FROM licenses 
        WHERE tenant_id = ?
        ORDER BY license_display_name
        """
        licenses_result = query(licenses_query, (tenant_id,))

        # transform license data for frontend consumption (same pattern as subscriptions)
        licenses_data = []
        for license in licenses_result:
            licenses_data.append(
                {
                    "license_display_name": license["license_display_name"],
                    "license_partnumber": license["license_partnumber"],
                    "monthly_cost": license["monthly_cost"],
                    "status": license["status"],
                }
            )

        # calculate metrics
        total_license_types = total_licenses_result[0]["count"] if total_licenses_result else 0
        total_assignments = total_assignments_result[0]["count"] if total_assignments_result else 0
        active_assignments = active_assignments_result[0]["count"] if active_assignments_result else 0
        inactive_assignments = total_assignments - active_assignments
        monthly_cost = round(total_cost_result[0]["total_cost"] or 0, 2) if total_cost_result else 0
        utilization_rate = license_optimization.get("utilization_rate", 0)
        monthly_savings = license_optimization.get("estimated_monthly_savings", 0)

        # generate license-specific optimization actions
        actions = []

        # action 1: inactive license assignments (license-focused)
        if inactive_assignments > 0:
            actions.append(
                {
                    "title": "Remove Inactive License Assignments",
                    "description": f"{inactive_assignments} inactive license assignments wasting budget",
                    "action": "cleanup",
                }
            )

        # action 2: low utilization licenses
        if utilization_rate < 70:
            actions.append(
                {
                    "title": "Investigate Low License Utilization",
                    "description": f"Only {utilization_rate}% license utilization - review assignments",
                    "action": "optimize",
                }
            )

        # action 3: high cost savings opportunity
        if monthly_savings > 100:
            actions.append(
                {
                    "title": "Realize License Cost Savings",
                    "description": f"${monthly_savings}/month potential savings from license optimization",
                    "action": "optimize",
                }
            )

        # action 4: license portfolio consolidation (only for smaller deployments)
        if total_license_types > 5 and total_assignments < 50:
            actions.append(
                {
                    "title": "Consolidate License Types",
                    "description": f"{total_license_types} license types for small user base - consider consolidation",
                    "action": "optimize",
                }
            )

        # build response structure using utility function
        return create_success_response(
            data=licenses_data,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="get_tenant_licenses_by_id",
            metrics={
                "total_license_types": total_license_types,
                "total_license_assignments": total_assignments,
                "active_license_assignments": active_assignments,
                "monthly_license_cost": monthly_cost,
                "license_utilization_rate": utilization_rate,
                "underutilized_licenses": license_optimization.get("underutilized_licenses", 0),
                "estimated_monthly_savings": monthly_savings,
            },
            actions=actions,
        )

    except Exception as e:
        error_msg = f"Error retrieving license analysis: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.schedule(schedule="0 20 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def get_tenant_roles(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for roles analysis across all tenants"""
    if timer.past_due:
        logging.warning("Roles analysis timer is past due!")

    logging.info("Starting scheduled roles analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing roles for tenant: {tenant_name}")

            # Query role data for this tenant
            total_roles_query = "SELECT COUNT(*) as count FROM roles WHERE tenant_id = ?"
            total_roles_result = query(total_roles_query, (tenant_id,))

            total_assignments_query = "SELECT COUNT(*) as count FROM user_rolesV2 WHERE tenant_id = ?"
            total_assignments_result = query(total_assignments_query, (tenant_id,))

            users_with_roles_query = "SELECT COUNT(DISTINCT user_id) as count FROM user_rolesV2 WHERE tenant_id = ?"
            users_with_roles_result = query(users_with_roles_query, (tenant_id,))

            admin_roles_query = "SELECT COUNT(*) as count FROM roles WHERE tenant_id = ? AND (role_display_name LIKE '%Admin%' OR role_display_name LIKE '%Administrator%')"
            admin_roles_result = query(admin_roles_query, (tenant_id,))

            multi_role_users_query = "SELECT COUNT(*) as count FROM (SELECT user_id FROM user_rolesV2 WHERE tenant_id = ? GROUP BY user_id HAVING COUNT(role_id) > 1)"
            multi_role_users_result = query(multi_role_users_query, (tenant_id,))

            # Calculate metrics
            total_roles = total_roles_result[0]["count"] if total_roles_result else 0
            total_assignments = total_assignments_result[0]["count"] if total_assignments_result else 0
            users_with_roles = users_with_roles_result[0]["count"] if users_with_roles_result else 0
            admin_roles = admin_roles_result[0]["count"] if admin_roles_result else 0
            multi_role_users = multi_role_users_result[0]["count"] if multi_role_users_result else 0

            # Generate optimization actions
            actions = []
            if admin_roles > 0:
                actions.append(f"Review {admin_roles} admin roles for security")

            if multi_role_users > 0:
                actions.append(f"Review {multi_role_users} users with multiple roles")

            if total_assignments > 0 and users_with_roles > 0:
                avg_roles_per_user = total_assignments / users_with_roles
                if avg_roles_per_user > 2:
                    actions.append(f"High role density: {avg_roles_per_user:.1f} roles per user")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_roles": total_roles,
                "total_assignments": total_assignments,
                "users_with_roles": users_with_roles,
                "admin_roles": admin_roles,
                "multi_role_users": multi_role_users,
                "actions": actions,
            }

            logging.info(f"✓ {tenant_name}: {total_roles} roles, {users_with_roles} users, {admin_roles} admin roles")
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    successful_count = len([r for r in results if r["status"] == "completed"])
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Roles analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Roles analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_roles_all = sum(r.get("total_roles", 0) for r in results if r["status"] == "completed")
    total_assignments_all = sum(r.get("total_assignments", 0) for r in results if r["status"] == "completed")
    total_users_all = sum(r.get("users_with_roles", 0) for r in results if r["status"] == "completed")
    total_admin_roles_all = sum(r.get("admin_roles", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_roles_all} roles, {total_assignments_all} assignments, {total_users_all} users, {total_admin_roles_all} admin roles"
    )


@app.route(route="tenant/roles", methods=["GET"])
def get_tenant_roles_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant roles analysis"""
    # Returns structured response with role optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"Roles analysis API request for tenant: {tenant_id}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # single Graph API call - much faster (same pattern as get_tenant_subscriptions)
        graph_client = GraphBetaClient(tenant_id)
        tenant_details = graph_client.get_tenant_details(tenant_id)

        # handle fact get_tenant_details returns a list
        if tenant_details and len(tenant_details) > 0:
            tenant_name = tenant_details[0].get("displayName", tenant_id)
        else:
            tenant_name = tenant_id

        logging.info(f"Processing roles analysis for tenant: {tenant_name}")

        # grab roles data - simplified queries like get_tenant_subscriptions
        # basic role counts
        total_roles_query = "SELECT COUNT(*) as count FROM roles WHERE tenant_id = ?"
        total_roles_result = query(total_roles_query, (tenant_id,))

        # total role assignments
        total_assignments_query = "SELECT COUNT(*) as count FROM user_rolesV2 WHERE tenant_id = ?"
        total_assignments_result = query(total_assignments_query, (tenant_id,))

        # unique users with role assignments
        users_with_roles_query = "SELECT COUNT(DISTINCT user_id) as count FROM user_rolesV2 WHERE tenant_id = ?"
        users_with_roles_result = query(users_with_roles_query, (tenant_id,))

        # Admin roles (roles containing 'Admin' or 'Administrator')
        admin_roles_query = "SELECT COUNT(*) as count FROM roles WHERE tenant_id = ? AND (role_display_name LIKE '%Admin%' OR role_display_name LIKE '%Administrator%')"
        admin_roles_result = query(admin_roles_query, (tenant_id,))

        # Users with multiple roles (potential over-privileged)
        multi_role_users_query = "SELECT COUNT(*) as count FROM (SELECT user_id FROM user_rolesV2 WHERE tenant_id = ? GROUP BY user_id HAVING COUNT(role_id) > 1)"
        multi_role_users_result = query(multi_role_users_query, (tenant_id,))

        # fetch actual role data for the data field (same pattern as subscriptions)
        roles_query = """
            SELECT 
                role_id,
                role_display_name,
                role_description,
                member_count
            FROM roles 
            WHERE tenant_id = ?
            ORDER BY role_display_name
            """
        roles_result = query(roles_query, (tenant_id,))

        # transform role data for frontend consumption (same pattern as subscriptions)
        roles_data = []
        for role in roles_result:
            roles_data.append(
                {
                    "role_id": role["role_id"],
                    "role_display_name": role["role_display_name"],
                    "role_description": role["role_description"],
                    "member_count": role["member_count"],
                }
            )

        # calculate metrics
        total_roles = total_roles_result[0]["count"] if total_roles_result else 0
        total_assignments = total_assignments_result[0]["count"] if total_assignments_result else 0
        users_with_roles = users_with_roles_result[0]["count"] if users_with_roles_result else 0
        admin_roles = admin_roles_result[0]["count"] if admin_roles_result else 0
        multi_role_users = multi_role_users_result[0]["count"] if multi_role_users_result else 0

        avg_roles_per_user = round(total_assignments / users_with_roles, 1) if users_with_roles > 0 else 0

        # generate role-specific optimization actions
        actions = []

        # action 1: review over-privileged users
        if multi_role_users > 0:
            actions.append(
                {
                    "title": "Review Over-Privileged Users",
                    "description": f"{multi_role_users} users have multiple roles - verify necessity",
                    "action": "review",
                }
            )

        # action 2: admin role assignments audit
        if admin_roles > 0 and users_with_roles > 0:
            actions.append(
                {
                    "title": "Audit Admin Role Assignments",
                    "description": f"{admin_roles} admin roles assigned - ensure principle of least privilege",
                    "action": "audit",
                }
            )

        # action 3: role proliferation (optional, only if many roles)
        if total_roles > 10 and users_with_roles < 20:
            actions.append(
                {
                    "title": "Consolidate Role Definitions",
                    "description": f"{total_roles} roles for {users_with_roles} users - consider role consolidation",
                    "action": "optimize",
                }
            )

        # build response structure using utility function
        return create_success_response(
            data=roles_data,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="get_tenant_roles_by_id",
            metrics={
                "total_roles": total_roles,
                "total_role_assignments": total_assignments,
                "users_with_roles": users_with_roles,
                "admin_roles": admin_roles,
                "multi_role_users": multi_role_users,
                "avg_roles_per_user": avg_roles_per_user,
            },
            actions=actions,
        )

    except Exception as e:
        error_msg = f"Error retrieving roles analysis: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.schedule(schedule="0 15 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def get_tenant_groups(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for groups analysis across all tenants"""
    if timer.past_due:
        logging.warning("Groups analysis timer is past due!")

    logging.info("Starting scheduled groups analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing groups for tenant: {tenant_name}")

            # Query group data for this tenant
            total_groups_query = "SELECT COUNT(*) as count FROM groups WHERE tenant_id = ?"
            total_groups_result = query(total_groups_query, (tenant_id,))

            total_members_query = "SELECT COUNT(*) as count FROM user_groupsV2 WHERE tenant_id = ?"
            total_members_result = query(total_members_query, (tenant_id,))

            active_members_query = "SELECT COUNT(*) as count FROM user_groupsV2 WHERE tenant_id = ? AND is_active = 1"
            active_members_result = query(active_members_query, (tenant_id,))

            security_groups_query = "SELECT COUNT(*) as count FROM groups WHERE tenant_id = ? AND security_enabled = 1"
            security_groups_result = query(security_groups_query, (tenant_id,))

            mail_enabled_groups_query = "SELECT COUNT(*) as count FROM groups WHERE tenant_id = ? AND mail_enabled = 1"
            mail_enabled_groups_result = query(mail_enabled_groups_query, (tenant_id,))

            # Calculate metrics
            total_groups = total_groups_result[0]["count"] if total_groups_result else 0
            total_members = total_members_result[0]["count"] if total_members_result else 0
            active_members = active_members_result[0]["count"] if active_members_result else 0
            security_groups = security_groups_result[0]["count"] if security_groups_result else 0
            mail_enabled_groups = mail_enabled_groups_result[0]["count"] if mail_enabled_groups_result else 0

            # Generate optimization actions
            actions = []
            if total_members > 0 and active_members < total_members:
                inactive_count = total_members - active_members
                actions.append(f"Review {inactive_count} inactive group memberships")

            if security_groups > 0:
                actions.append(f"Monitor {security_groups} security groups")

            if mail_enabled_groups > 0:
                actions.append(f"Review {mail_enabled_groups} mail-enabled groups")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_groups": total_groups,
                "total_members": total_members,
                "active_members": active_members,
                "security_groups": security_groups,
                "mail_enabled_groups": mail_enabled_groups,
                "actions": actions,
            }

            logging.info(f"✓ {tenant_name}: {total_groups} groups, {active_members}/{total_members} active members")
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    successful_count = len([r for r in results if r["status"] == "completed"])
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Groups analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Groups analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_groups_all = sum(r.get("total_groups", 0) for r in results if r["status"] == "completed")
    total_members_all = sum(r.get("total_members", 0) for r in results if r["status"] == "completed")
    total_security_groups_all = sum(r.get("security_groups", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_groups_all} groups, {total_members_all} members, {total_security_groups_all} security groups"
    )


@app.route(route="tenant/groups", methods=["GET"])
def get_tenant_groups_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant groups analysis"""
    # Returns structured response with group optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"Groups API request for tenant: {tenant_id}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["display_name"] for t in tenants}

        if tenant_id not in tenant_names:
            return create_error_response(error_message=f"Tenant '{tenant_id}' not found", status_code=404)

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Processing groups data for tenant: {tenant_name}")

        # fetch group options for frontend dropdown
        group_options_query = """
        SELECT DISTINCT 
            group_id,
            group_display_name as display_name,
            group_description as description,
            group_type,
            member_count,
            owner_count,
            visibility,
            mail_enabled,
            security_enabled
        FROM groups 
        WHERE tenant_id = ?
        ORDER BY group_display_name
        """

        try:
            group_options = query(group_options_query, (tenant_id,))
        except Exception as e:
            # If groups table doesn't exist or query fails, provide empty list
            logging.warning(f"Could not fetch groups from database: {str(e)}")
            group_options = []

        # build response structure using utility function
        return create_success_response(
            data=group_options,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="get_tenant_groups_by_id",
            metrics={
                "total_groups": len(group_options),
                "endpoint": "get_tenant_groups_by_id",
            },
        )

    except Exception as e:
        error_msg = f"Error retrieving groups data: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


# create azure function with timer trigger for get tenant subscriptions for multi tenant functionality
@app.schedule(schedule="0 30 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def get_tenant_subscriptions(timer: func.TimerRequest) -> None:
    """V2 Timer trigger for subscriptions analysis across all tenants"""
    if timer.past_due:
        logging.warning("Subscriptions analysis timer is past due!")

    logging.info("Starting scheduled subscriptions analysis across all tenants")
    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["display_name"]

            logging.info(f"Analyzing subscriptions for tenant: {tenant_name}")

            # Query subscription data for this tenant
            total_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ?"
            total_subscriptions_result = query(total_subscriptions_query, (tenant_id,))

            active_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ? AND is_active = 1"
            active_subscriptions_result = query(active_subscriptions_query, (tenant_id,))

            trial_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ? AND is_trial = 1"
            trial_subscriptions_result = query(trial_subscriptions_query, (tenant_id,))

            expiring_soon_query = """
            SELECT COUNT(*) as count FROM subscriptions 
            WHERE tenant_id = ? AND next_lifecycle_date_time IS NOT NULL 
            AND date(next_lifecycle_date_time) <= date('now', '+30 days')
            """
            expiring_soon_result = query(expiring_soon_query, (tenant_id,))

            # Calculate metrics
            total_subscriptions = total_subscriptions_result[0]["count"] if total_subscriptions_result else 0
            active_subscriptions = active_subscriptions_result[0]["count"] if active_subscriptions_result else 0
            trial_subscriptions = trial_subscriptions_result[0]["count"] if trial_subscriptions_result else 0
            expiring_soon = expiring_soon_result[0]["count"] if expiring_soon_result else 0
            inactive_subscriptions = total_subscriptions - active_subscriptions

            # Generate optimization actions
            actions = []
            if inactive_subscriptions > 0:
                actions.append(f"Review {inactive_subscriptions} inactive subscriptions")

            if trial_subscriptions > 0:
                actions.append(f"Monitor {trial_subscriptions} trial subscriptions")

            if expiring_soon > 0:
                actions.append(f"Renew {expiring_soon} subscriptions expiring soon")

            result = {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "total_subscriptions": total_subscriptions,
                "active_subscriptions": active_subscriptions,
                "inactive_subscriptions": inactive_subscriptions,
                "trial_subscriptions": trial_subscriptions,
                "expiring_soon": expiring_soon,
                "actions": actions,
            }

            logging.info(
                f"✓ {tenant_name}: {active_subscriptions}/{total_subscriptions} active subscriptions, {trial_subscriptions} trials"
            )
            results.append(result)

        except Exception as e:
            logging.error(f"✗ {tenant_name}: {str(e)}")
            results.append({"status": "error", "tenant_id": tenant_id, "tenant_name": tenant_name, "error": str(e)})

    # Log summary
    successful_count = len([r for r in results if r["status"] == "completed"])
    failed_count = len([r for r in results if r["status"] == "error"])

    if failed_count > 0:
        logging.warning(f"Subscriptions analysis completed with {failed_count} errors out of {len(tenants)} tenants")
    else:
        logging.info(f"✓ Subscriptions analysis completed successfully for {len(tenants)} tenants")

    # Log total metrics across all tenants
    total_subscriptions_all = sum(r.get("total_subscriptions", 0) for r in results if r["status"] == "completed")
    active_subscriptions_all = sum(r.get("active_subscriptions", 0) for r in results if r["status"] == "completed")
    trial_subscriptions_all = sum(r.get("trial_subscriptions", 0) for r in results if r["status"] == "completed")
    expiring_soon_all = sum(r.get("expiring_soon", 0) for r in results if r["status"] == "completed")

    logging.info(
        f" Total across all tenants: {total_subscriptions_all} subscriptions, {active_subscriptions_all} active, {trial_subscriptions_all} trials, {expiring_soon_all} expiring soon"
    )


@app.route(route="tenant/subscriptions", methods=["GET"])
def get_tenant_subscription_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant subscription data"""
    # Returns structured response with subscription optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get("tenant_id")
        logging.info(f"Subscriptions API request for tenant: {tenant_id}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        # single Graph API call - much faster
        graph_client = GraphBetaClient(tenant_id)
        tenant_details = graph_client.get_tenant_details(tenant_id)

        # handle fact get_tenant_details returns a list
        if tenant_details and len(tenant_details) > 0:
            tenant_name = tenant_details[0].get("displayName", tenant_id)
        else:
            tenant_name = tenant_id

        logging.info(f"Processing subscription data for tenant: {tenant_name}")

        # grab subscription data
        # basic subscription counts
        total_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ?"
        total_subscriptions_result = query(total_subscriptions_query, (tenant_id,))

        active_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ? AND is_active = 1"
        active_subscriptions_result = query(active_subscriptions_query, (tenant_id,))

        # trial subscriptions count
        trial_subscriptions_query = "SELECT COUNT(*) as count FROM subscriptions WHERE tenant_id = ? AND is_trial = 1"
        trial_subscriptions_result = query(trial_subscriptions_query, (tenant_id,))

        # expiring soon subscriptions (within 30 days)
        expiring_soon_query = """
        SELECT COUNT(*) as count FROM subscriptions 
        WHERE tenant_id = ? AND next_lifecycle_date_time IS NOT NULL 
        AND date(next_lifecycle_date_time) <= date('now', '+30 days')
        """
        expiring_soon_result = query(expiring_soon_query, (tenant_id,))

        # calculate metrics
        total_subscriptions = total_subscriptions_result[0]["count"] if total_subscriptions_result else 0
        active_subscriptions = active_subscriptions_result[0]["count"] if active_subscriptions_result else 0
        trial_subscriptions = trial_subscriptions_result[0]["count"] if trial_subscriptions_result else 0
        expiring_soon = expiring_soon_result[0]["count"] if expiring_soon_result else 0
        inactive_subscriptions = total_subscriptions - active_subscriptions

        # fetch actual subscription data for the data field
        subscriptions_query = """
            SELECT 
                subscription_id,
                commerce_subscription_id,
                sku_id,
                sku_part_number,
                is_active,
                is_trial,
                total_licenses,
                next_lifecycle_date_time,
                created_at,
                last_updated
            FROM subscriptions 
            WHERE tenant_id = ? 
            ORDER BY sku_part_number
        """
        subscriptions_result = query(subscriptions_query, (tenant_id,))

        # transform subscription data for frontend consumption
        subscriptions_data = []
        for subscription in subscriptions_result:
            subscriptions_data.append(
                {
                    "subscription_id": subscription["subscription_id"],
                    "commerce_subscription_id": subscription["commerce_subscription_id"],
                    "sku_id": subscription["sku_id"],
                    "sku_part_number": subscription["sku_part_number"],
                    "is_active": bool(subscription["is_active"]),
                    "is_trial": bool(subscription["is_trial"]),
                    "total_licenses": subscription["total_licenses"],
                    "next_lifecycle_date_time": subscription["next_lifecycle_date_time"],
                    "created_at": subscription["created_at"],
                    "last_updated": subscription["last_updated"],
                }
            )

        # generate subscription optimization actions
        actions = []

        # action 1: trial subscriptions
        if trial_subscriptions > 0:
            actions.append(
                {
                    "title": "Review Trial Subscriptions",
                    "description": f"{trial_subscriptions} trial subscriptions - evaluate before expiration",
                    "action": "review",
                }
            )

        # action 2: expiring subscriptions
        if expiring_soon > 0:
            actions.append(
                {
                    "title": "Renew Expiring Subscriptions",
                    "description": f"{expiring_soon} subscriptions expiring within 30 days",
                    "action": "renew",
                }
            )

        # action 3: inactive subscriptions
        if inactive_subscriptions > 0:
            actions.append(
                {
                    "title": "Review Inactive Subscriptions",
                    "description": f"{inactive_subscriptions} inactive subscriptions - consider cancellation",
                    "action": "review",
                }
            )

        # action 4: license optimization
        total_licenses = sum(sub.get("total_licenses", 0) for sub in subscriptions_data)
        if total_licenses > 0:
            # Check if there are unused licenses (this would require user_licensesV2 data)
            actions.append(
                {
                    "title": "Optimize License Usage",
                    "description": f"{total_licenses} total licenses - review utilization",
                    "action": "optimize",
                }
            )

        # build response structure using utility function
        return create_success_response(
            data=subscriptions_data,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="get_tenant_subscription_by_id",
            metrics={
                "total_subscriptions": total_subscriptions,
                "active_subscriptions": active_subscriptions,
                "inactive_subscriptions": inactive_subscriptions,
                "trial_subscriptions": trial_subscriptions,
                "expiring_soon": expiring_soon,
                "total_licenses": total_licenses,
            },
            actions=actions,
        )

    except Exception as e:
        error_msg = f"Error retrieving subscription data: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


@app.route(route="users/{user_id}/disable", methods=["PATCH"])
def disable_user(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP PATCH endpoint to disable a single user account"""
    # single tenant, single resource operation

    try:
        # extract and validate request data
        logging.info("Processing user disable request")

        req_body = req.get_json()
        if not req_body:
            return create_error_response(error_message="Request body is required", status_code=400)

        # get tenant_id and user identifier from request
        tenant_id = req_body.get("tenant_id")
        user_id = req_body.get("user_id")
        user_principal_name = req_body.get("user_principal_name")

        # validate required parameters
        if not tenant_id:
            return create_error_response(error_message="tenant_id is required", status_code=400)

        if not user_id and not user_principal_name:
            return create_error_response(error_message="Either user_id or user_principal_name is required", status_code=400)

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["display_name"] for t in tenants}

        if tenant_id not in tenant_names:
            return create_error_response(error_message=f"Tenant '{tenant_id}' not found", status_code=404)

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Disabling user for tenant: {tenant_name}")

        # find and validate user exists in database
        if user_id:
            # query by user_id
            user_query = "SELECT * FROM usersV2 WHERE tenant_id = ? AND user_id = ?"
            user_result = query(user_query, (tenant_id, user_id))
            identifier = f"user_id: {user_id}"
        else:
            # query by user_principal_name
            user_query = "SELECT * FROM usersV2 WHERE tenant_id = ? AND user_principal_name = ?"
            user_result = query(user_query, (tenant_id, user_principal_name))
            identifier = f"user_principal_name: {user_principal_name}"

        if not user_result:
            return create_error_response(error_message=f"User not found ({identifier})", status_code=404)

        user = user_result[0]
        logging.info(f"Found user: {user['user_principal_name']}")

        # check if user is already disabled
        if not user.get("account_enabled", True):
            return create_error_response(
                error_message=f"User {user['user_principal_name']} is already disabled",
                status_code=409,
                data={"user_id": user["user_id"], "user_principal_name": user["user_principal_name"], "account_enabled": False},
            )

        # disable user account via graph api
        logging.info(f"Disabling user {user['user_principal_name']} via Graph API")
        graph_client = GraphBetaClient(tenant_id)

        # call Microsoft Graph to disable the user
        disable_result = graph_client.disable_user(user["user_id"])

        if disable_result.get("status") != "success":
            error_msg = disable_result.get("error", "Unknown error disabling user")
            logging.error(f"Failed to disable user via Graph API: {error_msg}")
            return create_error_response(
                error_message=f"Failed to disable user: {error_msg}",
                status_code=500,
                data={
                    "user_id": user["user_id"],
                    "user_principal_name": user["user_principal_name"],
                    "graph_api_error": error_msg,
                },
            )

        # update local database to reflect disabled status
        current_time = datetime.now().isoformat()
        update_query = "UPDATE usersV2 SET account_enabled = 0, last_updated = ? WHERE tenant_id = ? AND user_id = ?"

        try:
            execute_query(update_query, (current_time, tenant_id, user["user_id"]))
            logging.info(f"Updated local database for user {user['user_principal_name']}")
        except Exception as db_error:
            logging.warning(f"Graph API update succeeded but local DB update failed for user {user_id}: {str(db_error)}")
            # Note: user is updated in Graph but local DB might be out of sync

        # return success response using utility function
        return create_success_response(
            data={
                "user_id": user["user_id"],
                "user_principal_name": user["user_principal_name"],
                "display_name": user.get("display_name"),
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "disabled_at": current_time,
                "last_sign_in": user.get("last_sign_in_date"),
            },
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="disable_user",
            message=f"User {user['user_principal_name']} successfully disabled",
        )

    except Exception as e:
        # comprehensive error handling and logging
        error_msg = f"Error disabling user: {str(e)}"
        logging.error(error_msg)

        return create_error_response(
            error_message=error_msg,
            status_code=500,
            data={
                "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                "user_id": req_body.get("user_id") if "req_body" in locals() else None,
                "user_principal_name": req_body.get("user_principal_name") if "req_body" in locals() else None,
            },
        )

    except Exception as e:
        error_msg = f"Error updating user: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({"success": False, "error": error_msg}), status_code=500, headers={"Content-Type": "application/json"}
        )


@app.route(route="tenant/users/disable-all", methods=["PATCH"])
def disable_all_inactive_users(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP PATCH endpoint to disable ALL inactive users for a tenant"""
    # single tenant, multiple resource operation

    try:
        # extract and validate request data
        logging.info("Processing bulk user disable request")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Request body is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # get parameters from request
        tenant_id = req_body.get("tenant_id")
        inactivity_threshold_days = req_body.get("inactivity_threshold_days", 90)

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "tenant_id is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["display_name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"Tenant '{tenant_id}' not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        # logging.info(f"Processing bulk disable for tenant: {tenant_name} (dry_run: {dry_run})")

        # use existing analysis function to get potential savings
        inactive_analysis = calculate_inactive_users(tenant_id, inactivity_threshold_days)
        potential_savings = inactive_analysis.get("potential_monthly_savings", 0)

        # get all active users to process
        inactive_users_query = """
        SELECT user_id, display_name, user_principal_name, last_sign_in_date, account_enabled
        FROM usersV2
        WHERE tenant_id = ? AND account_enabled = 1
        """
        all_users = query(inactive_users_query, (tenant_id,))

        # filter to get actual inactive users based on threshold
        cutoff_date = datetime.now() - timedelta(days=inactivity_threshold_days)

        inactive_users = []
        for user in all_users:
            if user["last_sign_in_date"]:
                try:
                    last_sign_in_date = datetime.fromisoformat(user["last_sign_in_date"].replace("Z", "+00:00"))
                    days_inactive = (datetime.now() - last_sign_in_date.replace(tzinfo=None)).days
                    if last_sign_in_date.replace(tzinfo=None) < cutoff_date:
                        user["days_inactive"] = days_inactive
                        inactive_users.append(user)
                except:
                    # if date parsing fails, treat as inactive
                    user["days_inactive"] = None
                    inactive_users.append(user)
            else:
                # never signed in - treat as inactive
                user["days_inactive"] = None
                inactive_users.append(user)

        if not inactive_users:
            return func.HttpResponse(
                json.dumps(
                    {
                        "success": True,
                        "data": [],
                        "metadata": {
                            "tenant_id": tenant_id,
                            "tenant_name": tenant_name,
                            "operation": "bulk_disable_inactive_users",
                            "inactivity_threshold_days": inactivity_threshold_days,
                            "execution_time": execution_time,
                            "summary": {
                                "total_identified": 0,
                                "successfully_disabled": 0,
                                "already_disabled": 0,
                                "failed": 0,
                                "skipped": 0,
                            },
                            "potential_monthly_savings": potential_savings,
                        },
                        "actions": [],
                    }
                ),
                status_code=200,
                headers={"Content-Type": "application/json"},
            )

        logging.info(f"Found {len(inactive_users)} inactive users to process")

        # initialize tracking variables
        processed_users = []
        counters = {"successfully_disabled": 0, "already_disabled": 0, "failed": 0, "skipped": 0}

        # initialize Graph client (only if not dry run)

        graph_client = GraphBetaClient(tenant_id)

        # process each inactive user
        for user in inactive_users:
            user_id = user["user_id"]
            user_principal_name = user["user_principal_name"]
            display_name = user.get("display_name")
            last_sign_in = user.get("last_sign_in_date")
            days_inactive = user.get("days_inactive")

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
                "days_inactive": days_inactive,
            }

            try:
                # check if user is already disabled (shouldn't happen with our query, but safety check)
                if not user.get("account_enabled", True):
                    user_data["status"] = "already_disabled"
                    counters["already_disabled"] += 1
                    processed_users.append(user_data)
                    continue

                    # actually disable the user via Graph API
                    logging.info(f"Disabling user {user_principal_name} via Graph API")
                    disable_result = graph_client.disable_user(user_id)

                    if disable_result.get("status") != "success":
                        error_msg = disable_result.get("error", "Unknown error disabling user")
                        user_data["status"] = "failed"
                        user_data["error"] = error_msg
                        counters["failed"] += 1
                        processed_users.append(user_data)
                        logging.error(f"Failed to disable {user_principal_name}: {error_msg}")
                        continue

                    # update local database to reflect disabled status
                    current_time = datetime.now().isoformat()
                    update_query = "UPDATE usersV2 SET account_enabled = 0, last_updated = ? WHERE tenant_id = ? AND user_id = ?"

                    try:
                        execute_query(update_query, (current_time, tenant_id, user_id))
                        logging.info(f"Updated local database for user {user_principal_name}")
                    except Exception as db_error:
                        logging.warning(
                            f"Graph API disable succeeded but local DB update failed for {user_principal_name}: {str(db_error)}"
                        )
                        # note: user is disabled in Graph but local DB might be out of sync

                    user_data["status"] = "disabled"
                    user_data["disabled_at"] = current_time
                    counters["successfully_disabled"] += 1
                    processed_users.append(user_data)
                    logging.info(f"Successfully disabled user {user_principal_name}")

            except Exception as e:
                error_msg = f"Error processing user {user_principal_name}: {str(e)}"
                user_data["status"] = "failed"
                user_data["error"] = error_msg
                counters["failed"] += 1
                processed_users.append(user_data)
                logging.error(error_msg)

        # build actions array based on results
        actions = []

        if counters["failed"] > 0:
            actions.append(
                {
                    "type": "review_failures",
                    "description": f"{counters['failed']} user(s) failed to disable - review permissions",
                    "users_affected": counters["failed"],
                }
            )

        if counters["successfully_disabled"] > 0 and potential_savings > 0:
            actions.append(
                {
                    "type": "verify_savings",
                    "description": f"Potential monthly savings of ${potential_savings:.2f} achieved",
                    "amount": potential_savings,
                }
            )

        if counters["already_disabled"] > 0:
            actions.append(
                {
                    "type": "review_already_disabled",
                    "description": f"{counters['already_disabled']} user(s) were already disabled",
                    "users_affected": counters["already_disabled"],
                }
            )

        # build final response using utility function
        return create_bulk_operation_response(
            data=processed_users,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="bulk_disable_inactive_users",
            summary={
                "total_identified": len(inactive_users),
                "successfully_disabled": counters["successfully_disabled"],
                "already_disabled": counters["already_disabled"],
                "failed": counters["failed"],
                "skipped": counters["skipped"],
            },
            actions=actions,
            execution_time=execution_time,
            inactivity_threshold_days=inactivity_threshold_days,
            potential_monthly_savings=potential_savings,
        )

    except Exception as e:
        error_msg = f"Error in bulk disable operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "data": [],
                    "metadata": {
                        "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                        "operation": "bulk_disable_inactive_users",
                        "execution_time": datetime.now().isoformat(),
                    },
                    "actions": [],
                }
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


@app.route(route="users/{user_id}/reset-password", methods=["POST"])
def reset_user_password(req: func.HttpRequest) -> func.HttpResponse:
    """POST endpoint to reset a user's password with temporary password"""
    # single tenant, single resource operation
    try:
        # extract user_id from URL path
        user_id = req.route_params.get("user_id")

        # extract and validate request data
        logging.info(f"Processing password reset request for user {user_id}")

        req_body = req.get_json()
        if not req_body:
            return create_error_response(error_message="Request body is required", status_code=400)

        # get parameters from request
        tenant_id = req_body.get("tenant_id")

        # validate required parameters
        if not tenant_id:
            return create_error_response(error_message="tenant_id is required", status_code=400)

        if not user_id:
            return create_error_response(error_message="user_id is required in URL path", status_code=400)

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["display_name"] for t in tenants}

        if tenant_id not in tenant_names:
            return create_error_response(error_message=f"Tenant '{tenant_id}' not found", status_code=404)

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Resetting password for user {user_id} in tenant: {tenant_name}")

        # find and validate user exists in database
        user_query = "SELECT * FROM usersV2 WHERE tenant_id = ? AND user_id = ?"
        user_result = query(user_query, (tenant_id, user_id))

        if not user_result:
            return create_error_response(error_message=f"User {user_id} not found in tenant {tenant_id}", status_code=404)

        user = user_result[0]
        logging.info(f"Found user: {user['user_principal_name']}")

        # check if user is disabled
        if not user.get("account_enabled", True):
            return create_error_response(
                error_message=f"Cannot reset password for disabled user {user['user_principal_name']}",
                status_code=422,
                tenant_id=tenant_id,
                tenant_name=tenant_name,
                operation="reset_user_password",
                data=[{"user_id": user["user_id"], "user_principal_name": user["user_principal_name"], "status": "user_disabled"}],
                execution_time=execution_time,
            )

        # reset password via Graph API
        logging.info(f"Resetting password for user {user['user_principal_name']} via Graph API")
        graph_client = GraphBetaClient(tenant_id)

        reset_result = graph_client.reset_user_password(user["user_id"])

        if reset_result.get("status") != "success":
            error_msg = reset_result.get("error", "Unknown error resetting password")
            logging.error(f"Failed to reset password via Graph API: {error_msg}")
            return create_error_response(
                error_message=f"Failed to reset password: {error_msg}",
                status_code=500,
                tenant_id=tenant_id,
                tenant_name=tenant_name,
                operation="reset_user_password",
                data=[
                    {
                        "user_id": user["user_id"],
                        "user_principal_name": user["user_principal_name"],
                        "status": "failed",
                        "error": error_msg,
                    }
                ],
                execution_time=execution_time,
                summary={"passwords_reset": 0, "failed": 1},
                actions=[
                    {
                        "type": "review_permissions",
                        "description": "Review Graph API permissions for password reset",
                        "users_affected": 1,
                    }
                ],
            )

        # update local database to track password reset
        current_time = datetime.now().isoformat()
        update_query = "UPDATE usersV2 SET last_updated = ? WHERE tenant_id = ? AND user_id = ?"

        try:
            execute_query(update_query, (current_time, tenant_id, user["user_id"]))
            logging.info(f"Updated local database for user {user['user_principal_name']}")
        except Exception as db_error:
            logging.warning(f"Graph API reset succeeded but local DB update failed for {user['user_principal_name']}: {str(db_error)}")

        # prepare response data
        user_data = {
            "user_id": user["user_id"],
            "user_principal_name": user["user_principal_name"],
            "display_name": user.get("display_name"),
            "status": "password_reset",
            "reset_at": current_time,
            "temporary_password": reset_result["temporary_password"],
            "force_change_password": True,
        }

        # build actions
        actions = [
            {"type": "secure_delivery", "description": "Securely deliver temporary password to user", "users_affected": 1},
            {"type": "monitor_login", "description": "Monitor user's next login to confirm password change", "users_affected": 1},
        ]

        # build final response using utility function
        return create_success_response(
            data=[user_data],
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="reset_user_password",
            metrics={
                "execution_time": execution_time,
                "summary": {"passwords_reset": 1, "failed": 0},
            },
            actions=actions,
        )

    except Exception as e:
        error_msg = f"Error in password reset operation: {str(e)}"
        logging.error(error_msg)

        return create_error_response(
            error_message=error_msg,
            status_code=500,
            tenant_id=req_body.get("tenant_id") if "req_body" in locals() else None,
            operation="reset_user_password",
            execution_time=datetime.now().isoformat(),
        )


# REPORT GENERATION


@app.schedule(schedule="0 0 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def generate_user_report(timer: func.TimerRequest) -> None:
    """Generate daily JSON report"""
    if timer and timer.past_due:
        logging.warning("User report timer is past due!")

    all_tenants = get_tenants()
    total_tenants = len(all_tenants)

    logging.info(f"Starting report generation for {total_tenants} tenants")

    try:
        # Get sync error data
        recent_sync_errors = aggregate_recent_sync_errors()
        successful_tenants_info = recent_sync_errors["successful_tenants"]
        failed_count = recent_sync_errors["failed_count"]

        # Remove helper fields from final output
        recent_sync_errors.pop("successful_tenants", None)
        recent_sync_errors.pop("failed_count", None)

        logging.info(f"Processing {len(successful_tenants_info)} successful tenants (excluding {failed_count} failed syncs)")

        # Process successful tenants
        tenant_summaries = []

        for tenant_info in successful_tenants_info:
            tenant = next(
                (t for t in all_tenants if t["tenant_id"] == tenant_info["tenant_id"]),
                None,
            )
            if not tenant:
                continue

            try:
                tenant_id = tenant["tenant_id"]
                tenant_name = tenant["name"]

                # Get basic metrics
                total_users_result = query(
                    "SELECT COUNT(*) as count FROM usersV2 WHERE tenant_id = ?",
                    (tenant_id,),
                )
                active_users_result = query(
                    "SELECT COUNT(*) as count FROM usersV2 WHERE tenant_id = ? AND account_enabled = 1",
                    (tenant_id,),
                )

                # Get analysis results
                mfa_result = calculate_mfa_compliance(tenant_id)
                license_result = calculate_license_optimization(tenant_id)

                # Calculate metrics
                total_users = total_users_result[0]["count"] if total_users_result else 0
                active_users = active_users_result[0]["count"] if active_users_result else 0
                inactive_users = total_users - active_users

                # Generate warnings
                warnings = []
                mfa_compliance = mfa_result.get("compliance_rate", 0)
                admin_non_compliant = mfa_result.get("admin_non_compliant", 0)
                monthly_savings = license_result.get("estimated_monthly_savings", 0)
                underutilized_count = license_result.get("underutilized_licenses", 0)

                if admin_non_compliant > 0:
                    warnings.append(f"CRITICAL: {admin_non_compliant} admin users without MFA - HIGH SECURITY RISK")

                if mfa_compliance < 50:
                    warnings.append(f"WARNING: Low MFA compliance ({mfa_compliance}%) - Security risk")

                if monthly_savings > 100:
                    warnings.append(
                        f"COST OPPORTUNITY: ${monthly_savings}/month potential savings from {underutilized_count} unused licenses"
                    )

                inactive_percentage = round((inactive_users / total_users * 100), 1) if total_users > 0 else 0
                if inactive_percentage > 25:
                    warnings.append(f"WARNING: High inactive user rate ({inactive_percentage}%) may indicate cleanup needed")

                # Build tenant summary
                tenant_summary = {
                    "tenant_name": tenant_name,
                    "tenant_id": tenant_id,
                    "total_users": total_users,
                    "active_users": active_users,
                    "inactive_percentage": inactive_percentage,
                    "mfa_compliance_rate": mfa_compliance,
                    "mfa_enabled_users": mfa_result.get("mfa_enabled", 0),
                    "admin_non_compliant": admin_non_compliant,
                    "estimated_monthly_savings": monthly_savings,
                    "underutilized_licenses": underutilized_count,
                    "warnings": warnings,
                }

                tenant_summaries.append(tenant_summary)

                # Log individual tenant summary
                logging.info(f"Report for {tenant_name}:")
                logging.info(json.dumps(tenant_summary, indent=2))

            except Exception as e:
                logging.error(f"Error processing {tenant_name}: {e}")

        # Build comprehensive report
        comprehensive_report = {
            "report_summary": {
                "total_tenants": total_tenants,
                "successful_tenants": len(successful_tenants_info),
                "failed_tenants": failed_count,
                "generation_timestamp": datetime.now().isoformat(),
            },
            "tenant_reports": tenant_summaries,
            "recent_sync_errors": recent_sync_errors,
        }

        # Log comprehensive report
        logging.info(json.dumps(comprehensive_report, indent=2))
        logging.info(f"Report generation completed: {len(tenant_summaries)}/{total_tenants} successful")

    except Exception as e:
        logging.error(f"Critical error in report generation: {str(e)}")
        raise


@app.route(route="generate-report-now", methods=["GET", "POST"])
def generate_report_manual(req: func.HttpRequest) -> func.HttpResponse:
    """Manual HTTP trigger to run report generation"""
    try:
        logging.info("Manual report generation triggered via HTTP")

        # Run report generation in background (non-blocking)
        import asyncio

        asyncio.create_task(generate_user_report(None))

        return func.HttpResponse(
            "Report generation started in background. Check logs for results.",
            status_code=202,
        )

    except Exception as e:
        error_msg = f"Error triggering report generation: {str(e)}"
        logging.error(error_msg)
        return create_error_response(error_message=error_msg, status_code=500)


# @app.timer_trigger(
#     schedule="0 0 1 * * FRI",
#     arg_name="myTimer",
#     use_monitor=False,
#     run_on_startup=False,
# )
# def hibp_sync_timer(myTimer: func.TimerRequest) -> None:
#     """Check all users for data breaches weekly"""
#     if myTimer.past_due:
#         logging.warning("HIBP sync timer is past due!")

#     tenants = get_tenants()

#     # Create single database connection for all tenants
#     from sql.hibp_db import HIBPDB

#     db = HIBPDB()

#     try:
#         # Process all tenants using list comprehension
#         results = [
#             sync_hibp_breaches(tenant["tenant_id"], tenant["name"], db)
#             for tenant in tenants
#         ]

#         # Log results
#         for i, result in enumerate(results):
#             tenant_name = tenants[i]["name"]
#             if result["status"] == "success":
#                 logging.info(
#                     f"✓ {tenant_name}: {result['users_checked']} users checked, {result['breaches_found']} breaches found"
#                 )
#             else:
#                 logging.error(f"✗ {tenant_name}: {result['error']}")
#     finally:
#         db.close()


# @app.route(route="sync/hibp", methods=["POST"])
# def hibp_sync_http(req: func.HttpRequest) -> func.HttpResponse:
#     """Manual trigger for HIBP breach check for a specific tenant"""
#     try:
#         req_body = req.get_json()
#         tenant_id = (
#             req_body.get("tenant_id") if req_body else req.params.get("tenant_id")
#         )

#         # Get tenant info
#         tenants = get_tenants()
#         tenant = next((t for t in tenants if t["tenant_id"] == tenant_id), None)

#         if not tenant:
#             return func.HttpResponse(
#                 f"Error: Tenant {tenant_id} not found", status_code=400
#             )

#         result = sync_hibp_breaches(tenant_id, tenant["name"])

#         if result["status"] == "success":
#             return func.HttpResponse(
#                 f"HIBP sync completed for {tenant['name']}: {result['users_checked']} users checked, {result['breaches_found']} breaches found",
#                 status_code=200,
#             )
#         else:
#             return func.HttpResponse(
#                 f"HIBP sync failed: {result['error']}", status_code=500
#             )

#     except Exception as e:
#         logging.error(f"Error in HIBP HTTP sync: {str(e)}")
#         return func.HttpResponse(f"Error: {str(e)}", status_code=500)


# def get_azure_conditional_policies(tenant_id: str) -> list:
#     graph = GraphClient(tenant_id)
#     return graph.get("/policies/conditionalAccess/policies")


@app.route(route="tenant/users/create", methods=["POST"])
def create_user(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint to create a new user account"""

    try:
        # Parse request body
        try:
            request_data = req.get_json()
        except Exception as e:
            return create_error_response(error_message=f"Invalid JSON in request body: {str(e)}", status_code=400)

        # Extract and validate required fields
        tenant_id = request_data.get("tenant_id")
        user_data = request_data.get("user_data")

        # Add detailed logging for debugging
        logging.info("=== USER CREATION DEBUG LOG ===")
        logging.info(f"Full request data: {request_data}")
        logging.info(f"Tenant ID: {tenant_id}")
        logging.info(f"User data received: {user_data}")
        logging.info(f"User data type: {type(user_data)}")

        # Log specific fields we're looking for
        logging.info(f"Department field: '{user_data.get('department')}' (type: {type(user_data.get('department'))})")
        logging.info(f"Job Title field: '{user_data.get('jobTitle')}' (type: {type(user_data.get('jobTitle'))})")
        logging.info(f"Office Location field: '{user_data.get('officeLocation')}' (type: {type(user_data.get('officeLocation'))})")
        logging.info(f"Mobile Phone field: '{user_data.get('mobilePhone')}' (type: {type(user_data.get('mobilePhone'))})")
        logging.info(f"All user_data keys: {list(user_data.keys()) if user_data else 'None'}")

        if not tenant_id:
            return create_error_response(error_message="tenant_id is required", status_code=400)

        if not user_data:
            return create_error_response(error_message="user_data is required", status_code=400)

        # Validate required user data fields
        required_fields = ["displayName", "passwordProfile"]
        missing_fields = [field for field in required_fields if field not in user_data]

        if missing_fields:
            return create_error_response(
                error_message=f"Missing required fields in user_data: {', '.join(missing_fields)}", status_code=400
            )

        # Validate password profile
        password_profile = user_data.get("passwordProfile", {})
        if "password" not in password_profile:
            return create_error_response(error_message="password is required in passwordProfile", status_code=400)

        execution_time = datetime.now().isoformat()

        # Extract first and last names from displayName
        display_name = user_data.get("displayName", "")
        first_name, last_name = "", ""

        # Try to split displayName into first and last names
        name_parts = display_name.strip().split()
        if len(name_parts) >= 2:
            first_name = name_parts[0].lower()
            last_name = name_parts[-1].lower()
        elif len(name_parts) == 1:
            first_name = name_parts[0].lower()
            last_name = first_name
        else:
            # Fallback to a generic name if displayName is empty or invalid
            first_name = "user"
            last_name = "account"

        graph_client = GraphBetaClient(tenant_id)

        # Fetch a single user from the tenant to derive the domain
        try:
            users_response = graph_client.get("/users", top=1, select=["userPrincipalName"])
            if users_response and len(users_response) > 0:
                existing_user = users_response[0]
                existing_user_principal_name = existing_user.get("userPrincipalName", "")
                # Extract domain from existing user's userPrincipalName
                if "@" in existing_user_principal_name:
                    domain = existing_user_principal_name.split("@")[1]
                else:
                    # Fallback to default domain format
                    domain = f"{tenant_id}.onmicrosoft.com"
            else:
                # No existing users, use default domain format
                domain = f"{tenant_id}.onmicrosoft.com"
        except Exception as e:
            logging.warning(f"Could not fetch existing user to derive domain: {str(e)}")
            # Fallback to default domain format
            domain = f"{tenant_id}.onmicrosoft.com"

        # Construct userPrincipalName from first name, last name, and domain
        user_principal_name = f"{first_name}{last_name}@{domain}"

        # Construct mailNickname from first and last name (required by Graph API)
        mail_nickname = f"{first_name}{last_name}"

        # Update user_data with the constructed userPrincipalName and mailNickname
        user_data["userPrincipalName"] = user_principal_name
        user_data["mailNickname"] = mail_nickname

        # Extract role and license assignments before filtering user_data
        role_assignment = user_data.get("roleAssignment")
        license_type = user_data.get("licenseType")

        # Remove unsupported fields that might be sent by the frontend
        if "defaultGroups" in user_data:
            logging.info(f"Removing unsupported 'defaultGroups' field: {user_data['defaultGroups']}")
            del user_data["defaultGroups"]

        # Remove role and license fields as they need to be handled separately
        if "roleAssignment" in user_data:
            logging.info(f"Extracted role assignment: {role_assignment}")
            del user_data["roleAssignment"]
        if "licenseType" in user_data:
            logging.info(f"Extracted license type: {license_type}")
            del user_data["licenseType"]

        logging.info(f"Creating user {user_principal_name} in tenant: {tenant_id}")
        logging.info(f"Role assignment: {role_assignment}")
        logging.info(f"License type: {license_type}")

        # Check if user already exists
        existing_user_query = "SELECT user_id FROM usersV2 WHERE tenant_id = ? AND user_principal_name = ?"
        existing_user_result = query(existing_user_query, (tenant_id, user_principal_name))

        if existing_user_result:
            return create_error_response(
                error_message=f"User {user_principal_name} already exists in tenant {tenant_id}",
                status_code=409,
                data={
                    "user_principal_name": user_principal_name,
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "constructed_from": {
                        "display_name": display_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "domain": domain,
                    },
                },
            )

        # Create user via Graph API
        logging.info(f"Creating user {user_principal_name} via Graph API")

        try:
            create_result = graph_client.create_user(user_data)
        except Exception as graph_error:
            error_msg = f"Graph API error: {str(graph_error)}"
            logging.error(f"Failed to create user via Graph API: {error_msg}")
            return create_error_response(
                error_message=f"Failed to create user: {error_msg}",
                status_code=500,
                tenant_id=tenant_id,
                operation="create_user",
                data={
                    "user_principal_name": user_principal_name,
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "status": "failed",
                    "error": error_msg,
                },
                domain=domain,
                execution_time=execution_time,
            )

        if create_result.get("status") != "success":
            error_msg = create_result.get("error", "Unknown error creating user")
            logging.error(f"Failed to create user via Graph API: {error_msg}")
            return create_error_response(
                error_message=f"Failed to create user: {error_msg}",
                status_code=500,
                tenant_id=tenant_id,
                operation="create_user",
                data={
                    "user_principal_name": user_principal_name,
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "status": "failed",
                    "error": error_msg,
                },
                domain=domain,
                execution_time=execution_time,
            )

        # Extract created user data
        created_user_data = create_result.get("data", {})
        user_id = created_user_data.get("id")

        if not user_id:
            error_msg = "Graph API returned success but no user ID in response"
            logging.error(f"Failed to create user: {error_msg}")
            return func.HttpResponse(
                json.dumps(
                    {
                        "success": False,
                        "error": f"Failed to create user: {error_msg}",
                        "data": {
                            "user_principal_name": user_principal_name,
                            "tenant_id": tenant_id,
                            "domain": domain,
                            "status": "failed",
                            "error": error_msg,
                        },
                        "metadata": {
                            "tenant_id": tenant_id,
                            "domain": domain,
                            "operation": "create_user",
                            "execution_time": execution_time,
                        },
                    }
                ),
                status_code=500,
                headers={"Content-Type": "application/json"},
            )

        # Prepare user record for database
        user_record = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "user_principal_name": created_user_data.get("userPrincipalName"),
            "primary_email": created_user_data.get("mail") or created_user_data.get("userPrincipalName"),
            "display_name": created_user_data.get("displayName"),
            "department": user_data.get("department"),  # Use original request data
            "job_title": user_data.get("jobTitle"),  # Use original request data
            "office_location": user_data.get("officeLocation"),  # Use original request data
            "mobile_phone": user_data.get("mobilePhone"),  # Use original request data
            "account_type": created_user_data.get("userType"),
            "account_enabled": 1 if created_user_data.get("accountEnabled", True) else 0,
            "is_global_admin": 0,  # New users are not global admins by default
            "is_mfa_compliant": 0,  # New users haven't set up MFA yet
            "license_count": 0,  # New users have no licenses assigned
            "group_count": 0,  # New users are not in any groups yet
            "last_sign_in_date": None,  # New users haven't signed in yet
            "last_password_change": None,  # New users haven't changed password yet
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
        }

        # Insert user into database
        try:
            upsert_many("usersV2", [user_record])
            logging.info(f"Successfully added user {user_principal_name} to database")
        except Exception as db_error:
            logging.warning(f"Failed to add user to database: {str(db_error)}")
            # Continue anyway as the user was created in Graph API

        # Assign role if specified
        if role_assignment:
            try:
                logging.info(f"Assigning role '{role_assignment}' to user {user_id}")
                role_result = graph_client.assign_role(user_id, role_assignment)

                if role_result.get("status") == "success":
                    logging.info(f"Successfully assigned role '{role_assignment}' to user {user_id}")

                    # Insert role assignment into database
                    role_record = {
                        "user_id": user_id,
                        "tenant_id": tenant_id,
                        "role_id": role_assignment,
                        "user_principal_name": user_principal_name,
                        "role_display_name": role_assignment,
                        "role_description": "Role assigned via create_user endpoint",
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                    }

                    try:
                        logging.info(f"Attempting to insert role record: {role_record}")
                        upsert_many("user_rolesV2", [role_record])
                        logging.info(f"Successfully added role assignment to database for user {user_id}")
                    except Exception as role_db_error:
                        logging.error(f"Failed to add role assignment to database: {str(role_db_error)}")
                        logging.error(f"Role record that failed: {role_record}")
                        logging.error("Table name: user_rolesV2")
                else:
                    logging.warning(
                        f"Failed to assign role '{role_assignment}' to user {user_id}: {role_result.get('error', 'Unknown error')}"
                    )

            except Exception as role_error:
                logging.warning(f"Error assigning role '{role_assignment}' to user {user_id}: {str(role_error)}")

        # Assign license if specified
        if license_type:
            try:
                logging.info(f"Assigning license '{license_type}' to user {user_id}")
                license_result = graph_client.assign_license(user_id, license_type)

                if license_result.get("status") == "success":
                    logging.info(f"Successfully assigned license '{license_type}' to user {user_id}")

                    # Insert license assignment into database
                    license_record = {
                        "user_id": user_id,
                        "tenant_id": tenant_id,
                        "license_id": license_type,
                        "user_principal_name": user_principal_name,
                        "license_display_name": license_type,
                        "license_partnumber": license_type,
                        "is_active": 1,
                        "monthly_cost": 0.0,
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                    }

                    try:
                        upsert_many("user_licensesV2", [license_record])
                        logging.info(f"Successfully added license assignment to database for user {user_id}")
                    except Exception as license_db_error:
                        logging.warning(f"Failed to add license assignment to database: {str(license_db_error)}")
                else:
                    logging.warning(
                        f"Failed to assign license '{license_type}' to user {user_id}: {license_result.get('error', 'Unknown error')}"
                    )

            except Exception as license_error:
                logging.warning(f"Error assigning license '{license_type}' to user {user_id}: {str(license_error)}")

        # Return success response
        return func.HttpResponse(
            json.dumps(
                {
                    "success": True,
                    "message": f"User {user_principal_name} created successfully",
                    "data": {
                        "user_id": user_id,
                        "user_principal_name": user_principal_name,
                        "display_name": created_user_data.get("displayName"),
                        "tenant_id": tenant_id,
                        "domain": domain,
                        "account_enabled": created_user_data.get("accountEnabled", True),
                        "created_at": execution_time,
                        "role_assignment": role_assignment,
                        "license_type": license_type,
                        "constructed_from": {
                            "display_name": display_name,
                            "first_name": first_name,
                            "last_name": last_name,
                            "domain": domain,
                        },
                    },
                    "metadata": {"tenant_id": tenant_id, "domain": domain, "operation": "create_user", "execution_time": execution_time},
                }
            ),
            status_code=201,
            headers={"Content-Type": "application/json"},
        )

    except Exception as e:
        error_msg = f"Unexpected error during create_user: {str(e)}"
        logging.error(error_msg)
        logging.exception("Full exception details:")

        return func.HttpResponse(
            json.dumps(
                {"success": False, "error": error_msg, "metadata": {"operation": "create_user", "timestamp": datetime.now().isoformat()}}
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


@app.route(route="users/{user_id}/delete", methods=["DELETE"])
def delete_user(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP DELETE endpoint to delete a single user account"""
    # single tenant, single resource operation

    try:
        # extract and validate request data
        user_id = req.route_params.get("user_id")
        if not user_id:
            return create_error_response(error_message="user_id is required in the URL path", status_code=400)

        logging.info(f"Processing user delete request for user_id: {user_id}")

        # get tenant_id from query parameters
        tenant_id = req.params.get("tenant_id")
        if not tenant_id:
            return create_error_response(error_message="tenant_id parameter is required", status_code=400)

        logging.info(f"Using provided tenant_id: {tenant_id}")

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["display_name"] for t in tenants}

        if tenant_id not in tenant_names:
            return create_error_response(error_message=f"Tenant '{tenant_id}' not found", status_code=404)

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Deleting user for tenant: {tenant_name}")

        # find and validate user exists in database
        user_query = "SELECT * FROM usersV2 WHERE tenant_id = ? AND user_id = ?"
        user_result = query(user_query, (tenant_id, user_id))
        identifier = f"user_id: {user_id}"

        if not user_result:
            return create_error_response(error_message=f"User not found ({identifier})", status_code=404)

        user = user_result[0]
        logging.info(f"Found user: {user['user_principal_name']}")

        # Safety check: require disabled users before deletion
        if user.get("account_enabled", True):
            return create_error_response(
                error_message="User must be disabled before deletion for safety",
                status_code=422,
                tenant_id=tenant_id,
                tenant_name=tenant_name,
                operation="delete_user",
                data={
                    "user_id": user["user_id"],
                    "user_principal_name": user["user_principal_name"],
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_name,
                    "status": "active_user",
                    "note": "Disable user first, then delete",
                },
            )

        # Safety check: warn about global admins
        if user.get("is_global_admin", False):
            logging.warning(f"WARNING: Deleting global admin user {user['user_principal_name']}")

        # Initialize Graph Beta Client
        graph_client = GraphBetaClient(tenant_id)

        # Delete user via Graph API
        delete_result = graph_client.delete_user(user["user_id"])

        if delete_result.get("status") != "success":
            error_msg = delete_result.get("error", "Unknown error")
            logging.error(f"Failed to delete user: {error_msg}")

            return create_error_response(
                error_message=f"Failed to delete user: {error_msg}",
                status_code=500,
                tenant_id=tenant_id,
                tenant_name=tenant_name,
                operation="delete_user",
                data={
                    "user_id": user["user_id"],
                    "user_principal_name": user["user_principal_name"],
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_name,
                    "status": "failed",
                    "error": error_msg,
                },
            )

        # Remove user and related records from local database
        try:
            # Delete related records first (foreign key constraints)
            execute_query("DELETE FROM user_licensesV2 WHERE tenant_id = ? AND user_id = ?", (tenant_id, user["user_id"]))
            execute_query("DELETE FROM user_rolesV2 WHERE tenant_id = ? AND user_id = ?", (tenant_id, user["user_id"]))

            # Delete user record
            execute_query("DELETE FROM usersV2 WHERE tenant_id = ? AND user_id = ?", (tenant_id, user["user_id"]))
            logging.info(f"Successfully removed user {user['user_principal_name']} and related records from database")
        except Exception as db_error:
            logging.warning(f"Failed to remove user from database: {str(db_error)}")
            # Continue anyway as the user was deleted from Graph API

        # Return success response using utility function
        return create_success_response(
            data={
                "user_id": user["user_id"],
                "user_principal_name": user["user_principal_name"],
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "deleted_at": datetime.now().isoformat(),
            },
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="delete_user",
            message=f"User {user['user_principal_name']} deleted successfully",
        )

    except Exception as e:
        error_msg = f"Unexpected error during delete_user: {str(e)}"
        logging.error(error_msg)
        logging.exception("Full exception details:")

        return create_error_response(
            error_message=error_msg, status_code=500, operation="delete_user", timestamp=datetime.now().isoformat()
        )


@app.route(route="users/bulk-disable", methods=["POST"])
def disable_users_bulk(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint to disable multiple selected users for a tenant"""
    # single tenant, multiple resource operation

    try:
        # extract and validate request data
        logging.info("Processing bulk user disable request for selected users")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Request body is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # get parameters from request
        tenant_id = req_body.get("tenant_id")
        user_ids = req_body.get("user_ids", [])
        # dry_run = req_body.get("dry_run", False)  # Commented out for production testing

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "tenant_id is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not user_ids or not isinstance(user_ids, list):
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array is required and must be a list"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if len(user_ids) == 0:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array cannot be empty"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"Tenant '{tenant_id}' not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Processing bulk disable for {len(user_ids)} selected users in tenant: {tenant_name}")

        # get user details for the selected user IDs
        user_ids_str = ",".join(["?" for _ in user_ids])
        users_query = f"""
        SELECT user_id, display_name, user_principal_name, last_sign_in_date, account_enabled
        FROM usersV2
        WHERE tenant_id = ? AND user_id IN ({user_ids_str})
        """
        query_params = [tenant_id] + user_ids
        selected_users = query(users_query, query_params)

        if not selected_users:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "None of the specified user IDs were found in the tenant"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        # check if all requested users were found
        found_user_ids = {user["user_id"] for user in selected_users}
        missing_user_ids = set(user_ids) - found_user_ids
        if missing_user_ids:
            logging.warning(f"Some user IDs not found: {missing_user_ids}")

        # initialize tracking variables
        processed_users = []
        counters = {"successfully_disabled": 0, "already_disabled": 0, "failed": 0, "skipped": 0}

        # initialize Graph client (only if not dry run)

        graph_client = GraphBetaClient(tenant_id)

        # process each selected user
        for user in selected_users:
            user_id = user["user_id"]
            user_principal_name = user["user_principal_name"]
            display_name = user.get("display_name")
            last_sign_in = user.get("last_sign_in_date")

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
            }

            try:
                # check if user is already disabled
                if not user.get("account_enabled", True):
                    user_data["status"] = "already_disabled"
                    user_data["note"] = "User was already disabled"
                    counters["already_disabled"] += 1
                    processed_users.append(user_data)
                    logging.info(f"User {user_principal_name} already disabled, skipping")
                    continue

                else:
                    # actually disable the user via Graph API
                    logging.info(f"Disabling user {user_principal_name} via Graph API")
                    disable_result = graph_client.disable_user(user_id)

                    if disable_result.get("status") != "success":
                        error_msg = disable_result.get("error", "Unknown error disabling user")
                        user_data["status"] = "failed"
                        user_data["error"] = error_msg
                        counters["failed"] += 1
                        processed_users.append(user_data)
                        logging.error(f"Failed to disable {user_principal_name}: {error_msg}")
                        continue

                    # update local database to reflect disabled status
                    current_time = datetime.now().isoformat()
                    update_query = "UPDATE usersV2 SET account_enabled = 0, last_updated = ? WHERE tenant_id = ? AND user_id = ?"

                    try:
                        execute_query(update_query, (current_time, tenant_id, user_id))
                        logging.info(f"Updated local database for user {user_principal_name}")
                    except Exception as db_error:
                        logging.warning(
                            f"Graph API disable succeeded but local DB update failed for {user_principal_name}: {str(db_error)}"
                        )
                        # note: user is disabled in Graph but local DB might be out of sync

                    user_data["status"] = "disabled"
                    user_data["disabled_at"] = current_time
                    counters["successfully_disabled"] += 1
                    processed_users.append(user_data)
                    logging.info(f"Successfully disabled user {user_principal_name}")

            except Exception as e:
                error_msg = f"Error processing user {user_principal_name}: {str(e)}"
                user_data["status"] = "failed"
                user_data["error"] = error_msg
                counters["failed"] += 1
                processed_users.append(user_data)
                logging.error(error_msg)

        # build actions array based on results
        actions = []

        if counters["failed"] > 0:
            actions.append(
                {
                    "type": "review_failures",
                    "description": f"{counters['failed']} user(s) failed to disable - review permissions",
                    "users_affected": counters["failed"],
                }
            )

        if counters["successfully_disabled"] > 0:
            actions.append(
                {
                    "type": "verify_disabled",
                    "description": f"{counters['successfully_disabled']} user(s) successfully disabled",
                    "users_affected": counters["successfully_disabled"],
                }
            )

        if counters["already_disabled"] > 0:
            actions.append(
                {
                    "type": "review_already_disabled",
                    "description": f"{counters['already_disabled']} user(s) were already disabled",
                    "users_affected": counters["already_disabled"],
                }
            )

        # build final response using utility function
        return create_bulk_operation_response(
            data=processed_users,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="bulk_disable_selected_users",
            summary={
                "total_requested": len(user_ids),
                "total_found": len(selected_users),
                "missing_user_ids": list(missing_user_ids) if missing_user_ids else [],
                "successfully_disabled": counters["successfully_disabled"],
                "already_disabled": counters["already_disabled"],
                "failed": counters["failed"],
                "skipped": counters["skipped"],
            },
            actions=actions,
            execution_time=execution_time,
        )

    except Exception as e:
        error_msg = f"Error in bulk disable operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "data": [],
                    "metadata": {
                        "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                        "operation": "bulk_disable_selected_users",
                        "execution_time": datetime.now().isoformat(),
                    },
                    "actions": [],
                }
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


@app.route(route="users/bulk-reset-password", methods=["POST"])
def reset_password_bulk(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint to reset passwords for multiple selected users for a tenant"""
    # single tenant, multiple resource operation

    try:
        # extract and validate request data
        logging.info("Processing bulk password reset request for selected users")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Request body is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # get parameters from request
        tenant_id = req_body.get("tenant_id")
        user_ids = req_body.get("user_ids", [])

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "tenant_id is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not user_ids or not isinstance(user_ids, list):
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array is required and must be a list"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if len(user_ids) == 0:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array cannot be empty"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"Tenant '{tenant_id}' not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Processing bulk password reset for {len(user_ids)} selected users in tenant: {tenant_name}")

        # get user details for the selected user IDs
        user_ids_str = ",".join(["?" for _ in user_ids])
        users_query = f"""
        SELECT user_id, display_name, user_principal_name, last_sign_in_date, account_enabled
        FROM usersV2
        WHERE tenant_id = ? AND user_id IN ({user_ids_str})
        """
        query_params = [tenant_id] + user_ids
        selected_users = query(users_query, query_params)

        if not selected_users:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "None of the specified user IDs were found in the tenant"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        # check if all requested users were found
        found_user_ids = {user["user_id"] for user in selected_users}
        missing_user_ids = set(user_ids) - found_user_ids
        if missing_user_ids:
            logging.warning(f"Some user IDs not found: {missing_user_ids}")

        # initialize tracking variables
        processed_users = []
        counters = {"successfully_reset": 0, "failed": 0, "skipped": 0}

        # initialize Graph client
        graph_client = GraphBetaClient(tenant_id)

        # process each selected user
        for user in selected_users:
            user_id = user["user_id"]
            user_principal_name = user["user_principal_name"]
            display_name = user.get("display_name")
            last_sign_in = user.get("last_sign_in_date")

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
            }

            try:
                # check if user is disabled
                if not user.get("account_enabled", True):
                    user_data["status"] = "skipped"
                    user_data["note"] = "User is disabled - cannot reset password"
                    counters["skipped"] += 1
                    processed_users.append(user_data)
                    logging.info(f"User {user_principal_name} is disabled, skipping password reset")
                    continue

                # reset password via Graph API
                logging.info(f"Resetting password for user {user_principal_name} via Graph API")
                reset_result = graph_client.reset_user_password(user_id)

                if reset_result.get("status") != "success":
                    error_msg = reset_result.get("error", "Unknown error resetting password")
                    user_data["status"] = "failed"
                    user_data["error"] = error_msg
                    counters["failed"] += 1
                    processed_users.append(user_data)
                    logging.error(f"Failed to reset password for {user_principal_name}: {error_msg}")
                    continue

                # update local database to track password reset
                current_time = datetime.now().isoformat()
                update_query = "UPDATE usersV2 SET last_updated = ? WHERE tenant_id = ? AND user_id = ?"

                try:
                    execute_query(update_query, (current_time, tenant_id, user_id))
                    logging.info(f"Updated local database for user {user_principal_name}")
                except Exception as db_error:
                    logging.warning(
                        f"Graph API password reset succeeded but local DB update failed for {user_principal_name}: {str(db_error)}"
                    )

                user_data["status"] = "password_reset"
                user_data["reset_at"] = current_time
                user_data["temporary_password"] = reset_result.get("temporary_password")
                user_data["force_change_password"] = True
                counters["successfully_reset"] += 1
                processed_users.append(user_data)
                logging.info(f"Successfully reset password for user {user_principal_name}")

            except Exception as e:
                error_msg = f"Error processing user {user_principal_name}: {str(e)}"
                user_data["status"] = "failed"
                user_data["error"] = error_msg
                counters["failed"] += 1
                processed_users.append(user_data)
                logging.error(error_msg)

        # build actions array based on results
        actions = []

        if counters["failed"] > 0:
            actions.append(
                {
                    "type": "review_failures",
                    "description": f"{counters['failed']} user(s) failed password reset - review permissions",
                    "users_affected": counters["failed"],
                }
            )

        if counters["successfully_reset"] > 0:
            actions.append(
                {
                    "type": "secure_delivery",
                    "description": f"Securely deliver {counters['successfully_reset']} temporary password(s) to users",
                    "users_affected": counters["successfully_reset"],
                }
            )

        if counters["skipped"] > 0:
            actions.append(
                {
                    "type": "review_skipped",
                    "description": f"{counters['skipped']} user(s) were skipped (disabled accounts)",
                    "users_affected": counters["skipped"],
                }
            )

        # build final response using utility function
        return create_bulk_operation_response(
            data=processed_users,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="bulk_reset_password_selected_users",
            summary={
                "total_requested": len(user_ids),
                "total_found": len(selected_users),
                "missing_user_ids": list(missing_user_ids) if missing_user_ids else [],
                "successfully_reset": counters["successfully_reset"],
                "failed": counters["failed"],
                "skipped": counters["skipped"],
            },
            actions=actions,
            execution_time=execution_time,
        )

    except Exception as e:
        error_msg = f"Error in bulk password reset operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "data": [],
                    "metadata": {
                        "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                        "operation": "bulk_reset_password_selected_users",
                        "execution_time": datetime.now().isoformat(),
                    },
                    "actions": [],
                }
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


@app.route(route="users/bulk-assign-license", methods=["POST"])
def assign_license_bulk(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint to assign the same license to multiple selected users for a tenant"""
    # single tenant, multiple resource operation

    try:
        # extract and validate request data
        logging.info("Processing bulk license assignment request for selected users")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Request body is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # get parameters from request
        tenant_id = req_body.get("tenant_id")
        user_ids = req_body.get("user_ids", [])
        license_sku = req_body.get("license_sku")

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "tenant_id is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not user_ids or not isinstance(user_ids, list):
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array is required and must be a list"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if len(user_ids) == 0:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array cannot be empty"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not license_sku:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "license_sku is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"Tenant '{tenant_id}' not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Processing bulk license assignment for {len(user_ids)} selected users in tenant: {tenant_name}")

        # get user details for the selected user IDs
        user_ids_str = ",".join(["?" for _ in user_ids])
        users_query = f"""
        SELECT user_id, display_name, user_principal_name, last_sign_in_date, account_enabled, license_count
        FROM usersV2
        WHERE tenant_id = ? AND user_id IN ({user_ids_str})
        """
        query_params = [tenant_id] + user_ids
        selected_users = query(users_query, query_params)

        if not selected_users:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "None of the specified user IDs were found in the tenant"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        # check if all requested users were found
        found_user_ids = {user["user_id"] for user in selected_users}
        missing_user_ids = set(user_ids) - found_user_ids
        if missing_user_ids:
            logging.warning(f"Some user IDs not found: {missing_user_ids}")

        # initialize tracking variables
        processed_users = []
        counters = {"successfully_assigned": 0, "failed": 0, "skipped": 0}

        # initialize Graph client
        graph_client = GraphBetaClient(tenant_id)

        # process each selected user
        for user in selected_users:
            user_id = user["user_id"]
            user_principal_name = user["user_principal_name"]
            display_name = user.get("display_name")
            last_sign_in = user.get("last_sign_in_date")
            current_license_count = user.get("license_count", 0)

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
                "previous_license_count": current_license_count,
            }

            try:
                # check if user is disabled
                if not user.get("account_enabled", True):
                    user_data["status"] = "skipped"
                    user_data["note"] = "User is disabled - cannot assign license"
                    counters["skipped"] += 1
                    processed_users.append(user_data)
                    logging.info(f"User {user_principal_name} is disabled, skipping license assignment")
                    continue

                # assign license via Graph API
                logging.info(f"Assigning license '{license_sku}' to user {user_principal_name} via Graph API")
                license_result = graph_client.assign_license(user_id, license_sku)

                if license_result.get("status") != "success":
                    error_msg = license_result.get("error", "Unknown error assigning license")
                    user_data["status"] = "failed"
                    user_data["error"] = error_msg
                    counters["failed"] += 1
                    processed_users.append(user_data)
                    logging.error(f"Failed to assign license to {user_principal_name}: {error_msg}")
                    continue

                # update local database to track license assignment
                current_time = datetime.now().isoformat()
                update_query = "UPDATE usersV2 SET license_count = license_count + 1, last_updated = ? WHERE tenant_id = ? AND user_id = ?"

                try:
                    execute_query(update_query, (current_time, tenant_id, user_id))
                    logging.info(f"Updated local database for user {user_principal_name}")
                except Exception as db_error:
                    logging.warning(
                        f"Graph API license assignment succeeded but local DB update failed for {user_principal_name}: {str(db_error)}"
                    )

                user_data["status"] = "license_assigned"
                user_data["assigned_at"] = current_time
                user_data["license_sku"] = license_sku
                user_data["new_license_count"] = current_license_count + 1
                user_data["license_details"] = license_result.get("data", {})
                counters["successfully_assigned"] += 1
                processed_users.append(user_data)
                logging.info(f"Successfully assigned license '{license_sku}' to user {user_principal_name}")

            except Exception as e:
                error_msg = f"Error processing user {user_principal_name}: {str(e)}"
                user_data["status"] = "failed"
                user_data["error"] = error_msg
                counters["failed"] += 1
                processed_users.append(user_data)
                logging.error(error_msg)

        # build actions array based on results
        actions = []

        if counters["failed"] > 0:
            actions.append(
                {
                    "type": "review_failures",
                    "description": f"{counters['failed']} user(s) failed license assignment - review permissions",
                    "users_affected": counters["failed"],
                }
            )

        if counters["successfully_assigned"] > 0:
            actions.append(
                {
                    "type": "verify_licenses",
                    "description": f"Verify {counters['successfully_assigned']} license assignment(s) in Microsoft 365 admin center",
                    "users_affected": counters["successfully_assigned"],
                }
            )

        if counters["skipped"] > 0:
            actions.append(
                {
                    "type": "review_skipped",
                    "description": f"{counters['skipped']} user(s) were skipped (disabled accounts)",
                    "users_affected": counters["skipped"],
                }
            )

        # build final response using utility function
        return create_bulk_operation_response(
            data=processed_users,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="bulk_assign_license_selected_users",
            summary={
                "total_requested": len(user_ids),
                "total_found": len(selected_users),
                "missing_user_ids": list(missing_user_ids) if missing_user_ids else [],
                "successfully_assigned": counters["successfully_assigned"],
                "failed": counters["failed"],
                "skipped": counters["skipped"],
            },
            actions=actions,
            execution_time=execution_time,
            license_sku=license_sku,
        )

    except Exception as e:
        error_msg = f"Error in bulk license assignment operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "data": [],
                    "metadata": {
                        "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                        "license_sku": req_body.get("license_sku") if "req_body" in locals() else None,
                        "operation": "bulk_assign_license_selected_users",
                        "execution_time": datetime.now().isoformat(),
                    },
                    "actions": [],
                }
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


@app.route(route="users/bulk-assign-role", methods=["POST"])
def assign_role_bulk(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint to assign the same directory role to multiple selected users for a tenant"""
    # single tenant, multiple resource operation

    try:
        # extract and validate request data
        logging.info("Processing bulk role assignment request for selected users")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Request body is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # get parameters from request
        tenant_id = req_body.get("tenant_id")
        user_ids = req_body.get("user_ids", [])
        role_name = req_body.get("role_name")

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "tenant_id is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not user_ids or not isinstance(user_ids, list):
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array is required and must be a list"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if len(user_ids) == 0:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array cannot be empty"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not role_name:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "role_name is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"Tenant '{tenant_id}' not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Processing bulk role assignment for {len(user_ids)} selected users in tenant: {tenant_name}")

        # get user details for the selected user IDs
        user_ids_str = ",".join(["?" for _ in user_ids])
        users_query = f"""
        SELECT user_id, display_name, user_principal_name, last_sign_in_date, account_enabled, is_global_admin
        FROM usersV2
        WHERE tenant_id = ? AND user_id IN ({user_ids_str})
        """
        query_params = [tenant_id] + user_ids
        selected_users = query(users_query, query_params)

        if not selected_users:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "None of the specified user IDs were found in the tenant"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        # check if all requested users were found
        found_user_ids = {user["user_id"] for user in selected_users}
        missing_user_ids = set(user_ids) - found_user_ids
        if missing_user_ids:
            logging.warning(f"Some user IDs not found: {missing_user_ids}")

        # initialize tracking variables
        processed_users = []
        counters = {"successfully_assigned": 0, "failed": 0, "skipped": 0}

        # initialize Graph client
        graph_client = GraphBetaClient(tenant_id)

        # process each selected user
        for user in selected_users:
            user_id = user["user_id"]
            user_principal_name = user["user_principal_name"]
            display_name = user.get("display_name")
            last_sign_in = user.get("last_sign_in_date")
            is_currently_admin = user.get("is_global_admin", False)

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
                "was_admin_before": is_currently_admin,
            }

            try:
                # check if user is disabled
                if not user.get("account_enabled", True):
                    user_data["status"] = "skipped"
                    user_data["note"] = "User is disabled - cannot assign role"
                    counters["skipped"] += 1
                    processed_users.append(user_data)
                    logging.info(f"User {user_principal_name} is disabled, skipping role assignment")
                    continue

                # assign role via Graph API
                logging.info(f"Assigning role '{role_name}' to user {user_principal_name} via Graph API")
                role_result = graph_client.assign_role(user_id, role_name)

                if role_result.get("status") != "success":
                    error_msg = role_result.get("error", "Unknown error assigning role")
                    user_data["status"] = "failed"
                    user_data["error"] = error_msg
                    counters["failed"] += 1
                    processed_users.append(user_data)
                    logging.error(f"Failed to assign role to {user_principal_name}: {error_msg}")
                    continue

                # update local database to track role assignment
                current_time = datetime.now().isoformat()

                # Update role-specific fields based on the role being assigned
                if role_name.lower() in ["global administrator", "global admin"]:
                    update_query = "UPDATE usersV2 SET is_global_admin = 1, last_updated = ? WHERE tenant_id = ? AND user_id = ?"
                else:
                    # For other roles, just update the last_updated timestamp
                    update_query = "UPDATE usersV2 SET last_updated = ? WHERE tenant_id = ? AND user_id = ?"

                try:
                    execute_query(update_query, (current_time, tenant_id, user_id))
                    logging.info(f"Updated local database for user {user_principal_name}")
                except Exception as db_error:
                    logging.warning(
                        f"Graph API role assignment succeeded but local DB update failed for {user_principal_name}: {str(db_error)}"
                    )

                user_data["status"] = "role_assigned"
                user_data["assigned_at"] = current_time
                user_data["role_name"] = role_name
                user_data["is_admin_after"] = role_name.lower() in ["global administrator", "global admin"]
                counters["successfully_assigned"] += 1
                processed_users.append(user_data)
                logging.info(f"Successfully assigned role '{role_name}' to user {user_principal_name}")

            except Exception as e:
                error_msg = f"Error processing user {user_principal_name}: {str(e)}"
                user_data["status"] = "failed"
                user_data["error"] = error_msg
                counters["failed"] += 1
                processed_users.append(user_data)
                logging.error(error_msg)

        # build actions array based on results
        actions = []

        if counters["failed"] > 0:
            actions.append(
                {
                    "type": "review_failures",
                    "description": f"{counters['failed']} user(s) failed role assignment - review permissions",
                    "users_affected": counters["failed"],
                }
            )

        if counters["successfully_assigned"] > 0:
            actions.append(
                {
                    "type": "verify_roles",
                    "description": f"Verify {counters['successfully_assigned']} role assignment(s) in Microsoft 365 admin center",
                    "users_affected": counters["successfully_assigned"],
                }
            )

        if counters["skipped"] > 0:
            actions.append(
                {
                    "type": "review_skipped",
                    "description": f"{counters['skipped']} user(s) were skipped (disabled accounts)",
                    "users_affected": counters["skipped"],
                }
            )

        # build final response using utility function
        return create_bulk_operation_response(
            data=processed_users,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="bulk_assign_role_selected_users",
            summary={
                "total_requested": len(user_ids),
                "total_found": len(selected_users),
                "missing_user_ids": list(missing_user_ids) if missing_user_ids else [],
                "successfully_assigned": counters["successfully_assigned"],
                "failed": counters["failed"],
                "skipped": counters["skipped"],
            },
            actions=actions,
            execution_time=execution_time,
            role_name=role_name,
        )

    except Exception as e:
        error_msg = f"Error in bulk role assignment operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "data": [],
                    "metadata": {
                        "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                        "role_name": req_body.get("role_name") if "req_body" in locals() else None,
                        "operation": "bulk_assign_role_selected_users",
                        "execution_time": datetime.now().isoformat(),
                    },
                    "actions": [],
                }
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


@app.route(route="users/bulk-delete", methods=["POST"])
def delete_user_bulk(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP POST endpoint to delete multiple selected users for a tenant"""
    # single tenant, multiple resource operation

    try:
        # extract and validate request data
        logging.info("Processing bulk user delete request for selected users")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Request body is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # get parameters from request
        tenant_id = req_body.get("tenant_id")
        user_ids = req_body.get("user_ids", [])

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "tenant_id is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if not user_ids or not isinstance(user_ids, list):
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array is required and must be a list"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        if len(user_ids) == 0:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "user_ids array cannot be empty"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"Tenant '{tenant_id}' not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Processing bulk delete for {len(user_ids)} selected users in tenant: {tenant_name}")

        # get user details for the selected user IDs
        user_ids_str = ",".join(["?" for _ in user_ids])
        users_query = f"""
        SELECT user_id, display_name, user_principal_name, last_sign_in_date, account_enabled, 
               license_count, group_count, is_global_admin
        FROM usersV2
        WHERE tenant_id = ? AND user_id IN ({user_ids_str})
        """
        query_params = [tenant_id] + user_ids
        selected_users = query(users_query, query_params)

        if not selected_users:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "None of the specified user IDs were found in the tenant"}),
                status_code=404,
                headers={"Content-Type": "application/json"},
            )

        # check if all requested users were found
        found_user_ids = {user["user_id"] for user in selected_users}
        missing_user_ids = set(user_ids) - found_user_ids
        if missing_user_ids:
            logging.warning(f"Some user IDs not found: {missing_user_ids}")

        # initialize tracking variables
        processed_users = []
        counters = {"successfully_deleted": 0, "failed": 0, "skipped": 0}

        # initialize Graph
        graph_client = GraphBetaClient(tenant_id)

        # process each selected user
        for user in selected_users:
            user_id = user["user_id"]
            user_principal_name = user["user_principal_name"]
            display_name = user.get("display_name")
            last_sign_in = user.get("last_sign_in_date")
            account_enabled = user.get("account_enabled", True)
            license_count = user.get("license_count", 0)
            group_count = user.get("group_count", 0)
            is_global_admin = user.get("is_global_admin", False)

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
                "account_enabled": account_enabled,
                "license_count": license_count,
                "group_count": group_count,
                "is_global_admin": is_global_admin,
            }

            try:
                # Safety check: require disabled users before deletion
                if account_enabled:
                    user_data["status"] = "skipped"
                    user_data["note"] = "User is active - must disable before deletion"
                    counters["skipped"] += 1
                    processed_users.append(user_data)
                    logging.warning(f"User {user_principal_name} is active, skipping deletion for safety")
                    continue

                # Safety check: warn about global admins
                if is_global_admin:
                    logging.warning(f"WARNING: Deleting global admin user {user_principal_name}")

                # Delete user via Graph API first
                logging.info(f"Deleting user {user_principal_name} via Graph API")
                delete_result = graph_client.delete_user(user_id)

                if delete_result.get("status") != "success":
                    error_msg = delete_result.get("error", "Unknown error deleting user")
                    user_data["status"] = "failed"
                    user_data["error"] = error_msg
                    counters["failed"] += 1
                    processed_users.append(user_data)
                    logging.error(f"Failed to delete {user_principal_name}: {error_msg}")
                    continue

                # Remove user and related records from local database
                try:
                    # Delete related records first (foreign key constraints)
                    execute_query("DELETE FROM user_licensesV2 WHERE tenant_id = ? AND user_id = ?", (tenant_id, user_id))
                    execute_query("DELETE FROM user_rolesV2 WHERE tenant_id = ? AND user_id = ?", (tenant_id, user_id))

                    # Delete user record
                    execute_query("DELETE FROM usersV2 WHERE tenant_id = ? AND user_id = ?", (tenant_id, user_id))

                    logging.info(f"Successfully cleaned up local database for user {user_principal_name}")

                except Exception as db_error:
                    logging.error(f"Graph API deletion succeeded but local DB cleanup failed for {user_principal_name}: {str(db_error)}")
                    # Note: User is deleted in Graph but local DB might be out of sync

                user_data["status"] = "deleted"
                user_data["deleted_at"] = execution_time
                user_data["dependencies_cleaned"] = {
                    "licenses_removed": license_count,
                    "roles_removed": group_count,
                    "was_global_admin": is_global_admin,
                }
                counters["successfully_deleted"] += 1
                processed_users.append(user_data)
                logging.info(f"Successfully deleted user {user_principal_name}")

            except Exception as e:
                error_msg = f"Error processing user {user_principal_name}: {str(e)}"
                user_data["status"] = "failed"
                user_data["error"] = error_msg
                counters["failed"] += 1
                processed_users.append(user_data)
                logging.error(error_msg)

        # build actions array based on results
        actions = []

        if counters["failed"] > 0:
            actions.append(
                {
                    "type": "review_failures",
                    "description": f"{counters['failed']} user(s) failed deletion - review permissions",
                    "users_affected": counters["failed"],
                }
            )

        if counters["successfully_deleted"] > 0:
            actions.append(
                {
                    "type": "verify_deletion",
                    "description": f"Verify {counters['successfully_deleted']} user deletion(s) in Microsoft 365 admin center",
                    "users_affected": counters["successfully_deleted"],
                }
            )

        if counters["skipped"] > 0:
            actions.append(
                {
                    "type": "disable_users_first",
                    "description": f"{counters['skipped']} user(s) were skipped - disable them first before deletion",
                    "users_affected": counters["skipped"],
                }
            )

        # build final response using utility function
        return create_bulk_operation_response(
            data=processed_users,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            operation="bulk_delete_selected_users",
            summary={
                "total_requested": len(user_ids),
                "total_found": len(selected_users),
                "missing_user_ids": list(missing_user_ids) if missing_user_ids else [],
                "successfully_deleted": counters["successfully_deleted"],
                "failed": counters["failed"],
                "skipped": counters["skipped"],
            },
            actions=actions,
            execution_time=execution_time,
        )

    except Exception as e:
        error_msg = f"Error in bulk user deletion operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "data": [],
                    "metadata": {
                        "tenant_id": req_body.get("tenant_id") if "req_body" in locals() else None,
                        "operation": "bulk_delete_selected_users",
                        "execution_time": datetime.now().isoformat(),
                    },
                    "actions": [],
                }
            ),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )
