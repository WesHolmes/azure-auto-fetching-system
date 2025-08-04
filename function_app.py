import logging
import azure.functions as func
from core.graph_client import GraphClient
from core.tenant_manager import get_tenants
from core.graph_client import GraphClient
from sync.user_sync import sync_users
from sync.service_principal_sync import sync_service_principals
from sync.license_sync import sync_licenses
from sync.role_sync import sync_roles_for_tenants
from core.error_reporting import categorize_sync_errors, aggregate_recent_sync_errors
from sync.policy_sync import sync_conditional_access_policies
from analysis.user_analysis import (
    calculate_inactive_users,
    calculate_mfa_compliance,
    calculate_license_optimization,
)
from datetime import datetime, timedelta
from core.database import query
import json
import re
# from sync.hibp_sync import sync_hibp_breaches

app = func.FunctionApp()


def extract_error_code(error_message):
    """Extract HTTP error code from error message"""
    # Look for patterns like "403 Forbidden", "401 Unauthorized", etc.
    match = re.search(r"(\d{3})\s+\w+", str(error_message))
    if match:
        return int(match.group(1))
    return None


def log_error_summary(error_counts, sync_type):
    """Log a summary of errors encountered during sync"""
    if error_counts:
        logging.info(f"{sync_type} error summary:")
        for error_code, count in sorted(error_counts.items()):
            logging.info(f"error {error_code}: {count}")
    else:
        logging.info(f"{sync_type} completed with no errors")


# TIMER TRIGGERS (Scheduled Functions)


@app.schedule(
    schedule="0 0 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def users_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("User sync timer is past due!")

    tenants = get_tenants()
    tenants.reverse()  # Process in reverse order
    results = []

    for tenant in tenants:
        try:
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['users_synced']} users synced"
                )
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "users_synced": result["users_synced"],
                    }
                )

                # Run analysis after successful sync
                try:
                    inactive_result = calculate_inactive_users(tenant["tenant_id"])
                    logging.info(
                        f"  Inactive users: {inactive_result.get('inactive_count', 0)}"
                    )

                    mfa_result = calculate_mfa_compliance(tenant["tenant_id"])
                    logging.info(
                        f"  MFA compliance: {mfa_result.get('compliance_rate', 0)}%"
                    )

                except Exception as e:
                    logging.error(f"Analysis error: {str(e)}")

            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            results.append(
                {"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)}
            )

    # Use centralized error reporting
    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "User")


@app.schedule(
    schedule="0 30 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def licenses_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("License sync timer is past due!")

    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_licenses(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['licenses_synced']} licenses synced"
                )
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "licenses_synced": result["licenses_synced"],
                    }
                )
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            results.append(
                {"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)}
            )

    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "License")


@app.schedule(
    schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def applications_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Service principal sync timer is past due!")

    tenants = get_tenants()
    results = []

    for tenant in tenants:
        try:
            result = sync_service_principals(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['service_principals_synced']} service principals synced"
                )
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "service_principals_synced": result[
                            "service_principals_synced"
                        ],
                    }
                )
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            results.append(
                {"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)}
            )

    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "Service Principal")


@app.schedule(
    schedule="0 30 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def role_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for role sync"""
    if timer.past_due:
        logging.info("Role sync timer is past due!")

    logging.info("Starting scheduled role sync")
    tenants = get_tenants()
    tenant_ids = [tenant["tenant_id"] for tenant in tenants]

    result = sync_roles_for_tenants(tenant_ids)

    if result["status"] == "completed":
        logging.info(
            f"  Role sync completed: {result['total_roles_synced']} roles, {result['total_role_assignments_synced']} role assignments across {result['successful_tenants']} tenants"
        )
        if result["failed_tenants"] > 0:
            categorize_sync_errors(result["results"], "Role")
    else:
        logging.error(f"  Role sync failed: {result.get('error', 'Unknown error')}")


@app.schedule(
    schedule="0 15 0 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def policies_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Conditional access policy sync timer is past due!")

    tenants = get_tenants()
    error_counts = {}

    for tenant in tenants:
        try:
            result = sync_conditional_access_policies(
                tenant["tenant_id"], tenant["name"]
            )
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['policies_synced']} conditional access policies, {result['policy_users_synced']} user assignments, {result['policy_applications_synced']} application assignments synced"
                )
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                # Track error codes from the sync result
                error_code = extract_error_code(result["error"])
                if error_code:
                    error_counts[error_code] = error_counts.get(error_code, 0) + 1
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            # Track error codes from exceptions
            error_code = extract_error_code(str(e))
            if error_code:
                error_counts[error_code] = error_counts.get(error_code, 0) + 1

    # Log error summary at the end
    log_error_summary(error_counts, "Policies sync")


# HTTP TRIGGERS (Manual Endpoints)


@app.route(route="sync/users", methods=["POST"])
def user_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()
    total = 0
    results = []

    for tenant in tenants:
        try:
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total += result["users_synced"]
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "users_synced": result["users_synced"],
                    }
                )
            else:
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(f"Error syncing users for {tenant['name']}: {str(e)}")
            results.append(
                {"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)}
            )

    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "User")

    return func.HttpResponse(f"Synced {total} users", status_code=200)


@app.route(route="sync/licenses", methods=["POST"])
def license_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()
    total_licenses = 0
    total_assignments = 0
    results = []

    for tenant in tenants:
        try:
            result = sync_licenses(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total_licenses += result["licenses_synced"]
                total_assignments += result["user_licenses_synced"]
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "licenses_synced": result["licenses_synced"],
                        "user_licenses_synced": result["user_licenses_synced"],
                    }
                )
            else:
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(f"Error syncing licenses for {tenant['name']}: {str(e)}")
            results.append(
                {"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)}
            )

    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "License")

    return func.HttpResponse(
        f"Synced {total_licenses} licenses and {total_assignments} user assignments",
        status_code=200,
    )


@app.route(route="sync/serviceprincipals", methods=["POST"])
def application_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()
    total = 0
    results = []

    for tenant in tenants:
        try:
            result = sync_service_principals(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total += result["service_principals_synced"]
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "service_principals_synced": result[
                            "service_principals_synced"
                        ],
                    }
                )
            else:
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(
                f"Error syncing service principals for {tenant['name']}: {str(e)}"
            )
            results.append(
                {"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)}
            )

    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "Service Principal")

    return func.HttpResponse(f"Synced {total} service principals", status_code=200)


@app.route(route="sync/roles", methods=["POST"])
def role_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for role sync"""
    logging.info("Starting manual role sync")
    tenants = get_tenants()
    tenant_ids = [tenant["tenant_id"] for tenant in tenants]

    result = sync_roles_for_tenants(tenant_ids)

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

        return func.HttpResponse(response_msg, status_code=200)
    else:
        error_msg = f"Role sync failed: {result.get('error', 'Unknown error')}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)


@app.route(route="sync/policies", methods=["POST"])
def policies_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()
    total_policies = 0
    total_policy_users = 0
    error_counts = {}

    for tenant in tenants:
        try:
            result = sync_conditional_access_policies(
                tenant["tenant_id"], tenant["name"]
            )
            if result["status"] == "success":
                total_policies += result["policies_synced"]
                total_policy_users += result["policy_users_synced"]
            else:
                # Track error codes from the sync result
                error_code = extract_error_code(result["error"])
                if error_code:
                    error_counts[error_code] = error_counts.get(error_code, 0) + 1
        except Exception as e:
            logging.error(
                f"Error syncing conditional access policies for {tenant['name']}: {str(e)}"
            )
            # Track error codes from exceptions
            error_code = extract_error_code(str(e))
            if error_code:
                error_counts[error_code] = error_counts.get(error_code, 0) + 1

    # Log error summary
    log_error_summary(error_counts, "Policies HTTP sync")

    return func.HttpResponse(
        f"Synced {total_policies} conditional access policies and {total_policy_users} user assignments",
        status_code=200,
    )


@app.route(route="tenant/users", methods=["GET"])
def get_tenant_users(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant user data"""
    # Returns structured response with user optimization actions

    try:
        # extract & validate tenant id
        tenant_id = req.params.get('tenant_id')
        logging.info(f"Users API request for tenant: {tenant_id}")

        if not tenant_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "tenant_id parameter is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Tenant '{tenant_id}' not found"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Processing user data for tenant: {tenant_name}")

        # grab user data
        # basic user counts
        total_users_query = "SELECT COUNT(*) as count FROM users WHERE tenant_id = ?"
        total_users_result = query(total_users_query, (tenant_id,))

        active_users_query = "SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1"
        active_users_result = query(active_users_query, (tenant_id,))

        # admin users count
        admin_users_query = "SELECT COUNT(DISTINCT user_id) as count FROM user_roles WHERE tenant_id = ?"
        admin_users_result = query(admin_users_query, (tenant_id,))

        # never signed in users
        never_signed_in_query = "SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND last_sign_in IS NULL AND account_enabled = 1"
        never_signed_in_result = query(never_signed_in_query, (tenant_id,))

        # grab analysis data
        mfa_result = calculate_mfa_compliance(tenant_id)
        inactive_result = calculate_inactive_users(tenant_id)

        # calculate metrics
        total_users = total_users_result[0]["count"] if total_users_result else 0
        active_users = active_users_result[0]["count"] if active_users_result else 0
        inactive_users = total_users - active_users
        admin_users = admin_users_result[0]["count"] if admin_users_result else 0
        never_signed_in = never_signed_in_result[0]["count"] if never_signed_in_result else 0

        # generate user optimization actions
        actions = []

        # action 1: never signed in users
        if never_signed_in > 0:
            actions.append({
                "title": "Review Unused Accounts",
                "description": f"{never_signed_in} users have never signed in - consider deactivating",
                "action": "review"
            })

        # action 2: inactive users
        if inactive_users > 0:
            actions.append({
                "title": "Review Inactive Users",
                "description": f"{inactive_users} inactive user accounts - verify if still needed",
                "action": "review"
            })

        # action 3: MFA non-compliance
        non_compliant_users = mfa_result.get('non_compliant', 0)
        if non_compliant_users > 0:
            actions.append({
                "title": "Enable MFA for Users",
                "description": f"{non_compliant_users} users without MFA enabled - security risk",
                "action": "secure"
            })

        # action 4: admin MFA compliance
        admin_non_compliant = mfa_result.get('admin_non_compliant', 0)
        if admin_non_compliant > 0:
            actions.append({
                "title": "Secure Admin Accounts",
                "description": f"{admin_non_compliant} admin users without MFA - critical security risk",
                "action": "secure"
            })

        # build response structure
        response_data = {
            "success": True,
            "data": [],  # empty for metadata endpoints
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "timestamp": datetime.now().isoformat(),
                "total_users": total_users,
                "active_users": active_users,
                "inactive_users": inactive_users,
                "admin_users": admin_users,
                "never_signed_in_users": never_signed_in,
                "mfa_compliance_rate": mfa_result.get('compliance_rate', 0),
                "mfa_enabled_users": mfa_result.get('mfa_enabled', 0),
                "risk_level": mfa_result.get('risk_level', 'unknown')
            },
            "actions": actions[:4]  # limit to maximum 4 actions
        }

        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        error_msg = f"Error retrieving user data: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": error_msg
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.route(route="tenant/licenses", methods=["GET"])
def get_tenant_licenses(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant license data"""
    # Returns structured response with license optimization actions only

    try:
        # extract & validate tenant id
        tenant_id = req.params.get('tenant_id')
        logging.info(f"Licenses API request for tenant: {tenant_id}")

        if not tenant_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "tenant_id parameter is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Tenant '{tenant_id}' not found"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Processing license data for tenant: {tenant_name}")

        # grab license data
        # total license types
        total_licenses_query = "SELECT COUNT(DISTINCT license_display_name) as count FROM licenses WHERE tenant_id = ?"
        total_licenses_result = query(total_licenses_query, (tenant_id,))

        # total license assignments
        total_assignments_query = "SELECT COUNT(*) as count FROM user_licenses WHERE tenant_id = ?"
        total_assignments_result = query(total_assignments_query, (tenant_id,))

        # active license assignments
        active_assignments_query = "SELECT COUNT(*) as count FROM user_licenses WHERE tenant_id = ? AND is_active = 1"
        active_assignments_result = query(active_assignments_query, (tenant_id,))

        # total monthly cost for active licenses
        total_cost_query = "SELECT SUM(monthly_cost) as total_cost FROM user_licenses WHERE tenant_id = ? AND is_active = 1"
        total_cost_result = query(total_cost_query, (tenant_id,))

        # grab license optimization data
        license_optimization = calculate_license_optimization(tenant_id)

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
            actions.append({
                "title": "Remove Inactive License Assignments",
                "description": f"{inactive_assignments} inactive license assignments wasting budget",
                "action": "cleanup"
            })

        # action 2: low utilization licenses
        if utilization_rate < 70:
            actions.append({
                "title": "Investigate Low License Utilization",
                "description": f"Only {utilization_rate}% license utilization - review assignments",
                "action": "optimize"
            })

        # action 3: high cost savings opportunity
        if monthly_savings > 100:
            actions.append({
                "title": "Realize License Cost Savings",
                "description": f"${monthly_savings}/month potential savings from license optimization",
                "action": "optimize"
            })

        # action 4: license portfolio consolidation (only for smaller deployments)
        if total_license_types > 5 and total_assignments < 50:
            actions.append({
                "title": "Consolidate License Types",
                "description": f"{total_license_types} license types for small user base - consider consolidation",
                "action": "optimize"
            })

        # build response structure
        response_data = {
            "success": True,
            "data": [],  # empty for metadata endpoints
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "timestamp": datetime.now().isoformat(),
                "total_license_types": total_license_types,
                "total_license_assignments": total_assignments,
                "active_license_assignments": active_assignments,
                "monthly_license_cost": monthly_cost,
                "license_utilization_rate": utilization_rate,
                "underutilized_licenses": license_optimization.get("underutilized_licenses", 0),
                "estimated_monthly_savings": monthly_savings
            },
            "actions": actions[:4]  # limit to maximum 4 actions
        }

        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        error_msg = f"Error retrieving license data: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": error_msg
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@app.route(route="tenant/roles", methods=["GET"])
def get_tenant_roles(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP GET endpoint for single tenant roles data"""
    # Returns structured response with role optimization actions only

    try:
        # extract & validate tenant id
        tenant_id = req.params.get('tenant_id')
        logging.info(f"Roles API request for tenant: {tenant_id}")

        if not tenant_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "tenant_id parameter is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Tenant '{tenant_id}' not found"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Processing roles data for tenant: {tenant_name}")

        # grab roles data
        # total unique roles
        total_roles_query = "SELECT COUNT(DISTINCT role_id) as count FROM roles WHERE tenant_id = ?"
        total_roles_result = query(total_roles_query, (tenant_id,))

        # total role assignments
        total_assignments_query = "SELECT COUNT(*) as count FROM user_roles WHERE tenant_id = ?"
        total_assignments_result = query(total_assignments_query, (tenant_id,))

        # unique users with role assignments
        users_with_roles_query = "SELECT COUNT(DISTINCT user_id) as count FROM user_roles WHERE tenant_id = ?"
        users_with_roles_result = query(users_with_roles_query, (tenant_id,))

        # Admin roles (roles containing 'Admin' or 'Administrator')
        admin_roles_query = "SELECT COUNT(DISTINCT role_id) as count FROM roles WHERE tenant_id = ? AND (role_display_name LIKE '%Admin%' OR role_display_name LIKE '%Administrator%')"
        admin_roles_result = query(admin_roles_query, (tenant_id,))

        # Users with multiple roles (potential over-privileged)
        multi_role_users_query = "SELECT COUNT(*) as count FROM (SELECT user_id FROM user_roles WHERE tenant_id = ? GROUP BY user_id HAVING COUNT(role_id) > 1)"
        multi_role_users_result = query(multi_role_users_query, (tenant_id,))

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
            actions.append({
                "title": "Review Over-Privileged Users",
                "description": f"{multi_role_users} users have multiple roles - verify necessity",
                "action": "review"
            })

        # action 2: admin role assignments audit
        if admin_roles > 0 and users_with_roles > 0:
            actions.append({
                "title": "Audit Admin Role Assignments",
                "description": f"{admin_roles} admin roles assigned - ensure principle of least privilege",
                "action": "audit"
            })

        # action 3: role proliferation (optional, only if many roles)
        if total_roles > 10 and users_with_roles < 20:
            actions.append({
                "title": "Consolidate Role Definitions",
                "description": f"{total_roles} roles for {users_with_roles} users - consider role consolidation",
                "action": "optimize"
            })

        # build response structure
        response_data = {
            "success": True,
            "data": [],  # empty for metadata endpoints
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "timestamp": datetime.now().isoformat(),
                "total_roles": total_roles,
                "total_role_assignments": total_assignments,
                "users_with_roles": users_with_roles,
                "admin_roles": admin_roles,
                "multi_role_users": multi_role_users,
                "avg_roles_per_user": avg_roles_per_user
            },
            "actions": actions[:3]  # limit to maximum 3 actions (roles tend to have fewer optimization opportunities)
        }

        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        error_msg = f"Error retrieving roles data: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": error_msg
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )



@app.route(route="users/{user_id}/disable", methods=["PATCH"])
def disable_inactive_user(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP PATCH endpoint to disable a single inactive user account"""
    # single tenant, single resource operation

    try:
        # extract and validate request data
        logging.info("Processing user disable request")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "Request body is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # get tenant_id and user identifier from request
        tenant_id = req_body.get('tenant_id')
        user_id = req_body.get('user_id')
        user_principal_name = req_body.get('user_principal_name')

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "tenant_id is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        if not user_id and not user_principal_name:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "Either user_id or user_principal_name is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Tenant '{tenant_id}' not found"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        tenant_name = tenant_names[tenant_id]
        logging.info(f"Disabling user for tenant: {tenant_name}")

        # find and validate user exists in database
        if user_id:
            # query by user_id
            user_query = "SELECT * FROM users WHERE tenant_id = ? AND id = ?"
            user_result = query(user_query, (tenant_id, user_id))
            identifier = f"user_id: {user_id}"
        else:
            # query by user_principal_name
            user_query = "SELECT * FROM users WHERE tenant_id = ? AND user_principal_name = ?"
            user_result = query(user_query, (tenant_id, user_principal_name))
            identifier = f"user_principal_name: {user_principal_name}"

        if not user_result:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"User not found ({identifier})"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        user = user_result[0]
        logging.info(f"Found user: {user['user_principal_name']}")

        # check if user is already disabled
        if not user.get('account_enabled', True):
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"User {user['user_principal_name']} is already disabled",
                    "data": {
                        "user_id": user['id'],
                        "user_principal_name": user['user_principal_name'],
                        "account_enabled": False
                    }
                }),
                status_code=409,
                headers={"Content-Type": "application/json"}
            )

        # check if user is actually inactive (business rule validation)
        last_sign_in = user.get('last_sign_in')
        inactivity_threshold_days = 90

        if last_sign_in:
            try:
                # parse last sign-in date
                last_sign_in_date = datetime.fromisoformat(last_sign_in.replace('Z', '+00:00'))
                days_since_last_signin = (datetime.now() - last_sign_in_date.replace(tzinfo=None)).days

                if days_since_last_signin < inactivity_threshold_days:
                    return func.HttpResponse(
                        json.dumps({
                            "success": False,
                            "error": f"User {user['user_principal_name']} is not inactive (last sign-in: {days_since_last_signin} days ago, threshold: {inactivity_threshold_days} days)",
                            "data": {
                                "user_id": user['id'],
                                "user_principal_name": user['user_principal_name'],
                                "last_sign_in": last_sign_in,
                                "days_since_last_signin": days_since_last_signin,
                                "inactivity_threshold_days": inactivity_threshold_days
                            }
                        }),
                        status_code=422,
                        headers={"Content-Type": "application/json"}
                    )
            except Exception as date_parse_error:
                logging.warning(f"Could not parse last_sign_in date: {last_sign_in}, proceeding with disable")

        # disable user account via graph api
        logging.info(f"Disabling user {user['user_principal_name']} via Graph API")
        graph_client = GraphClient(tenant_id)

        # call Microsoft Graph to disable the user
        disable_result = graph_client.disable_user(user['id'])

        if disable_result.get('status') != 'success':
            error_msg = disable_result.get('error', 'Unknown error disabling user')
            logging.error(f"Failed to disable user via Graph API: {error_msg}")
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Failed to disable user: {error_msg}",
                    "data": {
                        "user_id": user['id'],
                        "user_principal_name": user['user_principal_name'],
                        "graph_api_error": error_msg
                    }
                }),
                status_code=500,
                headers={"Content-Type": "application/json"}
            )

        # update local database to reflect disabled status
        current_time = datetime.now().isoformat()
        update_query = "UPDATE users SET account_enabled = 0, synced_at = ? WHERE tenant_id = ? AND id = ?"

        try:
            query(update_query, (current_time, tenant_id, user['id']))
            logging.info(f"Updated local database for user {user['user_principal_name']}")
        except Exception as db_error:
            logging.error(f"Failed to update local database: {str(db_error)}")
            # note: user is disabled in Graph but local DB might be out of sync

        # return success response
        response_data = {
            "success": True,
            "message": f"User {user['user_principal_name']} successfully disabled",
            "data": {
                "user_id": user['id'],
                "user_principal_name": user['user_principal_name'],
                "display_name": user.get('display_name'),
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "disabled_at": current_time,
                "was_inactive_since": last_sign_in,
                "days_inactive": (datetime.now() - datetime.fromisoformat(last_sign_in.replace('Z', '+00:00')).replace(tzinfo=None)).days if last_sign_in else "Never signed in"
            }
        }

        logging.info(f"Successfully disabled user {user['user_principal_name']}")
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        # comprehensive error handling and logging
        error_msg = f"Error disabling user: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": error_msg,
                "data": {
                    "tenant_id": req_body.get('tenant_id') if 'req_body' in locals() else None,
                    "user_id": req_body.get('user_id') if 'req_body' in locals() else None,
                    "user_principal_name": req_body.get('user_principal_name') if 'req_body' in locals() else None
                }
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
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
                json.dumps({
                    "success": False,
                    "error": "Request body is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # get parameters from request
        tenant_id = req_body.get('tenant_id')
        inactivity_threshold_days = req_body.get('inactivity_threshold_days', 90)
        dry_run = req_body.get('dry_run', False)

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "tenant_id is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Tenant '{tenant_id}' not found"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Processing bulk disable for tenant: {tenant_name} (dry_run: {dry_run})")

        # use existing analysis function to get potential savings
        inactive_analysis = calculate_inactive_users(tenant_id, inactivity_threshold_days)
        potential_savings = inactive_analysis.get('potential_monthly_savings', 0)

        # get all active users to process
        inactive_users_query = """
        SELECT id, display_name, user_principal_name, last_sign_in, account_enabled
        FROM users
        WHERE tenant_id = ? AND account_enabled = 1
        """
        all_users = query(inactive_users_query, (tenant_id,))

        # filter to get actual inactive users based on threshold
        cutoff_date = datetime.now() - timedelta(days=inactivity_threshold_days)

        inactive_users = []
        for user in all_users:
            if user['last_sign_in']:
                try:
                    last_sign_in_date = datetime.fromisoformat(user['last_sign_in'].replace('Z', '+00:00'))
                    days_inactive = (datetime.now() - last_sign_in_date.replace(tzinfo=None)).days
                    if last_sign_in_date.replace(tzinfo=None) < cutoff_date:
                        user['days_inactive'] = days_inactive
                        inactive_users.append(user)
                except:
                    # if date parsing fails, treat as inactive
                    user['days_inactive'] = None
                    inactive_users.append(user)
            else:
                # never signed in - treat as inactive
                user['days_inactive'] = None
                inactive_users.append(user)

        if not inactive_users:
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "data": [],
                    "metadata": {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "operation": "bulk_disable_inactive_users",
                        "dry_run": dry_run,
                        "inactivity_threshold_days": inactivity_threshold_days,
                        "execution_time": execution_time,
                        "summary": {
                            "total_identified": 0,
                            "successfully_disabled": 0,
                            "already_disabled": 0,
                            "failed": 0,
                            "skipped": 0
                        },
                        "potential_monthly_savings": potential_savings
                    },
                    "actions": []
                }),
                status_code=200,
                headers={"Content-Type": "application/json"}
            )

        logging.info(f"Found {len(inactive_users)} inactive users to process")

        # initialize tracking variables
        processed_users = []
        counters = {
            "successfully_disabled": 0,
            "already_disabled": 0,
            "failed": 0,
            "skipped": 0
        }

        # initialize Graph client (only if not dry run)
        if not dry_run:
            from core.graph_client import GraphClient
            graph_client = GraphClient(tenant_id)

        # process each inactive user
        for user in inactive_users:
            user_id = user['id']
            user_principal_name = user['user_principal_name']
            display_name = user.get('display_name')
            last_sign_in = user.get('last_sign_in')
            days_inactive = user.get('days_inactive')

            user_data = {
                "user_id": user_id,
                "user_principal_name": user_principal_name,
                "display_name": display_name,
                "last_sign_in": last_sign_in,
                "days_inactive": days_inactive
            }

            try:
                # check if user is already disabled (shouldn't happen with our query, but safety check)
                if not user.get('account_enabled', True):
                    user_data["status"] = "already_disabled"
                    counters["already_disabled"] += 1
                    processed_users.append(user_data)
                    continue

                if dry_run:
                    # dry run mode - just simulate
                    user_data["status"] = "would_be_disabled"
                    user_data["note"] = "Dry run - no actual changes made"
                    counters["successfully_disabled"] += 1
                    processed_users.append(user_data)
                    logging.info(f"DRY RUN: Would disable user {user_principal_name}")
                else:
                    # actually disable the user via Graph API
                    logging.info(f"Disabling user {user_principal_name} via Graph API")
                    disable_result = graph_client.disable_user(user_id)

                    if disable_result.get('status') != 'success':
                        error_msg = disable_result.get('error', 'Unknown error disabling user')
                        user_data["status"] = "failed"
                        user_data["error"] = error_msg
                        counters["failed"] += 1
                        processed_users.append(user_data)
                        logging.error(f"Failed to disable {user_principal_name}: {error_msg}")
                        continue

                    # update local database to reflect disabled status
                    current_time = datetime.now().isoformat()
                    update_query = "UPDATE users SET account_enabled = 0, synced_at = ? WHERE tenant_id = ? AND id = ?"

                    try:
                        query(update_query, (current_time, tenant_id, user_id))
                        logging.info(f"Updated local database for user {user_principal_name}")
                    except Exception as db_error:
                        logging.warning(f"Graph API disable succeeded but local DB update failed for {user_principal_name}: {str(db_error)}")
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
            actions.append({
                "type": "review_failures",
                "description": f"{counters['failed']} user(s) failed to disable - review permissions",
                "users_affected": counters["failed"]
            })

        if counters["successfully_disabled"] > 0 and potential_savings > 0:
            actions.append({
                "type": "verify_savings",
                "description": f"Potential monthly savings of ${potential_savings:.2f} achieved",
                "amount": potential_savings
            })

        if counters["already_disabled"] > 0:
            actions.append({
                "type": "review_already_disabled",
                "description": f"{counters['already_disabled']} user(s) were already disabled",
                "users_affected": counters["already_disabled"]
            })

        # determine overall success status
        success_status = counters["successfully_disabled"] > 0
        
        # determine HTTP status code
        if counters["successfully_disabled"] == 0:
            status_code = 500  # Complete failure - no users disabled
        elif counters["failed"] == 0:
            status_code = 200  # Complete success - all users disabled
        else:
            status_code = 207  # Partial success - some disabled, some failed

        # build final response in your standard format
        response_data = {
            "success": success_status,
            "data": processed_users,
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "operation": "bulk_disable_inactive_users",
                "dry_run": dry_run,
                "inactivity_threshold_days": inactivity_threshold_days,
                "execution_time": execution_time,
                "summary": {
                    "total_identified": len(inactive_users),
                    "successfully_disabled": counters["successfully_disabled"],
                    "already_disabled": counters["already_disabled"],
                    "failed": counters["failed"],
                    "skipped": counters["skipped"]
                },
                "potential_monthly_savings": potential_savings
            },
            "actions": actions
        }

        logging.info(f"Bulk disable operation completed: {counters}")
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=status_code,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        error_msg = f"Error in bulk disable operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": error_msg,
                "data": [],
                "metadata": {
                    "tenant_id": req_body.get('tenant_id') if 'req_body' in locals() else None,
                    "operation": "bulk_disable_inactive_users",
                    "execution_time": datetime.now().isoformat()
                },
                "actions": []
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )

@app.route(route="users/{user_id}/reset-password", methods=["POST"])
def reset_user_password(req: func.HttpRequest) -> func.HttpResponse:
    """POST endpoint to reset a user's password with temporary password"""
    # single tenant, single resource operation

    from datetime import datetime

    try:
        # extract user_id from URL path
        user_id = req.route_params.get('user_id')

        # extract and validate request data
        logging.info(f"Processing password reset request for user {user_id}")

        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "Request body is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # get parameters from request
        tenant_id = req_body.get('tenant_id')

        # validate required parameters
        if not tenant_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "tenant_id is required"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        if not user_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": "user_id is required in URL path"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # check if tenant exists
        tenants = get_tenants()
        tenant_names = {t["tenant_id"]: t["name"] for t in tenants}

        if tenant_id not in tenant_names:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Tenant '{tenant_id}' not found"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        tenant_name = tenant_names[tenant_id]
        execution_time = datetime.now().isoformat()
        logging.info(f"Resetting password for user {user_id} in tenant: {tenant_name}")

        # find and validate user exists in database
        user_query = "SELECT * FROM users WHERE tenant_id = ? AND id = ?"
        user_result = query(user_query, (tenant_id, user_id))

        if not user_result:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"User {user_id} not found in tenant {tenant_id}"
                }),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )

        user = user_result[0]
        logging.info(f"Found user: {user['user_principal_name']}")

        # check if user is disabled
        if not user.get('account_enabled', True):
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Cannot reset password for disabled user {user['user_principal_name']}",
                    "data": [{
                        "user_id": user['id'],
                        "user_principal_name": user['user_principal_name'],
                        "status": "user_disabled"
                    }],
                    "metadata": {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "operation": "reset_user_password",
                        "execution_time": execution_time
                    },
                    "actions": []
                }),
                status_code=422,
                headers={"Content-Type": "application/json"}
            )

        # reset password via Graph API
        logging.info(f"Resetting password for user {user['user_principal_name']} via Graph API")
        from core.graph_client import GraphClient
        graph_client = GraphClient(tenant_id)

        reset_result = graph_client.reset_user_password(user['id'])

        if reset_result.get('status') != 'success':
            error_msg = reset_result.get('error', 'Unknown error resetting password')
            logging.error(f"Failed to reset password via Graph API: {error_msg}")
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": f"Failed to reset password: {error_msg}",
                    "data": [{
                        "user_id": user['id'],
                        "user_principal_name": user['user_principal_name'],
                        "status": "failed",
                        "error": error_msg
                    }],
                    "metadata": {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "operation": "reset_user_password",
                        "execution_time": execution_time,
                        "summary": {
                            "passwords_reset": 0,
                            "failed": 1
                        }
                    },
                    "actions": [{
                        "type": "review_permissions",
                        "description": "Review Graph API permissions for password reset",
                        "users_affected": 1
                    }]
                }),
                status_code=500,
                headers={"Content-Type": "application/json"}
            )

        # update local database to track password reset
        current_time = datetime.now().isoformat()
        update_query = "UPDATE users SET synced_at = ? WHERE tenant_id = ? AND id = ?"

        try:
            query(update_query, (current_time, tenant_id, user['id']))
            logging.info(f"Updated local database for user {user['user_principal_name']}")
        except Exception as db_error:
            logging.warning(f"Graph API reset succeeded but local DB update failed for {user['user_principal_name']}: {str(db_error)}")

        # prepare response data
        user_data = {
            "user_id": user['id'],
            "user_principal_name": user['user_principal_name'],
            "display_name": user.get('display_name'),
            "status": "password_reset",
            "reset_at": current_time,
            "temporary_password": reset_result['temporary_password'],
            "force_change_password": True
        }

        # build actions
        actions = [
            {
                "type": "secure_delivery",
                "description": "Securely deliver temporary password to user",
                "users_affected": 1
            },
            {
                "type": "monitor_login",
                "description": "Monitor user's next login to confirm password change",
                "users_affected": 1
            }
        ]

        # build final response
        response_data = {
            "success": True,
            "data": [user_data],
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "operation": "reset_user_password",
                "execution_time": execution_time,
                "summary": {
                    "passwords_reset": 1,
                    "failed": 0
                }
            },
            "actions": actions
        }

        logging.info(f"Successfully reset password for user {user['user_principal_name']}")
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        error_msg = f"Error in password reset operation: {str(e)}"
        logging.error(error_msg)

        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": error_msg,
                "data": [],
                "metadata": {
                    "tenant_id": req_body.get('tenant_id') if 'req_body' in locals() else None,
                    "operation": "reset_user_password",
                    "execution_time": datetime.now().isoformat()
                },
                "actions": []
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )

# REPORT GENERATION


@app.schedule(
    schedule="0 0 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
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

        logging.info(
            f"Processing {len(successful_tenants_info)} successful tenants (excluding {failed_count} failed syncs)"
        )

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
                    "SELECT COUNT(*) as count FROM users WHERE tenant_id = ?",
                    (tenant_id,),
                )
                active_users_result = query(
                    "SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1",
                    (tenant_id,),
                )

                # Get analysis results
                mfa_result = calculate_mfa_compliance(tenant_id)
                license_result = calculate_license_optimization(tenant_id)

                # Calculate metrics
                total_users = (
                    total_users_result[0]["count"] if total_users_result else 0
                )
                active_users = (
                    active_users_result[0]["count"] if active_users_result else 0
                )
                inactive_users = total_users - active_users

                # Generate warnings
                warnings = []
                mfa_compliance = mfa_result.get("compliance_rate", 0)
                admin_non_compliant = mfa_result.get("admin_non_compliant", 0)
                monthly_savings = license_result.get("estimated_monthly_savings", 0)
                underutilized_count = license_result.get("underutilized_licenses", 0)

                if admin_non_compliant > 0:
                    warnings.append(
                        f"CRITICAL: {admin_non_compliant} admin users without MFA - HIGH SECURITY RISK"
                    )

                if mfa_compliance < 50:
                    warnings.append(
                        f"WARNING: Low MFA compliance ({mfa_compliance}%) - Security risk"
                    )

                if monthly_savings > 100:
                    warnings.append(
                        f"COST OPPORTUNITY: ${monthly_savings}/month potential savings from {underutilized_count} unused licenses"
                    )

                inactive_percentage = (
                    round((inactive_users / total_users * 100), 1)
                    if total_users > 0
                    else 0
                )
                if inactive_percentage > 25:
                    warnings.append(
                        f"WARNING: High inactive user rate ({inactive_percentage}%) may indicate cleanup needed"
                    )

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
                logging.error(f"Error processing {tenant['name']}: {e}")

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
        logging.info(
            f"Report generation completed: {len(tenant_summaries)}/{total_tenants} successful"
        )

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
        return func.HttpResponse(error_msg, status_code=500)


@app.schedule(
    schedule="0 30 8 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def service_principal_analytics(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Service principal analytics timer is past due!")

    try:
        import json
        from analysis.sp_analysis import (
            analyze_service_principals,
            format_analytics_json,
        )

        # Determine tenant mode
        tenants = get_tenants()
        tenant_mode = (
            "single"
            if len(tenants) == 1
            and tenants[0]["tenant_id"] == "3aae0fb1-276f-42f8-8e4d-36ca10cbb779"
            else "multi"
        )

        logging.info(f"Starting service principal analytics in {tenant_mode} mode")

        # Perform analytics
        analytics_result = analyze_service_principals(tenant_mode)

        if analytics_result["status"] == "success":
            # Format and log as clean JSON
            json_result = format_analytics_json(analytics_result)
            logging.info(
                f"Service Principal Analytics Result: {json.dumps(json_result, indent=2)}"
            )

        else:
            error_result = {"status": "error", "error": analytics_result["error"]}
            logging.error(f"Analytics failed: {json.dumps(error_result)}")

    except Exception as e:
        error_result = {"status": "error", "error": str(e)}
        logging.error(f"Service principal analytics failed: {json.dumps(error_result)}")


@app.route(route="analytics/serviceprincipals", methods=["GET"])
def service_principal_analytics_http(req: func.HttpRequest) -> func.HttpResponse:
    try:
        import json
        from analysis.sp_analysis import (
            analyze_service_principals,
            format_analytics_json,
        )

        # Determine tenant mode
        tenants = get_tenants()
        tenant_mode = (
            "single"
            if len(tenants) == 1
            and tenants[0]["tenant_id"] == "3aae0fb1-276f-42f8-8e4d-36ca10cbb779"
            else "multi"
        )

        analytics_result = analyze_service_principals(tenant_mode)

        if analytics_result["status"] == "success":
            json_result = format_analytics_json(analytics_result)
            return func.HttpResponse(
                json.dumps(json_result, indent=2),
                status_code=200,
                mimetype="application/json",
            )
        else:
            error_result = {"status": "error", "error": analytics_result["error"]}
            return func.HttpResponse(
                json.dumps(error_result, indent=2),
                status_code=500,
                mimetype="application/json",
            )

    except Exception as e:
        logging.error(f"Service principal analytics HTTP failed: {str(e)}")
        error_result = {"status": "error", "error": str(e)}

        return func.HttpResponse(
            json.dumps(error_result, indent=2),
            status_code=500,
            mimetype="application/json",
        )



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



@app.route(route="tenant/serviceprincipals", methods=["GET", "POST"])
def get_all_sps(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get all service principals for a specific tenant from database
    GET /api/tenant/serviceprincipals?tenant_id={tenant_id}
    POST /api/tenant/serviceprincipals with JSON body: {"tenant_id": "..."}
    """
    try:
        logging.info(f"get_all_sps endpoint called via {req.method}")

        # Extract tenant_id from query parameters (GET) or request body (POST)
        tenant_id = None

        if req.method == "GET":
            tenant_id = req.params.get('tenant_id')
            logging.info(f"GET request with tenant_id parameter: {tenant_id}")
        elif req.method == "POST":
            try:
                req_body = req.get_json()
                if req_body:
                    tenant_id = req_body.get('tenant_id')
                    logging.info(f"POST request with tenant_id in body: {tenant_id}")
                else:
                    # Try query parameter as fallback for POST
                    tenant_id = req.params.get('tenant_id')
                    logging.info(f"POST request with tenant_id parameter: {tenant_id}")
            except ValueError:
                # If JSON parsing fails, try query parameter
                tenant_id = req.params.get('tenant_id')
                logging.info(f"POST request with tenant_id parameter (JSON parse failed): {tenant_id}")

        if not tenant_id:
            error_response = {
                "success": False,
                "data": [],
                "metadata": {},
                "actions": [],
                "error": "Missing required parameter: tenant_id",
                "usage": {
                    "GET": "/api/tenant/serviceprincipals?tenant_id={tenant_id}",
                    "POST": "/api/tenant/serviceprincipals with JSON body: {\"tenant_id\": \"...\"}"
                }
            }
            logging.error("get_all_sps called without tenant_id parameter")
            return func.HttpResponse(
                json.dumps(error_response, indent=2),
                status_code=400,
                mimetype="application/json"
            )

        # Get all configured tenants to validate and get tenant name
        logging.info("Retrieving configured tenants...")
        tenants = get_tenants()
        logging.info(f"Found {len(tenants)} configured tenant(s)")

        # Validate tenant_id against configured tenants
        target_tenant = None
        for tenant in tenants:
            if tenant["tenant_id"] == tenant_id:
                target_tenant = tenant
                break

        if not target_tenant:
            available_tenants = [{"name": t["name"], "tenant_id": t["tenant_id"]} for t in tenants]
            error_response = {
                "success": False,
                "data": [],
                "metadata": {},
                "actions": [],
                "error": f"Invalid tenant_id: {tenant_id}. Tenant not found in configured tenants.",
                "provided_tenant_id": tenant_id,
                "available_tenants": available_tenants
            }
            logging.error(f"get_all_sps called with invalid tenant_id: {tenant_id}")
            return func.HttpResponse(
                json.dumps(error_response, indent=2),
                status_code=404,
                mimetype="application/json"
            )

        # Query service principals from database
        logging.info(f"Querying service principals for tenant '{target_tenant['name']}' ({tenant_id})")

        service_principals_query = """
        SELECT
            id,
            tenant_id,
            app_id,
            display_name,
            publisher_name,
            service_principal_type,
            owners,
            credential_exp_date,
            credential_type,
            enabled_sp,
            last_sign_in,
            synced_at
        FROM service_principals
        WHERE tenant_id = ?
        ORDER BY display_name ASC
        """

        service_principals = query(service_principals_query, (tenant_id,))

        # Get count statistics for metadata
        total_count = len(service_principals)
        enabled_count = len([sp for sp in service_principals if sp.get('enabled_sp')])
        disabled_count = total_count - enabled_count

        # Count by type
        type_counts = {}
        for sp in service_principals:
            sp_type = sp.get('service_principal_type', 'Unknown')
            type_counts[sp_type] = type_counts.get(sp_type, 0) + 1

        # Build response following REST API guidance format
        response = {
            "success": True,
            "data": service_principals,
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": target_tenant['name'],
                "total_service_principals": total_count,
                "enabled_count": enabled_count,
                "disabled_count": disabled_count,
                "service_principal_types": type_counts,
                "last_queried": datetime.now().isoformat(),
                "endpoint": "get_all_sps",
                "request_method": req.method
            },
            "actions": []
        }

        logging.info(f"✓ get_all_sps completed for {target_tenant['name']}: returned {total_count} service principals")
        return func.HttpResponse(
            json.dumps(response, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        error_msg = f"Unexpected error during get_all_sps: {str(e)}"
        logging.error(error_msg)
        logging.exception("Full exception details:")

        error_result = {
            "success": False,
            "data": [],
            "metadata": {},
            "actions": [],
            "error": error_msg,
            "endpoint": "get_all_sps",
            "timestamp": datetime.now().isoformat()
        }
        return func.HttpResponse(
            json.dumps(error_result, indent=2),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="tenant/analysis", methods=["GET", "POST"])
def get_sps_report(req: func.HttpRequest) -> func.HttpResponse:
    """
    Analyze service principals for a specific tenant and provide actionable insights
    GET /api/tenant/analysis?tenant_id={tenant_id}
    POST /api/tenant/analysis with JSON body: {"tenant_id": "..."}
    """
    try:
        logging.info(f"get_sps_report endpoint called via {req.method}")

        # Extract tenant_id from query parameters (GET) or request body (POST)
        tenant_id = None

        if req.method == "GET":
            tenant_id = req.params.get('tenant_id')
            logging.info(f"GET request with tenant_id parameter: {tenant_id}")
        elif req.method == "POST":
            try:
                req_body = req.get_json()
                if req_body:
                    tenant_id = req_body.get('tenant_id')
                    logging.info(f"POST request with tenant_id in body: {tenant_id}")
                else:
                    # Try query parameter as fallback for POST
                    tenant_id = req.params.get('tenant_id')
                    logging.info(f"POST request with tenant_id parameter: {tenant_id}")
            except ValueError:
                # If JSON parsing fails, try query parameter
                tenant_id = req.params.get('tenant_id')
                logging.info(f"POST request with tenant_id parameter (JSON parse failed): {tenant_id}")

        if not tenant_id:
            error_response = {
                "success": False,
                "data": [],
                "metadata": {},
                "actions": [],
                "error": "Missing required parameter: tenant_id",
                "usage": {
                    "GET": "/api/tenant/analysis?tenant_id={tenant_id}",
                    "POST": "/api/tenant/analysis with JSON body: {\"tenant_id\": \"...\"}"
                }
            }
            logging.error("get_sps_report called without tenant_id parameter")
            return func.HttpResponse(
                json.dumps(error_response, indent=2),
                status_code=400,
                mimetype="application/json"
            )

        # Get all configured tenants to validate and get tenant name
        logging.info("Retrieving configured tenants...")
        tenants = get_tenants()
        logging.info(f"Found {len(tenants)} configured tenant(s)")

        # Validate tenant_id against configured tenants
        target_tenant = None
        for tenant in tenants:
            if tenant["tenant_id"] == tenant_id:
                target_tenant = tenant
                break

        if not target_tenant:
            available_tenants = [{"name": t["name"], "tenant_id": t["tenant_id"]} for t in tenants]
            error_response = {
                "success": False,
                "data": [],
                "metadata": {},
                "actions": [],
                "error": f"Invalid tenant_id: {tenant_id}. Tenant not found in configured tenants.",
                "provided_tenant_id": tenant_id,
                "available_tenants": available_tenants
            }
            logging.error(f"get_sps_report called with invalid tenant_id: {tenant_id}")
            return func.HttpResponse(
                json.dumps(error_response, indent=2),
                status_code=404,
                mimetype="application/json"
            )

        # Perform service principal analysis for the specific tenant
        logging.info(f"Starting service principal analysis for tenant '{target_tenant['name']}' ({tenant_id})")

        # Use existing service principal analytics functionality
        try:
            from analysis.sp_analysis import (
                analyze_service_principals,
                format_analytics_json,
            )

            # Determine tenant mode
            tenants = get_tenants()
            tenant_mode = (
                "single"
                if len(tenants) == 1
                and tenants[0]["tenant_id"] == "3aae0fb1-276f-42f8-8e4d-36ca10cbb779"
                else "multi"
            )

            # Perform analytics using existing function
            analytics_result = analyze_service_principals(tenant_mode)

            if analytics_result["status"] == "success":
                # Format analytics result
                json_result = format_analytics_json(analytics_result)

                # Extract relevant data for our tenant
                tenant_data = None
                if tenant_mode == "single":
                    tenant_data = json_result.get("tenant_analytics", {})

                    # Try alternative keys if tenant_analytics doesn't exist
                    if not tenant_data:
                        if "total_service_principals" in json_result:
                            tenant_data = json_result
                        elif "analytics" in json_result:
                            tenant_data = json_result["analytics"]
                        elif "data" in json_result:
                            tenant_data = json_result["data"]
                else:
                    # For multi-tenant, find our specific tenant
                    for tenant_analytics in json_result.get("tenant_analytics", []):
                        if tenant_analytics.get("tenant_id") == tenant_id:
                            tenant_data = tenant_analytics
                            break

                if not tenant_data:
                    available_tenants = []
                    if tenant_mode == "multi":
                        available_tenants = [t.get("tenant_id") for t in json_result.get("tenant_analytics", [])]
                    raise Exception(f"No analytics data found for tenant {tenant_id}. Available: {available_tenants}")

                # Calculate custom metrics
                total_sps = tenant_data.get("total_service_principals", 0)
                disabled_sps = tenant_data.get("disabled_service_principals", 0)
                enabled_sps = total_sps - disabled_sps  # Custom calculation

                # Calculate expiring credentials (within 30 days)
                from datetime import datetime, timedelta
                now = datetime.now()
                thirty_days_from_now = now + timedelta(days=30)

                # Query service principals to check credential expiration dates
                expiring_query = """
                SELECT COUNT(*) as count
                FROM service_principals
                WHERE tenant_id = ?
                AND credential_exp_date IS NOT NULL
                AND credential_exp_date != ''
                AND datetime(credential_exp_date) BETWEEN datetime('now') AND datetime('now', '+30 days')
                """

                expiring_result = query(expiring_query, (tenant_id,))
                expiring_creds = expiring_result[0]["count"] if expiring_result else 0

                # Calculate owners count (service principals with owners)
                owners_query = """
                SELECT COUNT(*) as count
                FROM service_principals
                WHERE tenant_id = ?
                AND owners IS NOT NULL
                AND owners != ''
                """

                owners_result = query(owners_query, (tenant_id,))
                owners_count = owners_result[0]["count"] if owners_result else 0

                # Calculate service principals with sign-in
                sps_with_signin_query = """
                SELECT COUNT(*) as count
                FROM service_principals
                WHERE tenant_id = ?
                AND last_sign_in IS NOT NULL
                AND last_sign_in != ''
                """

                sps_with_signin_result = query(sps_with_signin_query, (tenant_id,))
                sps_with_signin_count = sps_with_signin_result[0]["count"] if sps_with_signin_result else 0

                # Calculate inactive service principals (no sign-in within past year only)
                one_year_ago = now - timedelta(days=365)
                inactive_query = """
                SELECT COUNT(*) as count
                FROM service_principals
                WHERE tenant_id = ?
                AND (last_sign_in IS NULL OR last_sign_in = '' OR datetime(last_sign_in) < datetime('now', '-365 days'))
                """

                inactive_result = query(inactive_query, (tenant_id,))
                inactive_count = inactive_result[0]["count"] if inactive_result else 0

                # Calculate risk level as percentage of inactive SPs
                risk_level_percentage = round((inactive_count / total_sps * 100), 1) if total_sps > 0 else 0

                # Generate actions based on analytics results
                actions = []

                # Check for inactive findings (use our calculated value)
                if inactive_count > 0:
                    actions.append({
                        "title": "Review Inactive Service Principals",
                        "description": f"{inactive_count} service principals with no sign-in activity within past year",
                        "action": "review_inactive",
                        "priority": "high",
                        "count": inactive_count
                    })

                # Check for expired credentials
                expired_creds = tenant_data.get("expired_credentials", 0)
                if expired_creds > 0:
                    actions.append({
                        "title": "Update Expired Credentials",
                        "description": f"{expired_creds} service principals have expired credentials",
                        "action": "update_credentials",
                        "priority": "high",
                        "count": expired_creds
                    })

                # Check for expiring credentials (use our calculated value)
                if expiring_creds > 0:
                    actions.append({
                        "title": "Renew Expiring Credentials",
                        "description": f"{expiring_creds} service principals have credentials expiring within 30 days",
                        "action": "renew_credentials",
                        "priority": "medium",
                        "count": expiring_creds
                    })

                # Check for unused service principals
                unused_count = tenant_data.get("unused_service_principals", 0)
                if unused_count > 0:
                    actions.append({
                        "title": "Review Unused Service Principals",
                        "description": f"{unused_count} service principals appear to be unused and could be removed",
                        "action": "review_unused",
                        "priority": "medium",
                        "count": unused_count
                    })

                # Check for overprivileged service principals
                overprivileged_count = tenant_data.get("overprivileged_service_principals", 0)
                if overprivileged_count > 0:
                    actions.append({
                        "title": "Review Overprivileged Service Principals",
                        "description": f"{overprivileged_count} service principals may have excessive permissions",
                        "action": "review_permissions",
                        "priority": "medium",
                        "count": overprivileged_count
                    })

                # Build response following REST API guidance format for analysis
                response = {
                    "success": True,
                    "data": [],  # Empty for analysis endpoints
                    "metadata": {
                        "tenant_id": tenant_id,
                        "tenant_name": target_tenant['name'],
                        "total_service_principals": total_sps,
                        "enabled_service_principals": enabled_sps,  # Custom calculation
                        "disabled_service_principals": disabled_sps,
                        "inactive_service_principals": inactive_count,  # Custom calculation
                        "expired_credentials": tenant_data.get("expired_credentials", 0),
                        "expiring_credentials": expiring_creds,  # Custom calculation
                        "owners": owners_count,  # New metric
                        "sps_with_sign_in": sps_with_signin_count,  # New metric
                        "risk_level": risk_level_percentage,  # Percentage of high-risk SPs
                        "analysis_timestamp": datetime.now().isoformat(),
                        "endpoint": "get_sps_report",
                        "request_method": req.method
                    },
                    "actions": actions
                }

            else:
                # Analytics failed, return error in proper format
                error_msg = analytics_result.get('error', 'Unknown error')
                error_response = {
                    "success": False,
                    "data": [],
                    "metadata": {
                        "tenant_id": tenant_id,
                        "tenant_name": target_tenant['name'],
                        "endpoint": "get_sps_report",
                        "request_method": req.method
                    },
                    "actions": [],
                    "error": f"Service principal analytics failed: {error_msg}"
                }
                logging.error(f"✗ get_sps_report failed for {target_tenant['name']}: {error_msg}")
                return func.HttpResponse(
                    json.dumps(error_response, indent=2),
                    status_code=500,
                    mimetype="application/json"
                )

        except Exception as analytics_error:
            # Fallback to basic analysis if sp_analysis fails
            logging.warning(f"Service principal analytics failed, falling back to basic analysis: {str(analytics_error)}")

            # Query service principals for basic analysis
            service_principals_query = """
            SELECT
                id,
                app_id,
                display_name,
                publisher_name,
                service_principal_type,
                owners,
                credential_exp_date,
                credential_type,
                enabled_sp,
                last_sign_in,
                synced_at
            FROM service_principals
            WHERE tenant_id = ?
            """

            service_principals = query(service_principals_query, (tenant_id,))

            # Basic analysis calculations
            total_count = len(service_principals)
            disabled_count = len([sp for sp in service_principals if not sp.get('enabled_sp')])
            enabled_count = total_count - disabled_count  # Custom calculation

            # Calculate expiring credentials (within 30 days) for fallback
            expiring_query = """
            SELECT COUNT(*) as count
            FROM service_principals
            WHERE tenant_id = ?
            AND credential_exp_date IS NOT NULL
            AND credential_exp_date != ''
            AND datetime(credential_exp_date) BETWEEN datetime('now') AND datetime('now', '+30 days')
            """

            expiring_result = query(expiring_query, (tenant_id,))
            expiring_creds = expiring_result[0]["count"] if expiring_result else 0

            # Calculate owners count for fallback
            owners_query = """
            SELECT COUNT(*) as count
            FROM service_principals
            WHERE tenant_id = ?
            AND owners IS NOT NULL
            AND owners != ''
            """

            owners_result = query(owners_query, (tenant_id,))
            owners_count = owners_result[0]["count"] if owners_result else 0

            # Calculate service principals with sign-in for fallback
            sps_with_signin_query = """
            SELECT COUNT(*) as count
            FROM service_principals
            WHERE tenant_id = ?
            AND last_sign_in IS NOT NULL
            AND last_sign_in != ''
            """

            sps_with_signin_result = query(sps_with_signin_query, (tenant_id,))
            sps_with_signin_count = sps_with_signin_result[0]["count"] if sps_with_signin_result else 0

            # Calculate inactive service principals for fallback
            inactive_query = """
            SELECT COUNT(*) as count
            FROM service_principals
            WHERE tenant_id = ?
            AND (last_sign_in IS NULL OR last_sign_in = '' OR datetime(last_sign_in) < datetime('now', '-365 days'))
            """

            inactive_result = query(inactive_query, (tenant_id,))
            inactive_count = inactive_result[0]["count"] if inactive_result else 0

            # Calculate risk level as percentage for fallback
            risk_level_percentage = round((inactive_count / total_count * 100), 1) if total_count > 0 else 0

            # Build basic response
            response = {
                "success": True,
                "data": [],
                "metadata": {
                    "tenant_id": tenant_id,
                    "tenant_name": target_tenant['name'],
                    "total_service_principals": total_count,
                    "enabled_service_principals": enabled_count,
                    "disabled_service_principals": disabled_count,
                    "inactive_service_principals": inactive_count,
                    "expiring_credentials": expiring_creds,
                    "owners": owners_count,
                    "sps_with_sign_in": sps_with_signin_count,
                    "risk_level": risk_level_percentage,
                    "analysis_timestamp": datetime.now().isoformat(),
                    "endpoint": "get_sps_report",
                    "request_method": req.method,
                    "note": "Basic analysis used due to analytics engine failure"
                },
                "actions": []
            }

        # Log completion and return response
        action_count = len(response.get("actions", []))
        total_sps = response["metadata"].get("total_service_principals", 0)

        logging.info(f"✓ get_sps_report completed for {target_tenant['name']}: analyzed {total_sps} service principals, generated {action_count} recommendations")
        return func.HttpResponse(
            json.dumps(response, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        error_msg = f"Unexpected error during get_sps_report: {str(e)}"
        logging.error(error_msg)
        logging.exception("Full exception details:")

        error_result = {
            "success": False,
            "data": [],
            "metadata": {},
            "actions": [],
            "error": error_msg,
            "endpoint": "get_sps_report",
            "timestamp": datetime.now().isoformat()
        }
        return func.HttpResponse(
            json.dumps(error_result, indent=2),
            status_code=500,
            mimetype="application/json"
        )