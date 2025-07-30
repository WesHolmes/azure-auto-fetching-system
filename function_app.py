import logging
import azure.functions as func
from core.tenant_manager import get_tenants
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
from datetime import datetime
from core.database import query
import json
import re

app = func.FunctionApp()

def extract_error_code(error_message):
    """Extract HTTP error code from error message"""
    # Look for patterns like "403 Forbidden", "401 Unauthorized", etc.
    match = re.search(r'(\d{3})\s+\w+', str(error_message))
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
                logging.info(f"✓ {tenant['name']}: {result['users_synced']} users synced")
                results.append({
                    'status': 'completed',
                    'tenant_id': tenant["tenant_id"],
                    'users_synced': result["users_synced"]
                })
                
                # Run analysis after successful sync
                try:
                    inactive_result = calculate_inactive_users(tenant["tenant_id"])
                    logging.info(f"  Inactive users: {inactive_result.get('inactive_count', 0)}")
                    
                    mfa_result = calculate_mfa_compliance(tenant["tenant_id"])
                    logging.info(f"  MFA compliance: {mfa_result.get('compliance_rate', 0)}%")
                    
                except Exception as e:
                    logging.error(f"Analysis error: {str(e)}")
                    
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                results.append({
                    'status': 'error',
                    'tenant_id': tenant["tenant_id"],
                    'error': result.get("error", "Unknown error")
                })
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            results.append({
                'status': 'error',
                'tenant_id': tenant["tenant_id"],
                'error': str(e)
            })
    
    # Use centralized error reporting
    failed_count = len([r for r in results if r['status'] == 'error'])
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
                logging.info(f"✓ {tenant['name']}: {result['licenses_synced']} licenses synced")
                results.append({
                    'status': 'completed',
                    'tenant_id': tenant["tenant_id"],
                    'licenses_synced': result["licenses_synced"]
                })
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                results.append({
                    'status': 'error',
                    'tenant_id': tenant["tenant_id"],
                    'error': result.get("error", "Unknown error")
                })
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            results.append({
                'status': 'error',
                'tenant_id': tenant["tenant_id"],
                'error': str(e)
            })
    
    failed_count = len([r for r in results if r['status'] == 'error'])
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
                logging.info(f"✓ {tenant['name']}: {result['service_principals_synced']} service principals synced")
                results.append({
                    'status': 'completed',
                    'tenant_id': tenant["tenant_id"],
                    'service_principals_synced': result["service_principals_synced"]
                })
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                results.append({
                    'status': 'error',
                    'tenant_id': tenant["tenant_id"],
                    'error': result.get("error", "Unknown error")
                })
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")
            results.append({
                'status': 'error',
                'tenant_id': tenant["tenant_id"],
                'error': str(e)
            })
    
    failed_count = len([r for r in results if r['status'] == 'error'])
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
            result = sync_conditional_access_policies(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['policies_synced']} conditional access policies, {result['policy_users_synced']} user assignments, {result['policy_applications_synced']} application assignments synced"
                )
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
                # Track error codes from the sync result
                error_code = extract_error_code(result['error'])
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


# ============================================================================
# HTTP TRIGGERS (Manual Endpoints)
# ============================================================================

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
                results.append({
                    'status': 'completed',
                    'tenant_id': tenant["tenant_id"],
                    'users_synced': result["users_synced"]
                })
            else:
                results.append({
                    'status': 'error',
                    'tenant_id': tenant["tenant_id"],
                    'error': result.get("error", "Unknown error")
                })
        except Exception as e:
            logging.error(f"Error syncing users for {tenant['name']}: {str(e)}")
            results.append({
                'status': 'error',
                'tenant_id': tenant["tenant_id"],
                'error': str(e)
            })

    failed_count = len([r for r in results if r['status'] == 'error'])
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
                results.append({
                    'status': 'completed',
                    'tenant_id': tenant["tenant_id"],
                    'licenses_synced': result["licenses_synced"],
                    'user_licenses_synced': result["user_licenses_synced"]
                })
            else:
                results.append({
                    'status': 'error',
                    'tenant_id': tenant["tenant_id"],
                    'error': result.get("error", "Unknown error")
                })
        except Exception as e:
            logging.error(f"Error syncing licenses for {tenant['name']}: {str(e)}")
            results.append({
                'status': 'error',
                'tenant_id': tenant["tenant_id"],
                'error': str(e)
            })

    failed_count = len([r for r in results if r['status'] == 'error'])
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
                results.append({
                    'status': 'completed',
                    'tenant_id': tenant["tenant_id"],
                    'service_principals_synced': result["service_principals_synced"]
                })
            else:
                results.append({
                    'status': 'error',
                    'tenant_id': tenant["tenant_id"],
                    'error': result.get("error", "Unknown error")
                })
        except Exception as e:
            logging.error(f"Error syncing service principals for {tenant['name']}: {str(e)}")
            results.append({
                'status': 'error',
                'tenant_id': tenant["tenant_id"],
                'error': str(e)
            })

    failed_count = len([r for r in results if r['status'] == 'error'])
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
            result = sync_conditional_access_policies(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total_policies += result["policies_synced"]
                total_policy_users += result["policy_users_synced"]
            else:
                # Track error codes from the sync result
                error_code = extract_error_code(result['error'])
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

    return func.HttpResponse(f"Synced {total_policies} conditional access policies and {total_policy_users} user assignments", status_code=200)


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
        
        logging.info(f"Processing {len(successful_tenants_info)} successful tenants (excluding {failed_count} failed syncs)")
        
        # Process successful tenants
        tenant_summaries = []

        for tenant_info in successful_tenants_info:
            tenant = next((t for t in all_tenants if t["tenant_id"] == tenant_info["tenant_id"]), None)
            if not tenant:
                continue
                
            try:
                tenant_id = tenant["tenant_id"]
                tenant_name = tenant["name"]
                
                # Get basic metrics
                total_users_result = query("SELECT COUNT(*) as count FROM users WHERE tenant_id = ?", (tenant_id,))
                active_users_result = query("SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1", (tenant_id,))
                
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
                    warnings.append(f"COST OPPORTUNITY: ${monthly_savings}/month potential savings from {underutilized_count} unused licenses")
                
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
                    "warnings": warnings
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
                "generation_timestamp": datetime.now().isoformat()
            },
            "tenant_reports": tenant_summaries,
            "recent_sync_errors": recent_sync_errors
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
            status_code=202
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
        from analytics.service_principal_analytics import (
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
            logging.info(f"Service Principal Analytics Result: {json.dumps(json_result, indent=2)}")

        else:
            error_result = {"status": "error", "error": analytics_result['error']}
            logging.error(f"Analytics failed: {json.dumps(error_result)}")

    except Exception as e:
        error_result = {"status": "error", "error": str(e)}
        logging.error(f"Service principal analytics failed: {json.dumps(error_result)}")


@app.route(route="analytics/serviceprincipals", methods=["GET"])
def service_principal_analytics_http(req: func.HttpRequest) -> func.HttpResponse:
    try:
        import json
        from analytics.service_principal_analytics import (
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
                mimetype="application/json"
            )
        else:
            error_result = {"status": "error", "error": analytics_result['error']}
            return func.HttpResponse(
                json.dumps(error_result, indent=2), 
                status_code=500, 
                mimetype="application/json"
            )

    except Exception as e:
        logging.error(f"Service principal analytics HTTP failed: {str(e)}")
        error_result = {"status": "error", "error": str(e)}
        import json
        return func.HttpResponse(
            json.dumps(error_result, indent=2), 
            status_code=500, 
            mimetype="application/json"
        )
