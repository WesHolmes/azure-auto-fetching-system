import logging
import azure.functions as func
from core.tenant_manager import get_tenants
from sync.user_sync import sync_users
from sync.service_principal_sync import sync_service_principals
from sync.license_sync import sync_licenses
from sync.role_sync import sync_roles_for_tenants
from analysis.user_analysis import (
    calculate_inactive_users,
    calculate_mfa_compliance,
    calculate_license_optimization,
)
from datetime import datetime
from core.database import query


app = func.FunctionApp()


@app.schedule(
    schedule="0 0 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def users_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("User sync timer is past due!")

    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    # reverse the list of tenants
    tenants.reverse()
    for tenant in tenants:
        try:
            # Main data sync
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['users_synced']} users synced"
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
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")


@app.schedule(
    schedule="0 30 * * * *",  # 30 minutes after user sync
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def licenses_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("License sync timer is past due!")

    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    for tenant in tenants:
        try:
            result = sync_licenses(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['licenses_synced']} licenses synced"
                )
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")


@app.schedule(
    schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def applications_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Service principal sync timer is past due!")

    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    for tenant in tenants:
        try:
            result = sync_service_principals(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['service_principals_synced']} service principals synced"
                )
            else:
                logging.error(f"✗ {tenant['name']}: {result['error']}")
        except Exception as e:
            logging.error(f"✗ {tenant['name']}: {str(e)}")


@app.schedule(
    schedule="0 30 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def role_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for role sync"""
    if timer.past_due:
        logging.info("Role sync timer is past due!")

    logging.info("Starting scheduled role sync")
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    tenant_ids = [tenant["tenant_id"] for tenant in tenants]

    result = sync_roles_for_tenants(tenant_ids)

    if result["status"] == "completed":
        logging.info(
            f"  Role sync completed: {result['total_roles_synced']} roles, {result['total_role_assignments_synced']} role assignments across {result['successful_tenants']} tenants"
        )
        if result["failed_tenants"] > 0:
            logging.warning(f"  {result['failed_tenants']} tenants failed role sync")
    else:
        logging.error(f"  Role sync failed: {result.get('error', 'Unknown error')}")


# HTTP endpoints remain the same
@app.route(route="sync/users", methods=["POST"])
def user_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    total = 0

    for tenant in tenants:
        try:
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total += result["users_synced"]
        except Exception as e:
            logging.error(f"Error syncing users for {tenant['name']}: {str(e)}")

    return func.HttpResponse(f"Synced {total} users", status_code=200)


@app.route(route="sync/licenses", methods=["POST"])
def license_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    total_licenses = 0
    total_assignments = 0

    for tenant in tenants:
        try:
            result = sync_licenses(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total_licenses += result["licenses_synced"]
                total_assignments += result["user_licenses_synced"]
        except Exception as e:
            logging.error(f"Error syncing licenses for {tenant['name']}: {str(e)}")

    return func.HttpResponse(
        f"Synced {total_licenses} licenses and {total_assignments} user assignments",
        status_code=200,
    )


@app.route(route="sync/serviceprincipals", methods=["POST"])
def application_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    total = 0

    for tenant in tenants:
        try:
            result = sync_service_principals(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total += result["service_principals_synced"]
        except Exception as e:
            logging.error(
                f"Error syncing service principals for {tenant['name']}: {str(e)}"
            )

    return func.HttpResponse(f"Synced {total} service principals", status_code=200)


@app.route(route="sync/roles", methods=["POST"])
def role_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for role sync"""
    logging.info("Starting manual role sync")
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    tenant_ids = [tenant["tenant_id"] for tenant in tenants]

    result = sync_roles_for_tenants(tenant_ids)

    if result["status"] == "completed":
        successful_tenants = result["successful_tenants"]
        failed_tenants = result["failed_tenants"]
        total_roles = result["total_roles_synced"]
        total_role_assignments = result["total_role_assignments_synced"]

        # Enhanced error reporting for multi-tenant scenarios
        if failed_tenants > 0:
            failed_results = [r for r in result["results"] if r["status"] == "error"]
            
            # Categorize errors
            auth_errors = sum(1 for r in failed_results if "401" in str(r.get("error", "")) or "Authorization_IdentityNotFound" in str(r.get("error", "")))
            permission_errors = sum(1 for r in failed_results if "403" in str(r.get("error", "")) or "Forbidden" in str(r.get("error", "")))
            service_errors = sum(1 for r in failed_results if "503" in str(r.get("error", "")))
            other_errors = failed_tenants - auth_errors - permission_errors - service_errors

            logging.warning(f"Role Sync Errors Summary:")
            if auth_errors > 0:
                logging.warning(f"  - {auth_errors} tenants: Needs admin consent (401/Authorization)")
            if permission_errors > 0:
                logging.warning(f"  - {permission_errors} tenants: Insufficient permissions (403)")
            if service_errors > 0:
                logging.warning(f"  - {service_errors} tenants: Service unavailable (503)")
            if other_errors > 0:
                logging.warning(f"  - {other_errors} tenants: Other errors")

        response_msg = f"Role sync completed: {total_roles} roles, {total_role_assignments} role assignments synced across {successful_tenants} tenants"
        if failed_tenants > 0:
            response_msg += f" ({failed_tenants} tenants failed)"

        return func.HttpResponse(response_msg, status_code=200)
    else:
        error_msg = f"Role sync failed: {result.get('error', 'Unknown error')}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)


def process_tenant_report(tenant: dict) -> dict:
    """Process report for a single tenant"""
    tenant_id = tenant["tenant_id"]
    tenant_name = tenant["name"]

    try:
        logging.info(f"Generating report for {tenant_name}")

        # Execute database queries
        total_users_result = query(
            "SELECT COUNT(*) as count FROM users WHERE tenant_id = ?", (tenant_id,)
        )
        active_users_result = query(
            "SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1",
            (tenant_id,),
        )
        inactive_licenses_result = query(
            """
            SELECT COUNT(DISTINCT u.id) as count 
            FROM users u 
            INNER JOIN user_licenses ul ON u.id = ul.user_id 
            WHERE u.tenant_id = ? AND ul.is_active = 0
        """,
            (tenant_id,),
        )

        # Run analysis functions
        mfa_result = calculate_mfa_compliance(tenant_id)
        license_result = calculate_license_optimization(tenant_id)

        # Process results
        total_users = total_users_result[0]["count"] if total_users_result else 0
        active_users = active_users_result[0]["count"] if active_users_result else 0
        inactive_users_with_licenses = (
            inactive_licenses_result[0]["count"] if inactive_licenses_result else 0
        )

        inactive_users = total_users - active_users
        mfa_compliance_rate = mfa_result.get("compliance_rate", 0)

        # Build comprehensive report
        report = {
            "tenant_name": tenant_name,
            "tenant_id": tenant_id,
            "report_date": datetime.now().isoformat(),
            "user_metrics": {
                "total_user_count": total_users,
                "active_user_count": active_users,
                "inactive_user_count": inactive_users,
                "inactive_user_percentage": round(
                    (inactive_users / total_users * 100), 1
                )
                if total_users > 0
                else 0,
            },
            "security_metrics": {
                "mfa_compliance_rate": mfa_compliance_rate,
                "mfa_enabled_users": mfa_result.get("mfa_enabled", 0),
                "non_compliant_users": mfa_result.get("non_compliant", 0),
                "admin_non_compliant": mfa_result.get("admin_non_compliant", 0),
                "total_admin_users": total_admin_users,
                "admin_mfa_compliance_rate": round(((total_admin_users - admin_non_compliant) / total_admin_users) * 100, 1) if total_admin_users > 0 else 100,
                "inactive_admins": inactive_admins,
                "total_role_assignments": total_role_assignments,
                "unique_roles_assigned": unique_roles_assigned,
                "risk_level": mfa_result.get("risk_level", "unknown"),
            },
            "license_metrics": {
                "license_utilization_rate": license_result.get("utilization_rate", 0),
                "estimated_monthly_savings": license_result.get(
                    "estimated_monthly_savings", 0
                ),
                "underutilized_licenses": license_result.get(
                    "underutilized_licenses", 0
                ),
            },
            "status": "success",
        }

        # Log report
        logging.info(f"  Report for {tenant_name}:")
        logging.info(
            f"    Users: {total_users} total, {inactive_users} inactive ({round((inactive_users / total_users * 100), 1) if total_users > 0 else 0}%)"
        )
        logging.info(
            f"    MFA: {mfa_compliance_rate}% compliance ({mfa_result.get('mfa_enabled', 0)}/{total_users})"
        )
        if total_admin_users > 0:
            admin_mfa_rate = round(((total_admin_users - admin_non_compliant) / total_admin_users) * 100, 1)
            logging.info(
                f"    Admin MFA: {admin_mfa_rate}% compliance ({total_admin_users - admin_non_compliant}/{total_admin_users})"
            )
            logging.info(
                f"    Roles: {total_admin_users} admin users, {total_role_assignments} role assignments, {unique_roles_assigned} unique roles"
            )
            if inactive_admins > 0:
                logging.warning(f"    ⚠️  {inactive_admins} disabled users still have admin roles")
        else:
            logging.info("    No admin role assignments found")
        logging.info(
            f"    Licenses: {underutilized_count} underutilized licenses, ${monthly_savings}/month potential savings"
        )
        logging.info(
            f"    Potential savings: ${license_result.get('estimated_monthly_savings', 0)}/month"
        )
        logging.warning(
            f"    Critical: {mfa_result.get('admin_non_compliant', 0)} admin users without MFA"
        )

        return report

    except Exception as e:
        logging.error(f"Failed to generate report for {tenant_name}: {str(e)}")
        return {
            "tenant_name": tenant_name,
            "tenant_id": tenant_id,
            "status": "error",
            "error": str(e),
        }


# Generate comprehensive user and license report
@app.schedule(
    schedule="0 0 6 * * *",  # Daily at 6 AM
    arg_name="timer",
    run_on_startup=False,  # Manual control - run only when triggered
    use_monitor=False,
)
def generate_user_report(timer: func.TimerRequest) -> None:
    """Generate daily JSON report"""
    if timer.past_due:
        logging.warning("User report timer is past due!")

    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    total_tenants = len(tenants)

    logging.info(f"Starting report generation for {total_tenants} tenants")

    try:
        # Process tenants and build JSON summaries
        tenant_summaries = []
        failed_reports = []

        for tenant in tenants:
            try:
                tenant_id = tenant["tenant_id"]
                tenant_name = tenant["name"]
                
                # Get basic user counts
                total_users_result = query(
                    "SELECT COUNT(*) as count FROM users WHERE tenant_id = ?", (tenant_id,)
                )
                active_users_result = query(
                    "SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1",
                    (tenant_id,),
                )

                
                # Get analysis results
                mfa_result = calculate_mfa_compliance(tenant_id)
                license_result = calculate_license_optimization(tenant_id)
                
                # Get role metrics
                role_metrics_result = query(
                    """
                    SELECT 
                        COUNT(DISTINCT user_id) as total_admin_users,
                        COUNT(*) as total_role_assignments,
                        COUNT(DISTINCT role_id) as unique_roles_assigned
                    FROM user_roles 
                    WHERE tenant_id = ?
                    """,
                    (tenant_id,)
                )
                
                # Get inactive users with admin roles
                inactive_admins_result = query(
                    """
                    SELECT COUNT(DISTINCT ur.user_id) as count
                    FROM user_roles ur
                    INNER JOIN users u ON ur.user_id = u.id AND ur.tenant_id = u.tenant_id
                    WHERE ur.tenant_id = ? AND u.account_enabled = 0
                    """,
                    (tenant_id,)
                )
                
                # Calculate metrics
                total_users = total_users_result[0]["count"] if total_users_result else 0
                active_users = active_users_result[0]["count"] if active_users_result else 0
                inactive_users = total_users - active_users
                
                # Role metrics
                role_metrics = role_metrics_result[0] if role_metrics_result else {}
                total_admin_users = role_metrics.get("total_admin_users", 0)
                total_role_assignments = role_metrics.get("total_role_assignments", 0)
                unique_roles_assigned = role_metrics.get("unique_roles_assigned", 0)
                inactive_admins = inactive_admins_result[0]["count"] if inactive_admins_result else 0
                
                # Generate critical warnings and alerts
                warnings = []
                alerts = {
                    "critical": [],
                    "warning": [],
                    "info": []
                }
                
                # MFA Security Alerts
                mfa_compliance = mfa_result.get("compliance_rate", 0)
                admin_non_compliant = mfa_result.get("admin_non_compliant", 0)
                
                if admin_non_compliant > 0:
                    if total_admin_users > 0:
                        admin_mfa_rate = round(((total_admin_users - admin_non_compliant) / total_admin_users) * 100, 1)
                        critical_msg = f"  CRITICAL: {admin_non_compliant} of {total_admin_users} admin users without MFA ({admin_mfa_rate}% admin MFA compliance) - HIGH SECURITY RISK"
                    else:
                        critical_msg = f"  CRITICAL: {admin_non_compliant} admin users without MFA - HIGH SECURITY RISK"
                    warnings.append(critical_msg)
                    alerts["critical"].append({
                        "type": "mfa_admin_risk",
                        "message": f"{admin_non_compliant} of {total_admin_users} admin users without MFA",
                        "severity": "critical",
                        "affected_count": admin_non_compliant,
                        "total_admin_users": total_admin_users
                    })
                
                if mfa_compliance < 50:
                    warning_msg = f"  WARNING: Low MFA compliance ({mfa_compliance}%) - Security risk"
                    warnings.append(warning_msg)
                    alerts["warning"].append({
                        "type": "low_mfa_compliance", 
                        "message": f"MFA compliance only {mfa_compliance}%",
                        "severity": "warning",
                        "compliance_rate": mfa_compliance
                    })
                
                # License Cost Alerts
                monthly_savings = license_result.get("estimated_monthly_savings", 0)
                underutilized_count = license_result.get("underutilized_licenses", 0)
                
                if monthly_savings > 100:
                    warning_msg = f"  COST OPPORTUNITY: ${monthly_savings}/month potential savings from {underutilized_count} unused licenses"
                    warnings.append(warning_msg)
                    alerts["warning"].append({
                        "type": "high_cost_savings",
                        "message": f"${monthly_savings}/month potential savings available",
                        "severity": "warning", 
                        "monthly_savings": monthly_savings,
                        "underutilized_licenses": underutilized_count
                    })
                elif underutilized_count > 0:
                    alerts["info"].append({
                        "type": "license_optimization",
                        "message": f"{underutilized_count} underutilized licenses found",
                        "severity": "info",
                        "underutilized_licenses": underutilized_count,
                        "monthly_savings": monthly_savings
                    })
                
                # Inactive Admin Alerts
                if inactive_admins > 0:
                    warning_msg = f"  WARNING: {inactive_admins} disabled users still have admin roles - ACCESS RISK"
                    warnings.append(warning_msg)
                    alerts["warning"].append({
                        "type": "inactive_admin_access",
                        "message": f"{inactive_admins} disabled users with admin roles",
                        "severity": "warning",
                        "inactive_admin_count": inactive_admins
                    })
                
                # Inactive User Alerts  
                inactive_percentage = round((inactive_users / total_users * 100), 1) if total_users > 0 else 0
                if inactive_percentage > 25:
                    warning_msg = f"  WARNING: High inactive user rate ({inactive_percentage}%) may indicate cleanup needed"
                    warnings.append(warning_msg)
                    alerts["warning"].append({
                        "type": "high_inactive_users",
                        "message": f"{inactive_percentage}% of users are inactive", 
                        "severity": "warning",
                        "inactive_percentage": inactive_percentage,
                        "inactive_count": inactive_users
                    })

                # Build JSON summary for this tenant
                tenant_summary = {
                    "tenant_name": tenant_name,
                    "tenant_id": tenant_id,
                    "total_users": total_users,
                    "active_users": active_users,
                    "inactive_users": inactive_users,
                    "inactive_percentage": inactive_percentage,
                    "mfa_compliance_rate": mfa_compliance,
                    "mfa_enabled_users": mfa_result.get("mfa_enabled", 0),
                    "admin_non_compliant": admin_non_compliant,
                    "estimated_monthly_savings": monthly_savings,
                    "underutilized_licenses": underutilized_count,
                    "warnings": warnings,
                    "alerts": alerts,
                    "timestamp": datetime.now().isoformat()
                }
                
                tenant_summaries.append(tenant_summary)
                
                # Log individual tenant summary as JSON
                import json
                logging.info(f"Report for {tenant_name}:")
                logging.info(json.dumps(tenant_summary, indent=2))
                
            except Exception as e:
                logging.error(f"Unexpected error processing {tenant['name']}: {e}")
                failed_reports.append({"tenant_name": tenant["name"], "error": str(e)})

        # Build comprehensive JSON report
        comprehensive_report = {
            "report_summary": {
                "total_tenants": total_tenants,
                "successful_tenants": len(tenant_summaries),
                "failed_tenants": len(failed_reports),
                "generation_timestamp": datetime.now().isoformat()
            },
            "tenant_reports": tenant_summaries,
            "failed_reports": failed_reports if failed_reports else []
        }
        
        # Log the comprehensive JSON report
        import json
        # logging.info("=== COMPREHENSIVE JSON REPORT ===")
        logging.info(json.dumps(comprehensive_report, indent=2))
        # logging.info("=== END JSON REPORT ===")

        # Summary logging for backwards compatibility
        logging.info(f"Report generation completed: {len(tenant_summaries)}/{total_tenants} successful")
        if failed_reports:
            logging.warning(f"Failed: {len(failed_reports)}/{total_tenants} tenants")
            for failed in failed_reports:
                logging.error(f"- {failed.get('tenant_name', 'Unknown')}: {failed.get('error', 'Unknown error')}")

    except Exception as e:
        logging.error(f"Critical error report generation: {str(e)}")
        raise


# Manual HTTP trigger for testing report generation
@app.route(route="generate-report-now", methods=["GET", "POST"])
def generate_report_manual(req: func.HttpRequest) -> func.HttpResponse:
    """Manual HTTP trigger to run report generation for testing"""
    try:
        logging.info("Manual report generation triggered via HTTP")

        tenants = get_tenants()
        total_tenants = len(tenants)

        logging.info(f"Starting report generation for {total_tenants} tenants")

        # Process reports synchronously
        completed_reports = []
        failed_reports = []

        for tenant in tenants:
            try:
                result = process_tenant_report(tenant)
                if result.get("status") == "success":
                    completed_reports.append(result)
                else:
                    failed_reports.append(result)
            except Exception as e:
                logging.error(f"Unexpected error processing {tenant['name']}: {e}")
                failed_reports.append({"tenant_name": tenant["name"], "error": str(e)})

        # Summary
        summary = {
            "total_tenants": total_tenants,
            "successful": len(completed_reports),
            "failed": len(failed_reports),
        }

        if failed_reports:
            logging.warning(f"Failed: {len(failed_reports)}/{total_tenants} tenants")
            for failed in failed_reports:
                logging.error(
                    f"- {failed.get('tenant_name', 'Unknown')}: {failed.get('error', 'Unknown error')}"
                )

        return func.HttpResponse(
            f"Report generation completed: {summary['successful']}/{summary['total_tenants']} successful",
            status_code=200,
        )

    except Exception as e:
        error_msg = f"Error generating reports: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)


@app.schedule(
    schedule="0 30 8 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def service_principal_analytics(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Service principal analytics timer is past due!")

    try:
        from analytics.service_principal_analytics import (
            analyze_service_principals,
            format_analytics_summary,
        )

        # Determine tenant mode
        tenants = get_tenants()
        tenant_mode = (
            "single"
            if len(tenants) == 1
            and tenants[0]["tenant_id"] == "3aae0fb1-276f-42f8-8e4d-36ca10cbb779"
            else "multi"
        )

        logging.info(f"Analyzing service principals in {tenant_mode} mode")

        # Perform analytics
        analytics_result = analyze_service_principals(tenant_mode)

        if analytics_result["status"] == "success":
            # Format and log the summary
            summary = format_analytics_summary(analytics_result)
            logging.info(summary)

            # Log key metrics
            data = analytics_result
            logging.info(f"Analytics completed: {data['total_sps']} SPs analyzed")

            if data["expired_sps"] > 0:
                logging.warning(
                    f"Security Alert: {data['expired_sps']} service principals have expired credentials"
                )

            if data["sps_no_credentials"] > 0:
                logging.warning(
                    f"Security Alert: {data['sps_no_credentials']} service principals have no credentials"
                )

            if data["disabled_sps"] > 0:
                logging.info(
                    f"Info: {data['disabled_sps']} service principals are disabled"
                )

        else:
            logging.error(f"Analytics failed: {analytics_result['error']}")

    except Exception as e:
        logging.error(f"Service principal analytics failed: {str(e)}")


@app.route(route="analytics/serviceprincipals", methods=["GET"])
def service_principal_analytics_http(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from analytics.service_principal_analytics import (
            analyze_service_principals,
            format_analytics_summary,
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
            summary = format_analytics_summary(analytics_result)
            return func.HttpResponse(summary, status_code=200, mimetype="text/plain")
        else:
            return func.HttpResponse(
                f"Analytics Error: {analytics_result['error']}", status_code=500
            )

    except Exception as e:
        logging.error(f"Service principal analytics HTTP failed: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
