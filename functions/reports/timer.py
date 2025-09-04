from datetime import datetime
import json
import logging

import azure.functions as func

from db.db_client import query
from functions.users.helpers import calculate_license_optimization, calculate_mfa_compliance
from shared.error_reporting import aggregate_recent_sync_errors
from shared.graph_client import get_tenants
from shared.utils import create_error_response


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


def generate_report_now(req: func.HttpRequest) -> func.HttpResponse:
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
