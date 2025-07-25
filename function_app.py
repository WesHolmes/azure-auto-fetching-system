import logging
import azure.functions as func
from core.tenant_manager import get_tenants
from sync.user_sync import sync_users
from sync.service_principal_sync import sync_service_principals
from sync.license_sync import sync_licenses
from analysis.user_analysis import (
    calculate_inactive_users,
    calculate_mfa_compliance,
    calculate_license_optimization
)
from datetime import datetime
from core.database import query
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import asyncio


app = func.FunctionApp()

@app.schedule(
    schedule="0 0 * * * *", 
    arg_name="timer", 
    run_on_startup=False, 
    use_monitor=False
)
def users_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("User sync timer is past due!")
    
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
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
                    logging.info(f"  Inactive users: {inactive_result.get('inactive_count', 0)}")
                    
                    mfa_result = calculate_mfa_compliance(tenant["tenant_id"])
                    logging.info(f"  MFA compliance: {mfa_result.get('compliance_rate', 0)}%")
                    
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
    use_monitor=False
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
    schedule="0 0 0 * * *", 
    arg_name="timer", 
    run_on_startup=False, 
    use_monitor=False
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
        except:
            pass
    
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
        except:
            pass
    
    return func.HttpResponse(
        f"Synced {total_licenses} licenses and {total_assignments} user assignments", 
        status_code=200
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
        except:
            pass
    
    return func.HttpResponse(f"Synced {total} service principals", status_code=200)

async def process_tenant_report_async(tenant: dict) -> dict:
    """Process report for a single tenant with async/await for maximum performance"""
    tenant_id = tenant["tenant_id"]
    tenant_name = tenant["name"]
    
    try:
        logging.info(f"Generating report for {tenant_name}")
        
        # Use asyncio.gather to run all operations concurrently
        async def run_query(sql, params):
            """Async wrapper for database queries"""
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, query, sql, params)
        
        async def run_analysis(func, tenant_id):
            """Async wrapper for analysis functions"""
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, func, tenant_id)
        
        # Execute all database queries and analysis functions concurrently with async/await
        results = await asyncio.gather(
            run_query("SELECT COUNT(*) as count FROM users WHERE tenant_id = ?", (tenant_id,)),
            run_query("SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1", (tenant_id,)),
            run_query("""
                SELECT COUNT(DISTINCT u.id) as count 
                FROM users u 
                INNER JOIN user_licenses ul ON u.id = ul.user_id 
                WHERE u.tenant_id = ? AND ul.is_active = 0
            """, (tenant_id,)),
            run_analysis(calculate_mfa_compliance, tenant_id),
            run_analysis(calculate_license_optimization, tenant_id),
            return_exceptions=True
        )
        
        # Process the async results
        total_users_result, active_users_result, inactive_licenses_result, mfa_result, license_result = results
        
        # Handle any exceptions in results
        total_users = total_users_result[0]['count'] if isinstance(total_users_result, list) and total_users_result else 0
        active_users = active_users_result[0]['count'] if isinstance(active_users_result, list) and active_users_result else 0
        inactive_users_with_licenses = inactive_licenses_result[0]['count'] if isinstance(inactive_licenses_result, list) and inactive_licenses_result else 0
        
        inactive_users = total_users - active_users
        
        # Handle analysis results
        if isinstance(mfa_result, Exception):
            logging.error(f"MFA analysis error for {tenant_name}: {mfa_result}")
            mfa_result = {}
        if isinstance(license_result, Exception):
            logging.error(f"License analysis error for {tenant_name}: {license_result}")
            license_result = {}
            
        mfa_compliance_rate = mfa_result.get('compliance_rate', 0)
        
        # Build comprehensive report
        report = {
            "tenant_name": tenant_name,
            "tenant_id": tenant_id,
            "report_date": datetime.now().isoformat(),
            "processing_method": "async/await",
            "user_metrics": {
                "total_user_count": total_users,
                "active_user_count": active_users,
                "inactive_user_count": inactive_users,
                "inactive_user_percentage": round((inactive_users / total_users * 100), 1) if total_users > 0 else 0
            },
            "security_metrics": {
                "mfa_compliance_rate": mfa_compliance_rate,
                "mfa_enabled_users": mfa_result.get('mfa_enabled', 0),
                "non_compliant_users": mfa_result.get('non_compliant', 0),
                "admin_non_compliant": mfa_result.get('admin_non_compliant', 0),
                "risk_level": mfa_result.get('risk_level', 'unknown')
            },
            "license_metrics": {
                "inactive_users_with_licenses": inactive_users_with_licenses,
                "license_utilization_rate": license_result.get('utilization_rate', 0),
                "estimated_monthly_savings": license_result.get('estimated_monthly_savings', 0),
                "underutilized_licenses": license_result.get('underutilized_licenses', 0)
            },
            "status": "success"
        }
        
        # logging report
        logging.info(f"  Report for {tenant_name}:")
        logging.info(f"    Users: {total_users} total, {inactive_users} inactive ({round((inactive_users / total_users * 100), 1) if total_users > 0 else 0}%)")
        logging.info(f"    MFA: {mfa_compliance_rate}% compliance ({mfa_result.get('mfa_enabled', 0)}/{total_users})")
        logging.info(f"    Licenses: {inactive_users_with_licenses} inactive users with licenses")
        logging.info(f"    Potential savings: ${license_result.get('estimated_monthly_savings', 0)}/month")
        logging.warning(f"    Critical: {mfa_result.get('admin_non_compliant', 0)} admin users without MFA")
        
        return report
        
    except Exception as e:
        logging.error(f"Failed to generate report for {tenant_name}: {str(e)}")
        return {
            "tenant_name": tenant_name,
            "tenant_id": tenant_id,
            "status": "error",
            "error": str(e),
            "processing_method": "async/await"
        }

# Generate comprehensive user and license report with async/await for maximum speed
@app.schedule(
    schedule="0 0 6 * * *",  # Daily at 6 AM
    arg_name="timer", 
    run_on_startup=False,  # Manual control - run only when triggered
    use_monitor=False
)
async def generate_user_report(timer: func.TimerRequest = None) -> None:
    """Generate daily JSON report with async/await for maximum performance"""
    if timer and timer.past_due:
        logging.warning("User report timer is past due!")
    
    tenants = get_tenants()  # Automatically uses TENANT_MODE environment variable
    total_tenants = len(tenants)
    
    logging.info(f"Starting report generation for {total_tenants} tenants")
    
    try:
        # use asyncio.gather for concurrent tenant processing
        if total_tenants == 1:
            # single tenant - direct async processing
            results = [await process_tenant_report_async(tenants[0])]
        else:
            # multi-tenant - concurrent async processing
            tasks = [process_tenant_report_async(tenant) for tenant in tenants]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # process results
        completed_reports = []
        failed_reports = []
        
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Unexpected error: {result}")
                failed_reports.append({"error": str(result)})
            elif result.get('status') == 'success':
                completed_reports.append(result)
            else:
                failed_reports.append(result)
        
        # Summary logging (You'll see this!)
        logging.info(f"    Report generation completed:")
        logging.info(f"    Successful: {len(completed_reports)}/{total_tenants} tenants")
        if failed_reports:
            logging.warning(f"    Failed: {len(failed_reports)}/{total_tenants} tenants")
            for failed in failed_reports:
                logging.error(f"    - {failed.get('tenant_name', 'Unknown')}: {failed.get('error', 'Unknown error')}")
        
        logging.info(f"Report generation finished successfully")
        
    except Exception as e:
        logging.error(f"Critical error report generation: {str(e)}")
        raise


# Manual HTTP trigger for testing report generation
@app.route(route="generate-report-now", methods=["GET", "POST"])
async def generate_report_manual(req: func.HttpRequest) -> func.HttpResponse:
    """Manual HTTP trigger to run report generation for testing"""
    try:
        logging.info("🔧 Manual report generation triggered via HTTP")
        
        # Get tenant information and log it
        tenants = get_tenants()
        total_tenants = len(tenants)
        
        logging.info(f"Starting report generation for {total_tenants} tenants")
        
        # Start the report generation without waiting for it to complete
        import asyncio
        asyncio.create_task(generate_user_report(None))
        
        # Return immediately
        return func.HttpResponse(
            f"✅ Report generation started for {total_tenants} tenant(s)! Check the function logs to monitor progress. This may take several minutes to complete.",
            status_code=202
        )
        
    except Exception as e:
        error_msg = f"❌ Error starting report generation: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            error_msg,
            status_code=500
        )