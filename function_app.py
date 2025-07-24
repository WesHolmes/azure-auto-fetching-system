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
    
    tenants = get_tenants()
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
    
    tenants = get_tenants()
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
    
    tenants = get_tenants()
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
    tenants = get_tenants()
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
    tenants = get_tenants()
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
    tenants = get_tenants()
    total = 0
    
    for tenant in tenants:
        try:
            result = sync_service_principals(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total += result["service_principals_synced"]
        except:
            pass
    
    return func.HttpResponse(f"Synced {total} service principals", status_code=200)

# Generate comprehensive user and license report
@app.schedule(
    schedule="0 0 6 * * *",  # Daily at 6 AM
    arg_name="timer", 
    run_on_startup=False, 
    use_monitor=False
)
def generate_user_report(timer: func.TimerRequest) -> None:
    """Generate daily JSON report with user analytics and license metrics"""
    if timer.past_due:
        logging.warning("User report timer is past due!")
    
    tenants = get_tenants()
    
    for tenant in tenants:
        try:
            tenant_id = tenant["tenant_id"]
            tenant_name = tenant["name"]
            
            logging.info(f"Generating report for {tenant_name}")
            
            # Get basic user counts
            total_users_query = "SELECT COUNT(*) as count FROM users WHERE tenant_id = ?"
            total_users = query(total_users_query, (tenant_id,))[0]['count']
            
            active_users_query = "SELECT COUNT(*) as count FROM users WHERE tenant_id = ? AND account_enabled = 1"
            active_users = query(active_users_query, (tenant_id,))[0]['count']
            
            inactive_users = total_users - active_users
            
            # Get MFA compliance data
            mfa_result = calculate_mfa_compliance(tenant_id)
            mfa_compliance_rate = mfa_result.get('compliance_rate', 0)
            
            # Get inactive users with licenses data (both disabled accounts and inactive usage)
            inactive_with_licenses_query = """
                SELECT COUNT(DISTINCT u.id) as count 
                FROM users u 
                INNER JOIN user_licenses ul ON u.id = ul.user_id 
                WHERE u.tenant_id = ? AND ul.is_active = 0
            """
            inactive_users_with_licenses = query(inactive_with_licenses_query, (tenant_id,))[0]['count']
            
            # Get license optimization data
            license_result = calculate_license_optimization(tenant_id)
            
            # Build comprehensive report
            report = {
                "tenant_name": tenant_name,
                "tenant_id": tenant_id,
                "report_date": datetime.now().isoformat(),
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
                    "risk_level": mfa_result.get('risk_level', 'unknown')
                },
                "license_metrics": {
                    "inactive_users_with_licenses": inactive_users_with_licenses,
                    "license_utilization_rate": license_result.get('utilization_rate', 0),
                    "estimated_monthly_savings": license_result.get('estimated_monthly_savings', 0),
                    "underutilized_licenses": license_result.get('underutilized_licenses', 0)
                }
            }
            
            # Log the report
            logging.info(f"  Report for {tenant_name}:")
            logging.info(f"    Users: {total_users} total, {inactive_users} inactive ({round((inactive_users / total_users * 100), 1) if total_users > 0 else 0}%)")
            logging.info(f"    MFA: {mfa_compliance_rate}% compliance")
            logging.info(f"    Licenses: {inactive_users_with_licenses} inactive users with licenses")
            logging.info(f"    Potential savings: ${license_result.get('estimated_monthly_savings', 0)}/month")
            
        except Exception as e:
            logging.error(f"Failed to generate report for {tenant_name}: {str(e)}")

# ask cursor to generate a timer trigger function that pulls from users database and generates a json report that includes the following:
# total user count, total inactive user count, total mfa compliance rate, how many inactive users with inactive licenses rate

# for single tenant & multi tenant^