import logging
import azure.functions as func
from core.tenant_manager import get_tenants
from sync.user_sync import sync_users
from sync.service_principal_sync import sync_service_principals
from analytics.service_principal_analytics import analyze_service_principals, format_analytics_summary

app = func.FunctionApp()


@app.schedule(
    schedule="0 0 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def users_sync(timer: func.TimerRequest) -> None:
    """Scheduled user synchronization function"""
    logging.info("🔄 USERS SYNC FUNCTION STARTED")
    
    if timer.past_due:
        logging.warning("User sync timer is past due!")

    tenants = get_tenants()
    logging.info(f"📊 Processing {len(tenants)} tenant(s)")
    
    successful_count = 0
    failed_count = 0
    total_synced = 0
    
    for tenant in tenants:
        logging.info(f"🏢 Processing tenant: {tenant['tenant_id']} ({tenant['name']})")
        try:
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                successful_count += 1
                total_synced += result["users_synced"]
                logging.info(f"✅ {tenant['name']}: {result['users_synced']} users synced")
            else:
                failed_count += 1
                logging.error(f"❌ {tenant['name']}: {result['error']}")
        except Exception as e:
            failed_count += 1
            logging.error(f"❌ {tenant['name']}: {str(e)}")
    
    logging.info(f"🏁 USERS SYNC COMPLETED - Success: {successful_count}, Failed: {failed_count}, Total Users: {total_synced}")


@app.schedule(
    schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def applications_sync(timer: func.TimerRequest) -> None:
    """Scheduled service principal synchronization function"""
    logging.info("🔄 SERVICE PRINCIPALS SYNC FUNCTION STARTED")
    
    if timer.past_due:
        logging.warning("Service principal sync timer is past due!")

    from sync.service_principal_sync import sync_service_principals_parallel
    
    tenants = get_tenants()
    logging.info(f"📊 Processing {len(tenants)} tenant(s) with parallel workers")
    
    # Use parallel processing for all tenants
    from datetime import datetime
    start_time = datetime.now()
    results = sync_service_principals_parallel(tenants, max_workers=3)
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Log summary
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    total_synced = sum(r.get('service_principals_synced', 0) for r in successful)
    
    logging.info(f"⚡ Parallel sync completed in {total_time:.2f}s: {len(successful)} tenants successful, {len(failed)} failed, {total_synced} total SPs synced")
    
    # Log any failures
    for result in failed:
        logging.error(f"❌ {result['tenant_name']}: {result['error']}")
    
    logging.info(f"🏁 SERVICE PRINCIPALS SYNC COMPLETED - Success: {len(successful)}, Failed: {len(failed)}, Total SPs: {total_synced}")


@app.route(route="sync/users", methods=["POST"])
def user_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint for manual user synchronization"""
    logging.info("🌐 USER SYNC HTTP ENDPOINT TRIGGERED")
    
    tenants = get_tenants()
    total = 0

    for tenant in tenants:
        try:
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                total += result["users_synced"]
        except:
            pass

    logging.info(f"🏁 USER SYNC HTTP COMPLETED - Total synced: {total}")
    return func.HttpResponse(f"Synced {total} users", status_code=200)


@app.route(route="sync/serviceprincipals", methods=["POST"])
def application_sync_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint for manual service principal synchronization"""
    logging.info("🌐 SERVICE PRINCIPALS SYNC HTTP ENDPOINT TRIGGERED")
    
    from sync.service_principal_sync import sync_service_principals_parallel
    from datetime import datetime
    
    tenants = get_tenants()
    
    # Use parallel processing
    start_time = datetime.now()
    results = sync_service_principals_parallel(tenants, max_workers=3)
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Calculate totals
    successful = [r for r in results if r['status'] == 'success']
    total = sum(r.get('service_principals_synced', 0) for r in successful)
    
    logging.info(f"🏁 SERVICE PRINCIPALS SYNC HTTP COMPLETED - Total synced: {total} in {total_time:.2f}s")
    return func.HttpResponse(
        f"Synced {total} service principals across {len(successful)}/{len(tenants)} tenants in {total_time:.2f}s", 
        status_code=200
    )


@app.route(route="sync/serviceprincipals/async", methods=["POST"])
def application_sync_async_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint for async service principal synchronization"""
    logging.info("🌐 ASYNC SERVICE PRINCIPALS SYNC HTTP ENDPOINT TRIGGERED")
    
    from sync.service_principal_sync import sync_service_principals_async_wrapper
    from datetime import datetime
    
    tenants = get_tenants()
    
    # Use async processing
    start_time = datetime.now()
    results = sync_service_principals_async_wrapper(tenants, max_workers=5)
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Calculate totals
    successful = [r for r in results if r['status'] == 'success']
    total = sum(r.get('service_principals_synced', 0) for r in successful)
    
    logging.info(f"🏁 ASYNC SERVICE PRINCIPALS SYNC HTTP COMPLETED - Total synced: {total} in {total_time:.2f}s")
    return func.HttpResponse(
        f"Async synced {total} service principals across {len(successful)}/{len(tenants)} tenants in {total_time:.2f}s", 
        status_code=200
    )


@app.schedule(
    schedule="0 5 0 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def applications_sync_async(timer: func.TimerRequest) -> None:
    """Scheduled async service principal synchronization function"""
    logging.info("🚀 ASYNC SERVICE PRINCIPALS SYNC FUNCTION STARTED")
    
    if timer.past_due:
        logging.warning("Async service principal sync timer is past due!")

    from sync.service_principal_sync import sync_service_principals_async_wrapper
    
    tenants = get_tenants()
    logging.info(f"📊 Processing {len(tenants)} tenant(s) with async workers")
    
    # Use async processing for better performance
    from datetime import datetime
    start_time = datetime.now()
    results = sync_service_principals_async_wrapper(tenants, max_workers=5)
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Log summary
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    total_synced = sum(r.get('service_principals_synced', 0) for r in successful)
    
    logging.info(f"⚡ Async parallel sync completed in {total_time:.2f}s: {len(successful)} tenants successful, {len(failed)} failed, {total_synced} total SPs synced")
    
    # Log any failures
    for result in failed:
        logging.error(f"❌ {result['tenant_name']}: {result['error']}")
    
    logging.info(f"🏁 ASYNC SERVICE PRINCIPALS SYNC COMPLETED - Success: {len(successful)}, Failed: {len(failed)}, Total SPs: {total_synced}")


@app.schedule(
    schedule="0 30 8 * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def service_principal_analytics(timer: func.TimerRequest) -> None:
    """Scheduled service principal analytics function"""
    logging.info("📊 SERVICE PRINCIPAL ANALYTICS FUNCTION STARTED")
    
    if timer.past_due:
        logging.warning("Service principal analytics timer is past due!")

    try:
        # Determine tenant mode by checking how many tenants we have
        tenants = get_tenants()
        tenant_mode = "single" if len(tenants) == 1 and tenants[0]['tenant_id'] == "3aae0fb1-276f-42f8-8e4d-36ca10cbb779" else "multi"
        
        logging.info(f"📈 Analyzing service principals in {tenant_mode.upper()} tenant mode")
        
        # Perform analytics
        analytics_result = analyze_service_principals(tenant_mode)
        
        if analytics_result['status'] == 'success':
            # Format and log the summary
            summary = format_analytics_summary(analytics_result)
            logging.info(summary)
            
            # Log individual metrics for monitoring
            data = analytics_result
            mode_info = f" in {data.get('tenant_mode', 'unknown').upper()} mode"
            logging.info(f"📊 Analytics completed{mode_info}: {data['total_sps']} SPs analyzed")
            
            if data['expired_sps'] > 0:
                logging.warning(f"🚨 Security Alert: {data['expired_sps']} service principals have expired credentials!")
            
            if data['sps_no_credentials'] > 0:
                logging.warning(f"🚨 Security Alert: {data['sps_no_credentials']} service principals have no credentials!")
                
            if data['disabled_sps'] > 0:
                logging.info(f"ℹ️ Info: {data['disabled_sps']} service principals are disabled")
                
            # Log credential analysis insights
            logging.info(f"🔐 Credential Analysis: {data['apps_with_credentials']} apps have credentials")
            
            logging.info(f"🏁 SERVICE PRINCIPAL ANALYTICS COMPLETED - {data['total_sps']} SPs analyzed")
            
        else:
            logging.error(f"❌ Analytics failed: {analytics_result['error']}")
            
    except Exception as e:
        logging.error(f"❌ Service principal analytics failed: {str(e)}")


@app.route(route="analytics/serviceprincipals", methods=["GET"])
def service_principal_analytics_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint for service principal analytics"""
    logging.info("🌐 SERVICE PRINCIPAL ANALYTICS HTTP ENDPOINT TRIGGERED")
    
    try:
        # Determine tenant mode by checking how many tenants we have
        tenants = get_tenants()
        tenant_mode = "single" if len(tenants) == 1 and tenants[0]['tenant_id'] == "3aae0fb1-276f-42f8-8e4d-36ca10cbb779" else "multi"
        
        analytics_result = analyze_service_principals(tenant_mode)
        
        if analytics_result['status'] == 'success':
            summary = format_analytics_summary(analytics_result)
            logging.info(f"🏁 SERVICE PRINCIPAL ANALYTICS HTTP COMPLETED - {analytics_result['total_sps']} SPs analyzed")
            return func.HttpResponse(summary, status_code=200, mimetype="text/plain")
        else:
            logging.error(f"❌ Analytics HTTP failed: {analytics_result['error']}")
            return func.HttpResponse(f"Analytics Error: {analytics_result['error']}", status_code=500)
            
    except Exception as e:
        logging.error(f"❌ Service principal analytics HTTP failed: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
