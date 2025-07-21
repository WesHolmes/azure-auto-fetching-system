import logging
import azure.functions as func
from core.tenant_manager import get_tenants
from sync.user_sync import sync_users
from sync.service_principal_sync import sync_service_principals

app = func.FunctionApp()


@app.schedule(
    schedule="0 0 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False
)
def users_sync(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("User sync timer is past due!")

    tenants = get_tenants()
    for tenant in tenants:
        try:
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['users_synced']} users synced"
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
