import logging
import azure.functions as func
import asyncio
from datetime import datetime, timezone, timedelta
from api.tenant_manager import get_tenants
from sync.user_sync import sync_users
from sync.service_principal_sync import sync_service_principals
from sync.backup_sync import sync_backups

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
            # this is the main data pipeline fxn
            result = sync_users(tenant["tenant_id"], tenant["name"])
            if result["status"] == "success":
                logging.info(
                    f"✓ {tenant['name']}: {result['users_synced']} users synced"
                )

            # add any analysis fxn based on the data populated in the SQL table
            # result = calculate_inactive_users()
            # result = calculate_mfa_compliance()

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





@app.timer_trigger(
    schedule="0 30 2,14 * * *",
    arg_name="myTimer",
    use_monitor=False,
    run_on_startup=False,
)
def backup_sync(myTimer: func.TimerRequest) -> None:
    """Azure function for syncing backup data on a schedule"""
    try:
        logging.info("Starting backup sync function")

        # Calculate target date (2 days ago)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        target_date = today - timedelta(days=2)

        # Run async sync function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(sync_backups(target_date))
        loop.close()

        # Log results
        if result['status'] == 'success':
            logging.info(
                f"Backup sync completed successfully: "
                f"{result['policies_fetched']} policies, "
                f"{result['records_processed']} records processed"
            )
        else:
            logging.error(f"Backup sync failed: {result.get('error', 'Unknown error')}")
            raise Exception(result.get('error', 'Backup sync failed'))

    except Exception as e:
        logging.error(f"Error during backup sync: {str(e)}", exc_info=True)
        raise
    finally:
        logging.info("Backup sync function finished")
