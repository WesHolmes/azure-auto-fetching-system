import azure.functions as func
from dotenv import load_dotenv

from functions.automox.http import (
    http_amx_devices_list,
    http_amx_devices_stats,
    http_amx_devices_sync,
    http_amx_orgs_list,
    http_amx_orgs_stats,
    http_amx_orgs_sync,
)
from functions.automox.timer import timer_amx_devices_sync, timer_amx_org_sync
from functions.backup_radar.http import http_backup_radar_health, http_backup_radar_status, http_backup_radar_sync
from functions.backup_radar.timer import timer_backup_radar_sync
from functions.devices.http import get_devices, http_azure_devices_sync, http_devices_sync
from functions.devices.timer import timer_devices_sync
from functions.groups.http import get_groups, http_group_sync
from functions.groups.timer import timer_groups_sync
from functions.licenses.http import get_licenses, http_licenses_sync, http_subscription_sync
from functions.licenses.timer import timer_licenses_sync, timer_subscriptions_sync
from functions.reports.timer import generate_report_now, generate_user_report
from functions.roles.http import get_roles, http_sync_roles
from functions.roles.timer import timer_roles_sync
from functions.users.http import (
    bulk_disable_users,
    create_user,
    delete_user,
    disable_user,
    edit_user,
    get_user,
    get_users,
    http_users_sync,
    reset_user_password,
)
from functions.users.timer import timer_tenants_sync


# Load environment variables first
load_dotenv()
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# =============================================================================
# TIMER TRIGGERS (Scheduled Functions)
# =============================================================================

# User sync - every minute at second 0
app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_tenants_sync)

# License sync - every minute at second 15
app.timer_trigger(schedule="15 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_licenses_sync)

# Role sync - every 2 minutes at second 0
app.timer_trigger(schedule="0 */2 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_roles_sync)

# Group sync - every minute at second 30
app.timer_trigger(schedule="30 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_groups_sync)

# # Subscription sync - every minute at second 45
app.timer_trigger(schedule="45 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_subscriptions_sync)

# Device sync - every 6 hours at minute 0
app.timer_trigger(schedule="0 0 */6 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_devices_sync)

# Backup Radar sync - daily at 2 AM
app.timer_trigger(schedule="0 0 2 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_backup_radar_sync)

# Automox organizations sync - daily at 3 AM
app.timer_trigger(schedule="0 0 3 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_amx_org_sync)

# Automox devices sync - daily at 4 AM
app.timer_trigger(schedule="0 0 4 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(timer_amx_devices_sync)

# # # License analysis - every hour at minute 25
# app.timer_trigger(schedule="0 25 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(get_licenses_analysis)

# # # Role analysis - every hour at minute 20
# app.timer_trigger(schedule="0 20 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(get_roles_analysis)

# # # Group analysis - every hour at minute 15
# app.timer_trigger(schedule="0 15 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(get_groups_analysis)

# Daily report generation - every day at 6 AM
app.timer_trigger(schedule="0 0 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)(generate_user_report)

# =============================================================================
# HTTP TRIGGERS (API Endpoints)
# =============================================================================

# Manual Sync Endpoints
app.route(route="sync/usersV2", methods=["POST"])(http_users_sync)

app.route(route="sync/licenses", methods=["POST"])(http_licenses_sync)

app.route(route="sync/roles", methods=["POST"])(http_sync_roles)

app.route(route="sync/groups", methods=["POST"])(http_group_sync)

app.route(route="sync/subscriptions", methods=["POST"])(http_subscription_sync)

app.route(route="sync/devices", methods=["POST"])(http_devices_sync)
app.route(route="sync/azure-devices", methods=["POST"])(http_azure_devices_sync)

app.route(route="sync/backup-radar", methods=["POST"])(http_backup_radar_sync)
app.route(route="backup-radar/status", methods=["GET"])(http_backup_radar_status)
app.route(route="backup-radar/health", methods=["GET"])(http_backup_radar_health)

# Automox Endpoints
app.route(route="sync/amx/orgs", methods=["POST"])(http_amx_orgs_sync)
app.route(route="amx/orgs", methods=["GET"])(http_amx_orgs_list)
app.route(route="amx/orgs/stats", methods=["GET"])(http_amx_orgs_stats)

# Automox Device Endpoints
app.route(route="sync/amx/devices", methods=["POST"])(http_amx_devices_sync)
app.route(route="amx/devices", methods=["GET"])(http_amx_devices_list)
app.route(route="amx/devices/stats", methods=["GET"])(http_amx_devices_stats)

# User Management Endpoints
app.route(route="tenant/users/{user_id}", methods=["GET"])(get_user)

app.route(route="tenant/users", methods=["GET"])(get_users)

app.route(route="tenant/users/edit", methods=["PATCH"])(edit_user)

app.route(route="users/{user_id}/disable", methods=["PATCH"])(disable_user)

app.route(route="users/{user_id}/reset-password", methods=["POST"])(reset_user_password)

app.route(route="tenant/users/create", methods=["POST"])(create_user)

app.route(route="users/{user_id}/delete", methods=["DELETE"])(delete_user)

app.route(route="users/bulk-disable", methods=["POST"])(bulk_disable_users)

# Tenant Data Endpoints
app.route(route="tenant/licenses", methods=["GET"])(get_licenses)

app.route(route="tenant/roles", methods=["GET"])(get_roles)

app.route(route="tenant/groups", methods=["GET"])(get_groups)

app.route(route="tenant/devices", methods=["GET"])(get_devices)


# Reporting Endpoints
app.route(route="generate-report-now", methods=["GET", "POST"])(generate_report_now)
