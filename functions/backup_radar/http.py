from datetime import datetime
import logging

import azure.functions as func

from db.db_client import query
from functions.backup_radar.helpers import sync_backup_radar_data, sync_backup_radar_for_tenant
from shared.graph_client import get_tenants
from shared.utils import clean_error_message, create_error_response, create_success_response


logger = logging.getLogger(__name__)


def http_backup_radar_sync(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint for manual Backup Radar sync.
    Supports syncing all tenants or a specific tenant.
    """
    try:
        logger.info("Starting manual Backup Radar sync via HTTP request")
        start_time = datetime.now()

        # Get tenant_id from query parameters if specified
        tenant_id = req.params.get("tenant_id")
        days_back = int(req.params.get("days_back", 7))

        if tenant_id:
            # Sync specific tenant
            logger.info(f"Syncing Backup Radar data for specific tenant: {tenant_id}")
            tenants = get_tenants()
            result = sync_backup_radar_for_tenant(tenant_id, tenants, days_back)

            if result["status"] == "success":
                response_data = {
                    "message": f"Backup Radar sync completed for tenant {tenant_id}",
                    "tenant_id": tenant_id,
                    "total_active_backups": result["total_active_backups"],
                    "total_retired_backups": result["total_retired_backups"],
                    "total_backups": result["total_backups"],
                    "duration_seconds": result["duration_seconds"],
                    "sync_timestamp": result["sync_timestamp"],
                }
                return create_success_response(response_data)
            else:
                return create_error_response(
                    f"Backup Radar sync failed for tenant {tenant_id}: {result.get('error', 'Unknown error')}", status_code=500
                )
        else:
            # Sync all tenants
            logger.info("Syncing Backup Radar data for all tenants")
            tenants = get_tenants()
            result = sync_backup_radar_data(tenants, days_back)

            if result["status"] == "success":
                response_data = {
                    "message": "Backup Radar sync completed for all tenants",
                    "total_active_backups": result["total_active_backups"],
                    "total_retired_backups": result["total_retired_backups"],
                    "total_backups": result["total_backups"],
                    "duration_seconds": result["duration_seconds"],
                    "sync_timestamp": result["sync_timestamp"],
                    "tenant_count": len(tenants),
                }

                # Include error summary if there were any errors
                if "errors" in result:
                    response_data["errors"] = result["errors"]

                return create_success_response(response_data)
            else:
                return create_error_response(f"Backup Radar sync failed: {result.get('error', 'Unknown error')}", status_code=500)

    except Exception as e:
        error_msg = f"Backup Radar HTTP sync failed: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return create_error_response(error_msg, status_code=500)


def http_backup_radar_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint to get Backup Radar sync status and recent data.
    """
    try:
        logger.info("Getting Backup Radar sync status")

        # Get query parameters
        tenant_id = req.params.get("tenant_id")
        limit = int(req.params.get("limit", 100))

        # Build query based on tenant filter
        if tenant_id:
            sql = """
                SELECT 
                    tenant_id,
                    company_name,
                    COUNT(*) as backup_count,
                    MAX(backup_datetime) as latest_backup,
                    SUM(CASE WHEN is_retired = 0 THEN 1 ELSE 0 END) as active_backups,
                    SUM(CASE WHEN is_retired = 1 THEN 1 ELSE 0 END) as retired_backups
                FROM backup_radar 
                WHERE tenant_id = ?
                GROUP BY tenant_id, company_name
                ORDER BY latest_backup DESC
                LIMIT ?
            """
            params = (tenant_id, limit)
        else:
            sql = """
                SELECT 
                    tenant_id,
                    company_name,
                    COUNT(*) as backup_count,
                    MAX(backup_datetime) as latest_backup,
                    SUM(CASE WHEN is_retired = 0 THEN 1 ELSE 0 END) as active_backups,
                    SUM(CASE WHEN is_retired = 1 THEN 1 ELSE 0 END) as retired_backups
                FROM backup_radar 
                GROUP BY tenant_id, company_name
                ORDER BY latest_backup DESC
                LIMIT ?
            """
            params = (limit,)

        results = query(sql, params)

        # Get total counts
        total_sql = "SELECT COUNT(*) as total FROM backup_radar"
        if tenant_id:
            total_sql += " WHERE tenant_id = ?"
            total_params = (tenant_id,)
        else:
            total_params = None

        total_result = query(total_sql, total_params)
        total_backups = total_result[0]["total"] if total_result else 0

        response_data = {
            "message": "Backup Radar status retrieved successfully",
            "total_backups": total_backups,
            "tenant_summaries": results,
            "query_timestamp": datetime.now().isoformat(),
        }

        if tenant_id:
            response_data["tenant_id"] = tenant_id

        return create_success_response(response_data)

    except Exception as e:
        error_msg = f"Failed to get Backup Radar status: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return create_error_response(error_msg, status_code=500)


def http_backup_radar_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint for Backup Radar health check.
    Tests API connectivity and returns basic status.
    """
    try:
        logger.info("Running Backup Radar health check")

        # Test API connectivity
        from shared.backup_radar_api import get_backup_overview

        try:
            overview = get_backup_overview()
            api_status = "healthy"
            api_message = "Backup Radar API is accessible"
        except Exception as api_error:
            api_status = "unhealthy"
            api_message = f"Backup Radar API error: {clean_error_message(str(api_error))}"

        # Check database connectivity
        try:
            db_result = query("SELECT COUNT(*) as count FROM backup_radar LIMIT 1")
            db_status = "healthy"
            db_message = f"Database accessible, {db_result[0]['count']} backup records"
        except Exception as db_error:
            db_status = "unhealthy"
            db_message = f"Database error: {clean_error_message(str(db_error))}"

        overall_status = "healthy" if api_status == "healthy" and db_status == "healthy" else "unhealthy"

        response_data = {
            "status": overall_status,
            "api": {"status": api_status, "message": api_message},
            "database": {"status": db_status, "message": db_message},
            "timestamp": datetime.now().isoformat(),
        }

        status_code = 200 if overall_status == "healthy" else 503
        return create_success_response(response_data, status_code=status_code)

    except Exception as e:
        error_msg = f"Backup Radar health check failed: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return create_error_response(error_msg, status_code=500)
