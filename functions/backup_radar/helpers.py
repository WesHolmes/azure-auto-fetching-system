from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import logging
from typing import Any

from db.db_client import upsert_many
from shared.backup_radar_api import get_backup_retired, get_backups
from shared.error_reporting import categorize_sync_errors
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def get_tenant_id_from_company_name(company_name: str, tenants: list[dict[str, Any]]) -> str:
    """
    Map company name to tenant ID by matching display_name or primary_domain.
    Returns the first matching tenant ID or a default if no match found.
    """
    # Robust null/type checking
    if company_name is None or not isinstance(company_name, str) or not company_name.strip():
        return "unknown"

    # Try exact match first
    for tenant in tenants:
        if tenant.get("display_name") == company_name:
            return tenant["tenant_id"]
        if tenant.get("primary_domain") == company_name:
            return tenant["tenant_id"]

    # Try case-insensitive match
    try:
        company_lower = company_name.lower()
        for tenant in tenants:
            display_name = tenant.get("display_name") or ""
            primary_domain = tenant.get("primary_domain") or ""

            if isinstance(display_name, str) and display_name.lower() == company_lower:
                return tenant["tenant_id"]
            if isinstance(primary_domain, str) and primary_domain.lower() == company_lower:
                return tenant["tenant_id"]

        # Try partial match (contains)
        for tenant in tenants:
            display_name = tenant.get("display_name") or ""
            primary_domain = tenant.get("primary_domain") or ""

            if isinstance(display_name, str) and isinstance(primary_domain, str):
                display_lower = display_name.lower()
                domain_lower = primary_domain.lower()
                if company_lower in display_lower or company_lower in domain_lower:
                    return tenant["tenant_id"]
    except (AttributeError, TypeError) as e:
        # If company_name somehow became None or invalid after the initial check
        logger.warning(f"Unexpected error processing company name '{company_name}': {e}")
        return "unknown"

    # No match found - this is normal for many company names
    return "unknown"


# accepts to list of backup devices (all and retired) and performs a lookup on whether the backup id exists in the retired list if so, set is_retired to 1 (True)
def map_backup_data(backup_item: dict[str, Any], tenant_id: str, is_retired: bool = False) -> dict[str, Any]:
    """
    Map Backup Radar API data to database schema.
    """
    # Extract nested values safely
    status_name = backup_item.get("status", {}).get("name", "") if backup_item.get("status") else ""

    # Calculate days since last good result and last result from date fields
    days_since_last_good_result = None
    days_since_last_result = None
    days_in_status = backup_item.get("daysInStatus")

    # Convert days_in_status to decimal if present
    if days_in_status is not None:
        try:
            days_in_status = float(days_in_status)
        except (ValueError, TypeError):
            days_in_status = None

    # Calculate days since last good result (from lastSuccess date)
    last_success = backup_item.get("lastSuccess")
    if last_success:
        try:
            # Parse the date string (format: "2025-09-03T13:36:40")
            # Add timezone info if not present
            if "Z" in last_success:
                success_date = datetime.fromisoformat(last_success.replace("Z", "+00:00"))
            else:
                success_date = datetime.fromisoformat(last_success)
                # Assume UTC if no timezone info
                success_date = success_date.replace(tzinfo=UTC)

            days_since_last_good_result = (datetime.now(UTC) - success_date).total_seconds() / 86400
            days_since_last_good_result = round(days_since_last_good_result, 2)
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"Failed to parse lastSuccess date '{last_success}': {e}")
            days_since_last_good_result = None

    # Calculate days since last result (from lastResult date)
    last_result = backup_item.get("lastResult")
    if last_result:
        try:
            # Parse the date string (format: "2025-09-03T13:36:40")
            # Add timezone info if not present
            if "Z" in last_result:
                result_date = datetime.fromisoformat(last_result.replace("Z", "+00:00"))
            else:
                result_date = datetime.fromisoformat(last_result)
                # Assume UTC if no timezone info
                result_date = result_date.replace(tzinfo=UTC)

            days_since_last_result = (datetime.now(UTC) - result_date).total_seconds() / 86400
            days_since_last_result = round(days_since_last_result, 2)
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"Failed to parse lastResult date '{last_result}': {e}")
            days_since_last_result = None

    # Map the data according to the schema
    mapped_data = {
        "tenant_id": tenant_id,
        "backup_id": backup_item.get("backupId"),
        "backup_datetime": datetime.now(UTC).isoformat(),
        "company_name": backup_item.get("companyName"),
        "device_name": backup_item.get("deviceName"),
        "device_type": backup_item.get("deviceType"),
        "days_since_last_good_result": days_since_last_good_result,
        "days_since_last_result": days_since_last_result,
        "days_in_status": days_in_status,
        "is_verified": 1 if backup_item.get("isVerified") else 0,
        "backup_result": status_name,
        "backup_policy_name": backup_item.get("jobName"),
        "is_retired": 1 if is_retired else 0,
        "updated_at": datetime.now(UTC).isoformat(),
    }

    return mapped_data


def process_backup_batch(
    backup_items: list[dict[str, Any]], tenants: list[dict[str, Any]], is_retired: bool = False
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Process a batch of backup items concurrently.
    Returns (mapped_data, errors).
    """
    mapped_data = []
    errors = []

    def process_single_backup(backup_item):
        """Process a single backup item"""
        try:
            company_name = backup_item.get("companyName")
            tenant_id = get_tenant_id_from_company_name(company_name, tenants)
            mapped_item = map_backup_data(backup_item, tenant_id, is_retired)
            return mapped_item, None
        except Exception as e:
            error_msg = f"Error processing {'retired' if is_retired else 'active'} backup {backup_item.get('backupId', 'unknown')}: {clean_error_message(str(e))}"
            return None, {
                "type": "error",
                "message": error_msg,
                "backup_id": backup_item.get("backupId"),
                "company_name": backup_item.get("companyName"),
            }

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_backup = {executor.submit(process_single_backup, backup_item): backup_item for backup_item in backup_items}

        processed_count = 0
        total_backups = len(backup_items)

        for future in as_completed(future_to_backup):
            backup_item = future_to_backup[future]
            try:
                mapped_item, error = future.result()
                if mapped_item:
                    mapped_data.append(mapped_item)
                if error:
                    errors.append(error)
            except Exception as e:
                logger.warning(f"Failed to process backup {backup_item.get('backupId', 'unknown')}: {str(e)}")
                continue

            processed_count += 1
            if processed_count % 50 == 0 or processed_count == total_backups:
                backup_type = "retired" if is_retired else "active"
                logger.info(f"Processed {processed_count}/{total_backups} {backup_type} backup records...")

    return mapped_data, errors


def sync_backup_radar_data(tenants: list[dict[str, Any]], days_back: int = 7) -> dict[str, Any]:
    """
    Sync Backup Radar data for all tenants.
    Fetches both active and retired backups and stores them in the database.
    """
    logger.info("Starting Backup Radar sync for all tenants")
    start_time = datetime.now()

    total_active_backups = 0
    total_retired_backups = 0
    sync_results = []

    try:
        # Fetch active backups
        logger.info("Fetching active backups from Backup Radar API")
        active_backups_response = get_backups(days_back)
        active_backups = active_backups_response.get("Results", [])

        # Fetch retired backups
        logger.info("Fetching retired backups from Backup Radar API")
        retired_backups_response = get_backup_retired()
        retired_backups = retired_backups_response.get("Results", [])

        # Process active backups concurrently
        if active_backups:
            logger.info(f"Processing {len(active_backups)} active backup records concurrently...")
            active_mapped_data, active_errors = process_backup_batch(active_backups, tenants, is_retired=False)
            sync_results.extend(active_errors)

            if active_mapped_data:
                upsert_many("backup_radar", active_mapped_data)
                total_active_backups = len(active_mapped_data)
                logger.info(f"Successfully synced {total_active_backups} active backup records")

        # Process retired backups concurrently
        if retired_backups:
            logger.info(f"Processing {len(retired_backups)} retired backup records concurrently...")
            retired_mapped_data, retired_errors = process_backup_batch(retired_backups, tenants, is_retired=True)
            sync_results.extend(retired_errors)

            if retired_mapped_data:
                upsert_many("backup_radar", retired_mapped_data)
                total_retired_backups = len(retired_mapped_data)
                logger.info(f"Successfully synced {total_retired_backups} retired backup records")

        # Calculate summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            "total_active_backups": total_active_backups,
            "total_retired_backups": total_retired_backups,
            "total_backups": total_active_backups + total_retired_backups,
            "duration_seconds": duration,
            "sync_timestamp": end_time.isoformat(),
            "status": "success",
        }

        # Categorize any errors
        if sync_results:
            error_summary = categorize_sync_errors(sync_results, "Backup Radar", log_output=False)
            summary["errors"] = error_summary

        logger.info(
            f"Backup Radar sync completed: {total_active_backups} active, {total_retired_backups} retired backups in {duration:.2f}s"
        )
        return summary

    except Exception as e:
        error_msg = f"Backup Radar sync failed: {clean_error_message(str(e))}"
        logger.error(error_msg)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "total_active_backups": total_active_backups,
            "total_retired_backups": total_retired_backups,
            "total_backups": total_active_backups + total_retired_backups,
            "duration_seconds": duration,
            "sync_timestamp": end_time.isoformat(),
            "status": "error",
            "error": error_msg,
        }


def sync_backup_radar_for_tenant(tenant_id: str, tenants: list[dict[str, Any]], days_back: int = 7) -> dict[str, Any]:
    """
    Sync Backup Radar data for a specific tenant.
    """
    logger.info(f"Starting Backup Radar sync for tenant: {tenant_id}")
    start_time = datetime.now()

    # Find the specific tenant
    tenant = next((t for t in tenants if t["tenant_id"] == tenant_id), None)
    if not tenant:
        return {"status": "error", "error": f"Tenant {tenant_id} not found", "total_backups": 0, "duration_seconds": 0}

    try:
        # Fetch active backups
        active_backups_response = get_backups(days_back)
        active_backups = active_backups_response.get("Results", [])

        # Fetch retired backups
        retired_backups_response = get_backup_retired()
        retired_backups = retired_backups_response.get("Results", [])

        # Filter backups for this tenant's company
        company_name = tenant.get("display_name")
        tenant_active_backups = [b for b in active_backups if b.get("companyName") == company_name]
        tenant_retired_backups = [b for b in retired_backups if b.get("companyName") == company_name]

        total_active = 0
        total_retired = 0

        # Process active backups for this tenant
        if tenant_active_backups:
            active_mapped_data = []
            for backup_item in tenant_active_backups:
                mapped_data = map_backup_data(backup_item, tenant_id, is_retired=False)
                active_mapped_data.append(mapped_data)

            if active_mapped_data:
                upsert_many("backup_radar", active_mapped_data)
                total_active = len(active_mapped_data)

        # Process retired backups for this tenant
        if tenant_retired_backups:
            retired_mapped_data = []
            for backup_item in tenant_retired_backups:
                mapped_data = map_backup_data(backup_item, tenant_id, is_retired=True)
                retired_mapped_data.append(mapped_data)

            if retired_mapped_data:
                upsert_many("backup_radar", retired_mapped_data)
                total_retired = len(retired_mapped_data)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(
            f"Backup Radar sync for tenant {tenant_id} completed: {total_active} active, {total_retired} retired backups in {duration:.2f}s"
        )

        return {
            "tenant_id": tenant_id,
            "total_active_backups": total_active,
            "total_retired_backups": total_retired,
            "total_backups": total_active + total_retired,
            "duration_seconds": duration,
            "sync_timestamp": end_time.isoformat(),
            "status": "success",
        }

    except Exception as e:
        error_msg = f"Backup Radar sync failed for tenant {tenant_id}: {clean_error_message(str(e))}"
        logger.error(error_msg)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "tenant_id": tenant_id,
            "total_active_backups": 0,
            "total_retired_backups": 0,
            "total_backups": 0,
            "duration_seconds": duration,
            "sync_timestamp": end_time.isoformat(),
            "status": "error",
            "error": error_msg,
        }
