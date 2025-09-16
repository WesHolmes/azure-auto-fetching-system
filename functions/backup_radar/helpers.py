from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import logging
import re
from typing import Any

from db.db_client import upsert_many
from shared.backup_radar_api import get_backup_retired, get_backups
from shared.error_reporting import categorize_sync_errors
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def get_tenant_id_from_company_name(company_name: str, tenants: list[dict[str, Any]]) -> str:
    """
    Map company name to tenant ID using intelligent matching strategies.
    Returns the first matching tenant ID or 'unknown' if no match found.
    """
    # Input validation
    if not company_name or not isinstance(company_name, str) or not company_name.strip():
        return "unknown"

    company_lower = company_name.lower()

    # Pre-process company name for word matching
    business_suffixes = {
        "inc",
        "llc",
        "corp",
        "corporation",
        "company",
        "co",
        "ltd",
        "limited",
        "the",
        "of",
        "and",
        "&",
        "associates",
        "assoc",
        "group",
        "partners",
        "partnership",
        "llp",
        "pc",
        "p.c.",
        "apc",
        "a.p.c.",
        "dba",
        "d.b.a.",
    }

    cleaned_words = re.sub(r"[^\w\s]", " ", company_lower).split()
    company_words = {word for word in cleaned_words if word not in business_suffixes}

    # Single pass through tenants with multiple matching strategies
    best_match = None
    best_score = 0.0

    for tenant in tenants:
        display_name = tenant.get("display_name") or ""
        primary_domain = tenant.get("primary_domain") or ""

        # Skip if both fields are empty
        if not display_name and not primary_domain:
            continue

        # Strategy 1: Exact matches (highest priority)
        if display_name == company_name or primary_domain == company_name:
            return tenant["tenant_id"]

        # Strategy 2: Case-insensitive exact matches
        if (isinstance(display_name, str) and display_name.lower() == company_lower) or (
            isinstance(primary_domain, str) and primary_domain.lower() == company_lower
        ):
            return tenant["tenant_id"]

        # Strategy 3: Partial matches (substring contains)
        if (isinstance(display_name, str) and company_lower in display_name.lower()) or (
            isinstance(primary_domain, str) and company_lower in primary_domain.lower()
        ):
            return tenant["tenant_id"]

        # Strategy 4: Word-based matching (only if we have meaningful words)
        if company_words:
            score = 0.0

            # Check display name
            if isinstance(display_name, str):
                cleaned_tenant_words = re.sub(r"[^\w\s]", " ", display_name.lower()).split()
                tenant_words = {word for word in cleaned_tenant_words if word not in business_suffixes}
                if tenant_words:
                    intersection = len(company_words.intersection(tenant_words))
                    union = len(company_words.union(tenant_words))
                    score = max(score, intersection / union if union > 0 else 0.0)

            # Check primary domain
            if isinstance(primary_domain, str):
                cleaned_domain_words = re.sub(r"[^\w\s]", " ", primary_domain.lower()).split()
                domain_words = {word for word in cleaned_domain_words if word not in business_suffixes}
                if domain_words:
                    intersection = len(company_words.intersection(domain_words))
                    union = len(company_words.union(domain_words))
                    score = max(score, intersection / union if union > 0 else 0.0)

            # Update best match if score is good enough
            if score > best_score and score >= 0.5:  # 50% word overlap threshold
                best_score = score
                best_match = tenant["tenant_id"]

        # Strategy 5: Character-based fuzzy matching (fallback)
        if not best_match:
            score = 0.0

            if isinstance(display_name, str):
                company_chars = set(company_lower)
                display_chars = set(display_name.lower())
                intersection = len(company_chars.intersection(display_chars))
                union = len(company_chars.union(display_chars))
                score = max(score, intersection / union if union > 0 else 0.0)

            if isinstance(primary_domain, str):
                domain_chars = set(primary_domain.lower())
                intersection = len(company_chars.intersection(domain_chars))
                union = len(company_chars.union(domain_chars))
                score = max(score, intersection / union if union > 0 else 0.0)

            if score > best_score and score >= 0.7:  # 70% character similarity threshold
                best_score = score
                best_match = tenant["tenant_id"]

    return best_match or "unknown"


def map_backup_data_unified(backup_item: dict[str, Any], tenant_id: str, retired_backup_ids: set[int]) -> dict[str, Any]:
    """
    Map Backup Radar API data to database schema with unified retired status lookup.
    Accepts a list of backup devices (all and retired) and performs a lookup on whether
    the backup id exists in the retired list - if so, sets is_retired to 1 (True).
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

    # Determine backup_datetime from lastResult (actual backup time)
    backup_datetime = None
    last_result = backup_item.get("lastResult")
    if last_result:
        try:
            # Parse the date string (format: "2025-09-10T13:32:41")
            # Add timezone info if not present
            if "Z" in last_result:
                backup_datetime = datetime.fromisoformat(last_result.replace("Z", "+00:00")).isoformat()
            else:
                backup_datetime = datetime.fromisoformat(last_result).replace(tzinfo=UTC).isoformat()
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"Failed to parse lastResult date '{last_result}' for backup_datetime: {e}")
            # Fallback to current time if parsing fails
            backup_datetime = datetime.now(UTC).isoformat()
    else:
        # Fallback to current time if no lastResult
        backup_datetime = datetime.now(UTC).isoformat()

    # Determine if this backup is retired by looking up its ID in the retired set
    backup_id = backup_item.get("backupId")
    is_retired = backup_id in retired_backup_ids if backup_id is not None else False

    # Map the data according to the schema
    mapped_data = {
        "tenant_id": tenant_id,
        "backup_id": backup_id,
        "backup_datetime": backup_datetime,
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


def process_backup_batch_unified(
    all_backup_items: list[dict[str, Any]], tenants: list[dict[str, Any]], retired_backup_ids: set[int]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Process all backup items (active and retired) concurrently with unified retired status lookup.
    Returns (mapped_data, errors).
    """
    mapped_data = []
    errors = []

    def process_single_backup(backup_item):
        """Process a single backup item with unified retired status lookup"""
        try:
            company_name = backup_item.get("companyName")
            tenant_id = get_tenant_id_from_company_name(company_name, tenants)
            mapped_item = map_backup_data_unified(backup_item, tenant_id, retired_backup_ids)
            return mapped_item, None
        except Exception as e:
            error_msg = f"Error processing backup {backup_item.get('backupId', 'unknown')}: {clean_error_message(str(e))}"
            return None, {
                "type": "error",
                "message": error_msg,
                "backup_id": backup_item.get("backupId"),
                "company_name": backup_item.get("companyName"),
            }

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_backup = {executor.submit(process_single_backup, backup_item): backup_item for backup_item in all_backup_items}

        processed_count = 0
        total_backups = len(all_backup_items)

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
                logger.info(f"Processed {processed_count}/{total_backups} backup records...")

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

        # Create set of retired backup IDs for efficient lookup
        retired_backup_ids = {backup.get("backupId") for backup in retired_backups if backup.get("backupId") is not None}
        logger.info(f"Found {len(retired_backup_ids)} retired backup IDs for lookup")

        # Combine all backups (active + retired) for unified processing
        all_backups = active_backups + retired_backups
        logger.info(f"Processing {len(all_backups)} total backup records (active + retired) with unified retired status lookup...")

        if all_backups:
            # Process all backups concurrently with unified retired status lookup
            all_mapped_data, all_errors = process_backup_batch_unified(all_backups, tenants, retired_backup_ids)
            sync_results.extend(all_errors)

            if all_mapped_data:
                upsert_many("backup_radar", all_mapped_data)

                # Count active vs retired from the processed data
                total_active_backups = sum(1 for item in all_mapped_data if item.get("is_retired") == 0)
                total_retired_backups = sum(1 for item in all_mapped_data if item.get("is_retired") == 1)

                logger.info(f"Successfully synced {total_active_backups} active and {total_retired_backups} retired backup records")

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

        # Create set of retired backup IDs for efficient lookup
        retired_backup_ids = {backup.get("backupId") for backup in retired_backups if backup.get("backupId") is not None}

        # Filter backups for this tenant's company
        company_name = tenant.get("display_name")
        tenant_active_backups = [b for b in active_backups if b.get("companyName") == company_name]
        tenant_retired_backups = [b for b in retired_backups if b.get("companyName") == company_name]

        # Combine all tenant backups for unified processing
        all_tenant_backups = tenant_active_backups + tenant_retired_backups
        total_active = 0
        total_retired = 0

        # Process all tenant backups with unified retired status lookup
        if all_tenant_backups:
            all_mapped_data = []
            for backup_item in all_tenant_backups:
                mapped_data = map_backup_data_unified(backup_item, tenant_id, retired_backup_ids)
                all_mapped_data.append(mapped_data)

            if all_mapped_data:
                upsert_many("backup_radar", all_mapped_data)

                # Count active vs retired from the processed data
                total_active = sum(1 for item in all_mapped_data if item.get("is_retired") == 0)
                total_retired = sum(1 for item in all_mapped_data if item.get("is_retired") == 1)

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
