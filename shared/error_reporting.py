from datetime import datetime
import logging
from typing import Any

from db.db_client import query


logger = logging.getLogger(__name__)

# Global storage for recent sync results (in-memory)
# In a production environment, this would be stored in a database
_recent_sync_results = {
    "user_sync": [],
    "license_sync": [],
    "role_sync": [],
    "service_principal_sync": [],
}


def categorize_sync_errors(results: list[dict], sync_type: str = "sync", log_output: bool = True) -> dict[str, Any]:
    """
    Centralized error categorization for all sync operations

    Args:
        results: List of sync results from any sync function
        sync_type: Type of sync for logging (e.g., "User", "License", "Role")

    Returns:
        Dictionary with categorized errors and summary statistics
    """

    # Initialize error categories
    auth_errors = []  # 401, Authorization_IdentityNotFound
    permission_errors = []  # 403, Forbidden
    service_errors = []  # 503, Service Unavailable
    timeout_errors = []  # Timeout, functionTimeout
    other_errors = []  # Everything else

    # Process results
    successful = [r for r in results if r.get("status") == "completed"]
    failed = [r for r in results if r.get("status") == "error"]

    # Categorize each failed result
    for result in failed:
        tenant_id = result.get("tenant_id", "unknown")
        error_msg = str(result.get("error", "")).lower()

        if "401" in error_msg or "authorization_identitynotfound" in error_msg or "unauthorized" in error_msg:
            auth_errors.append({"tenant_id": tenant_id, "error": result.get("error", "")})
        elif "403" in error_msg or "forbidden" in error_msg or "insufficient privileges" in error_msg:
            permission_errors.append({"tenant_id": tenant_id, "error": result.get("error", "")})
        elif "503" in error_msg or "service unavailable" in error_msg or "serviceunavailable" in error_msg:
            service_errors.append({"tenant_id": tenant_id, "error": result.get("error", "")})
        elif "timeout" in error_msg or "functiontimeout" in error_msg:
            timeout_errors.append({"tenant_id": tenant_id, "error": result.get("error", "")})
        else:
            other_errors.append({"tenant_id": tenant_id, "error": result.get("error", "")})

    # Store results globally for later retrieval
    global _recent_sync_results
    _recent_sync_results[f"{sync_type.lower()}_sync"] = results

    # Create summary
    error_summary = {
        "sync_type": sync_type,
        "timestamp": datetime.now().isoformat(),
        "total_tenants": len(results),
        "successful_tenants": len(successful),
        "failed_tenants": len(failed),
        "error_categories": {
            "authentication_errors": len(auth_errors),
            "permission_errors": len(permission_errors),
            "service_errors": len(service_errors),
            "timeout_errors": len(timeout_errors),
            "other_errors": len(other_errors),
        },
        "details": {
            "auth_errors": auth_errors,
            "permission_errors": permission_errors,
            "service_errors": service_errors,
            "timeout_errors": timeout_errors,
            "other_errors": other_errors,
        },
    }

    # Log summary if requested
    if log_output and failed:
        logger.warning(f"{sync_type} sync errors summary:")
        logger.warning(f"  Total: {len(failed)}/{len(results)} tenants failed")
        logger.warning(f"  Auth errors: {len(auth_errors)}")
        logger.warning(f"  Permission errors: {len(permission_errors)}")
        logger.warning(f"  Service errors: {len(service_errors)}")
        logger.warning(f"  Timeout errors: {len(timeout_errors)}")
        logger.warning(f"  Other errors: {len(other_errors)}")

        # Log top 3 most common errors for each category
        if auth_errors:
            logger.warning(f"  Auth errors - tenants: {[e['tenant_id'] for e in auth_errors[:3]]}")
        if permission_errors:
            logger.warning(f"  Permission errors - tenants: {[e['tenant_id'] for e in permission_errors[:3]]}")

    return error_summary


def aggregate_recent_sync_errors() -> dict[str, Any]:
    """
    Aggregate recent sync errors from database queries for reporting

    Returns:
        Dictionary with recent sync status and tenant information
    """
    try:
        # Query recent successful syncs to determine which tenants are healthy
        successful_tenants_query = """
        SELECT DISTINCT tenant_id,
               MAX(last_updated) as last_sync
        FROM usersV2
        WHERE last_updated >= datetime('now', '-24 hours')
        GROUP BY tenant_id
        ORDER BY last_sync DESC
        """

        successful_tenants = query(successful_tenants_query)

        # Query basic tenant info (if available)
        tenant_info_query = """
        SELECT tenant_id, COUNT(*) as user_count
        FROM usersV2
        GROUP BY tenant_id
        """

        tenant_info = query(tenant_info_query)
        tenant_map = {t["tenant_id"]: t["user_count"] for t in tenant_info}

        # Enhance successful tenants with user count
        for tenant in successful_tenants:
            tenant["user_count"] = tenant_map.get(tenant["tenant_id"], 0)

        # Get recent error patterns from global storage
        recent_errors = {}
        for sync_type, results in _recent_sync_results.items():
            if results:  # Only include if we have recent results
                failed = [r for r in results if r.get("status") == "error"]
                if failed:
                    recent_errors[sync_type] = {"count": len(failed), "sample_errors": [r.get("error", "") for r in failed[:3]]}

        return {
            "successful_tenants": successful_tenants,
            "failed_count": len(tenant_info) - len(successful_tenants),
            "recent_sync_errors": recent_errors,
            "summary": {
                "total_tenants": len(tenant_info),
                "synced_in_24h": len(successful_tenants),
                "not_synced_in_24h": len(tenant_info) - len(successful_tenants),
            },
        }

    except Exception as e:
        logger.error(f"Error aggregating sync errors: {e}")
        return {
            "successful_tenants": [],
            "failed_count": 0,
            "recent_sync_errors": {},
            "summary": {"total_tenants": 0, "synced_in_24h": 0, "not_synced_in_24h": 0},
        }
