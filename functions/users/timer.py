import logging

import azure.functions as func

from shared.error_reporting import categorize_sync_errors
from shared.graph_client import get_tenants
from shared.utils import clean_error_message

from .helpers import calculate_inactive_users, calculate_mfa_compliance, sync_users


# TIMER FUNCTIONS
def timer_tenants_sync(timer: func.TimerRequest) -> None:
    """Timer trigger for user sync across all tenants"""
    if timer.past_due:
        logging.warning("User sync V2 timer is past due!")

    tenants = get_tenants()
    tenants.reverse()  # Process in reverse order
    results = []

    for tenant in tenants:
        try:
            result = sync_users(tenant["tenant_id"], tenant["display_name"])
            if result["status"] == "success":
                logging.info(f"✓ V2 {tenant['display_name']}: {result['users_synced']} users synced")
                results.append(
                    {
                        "status": "completed",
                        "tenant_id": tenant["tenant_id"],
                        "users_synced": result["users_synced"],
                        "user_licenses_synced": result.get("user_licenses_replaced", 0),
                    }
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
                logging.error(f"✗ V2 {tenant['display_name']}: {result['error']}")
                results.append(
                    {
                        "status": "error",
                        "tenant_id": tenant["tenant_id"],
                        "error": result.get("error", "Unknown error"),
                    }
                )
        except Exception as e:
            logging.error(clean_error_message(str(e), tenant["display_name"]))
            results.append({"status": "error", "tenant_id": tenant["tenant_id"], "error": str(e)})

    # Use centralized error reporting
    failed_count = len([r for r in results if r["status"] == "error"])
    if failed_count > 0:
        categorize_sync_errors(results, "User V2")
