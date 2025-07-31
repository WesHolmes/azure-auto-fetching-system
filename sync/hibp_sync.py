import json
import logging
from datetime import datetime
from typing import Dict, Tuple

from core.graph_client import GraphClient
from core.hibp import HIBPClient
from sql.hibp_db import HIBPDB

logger = logging.getLogger(__name__)

# How often to check each user (in days)
CHECK_INTERVAL_DAYS = 7


def sync_hibp_breaches(tenant_id: str, tenant_name: str, db: HIBPDB = None) -> Dict:
    if tenant_id == "4cb9240e-2025-488f-97e7-7eb69335458a":  # United Talent
        logger.info(f"Skipping {tenant_name}.")
        return

    """Sync HIBP breaches for a single tenant."""
    hibp_client = HIBPClient()

    # Use provided db connection or create new one
    should_close_db = False
    if db is None:
        db = HIBPDB()
        should_close_db = True

    try:
        # Get active users from Graph API
        graph_client = GraphClient(tenant_id)
        users = graph_client.get(
            "/users",
            select=[
                "id",
                "userPrincipalName",
                "mail",
                "accountEnabled",
                "lastPasswordChangeDateTime",
                "signInActivity",
            ],
            filter="accountEnabled eq true",
            top=999,
        )

        # active_users = [
        #     user for user in users
        #     if user.get('signInActivity', {}).get('lastSignInDateTime') is not None
        # ]

        # logger.info(f"Processing {len(active_users)} active users for tenant {tenant_name}")

        # Check each user for breaches
        users_checked, breaches_found = upsert_user_breachs(
            tenant_id, tenant_name, db, hibp_client, users
        )

        return {
            "status": "success",
            "users_checked": users_checked,
            "breaches_found": breaches_found,
        }

    except Exception as e:
        logger.error(f"Error syncing {tenant_name}: {str(e)}")
        return {"status": "error", "error": str(e)}
    finally:
        if should_close_db:
            db.close()


def upsert_user_breachs(
    tenant_id, tenant_name, db: HIBPDB, hibp_client: HIBPClient, active_users
) -> Tuple[int, int]:
    users_checked = 0
    breaches_found = 0

    # Bulk query to get recently checked users
    recently_checked = db.get_recently_checked_users(tenant_id, CHECK_INTERVAL_DAYS)

    for user in active_users:
        user_id = user.get("id")
        upn = user.get("userPrincipalName")

        if not upn or not user_id:
            continue

        # Fast set lookup instead of DB query
        if upn in recently_checked:
            continue

        # Get known breaches
        known_breaches = db.get_user_breaches(tenant_id, upn)
        known_breach_names = {b["breach_name"] for b in known_breaches}

        # Check HIBP API
        breaches = hibp_client.check_email_breaches(upn)
        if breaches is None:
            continue

        users_checked += 1

        if not breaches:
            # Mark as no breaches
            if "NO_BREACH" not in known_breach_names:
                db.upsert_breach(
                    {
                        "tenant_id": tenant_id,
                        "user_principal_name": upn,
                        "user_id": user_id,
                        "breach_name": "NO_BREACH",
                        "breach_title": "No breaches found",
                        "breach_date": None,
                        "data_classes": "[]",
                        "is_verified": False,
                        "is_sensitive": False,
                        "is_spam_list": False,
                        "password_reset_required": False,
                    }
                )
        else:
            # Add new breaches
            for breach in breaches:
                breach_name = breach.get("Name", "")
                if breach_name not in known_breach_names:
                    db.upsert_breach(
                        {
                            "tenant_id": tenant_id,
                            "user_principal_name": upn,
                            "user_id": user_id,
                            "breach_name": breach_name,
                            "breach_title": breach.get("Title"),
                            "breach_date": breach.get("BreachDate"),
                            "data_classes": json.dumps(breach.get("DataClasses", [])),
                            "is_verified": breach.get("IsVerified", False),
                            "is_sensitive": breach.get("IsSensitive", False),
                            "is_spam_list": breach.get("IsSpamList", False),
                            "password_reset_required": False,
                        }
                    )
                    breaches_found += 1

        # Update password reset status
    password_dates = {}
    for user in active_users:
        upn = user.get("userPrincipalName")
        last_password_change = user.get("lastPasswordChangeDateTime")
        if upn and last_password_change:
            try:
                pwd_date = datetime.fromisoformat(
                    last_password_change.replace("Z", "+00:00")
                )
                password_dates[upn] = pwd_date.strftime("%Y-%m-%d")
            except Exception:
                pass

    if password_dates:
        db.bulk_update_password_reset_status(tenant_id, password_dates)

    logger.info(
        f"Completed sync for {tenant_name}: {users_checked} users checked, {breaches_found} breaches found"
    )
    return users_checked, breaches_found


def get_hibp_breach_report(tenant_id: str = None) -> Dict:
    """Get a breach report for a specific tenant or all tenants."""
    db = HIBPDB()

    try:
        if tenant_id:
            summary = db.get_tenant_breach_summary(tenant_id)
            return {"tenant_id": tenant_id, "summary": summary}
        else:
            # Would implement multi-tenant summary here
            return {"message": "Multi-tenant reports not yet implemented"}
    finally:
        db.close()
