import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from core.databaseV2 import query, execute_query

# configure logging for azure functions
logger = logging.getLogger(__name__)


def calculate_inactive_users(tenant_id: str, days: int = 90) -> Dict[str, Any]:
    """
    calculate inactive users based on last sign-in activity
    analyzes user activity patterns and potential license cost savings

    returns:
        dict: with analysis results and potential savings
    """
    try:
        logger.info(f"starting inactive users analysis for tenant {tenant_id}")

        # calculate the cutoff date for determining inactive users
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        logger.debug(f"cutoff date set to {cutoff_date}")

        # query users from database - using sqlite parameterized queries
        query_sql = """
        SELECT 
            user_id, display_name, user_principal_name, account_enabled,
            last_sign_in_date, license_count, is_global_admin
        FROM usersV2 
        WHERE tenant_id = ? AND account_enabled = 1
        """

        # execute database query with proper parameterization
        users = query(query_sql, (tenant_id,))
        logger.info(f"retrieved {len(users)} active users from database")

        # initialize lists to categorize users by activity status
        inactive_users = []
        active_users = []
        never_signed_in = []

        # process each user to determine activity status
        for user in users:
            if user["last_sign_in_date"]:
                # parse the last sign-in timestamp
                last_signin = datetime.fromisoformat(user["last_sign_in_date"])

                # check if user is inactive based on cutoff date
                if last_signin < cutoff_date:
                    days_inactive = (datetime.now(timezone.utc) - last_signin).days

                    # add to inactive users with potential savings calculation
                    inactive_users.append(
                        {
                            "user_id": user["user_id"],
                            "display_name": user["display_name"],
                            "user_principal_name": user["user_principal_name"],
                            "days_inactive": days_inactive,
                            "license_count": user.get("license_count", 0),
                        }
                    )
                else:
                    # user is active - signed in within threshold
                    active_users.append(user)
            else:
                # user has never signed in - potential cleanup candidate
                never_signed_in.append(user)

        # Calculate actual potential cost savings using real license costs
        inactive_user_ids = [u["user_id"] for u in inactive_users]
        if inactive_user_ids:
            # Get actual monthly costs for inactive users' licenses
            placeholders = ",".join(["?" for _ in inactive_user_ids])
            inactive_cost_query = f"""
            SELECT SUM(monthly_cost) as total_cost
            FROM user_licensesV2 
            WHERE user_id IN ({placeholders}) AND tenant_id = ?
            """
            cost_result = query(inactive_cost_query, inactive_user_ids + [tenant_id])
            monthly_savings = (
                cost_result[0]["total_cost"]
                if cost_result and cost_result[0]["total_cost"]
                else 0
            )
        else:
            monthly_savings = 0

        logger.info(
            f"analysis complete: {len(inactive_users)} inactive, {len(active_users)} active, {len(never_signed_in)} never signed in"
        )

        # prepare comprehensive result object
        result = {
            "tenant_id": tenant_id,
            "analysis_date": datetime.now(timezone.utc).isoformat(),
            "threshold_days": days,
            "inactive_count": len(inactive_users),
            "active_count": len(active_users),
            "never_signed_in_count": len(never_signed_in),
            "potential_monthly_savings": monthly_savings,
            "utilization_rate": round((len(active_users) / len(users)) * 100, 2)
            if users
            else 0,
            "inactive_users": inactive_users[:10],  # top 10 for summary report
        }

        return result

    except Exception as e:
        logger.error(f"error calculating inactive users: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}


def calculate_mfa_compliance(tenant_id: str) -> Dict[str, Any]:
    """
    calculate multi-factor authentication compliance across users
    identifies security risks from non-mfa users, especially admins

    returns:
        dictionary with mfa compliance metrics and risk assessment
    """
    try:
        logger.info(f"starting mfa compliance analysis for tenant {tenant_id}")

        # query users with mfa registration status
        query_sql = """
        SELECT 
            user_id, display_name, user_principal_name, 
            is_mfa_compliant, is_global_admin, account_enabled
        FROM usersV2 
        WHERE tenant_id = ? AND account_enabled = 1
        """

        # execute parameterized query
        users = query(query_sql, (tenant_id,))
        # logger.info(f"analyzing mfa status for {len(users)} active users")

        # initialize lists for compliance categorization
        compliant = []
        non_compliant = []
        admin_non_compliant = []

        # categorize users by mfa compliance status
        for user in users:
            if user.get("is_mfa_compliant", False):
                # user has mfa enabled - compliant
                compliant.append(user)
            else:
                # user does not have mfa - non-compliant
                non_compliant.append(user)

                # check if non-compliant user is an admin - high security risk
                if user.get("is_global_admin", False):
                    admin_non_compliant.append(user)

        # calculate compliance metrics
        total_users = len(users)
        compliance_rate = (len(compliant) / total_users * 100) if total_users > 0 else 0

        # logger.info(f"mfa compliance rate: {compliance_rate:.1f}% ({len(compliant)}/{total_users})")
        # logger.warning(f"critical: {len(admin_non_compliant)} admin users without mfa")

        # prepare comprehensive compliance report
        result = {
            "tenant_id": tenant_id,
            "analysis_date": datetime.now(timezone.utc).isoformat(),
            "total_users": total_users,
            "mfa_enabled": len(compliant),
            "non_compliant": len(non_compliant),
            "compliance_rate": round(compliance_rate, 1),
            "admin_non_compliant": len(admin_non_compliant),
            "risk_level": "high"
            if admin_non_compliant
            else ("medium" if non_compliant else "low"),
            "critical_users": admin_non_compliant[
                :10
            ],  # top 10 admin users without mfa - security priority
        }

        return result

    except Exception as e:
        logger.error(f"error calculating mfa compliance: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}


def calculate_license_optimization(tenant_id: str) -> Dict[str, Any]:
    """
    analyze license usage patterns and identify optimization opportunities
    helps reduce costs by identifying unused or underutilized licenses

    args:
        tenant_id: microsoft tenant identifier

    returns:
        dictionary with license usage analysis and cost optimization recommendations
    """
    try:
        # logger.info(f"starting license optimization analysis for tenant {tenant_id}")

        # simplified query without license table dependency
        # focuses on user activity patterns to estimate license utilization
        query_sql = """
        SELECT 
            user_id, display_name, user_principal_name, last_sign_in_date,
            account_enabled, account_type, license_count
        FROM usersV2
        WHERE tenant_id = ? AND account_enabled = 1
        """

        # execute query to get user activity data
        users = query(query_sql, (tenant_id,))
        # logger.info(f"analyzing license optimization for {len(users)} active users")

        # categorize users by usage patterns for license optimization
        active_users = 0
        inactive_users = 0
        never_signed_in = 0
        guest_users = 0

        # 90-day inactivity threshold for license optimization
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)

        # analyze each user's activity pattern
        for user in users:
            # count guest users (may not need paid licenses)
            if user.get("account_type") == "Guest":
                guest_users += 1
                continue

            if user["last_sign_in_date"]:
                # parse last sign-in date
                last_signin = datetime.fromisoformat(user["last_sign_in_date"])

                if last_signin >= cutoff_date:
                    # user is active - license is being utilized
                    active_users += 1
                else:
                    # user is inactive - potential license optimization candidate
                    inactive_users += 1
            else:
                # user never signed in - license potentially wasted
                never_signed_in += 1

        # Calculate optimization metrics using actual license costs
        total_paid_users = len(users) - guest_users
        underutilized_licenses = inactive_users + never_signed_in
        utilization_rate = (
            (active_users / total_paid_users * 100) if total_paid_users > 0 else 0
        )

        # Calculate actual cost savings using real license costs from database
        # Get actual monthly costs for underutilized licenses
        underutilized_cost_query = """
        SELECT SUM(ul.monthly_cost) as total_cost
        FROM usersV2 u
        INNER JOIN user_licensesV2 ul ON u.user_id = ul.user_id
        WHERE u.tenant_id = ? 
        AND u.account_enabled = 1
        AND (u.last_sign_in_date IS NULL OR datetime(u.last_sign_in_date) < datetime('now', '-90 days'))
        """

        cost_result = query(underutilized_cost_query, (tenant_id,))
        actual_monthly_savings = (
            cost_result[0]["total_cost"]
            if cost_result and cost_result[0]["total_cost"]
            else 0
        )
        actual_annual_savings = actual_monthly_savings * 12

        # Fallback estimate if no cost data available
        if actual_monthly_savings == 0 and underutilized_licenses > 0:
            estimated_monthly_savings = underutilized_licenses * 15  # Fallback estimate
            estimated_annual_savings = estimated_monthly_savings * 12
        else:
            estimated_monthly_savings = actual_monthly_savings
            estimated_annual_savings = actual_annual_savings

        # logger.info(f"license utilization: {utilization_rate:.1f}% ({active_users}/{total_paid_users})")
        # logger.info(f"potential monthly savings: ${estimated_monthly_savings}")

        # prepare comprehensive optimization report
        result = {
            "tenant_id": tenant_id,
            "analysis_date": datetime.now(timezone.utc).isoformat(),
            "total_users": len(users),
            "total_paid_users": total_paid_users,
            "active_users": active_users,
            "inactive_users": inactive_users,
            "never_signed_in": never_signed_in,
            "guest_users": guest_users,
            "utilization_rate": round(utilization_rate, 1),
            "underutilized_licenses": underutilized_licenses,
            "estimated_monthly_savings": estimated_monthly_savings,
            "estimated_annual_savings": estimated_annual_savings,
            "optimization_score": round(
                utilization_rate, 0
            ),  # simple score based on utilization
        }

        return result

    except Exception as e:
        logger.error(f"error calculating license optimization: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}


def fix_inactive_user_licenses(tenant_id: str) -> Dict[str, Any]:
    """
    Retroactively mark licenses as inactive for users who are disabled
    but still have active license records from before they were disabled.
    """
    try:
        logger.info(f"Starting retroactive license fix for tenant {tenant_id}")

        # Find inactive users who still have active license records
        query_sql = """
        SELECT DISTINCT u.user_id, u.user_principal_name, u.account_enabled
        FROM usersV2 u
        INNER JOIN user_licensesV2 ul ON u.user_id = ul.user_id
        WHERE u.tenant_id = ? 
        AND u.account_enabled = 0 
        AND ul.is_active = 1
        """

        inactive_users_with_active_licenses = query(query_sql, (tenant_id,))

        if not inactive_users_with_active_licenses:
            logger.info("No inactive users with active licenses found")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "users_updated": 0,
                "message": "No inactive users with active licenses found",
            }

        # Update their license records to mark as inactive
        updated_count = 0
        for user in inactive_users_with_active_licenses:
            rows_updated = execute_query(
                """
                UPDATE user_licensesV2 
                SET is_active = 0, 
                    unassigned_date = ?,
                    last_updated = ?
                WHERE user_id = ? AND tenant_id = ? AND is_active = 1
            """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    user["user_id"],
                    tenant_id,
                ),
            )

            if rows_updated > 0:
                updated_count += 1
                logger.info(
                    f"Marked {rows_updated} licenses as inactive for user: {user['user_principal_name']}"
                )

        logger.info(f"Fixed licenses for {updated_count} inactive users")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "users_updated": updated_count,
            "licenses_marked_inactive": sum(
                query(
                    "SELECT COUNT(*) as count FROM user_licensesV2 WHERE tenant_id = ? AND is_active = 0",
                    (tenant_id,),
                )[0].values()
            ),
        }

    except Exception as e:
        logger.error(f"Error fixing inactive user licenses: {str(e)}")
        return {"status": "error", "error": str(e), "tenant_id": tenant_id}
