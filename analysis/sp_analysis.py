import sqlite3
from core.database import get_connection


def analyze_service_principals(tenant_mode="auto"):
    """Analyze service principals from the database and return comprehensive statistics"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total SPs analyzed
        cursor.execute("SELECT COUNT(*) FROM service_principals")
        total_sps = cursor.fetchone()[0]

        # Expired SPs (credentials that have expired)
        cursor.execute("""
            SELECT COUNT(*) FROM service_principals 
            WHERE credential_exp_date IS NOT NULL 
            AND credential_exp_date < datetime('now')
        """)
        expired_sps = cursor.fetchone()[0]

        # SPs with owners
        cursor.execute(
            "SELECT COUNT(*) FROM service_principals WHERE owners IS NOT NULL AND owners != ''"
        )
        sps_with_owners = cursor.fetchone()[0]

        # Disabled SPs
        cursor.execute("SELECT COUNT(*) FROM service_principals WHERE enabled_sp = 0")
        disabled_sps = cursor.fetchone()[0]

        # SPs with sign-in date
        cursor.execute(
            "SELECT COUNT(*) FROM service_principals WHERE last_sign_in IS NOT NULL AND last_sign_in != ''"
        )
        sps_with_signin = cursor.fetchone()[0]

        # SPs with no credentials
        cursor.execute(
            "SELECT COUNT(*) FROM service_principals WHERE credential_type IS NULL"
        )
        sps_no_credentials = cursor.fetchone()[0]

        # Apps with credentials (breakdown of expired vs non-expired)
        cursor.execute(
            "SELECT COUNT(*) FROM service_principals WHERE credential_type IS NOT NULL"
        )
        apps_with_credentials = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM service_principals 
            WHERE credential_type IS NOT NULL 
            AND credential_exp_date IS NOT NULL 
            AND credential_exp_date < datetime('now')
        """)
        apps_with_expired_credentials = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM service_principals 
            WHERE credential_type IS NOT NULL 
            AND (credential_exp_date IS NULL OR credential_exp_date >= datetime('now'))
        """)
        apps_with_valid_credentials = cursor.fetchone()[0]

        # SPs by tenant breakdown
        cursor.execute("""
            SELECT tenant_id, COUNT(*) as count 
            FROM service_principals 
            GROUP BY tenant_id 
            ORDER BY count DESC
        """)
        tenant_breakdown = cursor.fetchall()

        conn.close()

        # Calculate percentages
        expired_percentage = (expired_sps / total_sps * 100) if total_sps > 0 else 0
        with_owners_percentage = (
            (sps_with_owners / total_sps * 100) if total_sps > 0 else 0
        )
        disabled_percentage = (disabled_sps / total_sps * 100) if total_sps > 0 else 0
        with_signin_percentage = (
            (sps_with_signin / total_sps * 100) if total_sps > 0 else 0
        )
        no_credentials_percentage = (
            (sps_no_credentials / total_sps * 100) if total_sps > 0 else 0
        )

        # Calculate percentages for apps with credentials
        apps_with_credentials_percentage = (
            (apps_with_credentials / total_sps * 100) if total_sps > 0 else 0
        )
        expired_of_credentialed_percentage = (
            (apps_with_expired_credentials / apps_with_credentials * 100)
            if apps_with_credentials > 0
            else 0
        )
        valid_of_credentialed_percentage = (
            (apps_with_valid_credentials / apps_with_credentials * 100)
            if apps_with_credentials > 0
            else 0
        )

        return {
            "status": "success",
            "tenant_mode": tenant_mode,
            "total_sps": total_sps,
            "expired_sps": expired_sps,
            "expired_percentage": round(expired_percentage, 1),
            "sps_with_owners": sps_with_owners,
            "with_owners_percentage": round(with_owners_percentage, 1),
            "disabled_sps": disabled_sps,
            "disabled_percentage": round(disabled_percentage, 1),
            "sps_with_signin": sps_with_signin,
            "with_signin_percentage": round(with_signin_percentage, 1),
            "sps_no_credentials": sps_no_credentials,
            "no_credentials_percentage": round(no_credentials_percentage, 1),
            "apps_with_credentials": apps_with_credentials,
            "apps_with_credentials_percentage": round(
                apps_with_credentials_percentage, 1
            ),
            "apps_with_expired_credentials": apps_with_expired_credentials,
            "expired_of_credentialed_percentage": round(
                expired_of_credentialed_percentage, 1
            ),
            "apps_with_valid_credentials": apps_with_valid_credentials,
            "valid_of_credentialed_percentage": round(
                valid_of_credentialed_percentage, 1
            ),
            "tenant_breakdown": tenant_breakdown,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


def format_analytics_json(analytics_result):
    """Format analytics results into a clean JSON object for easy parsing"""
    if analytics_result["status"] == "error":
        return {"status": "error", "error": analytics_result["error"]}

    data = analytics_result

    # Create tenant breakdown as a cleaner structure
    tenant_breakdown = {}
    for tenant_id, count in data.get("tenant_breakdown", []):
        tenant_breakdown[tenant_id] = count

    return {
        "status": "success",
        "tenant_mode": data.get("tenant_mode", "unknown"),
        "total_service_principals": data["total_sps"],
        "expired_credentials": data["expired_sps"],
        "expired_credentials_percentage": data["expired_percentage"],
        "service_principals_with_owners": data["sps_with_owners"],
        "service_principals_with_owners_percentage": data["with_owners_percentage"],
        "disabled_service_principals": data["disabled_sps"],
        "disabled_service_principals_percentage": data["disabled_percentage"],
        "service_principals_with_signin": data["sps_with_signin"],
        "service_principals_with_signin_percentage": data["with_signin_percentage"],
        "service_principals_no_credentials": data["sps_no_credentials"],
        "service_principals_no_credentials_percentage": data[
            "no_credentials_percentage"
        ],
        "apps_with_credentials": data["apps_with_credentials"],
        "apps_with_credentials_percentage": data["apps_with_credentials_percentage"],
        "apps_with_expired_credentials": data["apps_with_expired_credentials"],
        "apps_with_expired_credentials_percentage": data[
            "expired_of_credentialed_percentage"
        ],
        "apps_with_valid_credentials": data["apps_with_valid_credentials"],
        "apps_with_valid_credentials_percentage": data[
            "valid_of_credentialed_percentage"
        ],
        "tenant_breakdown": tenant_breakdown,
    }


def format_analytics_summary(analytics_result):
    """Format analytics results into a readable summary string"""
    if analytics_result["status"] == "error":
        return f"Analytics Error: {analytics_result['error']}"

    data = analytics_result
    mode_indicator = (
        f" ({data['tenant_mode'].upper()} TENANT MODE)" if "tenant_mode" in data else ""
    )

    summary = f"""
📊 Service Principal Analytics Summary{mode_indicator}
=====================================
Total SPs analyzed: {data["total_sps"]}
Expired credentials: {data["expired_sps"]} ({data["expired_percentage"]}%)
SPs with owners: {data["sps_with_owners"]} ({data["with_owners_percentage"]}%)
Disabled SPs: {data["disabled_sps"]} ({data["disabled_percentage"]}%)
SPs with sign-in data: {data["sps_with_signin"]} ({data["with_signin_percentage"]}%)
SPs with no credentials: {data["sps_no_credentials"]} ({data["no_credentials_percentage"]}%)

🔐 Apps with Credentials Analysis
=================================
Total apps with credentials: {data["apps_with_credentials"]} ({data["apps_with_credentials_percentage"]}% of all SPs)
  ├─ Apps with expired credentials: {data["apps_with_expired_credentials"]} ({data["expired_of_credentialed_percentage"]}% of credentialed apps)
  └─ Apps with valid credentials: {data["apps_with_valid_credentials"]} ({data["valid_of_credentialed_percentage"]}% of credentialed apps)

"""

    # Add tenant breakdown section based on mode
    if data.get("tenant_mode") == "single":
        summary += "🏢 Single Tenant Analysis\n"
        summary += "========================\n"
        if data["tenant_breakdown"]:
            tenant_id, count = data["tenant_breakdown"][0]
            summary += f"Tenant: {tenant_id}\n"
            summary += f"Service Principals: {count}\n"
    else:
        summary += "🏢 Multi-Tenant Breakdown\n"
        summary += "=========================\n"
        if data["tenant_breakdown"]:
            for tenant_id, count in data["tenant_breakdown"]:
                summary += f"  • {tenant_id}: {count} SPs\n"
        else:
            summary += "  No tenant data available\n"

    return summary.strip()
