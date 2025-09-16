import json
import sqlite3

from functions.backup_radar.helpers import get_tenant_id_from_company_name


def debug_danjaq_tenant_matching():
    # Load tenants
    with open("data/az_tenants.json") as f:
        tenants = json.load(f)

    # Check database records
    conn = sqlite3.connect("db/sqlite.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT company_name, device_name, tenant_id 
        FROM backup_radar 
        WHERE device_name LIKE '%danjaq%' OR company_name LIKE '%danjaq%'
    """)

    records = cursor.fetchall()
    print("Current Danjaq records in database:")
    print("=" * 60)
    for row in records:
        company_name, device_name, tenant_id = row
        print(f"Company: '{company_name}'")
        print(f"Device: '{device_name}'")
        print(f"Tenant ID: {tenant_id}")
        print()

    # Test the tenant matching logic
    print("Testing tenant matching logic:")
    print("=" * 60)

    test_cases = ["Danjaq, LLC.", "Danjaq-DP01", "danjaq", "DANJAQ"]

    for test_company in test_cases:
        result = get_tenant_id_from_company_name(test_company, tenants)
        print(f"Company: '{test_company}' -> Tenant ID: {result}")

    # Check if Danjaq tenant exists
    print("\nDanjaq tenant in tenants file:")
    print("=" * 60)
    danjaq_tenant = next((t for t in tenants if "danjaq" in t.get("display_name", "").lower()), None)
    if danjaq_tenant:
        print(f"Found: {danjaq_tenant['display_name']}")
        print(f"Tenant ID: {danjaq_tenant['tenant_id']}")
        print(f"Primary Domain: {danjaq_tenant['primary_domain']}")
    else:
        print("No Danjaq tenant found!")

    conn.close()


if __name__ == "__main__":
    debug_danjaq_tenant_matching()
