import os
from core.graph_client import GraphClient

def get_tenants(tenant_mode="auto"):
    """
    Get tenants based on the specified mode.
    
    Args:
        tenant_mode (str): "single" for single tenant, "multi" for multi-tenant, "auto" for auto-detect
    
    Returns:
        list: List of tenant dictionaries with 'tenant_id' and 'name'
    """
    
    # Auto-detect mode based on environment
    if tenant_mode == "auto":
        partner_tenant_id = os.getenv('PARTNER_TENANT_ID')
        if partner_tenant_id:
            print(f"🔍 Auto-detected multi-tenant mode (PARTNER_TENANT_ID set)")
            tenant_mode = "multi"
        else:
            print(f"🔍 Auto-detected single-tenant mode (PARTNER_TENANT_ID not set)")
            tenant_mode = "single"
    
    # Single tenant mode for testing/development
    if tenant_mode == "single":
        return [{
            'tenant_id': "3aae0fb1-276f-42f8-8e4d-36ca10cbb779",
            'name': "warp2"
        }]

    # Multi-tenant: fetch customer tenants from partner contracts
    partner_tenant_id = os.getenv('PARTNER_TENANT_ID')
    if not partner_tenant_id:
        print("❌ ERROR: Multi-tenant mode requested but PARTNER_TENANT_ID not set")
        print("💡 TIP: Set PARTNER_TENANT_ID environment variable or use single tenant mode")
        print("🔄 Falling back to single tenant mode")
        return [{
            'tenant_id': "3aae0fb1-276f-42f8-8e4d-36ca10cbb779",
            'name': "warp2 (fallback)"
        }]
    
    try:
        print(f"🔗 Fetching partner contracts from tenant: {partner_tenant_id}")
        client = GraphClient(partner_tenant_id)
        contracts = client.get('/contracts')
        
        tenants = [
            {
                'tenant_id': c['customerId'],
                'name': c['displayName']
            }
            for c in contracts if c.get('customerId')
        ]
        
        print(f"✅ Found {len(tenants)} customer tenants")
        return tenants
        
    except Exception as e:
        print(f"❌ ERROR: Failed to fetch partner contracts: {e}")
        print("🔄 Falling back to single tenant mode")
        return [{
            'tenant_id': "3aae0fb1-276f-42f8-8e4d-36ca10cbb779",
            'name': "warp2 (fallback)"
        }]