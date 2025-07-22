import os
from api.integrations.graph_client import GraphClient

def get_tenants(tenant_mode="single"):
    if tenant_mode == "single":
        return [{
            'tenant_id': "3aae0fb1-276f-42f8-8e4d-36ca10cbb779",  # Fixed: Added missing character
            'name': "warp2"
        }]

    # Multi-tenant: fetch customer tenants from contracts
    client = GraphClient(
    os.getenv('PARTNER_TENANT_ID'),
        os.getenv('CLIENT_ID'),
        os.getenv('CLIENT_SECRET')
    )

    contracts = client.get('/contracts')
    return [
        {
            'tenant_id': c['customerId'],
            'name': c['displayName']
        }
        for c in contracts if c.get('customerId')
    ]