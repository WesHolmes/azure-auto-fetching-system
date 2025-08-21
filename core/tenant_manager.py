import os
import json
from core.graph_beta_client import GraphBetaClient


def get_tenants(tenant_mode="single"):
    if tenant_mode == "single":
        return [{"tenant_id": "3aae0fb1-276f-42f8-8e4d-36ca10cbb779", "name": "warp2"}]
    # upload az_tenants.json to data folder
    with open("data/az_tenants.json", "r") as f:
        data = json.load(f)
    return data
    # Multi-tenant: fetch customer tenants from contracts
    # GraphClient only needs tenant_id - it gets CLIENT_ID and CLIENT_SECRET from env vars
    # client = GraphBetaClient(os.getenv("PARTNER_TENANT_ID"))

    # contracts = client.get("/contracts")
    # data = [{"tenant_id": c["customerId"], "name": c["displayName"]} for c in contracts if c.get("customerId")]
    # return data
