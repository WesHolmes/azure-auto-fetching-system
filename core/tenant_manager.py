import json


def get_tenants(tenant_mode="single"):
    # if tenant_mode == "single":
    #     return [{"tenant_id": "3aae0fb1-276f-42f8-8e4d-36ca10cbb779", "name": "warp2"}]

    # Load once at module import, not on every function call
    if not hasattr(get_tenants, "_cached_tenants"):
        try:
            with open("data/az_tenants.json") as f:
                get_tenants._cached_tenants = json.load(f)
        except Exception:
            # Fallback to single tenant if file read fails
            get_tenants._cached_tenants = [{"tenant_id": "3aae0fb1-276f-42f8-8e4d-36ca10cbb779", "name": "warp2"}]

    return get_tenants._cached_tenants

    # Multi-tenant: fetch customer tenants from contracts
    # GraphClient only needs tenant_id - it gets CLIENT_ID and CLIENT_SECRET from env vars
    # client = GraphBetaClient(os.getenv("PARTNER_TENANT_ID"))

    # contracts = client.get("/contracts")
    # data = [{"tenant_id": c["customerId"], "name": c["displayName"]} for c in contracts if c.get("customerId")]
    # return data
