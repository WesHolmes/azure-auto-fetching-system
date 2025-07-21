import os
from datetime import datetime
from core.graph_client import GraphClient
from core.database import upsert_many

def fetch_service_principals(tenant_id):
    """Fetch service principals from Graph API"""
    graph = GraphClient(tenant_id)
    return graph.get('/servicePrincipals', select=[
        'id', 'appId', 'displayName', 'publisherName', 'servicePrincipalType'
    ])

def transform_service_principal_records(service_principals, tenant_id):
    """Transform Graph API service principals to database records"""
    records = []
    for sp in service_principals:
        record = {
            'id': sp.get('id'),
            'tenant_id': tenant_id,
            'app_id': sp.get('appId'),
            'display_name': sp.get('displayName'),
            'publisher_name': sp.get('publisherName'),
            'service_principal_type': sp.get('servicePrincipalType'),
            'synced_at': datetime.now().isoformat()
        }
        records.append(record)
    return records

def sync_service_principals(tenant_id, tenant_name):
    """Orchestrate service principal synchronization"""
    try:
        service_principals = fetch_service_principals(tenant_id)
        records = transform_service_principal_records(service_principals, tenant_id)
        upsert_many('service_principals', records)

        return {
            'status': 'success',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'service_principals_synced': len(records)
        }

    except Exception as e:
        return {
            'status': 'error',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'error': str(e)
        }