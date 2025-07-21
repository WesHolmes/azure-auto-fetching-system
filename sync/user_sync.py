import os
from datetime import datetime
from core.graph_client import GraphClient
from core.database import upsert_many

def fetch_users(tenant_id):
    """Fetch users from Graph API"""
    graph = GraphClient(tenant_id)
    return graph.get('/users', select=[
        'id', 'displayName', 'userPrincipalName', 'mail',
        'accountEnabled', 'userType', 'department', 'jobTitle'
        'signInActivity'
    ])

def fetch_user_licenses(tenant_id):
    pass

def transform_user_records(users, tenant_id):
    """Transform Graph API users to database records"""
    records = []
    for user in users:
        record = {
            'id': user.get('id'),
            'tenant_id': tenant_id,
            'display_name': user.get('displayName'),
            'user_principal_name': user.get('userPrincipalName'),
            'mail': user.get('mail'),
            'account_enabled': user.get('accountEnabled'),
            'user_type': user.get('userType'),
            'department': user.get('department'),
            'job_title': user.get('jobTitle'),
            'last_sign_in': None,  # Requires AuditLog.Read.All permission
            'synced_at': datetime.now().isoformat()
        }
        records.append(record)
    return records

def sync_users(tenant_id, tenant_name):
    """Orchestrate user synchronization"""
    try:
        users = fetch_users(tenant_id)
        records = transform_user_records(users, tenant_id)
        upsert_many('users', records)

        return {
            'status': 'success',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'users_synced': len(records)
        }

    except Exception as e:
        return {
            'status': 'error',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'error': str(e)
        }