import os
from datetime import datetime
from core.graph_client import GraphClient
from core.database import upsert_many
import logging

logger = logging.getLogger(__name__)

def fetch_users(tenant_id):
    """Fetch users from Graph API"""
    try:
        logger.info(f"Starting user fetch for tenant {tenant_id}")
        graph = GraphClient(tenant_id)
        
        users = graph.get('/users', 
            select=[
                'id', 'displayName', 'userPrincipalName', 'mail',
                'accountEnabled', 'userType', 'department', 'jobTitle',
                'signInActivity', 'createdDateTime', 'assignedLicenses',
                'lastPasswordChangeDateTime'
            ],
            expand='manager($select=id,displayName)',
            top=999,
            filter='accountEnabled eq true'
        )
        
        logger.info(f"Successfully fetched {len(users)} users for tenant {tenant_id}")
        return users
        
    except Exception as e:
        logger.error(f"Failed to fetch users for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise

def fetch_user_licenses(tenant_id, user_id):
    """Fetch detailed license information for a user"""
    try:
        graph = GraphClient(tenant_id)
        licenses = graph.get(
            f'/users/{user_id}/licenseDetails',
            select=['id', 'skuId', 'skuPartNumber', 'servicePlans']
        )
        return licenses
    except Exception as e:
        logger.warning(f"Failed to fetch licenses for user {user_id}: {str(e)}")
        return []

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