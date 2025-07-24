import os
from datetime import datetime
from core.graph_client import GraphClient
from core.database import upsert_many
from sync.license_sync import estimate_license_cost  # Import from license_sync
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
            top=20
        )
        
        logger.info(f"Successfully fetched {len(users)} users for tenant {tenant_id}")
        return users
        
    except Exception as e:
        logger.error(f"Failed to fetch users for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise

def fetch_user_groups(tenant_id, user_id):
    """Check if user is admin"""
    try:
        graph = GraphClient(tenant_id)
        groups = graph.get(
            f'/users/{user_id}/memberOf',
            select=['id', 'displayName']
        )
        
        # check for admin roles
        admin_keywords = ['admin', 'administrator', 'global']
        is_admin = any(
            any(keyword in group.get('displayName', '').lower() 
                for keyword in admin_keywords) 
            for group in groups
        )
        return is_admin, len(groups)
        
    except Exception as e:
        logger.debug(f"Failed to fetch groups for user {user_id}: {str(e)}")
        return False, 0

def fetch_user_mfa_status(tenant_id):
    """Fetch MFA registration details for all users"""
    try:
        logger.info(f"Fetching MFA status for tenant {tenant_id}")
        graph = GraphClient(tenant_id)
        
        mfa_details = graph.get(
            '/reports/authenticationMethods/userRegistrationDetails',
            select=[
                'id', 'userPrincipalName', 'isMfaRegistered',
                'isMfaCapable', 'methodsRegistered'
            ]
        )
        
        # conv. to lookup dictionary
        mfa_lookup = {item['id']: item for item in mfa_details}
        logger.info(f"Successfully fetched MFA status for {len(mfa_lookup)} users")
        return mfa_lookup
        
    except Exception as e:
        logger.warning(f"Could not fetch MFA data for tenant {tenant_id}: {str(e)}")
        return {}

def transform_user_records(users, tenant_id, mfa_lookup):
    """Transform Graph API users to database records"""
    records = []
    license_records = []  # Add this back
    
    logger.info(f"Starting transformation of {len(users)} users")
    
    for i, user in enumerate(users, 1):
        user_id = user.get('id')
        display_name = user.get('displayName', 'Unknown')
        upn = user.get('userPrincipalName')
        account_enabled = user.get('accountEnabled', True)
        
        if i % 100 == 0:
            logger.info(f"Processing user {i}/{len(users)}")
        
        try:
            # Get last sign-in
            signin_activity = user.get('signInActivity', {})
            last_sign_in = signin_activity.get('lastSignInDateTime', None)
        
            # get license count
            assigned_licenses = user.get('assignedLicenses', [])
            license_count = len(assigned_licenses)
            is_active_license = 1 if account_enabled else 0
            
            # get mfa details
            mfa_data = mfa_lookup.get(user_id, {})
            is_mfa_registered = mfa_data.get('isMfaRegistered', False)
            
            
            # init. def. vals
            is_admin = False
            group_count = 0

            # get group count and admin status
            try:
                is_admin, group_count = fetch_user_groups(tenant_id, user_id)
            except Exception as e:
                logger.debug(f"Could not fetch group data for user {user_id}: {str(e)}")
            
            # Process assigned licenses for user_licenses table
            if assigned_licenses:
                try:
                    for assigned_license in assigned_licenses:
                        sku_id = assigned_license.get('skuId')
                        if sku_id:
                            # We'll populate display name and part number in license_sync
                            user_license_record = {
                                'tenant_id': tenant_id,
                                'user_id': user_id,
                                'license_id': sku_id,
                                'user_principal_name': upn,
                                'is_active': is_active_license,
                                'assigned_date': datetime.now().isoformat(),
                                'unassigned_date': None,
                                'license_display_name': 'Pending Sync',  # Will be updated by license sync
                                'license_partnumber': 'Pending Sync',    # Will be updated by license sync
                                'monthly_cost': 15.00,                   # Default, will be updated
                                'last_update': datetime.now().isoformat()
                            }
                            license_records.append(user_license_record)
                except Exception as e:
                    logger.warning(f"Could not process licenses for user {user_id}: {str(e)}")
                                
            record = {
                'id': user_id,
                'tenant_id': tenant_id,
                'display_name': display_name,
                'user_principal_name': upn,
                'mail': user.get('mail'),
                'account_enabled': 1 if user.get('accountEnabled') else 0,
                'user_type': user.get('userType'),
                'department': user.get('department'),
                'job_title': user.get('jobTitle'),
                'last_sign_in': last_sign_in,
                'is_mfa_compliant': 1 if is_mfa_registered else 0,
                'is_admin': 1 if is_admin else 0,
                'license_count': license_count,
                'group_count': group_count,
                'synced_at': datetime.now().isoformat()
            }
            records.append(record)
            
        except Exception as e:
            logger.error(f"Failed to process user {display_name}: {str(e)}")
            # Add basic record
            basic_record = {
                'id': user_id,
                'tenant_id': tenant_id,
                'display_name': display_name,
                'user_principal_name': upn,
                'mail': user.get('mail'),
                'account_enabled': 1 if user.get('accountEnabled') else 0,
                'user_type': user.get('userType'),
                'is_mfa_compliant': 0,
                'is_admin': 0,
                'license_count': 0,
                'group_count': 0,
                'synced_at': datetime.now().isoformat()
            }
            records.append(basic_record)
    
    logger.info(f"Transformation complete: {len(records)} users, {len(license_records)} licenses")
    return records, license_records  # Return both values

def sync_users(tenant_id, tenant_name):
    """Orchestrate user synchronization with enrichment"""
    start_time = datetime.now()
    logger.info(f"Starting user sync for {tenant_name} (tenant_id: {tenant_id})")
    
    try:
        # fetch all data
        users = fetch_users(tenant_id)
        
        if not users:
            logger.warning(f"No users found for {tenant_name}")
            return {
                'status': 'success',
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'users_synced': 0,
                'user_licenses_synced': 0,
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            }
        
        # fetch MFA status (optional enrichment)
        mfa_lookup = fetch_user_mfa_status(tenant_id)
        
        # transform data
        user_records, user_license_records = transform_user_records(users, tenant_id, mfa_lookup)
        
        # store in database with error handling
        users_stored = 0
        user_licenses_stored = 0
        
        try:
            if user_records:
                users_stored = upsert_many('users', user_records)
                logger.info(f"Stored {users_stored} users for {tenant_name}")
        except Exception as e:
            logger.error(f"Failed to store users for {tenant_name}: {str(e)}", exc_info=True)
            raise
        
        try:
            if user_license_records:
                user_licenses_stored = upsert_many('user_licenses', user_license_records)
                logger.info(f"Stored {user_licenses_stored} user licenses for {tenant_name}")
        except Exception as e:
            logger.error(f"Failed to store user licenses for {tenant_name}: {str(e)}", exc_info=True)
            # Don't raise here - users were stored successfully
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Completed user sync for {tenant_name}: "
            f"{users_stored} users, {user_licenses_stored} user licenses in {duration:.1f}s"
        )
        
        return {
            'status': 'success',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'users_synced': users_stored,
            'user_licenses_synced': user_licenses_stored,
            'duration_seconds': duration
        }
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"User sync failed for {tenant_name} after {duration:.1f}s: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            'status': 'error',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'error': str(e),
            'duration_seconds': duration
        }