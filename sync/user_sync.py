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

def estimate_license_cost(sku_part_number: str) -> float:
    """Estimate monthly cost for common Microsoft license SKUs"""
    sku_costs = {
        'ENTERPRISEPACK': 22.00,
        'ENTERPRISEPREMIUM': 35.00,
        'EXCHANGESTANDARD': 4.00,
        'EXCHANGEENTERPRISE': 8.00,
        'SPB': 12.50,
        'SMB_BUSINESS_ESSENTIALS': 6.00,
        'SMB_BUSINESS_PREMIUM': 22.00,
        'STANDARDWOFFPACK': 12.50,
        'POWER_BI_PRO': 10.00,
        'EMS': 10.60,
        'EMSPREMIUM': 16.40,
    }
    
    sku_upper = sku_part_number.upper()
    for sku_pattern, cost in sku_costs.items():
        if sku_pattern in sku_upper:
            return cost
    return 15.00  # def. estimate


def transform_user_records(users, tenant_id, mfa_lookup):
    """Transform Graph API users to database records"""
    records = []
    license_records = []
    failed_enrichments = []
    
    logger.info(f"Starting transformation of {len(users)} users")
    
    for i, user in enumerate(users, 1):
        user_id = user.get('id')
        display_name = user.get('displayName', 'Unknown')
        
        # Log progress every 100 users
        if i % 100 == 0:
            logger.info(f"Processing user {i}/{len(users)}")
        
        try:
            # Get last sign-in
            signin_activity = user.get('signInActivity', {})
            last_sign_in = signin_activity.get('lastSignInDateTime', None)
        
            # get license count
            assigned_licenses = user.get('assignedLicenses', [])
            license_count = len(assigned_licenses)
            
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
            
            # get detailed license info    
            if license_count > 0:
                try:
                    detailed_licenses = fetch_user_licenses(tenant_id, user_id)
                    # iterate thru licenses to extract relevant details
                    for license in detailed_licenses:
                        license_record = {
                            'user_id': user_id,
                            'tenant_id': tenant_id,
                            'sku_id': license.get('skuId'),
                            'sku_name': license.get('skuPartNumber'),
                            'monthly_cost': estimate_license_cost(license.get('skuPartNumber', '')),
                            'synced_at': datetime.now().isoformat()
                        }
                        license_records.append(license_record)
                except Exception as e:
                    logger.warning(f"Could not fetch license details for user {user_id}: {str(e)}")
                                
            record = {
                'id': user_id,
                'tenant_id': tenant_id,
                'display_name': display_name,
                'user_principal_name': user.get('userPrincipalName'),
                'mail': user.get('mail'),
                'account_enabled': user.get('accountEnabled'),
                'user_type': user.get('userType'),
                'department': user.get('department'),
                'job_title': user.get('jobTitle'),
                'last_sign_in': last_sign_in,
                'is_mfa_compliant': 1 if is_mfa_registered else 0,  # Changed to match column name and use 0/1
                'license_count': license_count,
                'group_count': group_count,
                'synced_at': datetime.now().isoformat()
            }
            records.append(record)
            
        except Exception as e:
            logger.error(f"Failed to process user {display_name} ({user_id}): {str(e)}")
            failed_enrichments.append({
                'user_id': user_id,
                'display_name': display_name,
                'error': str(e)
            })
            
            # add basic record even if enrichment fails
            basic_record = {
                'id': user_id,
                'tenant_id': tenant_id,
                'display_name': display_name,
                'user_principal_name': user.get('userPrincipalName'),
                'mail': user.get('mail'),
                'account_enabled': user.get('accountEnabled'),
                'user_type': user.get('userType'),
                'synced_at': datetime.now().isoformat()
            }
            records.append(basic_record)
    
    if failed_enrichments:
        logger.warning(f"Failed to fully enrich {len(failed_enrichments)} users")
        # log first 5 failures for debugging
        for failure in failed_enrichments[:5]:
            logger.debug(f"Enrichment failure: {failure}")
    
    logger.info(f"Transformation complete: {len(records)} users, {len(license_records)} licenses")
    return records, license_records


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
                'licenses_synced': 0,
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            }
        
        # fetch MFA status (optional enrichment)
        mfa_lookup = fetch_user_mfa_status(tenant_id)
        
        # transform data
        user_records, license_records = transform_user_records(users, tenant_id, mfa_lookup)
        
        # store in database with error handling
        users_stored = 0
        licenses_stored = 0
        
        try:
            if user_records:
                upsert_many('users', user_records)
                users_stored = len(user_records)
                logger.info(f"Stored {users_stored} users for {tenant_name}")
        
        except Exception as e:
            logger.error(f"Failed to store users for {tenant_name}: {str(e)}", exc_info=True)
            raise
        
        try:
            if license_records:
                upsert_many('user_licenses', license_records)
                licenses_stored = len(license_records)
                logger.info(f"Stored {licenses_stored} licenses for {tenant_name}")
        
        except Exception as e:
            logger.error(f"Failed to store licenses for {tenant_name}: {str(e)}", exc_info=True)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Completed user sync for {tenant_name}: "
            f"{users_stored} users, {licenses_stored} licenses in {duration:.1f}s"
        )
        
        return {
            'status': 'success',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'users_synced': users_stored,
            'licenses_synced': licenses_stored,
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