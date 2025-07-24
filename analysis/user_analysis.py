import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from collections import defaultdict
from core.database import query, execute_query

# configure logging for azure functions
logger = logging.getLogger(__name__)

def calculate_inactive_users(tenant_id: str, days: int = 90) -> Dict[str, Any]:
    """
    calculate inactive users based on last sign-in activity
    analyzes user activity patterns and potential license cost savings
    
    returns:
        dict: with analysis results and potential savings
    """
    try:
        logger.info(f"starting inactive users analysis for tenant {tenant_id}")
        
        # calculate the cutoff date for determining inactive users
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        logger.debug(f"cutoff date set to {cutoff_date}")
        
        # query users from database - using sqlite parameterized queries
        query_sql = """
        SELECT 
            id, display_name, user_principal_name, account_enabled,
            last_sign_in, license_count, is_admin
        FROM users 
        WHERE tenant_id = ? AND account_enabled = 1
        """
        
        # execute database query with proper parameterization
        users = query(query_sql, (tenant_id,))
        logger.info(f"retrieved {len(users)} active users from database")
        
        # initialize lists to categorize users by activity status
        inactive_users = []
        active_users = []
        never_signed_in = []
        
        # process each user to determine activity status
        for user in users:
            if user['last_sign_in']:
                # parse the last sign-in timestamp
                last_signin = datetime.fromisoformat(user['last_sign_in'])
                
                # check if user is inactive based on cutoff date
                if last_signin < cutoff_date:
                    days_inactive = (datetime.now(timezone.utc) - last_signin).days
                    
                    # add to inactive users with potential savings calculation
                    inactive_users.append({
                        'user_id': user['id'],
                        'display_name': user['display_name'],
                        'user_principal_name': user['user_principal_name'],
                        'days_inactive': days_inactive,
                        'potential_savings': user.get('license_count', 0) * 15  # $15 per license estimate
                    })
                else:
                    # user is active - signed in within threshold
                    active_users.append(user)
            else:
                # user has never signed in - potential cleanup candidate
                never_signed_in.append(user)
        
        # calculate total potential cost savings from inactive licenses
        total_inactive_licenses = sum(u.get('license_count', 0) for u in inactive_users)
        monthly_savings = total_inactive_licenses * 15  # estimate $15 per license per month
        
        logger.info(f"analysis complete: {len(inactive_users)} inactive, {len(active_users)} active, {len(never_signed_in)} never signed in")
        
        # prepare comprehensive result object
        result = {
            'tenant_id': tenant_id,
            'analysis_date': datetime.now(timezone.utc).isoformat(),
            'threshold_days': days,
            'inactive_count': len(inactive_users),
            'active_count': len(active_users),
            'never_signed_in_count': len(never_signed_in),
            'potential_monthly_savings': monthly_savings,
            'utilization_rate': round((len(active_users) / len(users)) * 100, 2) if users else 0,
            'inactive_users': inactive_users[:10]  # top 10 for summary report
        }
        
        return result
        
    except Exception as e:
        logger.error(f"error calculating inactive users: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'tenant_id': tenant_id
        }

def calculate_mfa_compliance(tenant_id: str) -> Dict[str, Any]:
    """
    calculate multi-factor authentication compliance across users
    identifies security risks from non-mfa users, especially admins
    
    returns:
        dictionary with mfa compliance metrics and risk assessment
    """
    try:
        logger.info(f"starting mfa compliance analysis for tenant {tenant_id}")
        
        # query users with mfa registration status
        query_sql = """
        SELECT 
            id, display_name, user_principal_name, 
            is_mfa_compliant, is_admin, account_enabled
        FROM users 
        WHERE tenant_id = ? AND account_enabled = 1
        """
        
        # execute parameterized query
        users = query(query_sql, (tenant_id,))
        logger.info(f"analyzing mfa status for {len(users)} active users")
        
        # initialize lists for compliance categorization
        compliant = []
        non_compliant = []
        admin_non_compliant = []
        
        # categorize users by mfa compliance status
        for user in users:
            if user.get('is_mfa_compliant', False):
                # user has mfa enabled - compliant
                compliant.append(user)
            else:
                # user does not have mfa - non-compliant
                non_compliant.append(user)
                
                # check if non-compliant user is an admin - high security risk
                if user.get('is_admin', False):
                    admin_non_compliant.append(user)
        
        # calculate compliance metrics
        total_users = len(users)
        compliance_rate = (len(compliant) / total_users * 100) if total_users > 0 else 0
        
        logger.info(f"mfa compliance rate: {compliance_rate:.1f}% ({len(compliant)}/{total_users})")
        logger.warning(f"critical: {len(admin_non_compliant)} admin users without mfa")
        
        # prepare comprehensive compliance report
        result = {
            'tenant_id': tenant_id,
            'analysis_date': datetime.now(timezone.utc).isoformat(),
            'total_users': total_users,
            'mfa_enabled': len(compliant),
            'non_compliant': len(non_compliant),
            'compliance_rate': round(compliance_rate, 1),
            'admin_non_compliant': len(admin_non_compliant),
            'risk_level': 'high' if admin_non_compliant else ('medium' if non_compliant else 'low'),
            'critical_users': admin_non_compliant[:10]  # top 10 admin users without mfa - security priority
        }
        
        return result
        
    except Exception as e:
        logger.error(f"error calculating mfa compliance: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'tenant_id': tenant_id
        }

def calculate_license_optimization(tenant_id: str) -> Dict[str, Any]:
    """
    analyze license usage patterns and identify optimization opportunities
    helps reduce costs by identifying unused or underutilized licenses
    
    args:
        tenant_id: microsoft tenant identifier
    
    returns:
        dictionary with license usage analysis and cost optimization recommendations
    """
    try:
        logger.info(f"starting license optimization analysis for tenant {tenant_id}")
        
        # simplified query without license table dependency
        # focuses on user activity patterns to estimate license utilization
        query_sql = """
        SELECT 
            id, display_name, user_principal_name, last_sign_in,
            account_enabled, user_type, license_count
        FROM users
        WHERE tenant_id = ? AND account_enabled = 1
        """
        
        # execute query to get user activity data
        users = query(query_sql, (tenant_id,))
        logger.info(f"analyzing license optimization for {len(users)} active users")
        
        # categorize users by usage patterns for license optimization
        active_users = 0
        inactive_users = 0
        never_signed_in = 0
        guest_users = 0
        
        # 90-day inactivity threshold for license optimization
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
        
        # analyze each user's activity pattern
        for user in users:
            # count guest users (may not need paid licenses)
            if user.get('user_type') == 'Guest':
                guest_users += 1
                continue
                
            if user['last_sign_in']:
                # parse last sign-in date
                last_signin = datetime.fromisoformat(user['last_sign_in'])
                
                if last_signin >= cutoff_date:
                    # user is active - license is being utilized
                    active_users += 1
                else:
                    # user is inactive - potential license optimization candidate
                    inactive_users += 1
            else:
                # user never signed in - license potentially wasted
                never_signed_in += 1
        
        # calculate optimization metrics
        total_paid_users = len(users) - guest_users
        underutilized_licenses = inactive_users + never_signed_in
        utilization_rate = (active_users / total_paid_users * 100) if total_paid_users > 0 else 0
        
        # estimate cost savings (using industry average of $15 per license per month)
        estimated_monthly_savings = underutilized_licenses * 15
        estimated_annual_savings = estimated_monthly_savings * 12
        
        logger.info(f"license utilization: {utilization_rate:.1f}% ({active_users}/{total_paid_users})")
        logger.info(f"potential monthly savings: ${estimated_monthly_savings}")
        
        # prepare comprehensive optimization report
        result = {
            'tenant_id': tenant_id,
            'analysis_date': datetime.now(timezone.utc).isoformat(),
            'total_users': len(users),
            'total_paid_users': total_paid_users,
            'active_users': active_users,
            'inactive_users': inactive_users,
            'never_signed_in': never_signed_in,
            'guest_users': guest_users,
            'utilization_rate': round(utilization_rate, 1),
            'underutilized_licenses': underutilized_licenses,
            'estimated_monthly_savings': estimated_monthly_savings,
            'estimated_annual_savings': estimated_annual_savings,
            'optimization_score': round(utilization_rate, 0)  # simple score based on utilization
        }
        
        return result
        
    except Exception as e:
        logger.error(f"error calculating license optimization: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'tenant_id': tenant_id
        }