import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, TypedDict, Optional
from api.integrations.backup_radar import BackupRadarAPI
from api.sql.backup_db import BackupRadarSQL

logger = logging.getLogger(__name__)


class BackupPolicyDict(TypedDict):
    """Type definition for backup policy data from API"""
    companyName: str
    deviceName: str
    deviceType: Optional[str]
    daysSinceLastGoodResult: float
    daysSinceLastResult: float
    daysInStatus: float
    isVerified: bool
    policyName: str
    methodName: str
    policyId: int
    results: List[Dict[str, Any]]


def transform_backup_records(policies: List[BackupPolicyDict]) -> List[Dict[str, Any]]:
    """Transform API backup policies to database records"""
    records = []
    now = datetime.now(timezone.utc)
    last_update = now.strftime("%Y-%m-%d %H:%M:%S")

    for policy in policies:
        # Process each backup result
        for result in policy.get('results', []):
            if 'dateTime' not in result:
                continue

            # Simple datetime handling - convert API format to SQL format
            backup_datetime = result['dateTime'].replace('T', ' ').replace('Z', '')

            # Determine result status - map to exact values
            status = 'No Result'  # default
            if result.get('success', False):
                status = 'Success'
            elif result.get('warning', False):
                status = 'Warning'
            elif result.get('failure', False):
                status = 'Failure'
            elif result.get('manual', False):
                status = 'Manual'
            elif result.get('pending', False):
                status = 'Pending'

            record = {
                'company_name': policy['companyName'],
                'device_name': policy['deviceName'],
                'device_type': policy.get('deviceType'),
                'days_since_last_good_result': policy['daysSinceLastGoodResult'],
                'days_since_last_result': policy['daysSinceLastResult'],
                'days_in_status': policy['daysInStatus'],
                'is_verified': policy['isVerified'],
                'backup_result': status,
                'backup_id': policy['policyId'],
                'backup_type': policy['methodName'],
                'backup_policy_name': policy['policyName'],
                'backup_datetime': backup_datetime,
                'last_updated': last_update,
                'is_retired': False
            }
            records.append(record)

    return records


async def sync_backups(target_date: datetime) -> Dict[str, Any]:
    """
    Sync backup data for the specified date.
    Returns a summary of the sync operation.
    """
    try:
        logger.info(f"Starting backup sync for date: {target_date.strftime('%Y-%m-%d')}")

        # Initialize API and SQL clients
        async with BackupRadarAPI() as api:
            sql = BackupRadarSQL()

            # Fetch backup policies from API
            policies = await api.fetch_policies(target_date, target_date, chunk_size_days=1)
            logger.info(f"Fetched {len(policies)} backup policies")

            # Transform to database records
            records = transform_backup_records(policies)
            logger.info(f"Transformed to {len(records)} backup records")

            # Save to database
            if records:
                processed = sql.upsert_backup_records(records)
            else:
                processed = 0

            # Return simple summary
            return {
                'status': 'success',
                'date': target_date.strftime('%Y-%m-%d'),
                'policies_fetched': len(policies),
                'records_processed': processed
            }

    except Exception as e:
        logger.error(f"Backup sync failed: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'date': target_date.strftime('%Y-%m-%d'),
            'error': str(e)
        }