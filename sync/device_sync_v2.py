from datetime import datetime
import logging

from core.database_v2 import upsert_many_v2
from core.graph_client import GraphClient


logger = logging.getLogger(__name__)


def fetch_managed_devices(tenant_id):
    """Fetch managed devices from Intune"""
    try:
        logger.info(f"Starting managed device fetch for tenant {tenant_id}")
        graph = GraphClient(tenant_id)

        # First try with basic fields to check if Intune is available
        try:
            managed_devices = graph.get(
                "/deviceManagement/managedDevices",
                select=[
                    "id",
                    "deviceName",
                    "userId",
                    "userPrincipalName",
                    "operatingSystem",
                    "osVersion",
                    "complianceState",
                    "managedDeviceOwnerType",
                    "deviceEnrollmentType",
                    "isEncrypted",
                    "isSupervised",
                    "azureADDeviceId",
                    "serialNumber",
                    "manufacturer",
                    "model",
                    "lastSyncDateTime",
                    "enrolledDateTime",
                ],
                top=999,
            )

            logger.info(f"Successfully fetched {len(managed_devices)} managed devices for tenant {tenant_id}")
            return managed_devices

        except Exception as detailed_error:
            # If detailed query fails, try with minimal fields
            logger.warning(f"Detailed managed device query failed, trying basic fields: {str(detailed_error)}")
            managed_devices = graph.get(
                "/deviceManagement/managedDevices",
                select=["id", "deviceName", "userId", "operatingSystem", "complianceState"],
                top=999,
            )

            logger.info(f"Successfully fetched {len(managed_devices)} basic managed devices for tenant {tenant_id}")
            return managed_devices

    except Exception as e:
        # Handle common error scenarios
        error_msg = str(e).lower()
        if "400" in error_msg and "bad request" in error_msg:
            logger.warning(f"Tenant {tenant_id} may not have Intune licensing or required permissions for managed devices")
            return []  # Return empty list instead of failing
        elif "403" in error_msg or "unauthorized" in error_msg:
            logger.warning(f"Insufficient permissions to access managed devices for tenant {tenant_id}")
            return []
        else:
            logger.error(f"Failed to fetch managed devices for tenant {tenant_id}: {str(e)}", exc_info=True)
            return []  # Return empty list instead of failing completely


def fetch_azure_ad_devices(tenant_id):
    """Fetch Azure AD registered devices"""
    try:
        logger.info(f"Starting Azure AD device fetch for tenant {tenant_id}")
        graph = GraphClient(tenant_id)

        # Try with full field set first
        try:
            ad_devices = graph.get(
                "/devices",
                select=[
                    "id",
                    "displayName",
                    "deviceId",
                    "operatingSystem",
                    "operatingSystemVersion",
                    "isCompliant",
                    "isManaged",
                    "enrollmentType",
                    "registrationDateTime",
                    "approximateLastSignInDateTime",
                    "deviceOwnership",
                    "manufacturer",
                    "model",
                ],
                top=999,
            )
            logger.info(f"Successfully fetched {len(ad_devices)} Azure AD devices for tenant {tenant_id}")
            return ad_devices

        except Exception as detailed_error:
            # If detailed query fails, try with basic fields
            logger.warning(f"Detailed Azure AD device query failed, trying basic fields: {str(detailed_error)}")
            ad_devices = graph.get(
                "/devices",
                select=["id", "displayName", "deviceId", "operatingSystem", "isCompliant"],
                top=999,
            )
            logger.info(f"Successfully fetched {len(ad_devices)} basic Azure AD devices for tenant {tenant_id}")
            return ad_devices

    except Exception as e:
        logger.warning(f"Failed to fetch Azure AD devices for tenant {tenant_id}: {str(e)}")
        return []


def transform_devices_v2(managed_devices, ad_devices, tenant_id):
    """Transform device data into V2 database records"""
    records = []
    current_time = datetime.now().isoformat()

    # Create lookup for Azure AD devices by deviceId
    ad_device_lookup = {device.get("deviceId"): device for device in ad_devices if device.get("deviceId")}

    # Process managed devices
    for device in managed_devices:
        azure_ad_device_id = device.get("azureADDeviceId")
        ad_device = ad_device_lookup.get(azure_ad_device_id, {})

        record = {
            "id": device["id"],
            "tenant_id": tenant_id,
            "device_name": device.get("deviceName"),
            "managed_device_name": device.get("deviceName"),
            "user_id": device.get("userId"),
            "user_principal_name": device.get("userPrincipalName"),
            "device_type": device.get("deviceType", "Managed Device"),  # Default fallback
            "operating_system": device.get("operatingSystem"),
            "os_version": device.get("osVersion"),
            "compliance_state": device.get("complianceState"),
            "managed_device_owner_type": device.get("managedDeviceOwnerType"),
            "enrollment_type": device.get("deviceEnrollmentType"),
            "management_state": device.get("managementState", "Managed"),  # Default fallback
            "is_encrypted": 1 if device.get("isEncrypted", False) else 0,
            "is_supervised": 1 if device.get("isSupervised", False) else 0,
            "azure_ad_device_id": azure_ad_device_id,
            "serial_number": device.get("serialNumber"),
            "manufacturer": device.get("manufacturer"),
            "model": device.get("model"),
            "last_contact_date_time": device.get("lastSyncDateTime"),  # Use lastSyncDateTime for contact time
            "enrollment_date_time": device.get("enrolledDateTime"),
            "last_updated": current_time,
        }
        records.append(record)

    # Process Azure AD only devices (not managed by Intune)
    managed_device_ids = {device.get("azureADDeviceId") for device in managed_devices if device.get("azureADDeviceId")}

    for device in ad_devices:
        device_id = device.get("deviceId")
        if device_id not in managed_device_ids:
            record = {
                "id": f"ad_{device['id']}",  # Prefix to avoid ID conflicts
                "tenant_id": tenant_id,
                "device_name": device.get("displayName"),
                "managed_device_name": None,
                "user_id": None,
                "user_principal_name": None,
                "device_type": "Azure AD Registered",
                "operating_system": device.get("operatingSystem"),
                "os_version": device.get("operatingSystemVersion"),
                "compliance_state": "Compliant" if device.get("isCompliant", False) else "NonCompliant",
                "managed_device_owner_type": device.get("deviceOwnership"),
                "enrollment_type": device.get("enrollmentType"),
                "management_state": "Managed" if device.get("isManaged", False) else "Unmanaged",
                "is_encrypted": None,
                "is_supervised": None,
                "azure_ad_device_id": device_id,
                "serial_number": None,
                "manufacturer": device.get("manufacturer"),
                "model": device.get("model"),
                "last_contact_date_time": device.get("approximateLastSignInDateTime"),
                "enrollment_date_time": device.get("registrationDateTime"),
                "last_updated": current_time,
            }
            records.append(record)

    return records


def sync_devices_v2(tenant_id, tenant_name):
    """Synchronize devices for a single tenant using V2 schema"""
    try:
        logger.info(f"Starting V2 device sync for tenant {tenant_name} ({tenant_id})")
        start_time = datetime.now()

        # Fetch data from Microsoft Graph
        managed_devices = fetch_managed_devices(tenant_id)
        ad_devices = fetch_azure_ad_devices(tenant_id)

        # Log what we got
        if not managed_devices and not ad_devices:
            logger.warning(f"No devices found for {tenant_name} - this may indicate missing Intune licensing or insufficient permissions")
        elif not managed_devices:
            logger.info(f"No managed devices found for {tenant_name} - tenant may not have Intune licensing")

        # Transform to database format
        records = transform_devices_v2(managed_devices, ad_devices, tenant_id)

        # Store in V2 database (even if empty, to update sync status)
        if records:
            upsert_many_v2("devices_v2", records)

        sync_duration = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"V2 device sync completed for {tenant_name}: {len(records)} devices ({len(managed_devices)} managed, {len(ad_devices)} Azure AD) in {sync_duration:.2f}s"
        )

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "devices_synced": len(records),
            "managed_devices_synced": len(managed_devices),
            "azure_ad_devices_synced": len(ad_devices),
            "sync_time_seconds": sync_duration,
            "notes": "No Intune licensing detected" if not managed_devices and ad_devices else None,
        }

    except Exception as e:
        logger.error(f"V2 device sync failed for {tenant_name}: {str(e)}")
        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
        }
